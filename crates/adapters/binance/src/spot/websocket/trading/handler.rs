// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Binance Spot WebSocket API message handler.
//!
//! The handler runs in a dedicated Tokio task as the I/O boundary between the client
//! orchestrator and the network layer. It exclusively owns the `WebSocketClient` and
//! processes commands from the client via an unbounded channel.
//!
//! ## Responsibilities
//!
//! - Command processing: Receives `BinanceSpotWsTradingCommand` from client, serializes to JSON requests.
//! - Response decoding: Parses SBE binary responses using schema 3 decoders.
//! - Request correlation: Matches responses to pending requests by ID.
//! - Message transformation: Emits `BinanceSpotWsTradingMessage` events to client via channel.

use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use ahash::AHashMap;
use nautilus_network::{RECONNECTED, websocket::WebSocketClient};
use tokio_tungstenite::tungstenite::Message;

use super::{
    client::BINANCE_WS_RATE_LIMIT_KEY_ORDER,
    error::{BinanceWsApiError, BinanceWsApiResult},
    messages::{
        BinanceSpotCancelAllResult, BinanceSpotOrderListCancelResult, BinanceSpotWsTradingCommand,
        BinanceSpotWsTradingMessage, BinanceSpotWsTradingRequest, BinanceSpotWsTradingRequestMeta,
        method,
    },
};
use crate::{
    common::credential::SigningCredential,
    spot::{
        enums::BinanceSpotUserDataEventType,
        http::{models::BinanceCancelOrderResponse, parse},
        sbe::spot::{
            ReadBuf, SBE_SCHEMA_ID, SBE_SCHEMA_VERSION, cancel_open_orders_response_codec,
            cancel_order_list_response_codec, cancel_order_response_codec,
            error_response_codec::ErrorResponseDecoder,
            message_header_codec, new_order_full_response_codec,
            web_socket_response_codec::{SBE_TEMPLATE_ID, WebSocketResponseDecoder},
        },
    },
};

const PLACE_ORDER_RESPONSE_TEMPLATES: &[u16] = &[new_order_full_response_codec::SBE_TEMPLATE_ID];
const CANCEL_ORDER_RESPONSE_TEMPLATES: &[u16] = &[cancel_order_response_codec::SBE_TEMPLATE_ID];
const CANCEL_REPLACE_RESPONSE_TEMPLATES: &[u16] = &[new_order_full_response_codec::SBE_TEMPLATE_ID];
const CANCEL_ALL_RESPONSE_TEMPLATES: &[u16] = &[
    cancel_open_orders_response_codec::SBE_TEMPLATE_ID,
    cancel_order_list_response_codec::SBE_TEMPLATE_ID,
];

#[derive(Debug, Clone, Copy)]
struct SbeFrameHeader {
    template_id: u16,
    schema_id: u16,
    version: u16,
}

/// Binance Spot WebSocket API handler.
///
/// Runs in a dedicated Tokio task, processing commands from the client
/// and transforming raw WebSocket messages into Nautilus domain events.
/// Messages are sent to the client via the output channel.
pub struct BinanceSpotWsTradingHandler {
    signal: Arc<AtomicBool>,
    inner: Option<WebSocketClient>,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<BinanceSpotWsTradingCommand>,
    raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
    out_tx: tokio::sync::mpsc::UnboundedSender<BinanceSpotWsTradingMessage>,
    credential: Arc<SigningCredential>,
    pending_requests: AHashMap<String, BinanceSpotWsTradingRequestMeta>,
    request_id_counter: AtomicU64,
}

impl Debug for BinanceSpotWsTradingHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BinanceSpotWsTradingHandler))
            .field("inner", &self.inner.as_ref().map(|_| "<client>"))
            .field(
                "pending_requests",
                &format!("{} pending", self.pending_requests.len()),
            )
            .finish_non_exhaustive()
    }
}

impl BinanceSpotWsTradingHandler {
    /// Creates a new handler instance.
    #[must_use]
    pub fn new(
        signal: Arc<AtomicBool>,
        cmd_rx: tokio::sync::mpsc::UnboundedReceiver<BinanceSpotWsTradingCommand>,
        raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
        out_tx: tokio::sync::mpsc::UnboundedSender<BinanceSpotWsTradingMessage>,
        credential: Arc<SigningCredential>,
    ) -> Self {
        Self {
            signal,
            inner: None,
            cmd_rx,
            raw_rx,
            out_tx,
            credential,
            pending_requests: AHashMap::new(),
            request_id_counter: AtomicU64::new(1000),
        }
    }

    /// Runs the main event loop for commands and raw messages.
    ///
    /// Sends output messages via `out_tx` channel. Returns `false` when disconnected
    /// or the signal is set, indicating the handler should exit.
    pub async fn run(&mut self) -> bool {
        loop {
            if self.signal.load(Ordering::Relaxed) {
                return false;
            }

            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        BinanceSpotWsTradingCommand::SetClient(client) => {
                            log::debug!("Handler received WebSocket client");
                            self.inner = Some(client);
                            self.emit(BinanceSpotWsTradingMessage::Connected);
                        }
                        BinanceSpotWsTradingCommand::Disconnect => {
                            log::debug!("Handler disconnecting WebSocket client");
                            self.inner = None;
                            return false;
                        }
                        BinanceSpotWsTradingCommand::PlaceOrder { id, params } => {
                            if let Err(e) = self.handle_place_order(id.clone(), params).await {
                                log::error!("Failed to handle place order command: {e}");
                                self.pending_requests.remove(&id);
                                self.emit(BinanceSpotWsTradingMessage::RequestFailed {
                                    request_id: id,
                                    msg: e.to_string(),
                                });
                            }
                        }
                        BinanceSpotWsTradingCommand::CancelOrder { id, params } => {
                            if let Err(e) = self.handle_cancel_order(id.clone(), params).await {
                                log::error!("Failed to handle cancel order command: {e}");
                                self.pending_requests.remove(&id);
                                self.emit(BinanceSpotWsTradingMessage::RequestFailed {
                                    request_id: id,
                                    msg: e.to_string(),
                                });
                            }
                        }
                        BinanceSpotWsTradingCommand::CancelReplaceOrder { id, params } => {
                            if let Err(e) = self.handle_cancel_replace_order(id.clone(), params).await {
                                log::error!("Failed to handle cancel replace command: {e}");
                                self.pending_requests.remove(&id);
                                self.emit(BinanceSpotWsTradingMessage::RequestFailed {
                                    request_id: id,
                                    msg: e.to_string(),
                                });
                            }
                        }
                        BinanceSpotWsTradingCommand::CancelAllOrders { id, symbol } => {
                            if let Err(e) = self.handle_cancel_all_orders(id.clone(), symbol).await {
                                log::error!("Failed to handle cancel all command: {e}");
                                self.pending_requests.remove(&id);
                                self.emit(BinanceSpotWsTradingMessage::RequestFailed {
                                    request_id: id,
                                    msg: e.to_string(),
                                });
                            }
                        }
                        BinanceSpotWsTradingCommand::SessionLogon => {
                            if let Err(e) = self.handle_session_logon().await {
                                log::error!("Session logon failed: {e}");
                                self.emit(BinanceSpotWsTradingMessage::Error(
                                    format!("Session logon failed: {e}"),
                                ));
                            }
                        }
                        BinanceSpotWsTradingCommand::SubscribeUserData => {
                            if let Err(e) = self.handle_subscribe_user_data().await {
                                log::error!("User data subscribe failed: {e}");
                                self.emit(BinanceSpotWsTradingMessage::Error(
                                    format!("User data subscribe failed: {e}"),
                                ));
                            }
                        }
                    }
                }
                Some(msg) = self.raw_rx.recv() => {
                    if let Message::Text(ref text) = msg
                        && text.as_str() == RECONNECTED
                    {
                        log::info!("Handler received reconnection signal");

                        // Fail any pending requests - they won't get responses on new connection
                        self.fail_pending_requests();

                        self.emit(BinanceSpotWsTradingMessage::Reconnected);
                        continue;
                    }

                    self.handle_message(msg);
                }
                else => {
                    // Both channels closed
                    return false;
                }
            }
        }
    }

    /// Sends a message to the output channel.
    fn emit(&self, msg: BinanceSpotWsTradingMessage) {
        if let Err(e) = self.out_tx.send(msg) {
            log::error!("Failed to send message to output channel: {e}");
        }
    }

    /// Fails all pending requests after a reconnection.
    fn fail_pending_requests(&mut self) {
        if self.pending_requests.is_empty() {
            return;
        }

        let count = self.pending_requests.len();
        log::warn!("Failing {count} pending requests after reconnection");

        let pending = std::mem::take(&mut self.pending_requests);
        for (request_id, _meta) in pending {
            self.emit(BinanceSpotWsTradingMessage::RequestFailed {
                request_id,
                msg: "Connection lost before response received".to_string(),
            });
        }
    }

    async fn handle_place_order(
        &mut self,
        id: String,
        params: crate::spot::http::query::NewOrderParams,
    ) -> BinanceWsApiResult<()> {
        let params_json = serde_json::to_value(&params)
            .map_err(|e| BinanceWsApiError::ClientError(e.to_string()))?;
        let signed_params = self.sign_params(params_json)?;

        let request = BinanceSpotWsTradingRequest::new(&id, method::ORDER_PLACE, signed_params);
        self.pending_requests
            .insert(id.clone(), BinanceSpotWsTradingRequestMeta::PlaceOrder);
        self.send_request(request).await
    }

    async fn handle_cancel_order(
        &mut self,
        id: String,
        params: crate::spot::http::query::CancelOrderParams,
    ) -> BinanceWsApiResult<()> {
        let params_json = serde_json::to_value(&params)
            .map_err(|e| BinanceWsApiError::ClientError(e.to_string()))?;
        let signed_params = self.sign_params(params_json)?;

        let request = BinanceSpotWsTradingRequest::new(&id, method::ORDER_CANCEL, signed_params);
        self.pending_requests
            .insert(id.clone(), BinanceSpotWsTradingRequestMeta::CancelOrder);
        self.send_request(request).await
    }

    async fn handle_cancel_replace_order(
        &mut self,
        id: String,
        params: crate::spot::http::query::CancelReplaceOrderParams,
    ) -> BinanceWsApiResult<()> {
        let params_json = serde_json::to_value(&params)
            .map_err(|e| BinanceWsApiError::ClientError(e.to_string()))?;
        let signed_params = self.sign_params(params_json)?;

        let request =
            BinanceSpotWsTradingRequest::new(&id, method::ORDER_CANCEL_REPLACE, signed_params);
        self.pending_requests.insert(
            id.clone(),
            BinanceSpotWsTradingRequestMeta::CancelReplaceOrder,
        );
        self.send_request(request).await
    }

    async fn handle_cancel_all_orders(
        &mut self,
        id: String,
        symbol: String,
    ) -> BinanceWsApiResult<()> {
        let params_json = serde_json::json!({ "symbol": symbol });
        let signed_params = self.sign_params(params_json)?;

        let request =
            BinanceSpotWsTradingRequest::new(&id, method::OPEN_ORDERS_CANCEL_ALL, signed_params);
        self.pending_requests
            .insert(id.clone(), BinanceSpotWsTradingRequestMeta::CancelAllOrders);
        self.send_request(request).await
    }

    async fn handle_session_logon(&mut self) -> BinanceWsApiResult<()> {
        let id = self.next_request_id();
        let params_json = serde_json::json!({});
        let signed_params = self.sign_params(params_json)?;

        let request = BinanceSpotWsTradingRequest::new(&id, "session.logon", signed_params);
        self.pending_requests
            .insert(id, BinanceSpotWsTradingRequestMeta::SessionLogon);
        self.send_request(request).await
    }

    async fn handle_subscribe_user_data(&mut self) -> BinanceWsApiResult<()> {
        let id = self.next_request_id();
        let request = BinanceSpotWsTradingRequest::new(
            &id,
            "userDataStream.subscribe",
            serde_json::json!({}),
        );
        self.pending_requests
            .insert(id, BinanceSpotWsTradingRequestMeta::SubscribeUserData);
        self.send_request(request).await
    }

    fn next_request_id(&self) -> String {
        let id = self.request_id_counter.fetch_add(1, Ordering::Relaxed);
        format!("ws-{id}")
    }

    fn sign_params(&self, mut params: serde_json::Value) -> BinanceWsApiResult<serde_json::Value> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| BinanceWsApiError::ClientError(e.to_string()))?
            .as_millis() as i64;

        if let Some(obj) = params.as_object_mut() {
            obj.insert("timestamp".to_string(), serde_json::json!(timestamp));
            obj.insert(
                "apiKey".to_string(),
                serde_json::json!(self.credential.api_key()),
            );
        }

        let query_string = serde_urlencoded::to_string(&params)
            .map_err(|e| BinanceWsApiError::ClientError(e.to_string()))?;
        let signature = self.credential.sign(&query_string);

        if let Some(obj) = params.as_object_mut() {
            obj.insert("signature".to_string(), serde_json::json!(signature));
        }

        Ok(params)
    }

    async fn send_request(
        &mut self,
        request: BinanceSpotWsTradingRequest,
    ) -> BinanceWsApiResult<()> {
        let client = self.inner.as_mut().ok_or_else(|| {
            BinanceWsApiError::ConnectionError("WebSocket not connected".to_string())
        })?;

        let json = serde_json::to_string(&request)
            .map_err(|e| BinanceWsApiError::ClientError(e.to_string()))?;

        log::debug!(
            "Sending WebSocket API request id={} method={}",
            request.id,
            request.method
        );

        // Apply rate limiting for order operations
        client
            .send_text(json, Some(BINANCE_WS_RATE_LIMIT_KEY_ORDER.as_slice()))
            .await
            .map_err(|e| {
                BinanceWsApiError::ConnectionError(format!("Failed to send request: {e}"))
            })?;

        Ok(())
    }

    fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::Binary(data) => self.handle_binary_response(&data),
            Message::Text(text) => self.handle_text_response(&text),
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(frame) => {
                log::debug!("WebSocket closed: {frame:?}");
            }
            Message::Frame(_) => {}
        }
    }

    fn handle_binary_response(&mut self, data: &[u8]) {
        match self.decode_ws_api_response(data) {
            Ok(response) => self.emit(response),
            Err(e) => {
                log::error!("Failed to decode WebSocket API response: {e}");
                self.emit(BinanceSpotWsTradingMessage::FatalError {
                    reason: e.to_string(),
                });
            }
        }
    }

    fn handle_text_response(&mut self, text: &str) {
        let json: serde_json::Value = match serde_json::from_str(text) {
            Ok(j) => j,
            Err(e) => {
                log::warn!("Failed to parse text response as JSON: {e}");
                return;
            }
        };

        // User data events arrive wrapped: {"subscriptionId": N, "event": {...}}
        if let Some(event) = json.get("event") {
            self.handle_user_data_event(event);
            return;
        }

        // WS API responses have an "id" field for request correlation
        if let Some(id) = json.get("id") {
            let id_str = match id {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                _ => return,
            };

            if let Some(meta) = self.pending_requests.remove(&id_str) {
                // Check for error: nested {"error": {"code": N, "msg": "..."}}
                // or top-level {"code": N, "msg": "..."}
                let error_info = json
                    .get("error")
                    .map(|e| {
                        (
                            e.get("code").and_then(|v| v.as_i64()).unwrap_or(-1),
                            e.get("msg")
                                .and_then(|v| v.as_str())
                                .unwrap_or("Unknown error")
                                .to_string(),
                        )
                    })
                    .or_else(|| {
                        json.get("code").and_then(|c| c.as_i64()).map(|code| {
                            let msg = json
                                .get("msg")
                                .and_then(|v| v.as_str())
                                .unwrap_or("Unknown error")
                                .to_string();
                            (code, msg)
                        })
                    });

                if let Some((code, msg)) = error_info {
                    let rejection = self.create_rejection(id_str, code as i32, msg, meta);
                    self.emit(rejection);
                    return;
                }

                // Success response
                match meta {
                    BinanceSpotWsTradingRequestMeta::SessionLogon => {
                        log::info!("Session authenticated");
                        self.emit(BinanceSpotWsTradingMessage::Authenticated);
                    }
                    BinanceSpotWsTradingRequestMeta::SubscribeUserData => {
                        let subscription_id = json
                            .get("result")
                            .and_then(|r| r.get("subscriptionId"))
                            .map(|v| v.to_string())
                            .unwrap_or_default();
                        log::info!("User data stream subscribed: id={subscription_id}");
                        self.emit(BinanceSpotWsTradingMessage::UserDataSubscribed {
                            subscription_id,
                        });
                    }
                    _ => {
                        // Order operation responses come as SBE binary, not JSON text.
                        // If we get a JSON success for an order operation, log it.
                        log::debug!("Unexpected JSON success for request {id_str}: {json}");
                    }
                }
                return;
            }

            // Error response without matching pending request
            if let Some(code) = json.get("code").and_then(|v| v.as_i64()) {
                let msg = json
                    .get("msg")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown error");
                log::warn!(
                    "Received error response without matching request ID: code={code} msg={msg}"
                );
            }
            return;
        }

        // Stream termination event
        if json.get("eventStreamTerminated").is_some() {
            log::warn!("User data stream terminated, resubscribe needed");
            return;
        }

        log::debug!("Unhandled text message: {text}");
    }

    fn handle_user_data_event(&self, event: &serde_json::Value) {
        if let Some(msg) = classify_user_data_event(event) {
            self.emit(msg);
        }
    }

    fn decode_ws_api_response(
        &mut self,
        data: &[u8],
    ) -> Result<BinanceSpotWsTradingMessage, BinanceWsApiError> {
        let top_header = read_sbe_frame_header(data, None)?;

        // User data stream events arrive as SBE with their own template IDs
        // (not wrapped in WebSocketResponse template 50). Request/response
        // traffic must arrive as WebSocketResponse template 50 envelopes.
        match top_header.template_id {
            601 => {
                log::debug!("Received SBE BalanceUpdateEvent ({} bytes)", data.len());
                match super::decode_sbe::decode_balance_update(data) {
                    Ok(msg) => {
                        log::debug!(
                            "SBE balance update: asset={}, delta={}",
                            msg.asset,
                            msg.delta
                        );
                        return Ok(BinanceSpotWsTradingMessage::BalanceUpdate(msg));
                    }
                    Err(e) => {
                        log::error!("Failed to decode SBE BalanceUpdateEvent: {e}");
                        return Ok(BinanceSpotWsTradingMessage::Error(format!(
                            "SBE BalanceUpdateEvent decode failed: {e}"
                        )));
                    }
                }
            }
            603 => {
                log::debug!("Received SBE ExecutionReportEvent ({} bytes)", data.len());
                match super::decode_sbe::decode_execution_report(data) {
                    Ok(report) => {
                        log::debug!(
                            "SBE execution report: symbol={}, order_id={}, exec={:?}, status={:?}",
                            report.symbol,
                            report.order_id,
                            report.execution_type,
                            report.order_status
                        );
                        return Ok(BinanceSpotWsTradingMessage::ExecutionReport(Box::new(
                            report,
                        )));
                    }
                    Err(e) => {
                        log::error!("Failed to decode SBE ExecutionReportEvent: {e}");
                        return Ok(BinanceSpotWsTradingMessage::Error(format!(
                            "SBE ExecutionReportEvent decode failed: {e}"
                        )));
                    }
                }
            }
            606 => {
                log::debug!("Received SBE ListStatusEvent ({} bytes)", data.len());
                match super::decode_sbe::decode_list_status(data) {
                    Ok(msg) => {
                        log::debug!(
                            "SBE list status: symbol={}, order_list_id={}, status_type={:?}, order_status={:?}",
                            msg.symbol,
                            msg.order_list_id,
                            msg.list_status_type,
                            msg.list_order_status
                        );
                        return Ok(BinanceSpotWsTradingMessage::ListStatus(msg));
                    }
                    Err(e) => {
                        log::error!("Failed to decode SBE ListStatusEvent: {e}");
                        return Ok(BinanceSpotWsTradingMessage::Error(format!(
                            "SBE ListStatusEvent decode failed: {e}"
                        )));
                    }
                }
            }
            607 => {
                log::debug!(
                    "Received SBE OutboundAccountPositionEvent ({} bytes)",
                    data.len()
                );

                match super::decode_sbe::decode_account_position(data) {
                    Ok(msg) => {
                        log::debug!("SBE account position: {} balance(s)", msg.balances.len());
                        return Ok(BinanceSpotWsTradingMessage::AccountPosition(msg));
                    }
                    Err(e) => {
                        log::error!("Failed to decode SBE OutboundAccountPositionEvent: {e}");
                        return Ok(BinanceSpotWsTradingMessage::Error(format!(
                            "SBE OutboundAccountPositionEvent decode failed: {e}"
                        )));
                    }
                }
            }
            610 => {
                let event_time = parse_server_shutdown_event_time_ms(data);
                log::warn!(
                    "Binance server shutdown notice (SBE, event_time={event_time}); disconnect expected within ~10 minutes",
                );
                return Ok(BinanceSpotWsTradingMessage::ServerShutdown { event_time });
            }
            SBE_TEMPLATE_ID => {}
            _ if is_direct_ws_api_response_template(top_header.template_id) => {
                return Err(BinanceWsApiError::UnsupportedDirectResponse {
                        template_id: top_header.template_id,
                        msg: "request/response SBE payloads must be wrapped in WebSocketResponse envelope template 50".to_string(),
                    });
            }
            _ => {
                return Err(BinanceWsApiError::DecodeError(
                    crate::spot::sbe::error::SbeDecodeError::UnknownTemplateId(
                        top_header.template_id,
                    ),
                ));
            }
        }

        // Standard WebSocketResponse envelope (template 50)
        let (request_id, status, result_data) = self.parse_envelope(data)?;

        // Look up the pending request by ID
        let meta = self.pending_requests.remove(&request_id).ok_or_else(|| {
            BinanceWsApiError::UnknownRequestId(format!("No pending request for ID: {request_id}"))
        })?;

        // Check for error status (non-200)
        if status != 200 {
            let (code, msg) = Self::try_decode_sbe_error(&result_data).unwrap_or((
                status as i32,
                format!("Request failed with status {status}"),
            ));
            return Ok(self.create_rejection(request_id, code, msg, meta));
        }

        match meta {
            BinanceSpotWsTradingRequestMeta::SessionLogon => {
                log::info!("Session authenticated (SBE response)");
                return Ok(BinanceSpotWsTradingMessage::Authenticated);
            }
            BinanceSpotWsTradingRequestMeta::SubscribeUserData => {
                log::info!("User data stream subscribed (SBE response)");
                return Ok(BinanceSpotWsTradingMessage::UserDataSubscribed {
                    subscription_id: request_id,
                });
            }
            _ => {}
        }

        let inner_template_id = read_sbe_frame_header(&result_data, Some(&request_id))?.template_id;

        // Decode the inner payload based on request type.
        match meta {
            BinanceSpotWsTradingRequestMeta::PlaceOrder => {
                expect_response_template(
                    &request_id,
                    meta,
                    PLACE_ORDER_RESPONSE_TEMPLATES,
                    inner_template_id,
                )?;
                let response = parse::decode_new_order_full(&result_data)?;
                Ok(BinanceSpotWsTradingMessage::OrderAccepted {
                    request_id,
                    response,
                })
            }
            BinanceSpotWsTradingRequestMeta::CancelOrder => {
                expect_response_template(
                    &request_id,
                    meta,
                    CANCEL_ORDER_RESPONSE_TEMPLATES,
                    inner_template_id,
                )?;
                let response = parse::decode_cancel_order(&result_data)?;
                Ok(BinanceSpotWsTradingMessage::OrderCanceled {
                    request_id,
                    response,
                })
            }
            BinanceSpotWsTradingRequestMeta::CancelReplaceOrder => {
                expect_response_template(
                    &request_id,
                    meta,
                    CANCEL_REPLACE_RESPONSE_TEMPLATES,
                    inner_template_id,
                )?;
                // Cancel-replace returns both cancel and new order info
                let new_order_response = parse::decode_new_order_full(&result_data)?;
                let cancel_response = BinanceCancelOrderResponse {
                    price_exponent: new_order_response.price_exponent,
                    qty_exponent: new_order_response.qty_exponent,
                    order_id: 0,
                    order_list_id: None,
                    transact_time: new_order_response.transact_time,
                    price_mantissa: 0,
                    orig_qty_mantissa: 0,
                    executed_qty_mantissa: 0,
                    cummulative_quote_qty_mantissa: 0,
                    status: crate::spot::sbe::spot::order_status::OrderStatus::Canceled,
                    time_in_force: new_order_response.time_in_force,
                    order_type: new_order_response.order_type,
                    side: new_order_response.side,
                    self_trade_prevention_mode: new_order_response.self_trade_prevention_mode,
                    client_order_id: String::new(),
                    orig_client_order_id: String::new(),
                    symbol: new_order_response.symbol.clone(),
                };
                Ok(BinanceSpotWsTradingMessage::CancelReplaceAccepted {
                    request_id,
                    cancel_response,
                    new_order_response,
                })
            }
            BinanceSpotWsTradingRequestMeta::CancelAllOrders => match inner_template_id {
                cancel_open_orders_response_codec::SBE_TEMPLATE_ID => {
                    let responses = parse::decode_cancel_open_orders(&result_data)?;
                    Ok(BinanceSpotWsTradingMessage::AllOrdersCanceled {
                        request_id,
                        result: BinanceSpotCancelAllResult::Orders(responses),
                    })
                }
                cancel_order_list_response_codec::SBE_TEMPLATE_ID => {
                    let response =
                        super::decode_sbe::decode_cancel_order_list_response(&result_data)
                            .map_err(|e| {
                                BinanceWsApiError::ClientError(format!(
                                    "SBE CancelOrderListResponse decode failed: {e}"
                                ))
                            })?;
                    validate_order_list_cancel_result(&request_id, &response)?;
                    Ok(BinanceSpotWsTradingMessage::AllOrdersCanceled {
                        request_id,
                        result: BinanceSpotCancelAllResult::OrderList(response),
                    })
                }
                actual => Err(unexpected_response_template(
                    &request_id,
                    meta,
                    CANCEL_ALL_RESPONSE_TEMPLATES,
                    actual,
                )),
            },
            BinanceSpotWsTradingRequestMeta::SessionLogon
            | BinanceSpotWsTradingRequestMeta::SubscribeUserData => unreachable!(
                "session and subscription responses return before inner payload dispatch"
            ),
        }
    }

    /// Parses the WebSocketResponse SBE envelope.
    ///
    /// Returns (request_id, status, result_payload).
    fn parse_envelope(&self, data: &[u8]) -> Result<(String, u16, Vec<u8>), BinanceWsApiError> {
        if data.len() < message_header_codec::ENCODED_LENGTH {
            return Err(BinanceWsApiError::DecodeError(
                crate::spot::sbe::error::SbeDecodeError::BufferTooShort {
                    expected: message_header_codec::ENCODED_LENGTH,
                    actual: data.len(),
                },
            ));
        }

        let buf = ReadBuf::new(data);

        // Parse message header
        let block_length = buf.get_u16_at(0);
        let template_id = buf.get_u16_at(2);

        if template_id != SBE_TEMPLATE_ID {
            return Err(BinanceWsApiError::DecodeError(
                crate::spot::sbe::error::SbeDecodeError::UnknownTemplateId(template_id),
            ));
        }

        let version = buf.get_u16_at(6);

        // Create decoder at offset after message header
        let decoder = WebSocketResponseDecoder::default().wrap(
            buf,
            message_header_codec::ENCODED_LENGTH,
            block_length,
            version,
        );

        // Read status from fixed block (offset 1 within block)
        let status = decoder.status();

        // Skip rate_limits group
        let mut rate_limits = decoder.rate_limits_decoder();
        while rate_limits.advance().unwrap_or(None).is_some() {}
        let mut decoder = rate_limits.parent().map_err(|e| {
            BinanceWsApiError::ClientError(format!("Failed to get parent from rate_limits: {e}"))
        })?;

        // Extract request ID
        let id_coords = decoder.id_decoder();
        let id_bytes = decoder.id_slice(id_coords);
        let request_id = String::from_utf8_lossy(id_bytes).to_string();

        // Extract result payload - copy to owned Vec to avoid lifetime issues
        let result_coords = decoder.result_decoder();
        let result_data = decoder.result_slice(result_coords).to_vec();

        Ok((request_id, status, result_data))
    }

    fn create_rejection(
        &self,
        request_id: String,
        code: i32,
        msg: String,
        meta: BinanceSpotWsTradingRequestMeta,
    ) -> BinanceSpotWsTradingMessage {
        match meta {
            BinanceSpotWsTradingRequestMeta::PlaceOrder => {
                BinanceSpotWsTradingMessage::OrderRejected {
                    request_id,
                    code,
                    msg,
                }
            }
            BinanceSpotWsTradingRequestMeta::CancelOrder => {
                BinanceSpotWsTradingMessage::CancelRejected {
                    request_id,
                    code,
                    msg,
                }
            }
            BinanceSpotWsTradingRequestMeta::CancelReplaceOrder => {
                BinanceSpotWsTradingMessage::CancelReplaceRejected {
                    request_id,
                    code,
                    msg,
                }
            }
            BinanceSpotWsTradingRequestMeta::CancelAllOrders => {
                BinanceSpotWsTradingMessage::CancelRejected {
                    request_id,
                    code,
                    msg,
                }
            }
            BinanceSpotWsTradingRequestMeta::SessionLogon
            | BinanceSpotWsTradingRequestMeta::SubscribeUserData => {
                BinanceSpotWsTradingMessage::Error(format!("code={code}: {msg}"))
            }
        }
    }

    // Decodes the SBE error response to extract the Binance error code and message
    fn try_decode_sbe_error(data: &[u8]) -> Option<(i32, String)> {
        const HEADER_LEN: usize = 8;

        if data.len()
            < HEADER_LEN + crate::spot::sbe::spot::error_response_codec::SBE_BLOCK_LENGTH as usize
        {
            return None;
        }

        let buf = ReadBuf::new(data);
        let header = message_header_codec::MessageHeaderDecoder::default().wrap(buf, 0);
        if header.template_id() != crate::spot::sbe::spot::error_response_codec::SBE_TEMPLATE_ID {
            return None;
        }

        let mut decoder = ErrorResponseDecoder::default().header(header, 0);
        let code = i32::from(decoder.code());
        let msg_coords = decoder.msg_decoder();
        let msg_bytes = decoder.msg_slice(msg_coords);
        let msg = String::from_utf8_lossy(msg_bytes).into_owned();

        Some((code, msg))
    }
}

fn read_sbe_frame_header(
    data: &[u8],
    request_id: Option<&str>,
) -> Result<SbeFrameHeader, BinanceWsApiError> {
    if data.len() < message_header_codec::ENCODED_LENGTH {
        return Err(BinanceWsApiError::DecodeError(
            crate::spot::sbe::error::SbeDecodeError::BufferTooShort {
                expected: message_header_codec::ENCODED_LENGTH,
                actual: data.len(),
            },
        ));
    }

    let buf = ReadBuf::new(data);
    let header = SbeFrameHeader {
        template_id: buf.get_u16_at(2),
        schema_id: buf.get_u16_at(4),
        version: buf.get_u16_at(6),
    };

    if header.schema_id != SBE_SCHEMA_ID {
        return Err(BinanceWsApiError::ProtocolViolation {
            request_id: request_id.map(ToOwned::to_owned),
            msg: format!(
                "SBE schema mismatch: expected {SBE_SCHEMA_ID}, received {}",
                header.schema_id
            ),
        });
    }

    if header.version != SBE_SCHEMA_VERSION {
        return Err(BinanceWsApiError::ProtocolViolation {
            request_id: request_id.map(ToOwned::to_owned),
            msg: format!(
                "SBE schema version mismatch: expected {SBE_SCHEMA_VERSION}, received {}",
                header.version
            ),
        });
    }

    Ok(header)
}

fn expect_response_template(
    request_id: &str,
    meta: BinanceSpotWsTradingRequestMeta,
    expected: &'static [u16],
    actual: u16,
) -> Result<(), BinanceWsApiError> {
    if expected.contains(&actual) {
        return Ok(());
    }

    Err(unexpected_response_template(
        request_id, meta, expected, actual,
    ))
}

fn unexpected_response_template(
    request_id: &str,
    meta: BinanceSpotWsTradingRequestMeta,
    expected: &'static [u16],
    actual: u16,
) -> BinanceWsApiError {
    BinanceWsApiError::UnexpectedResponseTemplate {
        request_id: request_id.to_string(),
        method: request_method(meta),
        expected,
        actual,
    }
}

fn request_method(meta: BinanceSpotWsTradingRequestMeta) -> &'static str {
    match meta {
        BinanceSpotWsTradingRequestMeta::PlaceOrder => method::ORDER_PLACE,
        BinanceSpotWsTradingRequestMeta::CancelOrder => method::ORDER_CANCEL,
        BinanceSpotWsTradingRequestMeta::CancelReplaceOrder => method::ORDER_CANCEL_REPLACE,
        BinanceSpotWsTradingRequestMeta::CancelAllOrders => method::OPEN_ORDERS_CANCEL_ALL,
        BinanceSpotWsTradingRequestMeta::SessionLogon => method::SESSION_LOGON,
        BinanceSpotWsTradingRequestMeta::SubscribeUserData => "userDataStream.subscribe",
    }
}

fn validate_order_list_cancel_result(
    request_id: &str,
    response: &BinanceSpotOrderListCancelResult,
) -> Result<(), BinanceWsApiError> {
    if response.list_order_status != "AllDone" {
        return Err(BinanceWsApiError::ProtocolViolation {
            request_id: Some(request_id.to_string()),
            msg: format!(
                "CancelOrderListResponse order_list_id={} has non-terminal list_order_status={}",
                response.order_list_id, response.list_order_status
            ),
        });
    }

    if response.order_reports.is_empty() {
        return Err(BinanceWsApiError::ProtocolViolation {
            request_id: Some(request_id.to_string()),
            msg: format!(
                "CancelOrderListResponse order_list_id={} contains no child order reports",
                response.order_list_id
            ),
        });
    }

    if let Some(report) = response
        .order_reports
        .iter()
        .find(|report| !is_terminal_order_status(&report.status))
    {
        return Err(BinanceWsApiError::ProtocolViolation {
            request_id: Some(request_id.to_string()),
            msg: format!(
                "CancelOrderListResponse order_list_id={} child order_id={} has non-terminal status={}",
                response.order_list_id, report.order_id, report.status
            ),
        });
    }

    Ok(())
}

fn is_terminal_order_status(status: &str) -> bool {
    matches!(status, "Canceled" | "Expired" | "ExpiredInMatch")
}

/// Classifies a JSON user-data event into a trading message, if any.
///
/// Returns `None` when the event type is unknown or the payload fails to
/// deserialize; in that case the caller logs and drops the event.
pub(crate) fn classify_user_data_event(
    event: &serde_json::Value,
) -> Option<BinanceSpotWsTradingMessage> {
    let event_type = event
        .get("e")
        .and_then(|v| serde_json::from_value::<BinanceSpotUserDataEventType>(v.clone()).ok())
        .unwrap_or(BinanceSpotUserDataEventType::Unknown);

    match event_type {
        BinanceSpotUserDataEventType::ExecutionReport => {
            match serde_json::from_value::<super::user_data::BinanceSpotExecutionReport>(
                event.clone(),
            ) {
                Ok(report) => {
                    log::debug!(
                        "Execution report: symbol={}, order_id={}, exec={:?}, status={:?}",
                        report.symbol,
                        report.order_id,
                        report.execution_type,
                        report.order_status
                    );
                    Some(BinanceSpotWsTradingMessage::ExecutionReport(Box::new(
                        report,
                    )))
                }
                Err(e) => {
                    log::warn!("Failed to parse execution report: {e}");
                    None
                }
            }
        }
        BinanceSpotUserDataEventType::OutboundAccountPosition => {
            match serde_json::from_value::<super::user_data::BinanceSpotAccountPositionMsg>(
                event.clone(),
            ) {
                Ok(msg) => {
                    log::debug!("Account position update: {} balance(s)", msg.balances.len());
                    Some(BinanceSpotWsTradingMessage::AccountPosition(msg))
                }
                Err(e) => {
                    log::warn!("Failed to parse account position: {e}");
                    None
                }
            }
        }
        BinanceSpotUserDataEventType::BalanceUpdate => {
            match serde_json::from_value::<super::user_data::BinanceSpotBalanceUpdateMsg>(
                event.clone(),
            ) {
                Ok(msg) => {
                    log::debug!("Balance update: asset={}, delta={}", msg.asset, msg.delta);
                    Some(BinanceSpotWsTradingMessage::BalanceUpdate(msg))
                }
                Err(e) => {
                    log::warn!("Failed to parse balance update: {e}");
                    None
                }
            }
        }
        BinanceSpotUserDataEventType::ServerShutdown => {
            let event_time = event.get("E").and_then(|v| v.as_i64()).unwrap_or_default();
            log::warn!(
                "Binance server shutdown notice (event_time={event_time}); disconnect expected within ~10 minutes",
            );
            Some(BinanceSpotWsTradingMessage::ServerShutdown { event_time })
        }
        BinanceSpotUserDataEventType::ListenKeyExpired
        | BinanceSpotUserDataEventType::ExternalLockUpdate
        | BinanceSpotUserDataEventType::EventStreamTerminated
        | BinanceSpotUserDataEventType::Unknown => {
            log::debug!("Unhandled user data event type: {event_type:?}");
            None
        }
    }
}

/// Parses the `event_time` from an SBE `ServerShutdownEvent` (template 610) frame.
///
/// The SBE field is microseconds; the trading message variant documents
/// milliseconds (matching the JSON dispatch), so this divides by 1_000.
/// Returns `0` when the buffer is too short to contain the field.
pub(crate) fn parse_server_shutdown_event_time_ms(data: &[u8]) -> i64 {
    if data.len() < message_header_codec::ENCODED_LENGTH + 8 {
        return 0;
    }
    let buf = ReadBuf::new(data);
    buf.get_i64_at(message_header_codec::ENCODED_LENGTH) / 1_000
}

fn is_direct_ws_api_response_template(template_id: u16) -> bool {
    matches!(
        template_id,
        51 | 52
            | 53
            | 100
            | 101
            | 300..=315
            | 400..=403
    )
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::consts::{
        BINANCE_SPOT_SBE_WS_API_DEMO_URL, BINANCE_SPOT_SBE_WS_API_TESTNET_URL,
        BINANCE_SPOT_SBE_WS_API_URL,
    };
    use crate::spot::sbe::spot::{
        Encoder, WriteBuf, bool_enum::BoolEnum, cancel_open_orders_response_codec,
        cancel_order_list_response_codec, contingency_type::ContingencyType,
        list_order_status::ListOrderStatus, list_status_event_codec,
        list_status_type::ListStatusType, order_side::OrderSide, order_status::OrderStatus,
        order_type::OrderType, time_in_force::TimeInForce, web_socket_response_codec,
    };

    fn encode_list_status() -> Vec<u8> {
        let symbol = "BTCUSDT";
        let list_client_order_id = "list-client-1";
        let orders = [
            (63562212148_i64, "BTCUSDT", "target-client"),
            (63562212149_i64, "BTCUSDT", "stop-client"),
        ];
        let orders_var_len: usize = orders
            .iter()
            .map(|(_, symbol, client_id)| 1 + symbol.len() + 1 + client_id.len())
            .sum();
        let parent_var_len = 1 + symbol.len() + 1 + list_client_order_id.len() + 1;
        let total = message_header_codec::ENCODED_LENGTH
            + list_status_event_codec::SBE_BLOCK_LENGTH as usize
            + 4
            + (orders.len() * 8)
            + orders_var_len
            + parent_var_len;
        let mut buf_vec = vec![0u8; total];

        let buf = WriteBuf::new(buf_vec.as_mut_slice());
        let enc = list_status_event_codec::ListStatusEventEncoder::default()
            .wrap(buf, message_header_codec::ENCODED_LENGTH);
        let mut header = enc.header(0);
        let mut enc = header.parent().unwrap();

        enc.event_time(1_709_654_400_123_000);
        enc.transact_time(1_709_654_400_124_000);
        enc.order_list_id(42);
        enc.contingency_type(ContingencyType::Oco);
        enc.list_status_type(ListStatusType::ExecStarted);
        enc.list_order_status(ListOrderStatus::Executing);
        enc.subscription_id(0xFFFF);

        let orders_enc = list_status_event_codec::encoder::OrdersEncoder::default();
        let mut orders_enc = enc.orders_encoder(orders.len() as u16, orders_enc);
        for (order_id, order_symbol, client_order_id) in orders {
            orders_enc.advance().unwrap();
            orders_enc.order_id(order_id);
            orders_enc.symbol(order_symbol);
            orders_enc.client_order_id(client_order_id);
        }
        let mut enc = orders_enc.parent().unwrap();
        enc.symbol(symbol);
        enc.list_client_order_id(list_client_order_id);
        enc.reject_reason("");

        buf_vec
    }

    fn encode_cancel_order_list_response() -> Vec<u8> {
        let mut buf_vec = vec![0u8; 512];
        let buf = WriteBuf::new(buf_vec.as_mut_slice());
        let enc = cancel_order_list_response_codec::CancelOrderListResponseEncoder::default()
            .wrap(buf, message_header_codec::ENCODED_LENGTH);
        let mut header = enc.header(0);
        let mut enc = header.parent().unwrap();

        enc.order_list_id(42);
        enc.contingency_type(ContingencyType::Oco);
        enc.list_status_type(ListStatusType::AllDone);
        enc.list_order_status(ListOrderStatus::AllDone);
        enc.transaction_time(1_709_654_400_124_000);
        enc.price_exponent(-2);
        enc.qty_exponent(-8);

        let orders_enc = cancel_order_list_response_codec::encoder::OrdersEncoder::default();
        let mut orders_enc = enc.orders_encoder(1, orders_enc);
        orders_enc.advance().unwrap();
        orders_enc.order_id(63562212148_i64);
        orders_enc.symbol("BTCUSDT");
        orders_enc.client_order_id("target-client");
        let enc = orders_enc.parent().unwrap();

        let reports_enc = cancel_order_list_response_codec::encoder::OrderReportsEncoder::default();
        let mut reports_enc = enc.order_reports_encoder(1, reports_enc);
        reports_enc.advance().unwrap();
        reports_enc.order_id(63562212148_i64);
        reports_enc.order_list_id(42);
        reports_enc.transact_time(1_709_654_400_124_000);
        reports_enc.status(OrderStatus::Canceled);
        reports_enc.time_in_force(TimeInForce::Gtc);
        reports_enc.order_type(OrderType::Limit);
        reports_enc.side(OrderSide::Sell);
        reports_enc.symbol("BTCUSDT");
        reports_enc.orig_client_order_id("target-client");
        reports_enc.client_order_id("target-client");
        let mut enc = reports_enc.parent().unwrap();

        enc.list_client_order_id("list-client-1");
        enc.symbol("BTCUSDT");
        let encoded_len = enc.get_limit();
        buf_vec.truncate(encoded_len);
        buf_vec
    }

    fn encode_cancel_open_orders_response() -> Vec<u8> {
        let mut buf_vec = vec![0u8; 64];
        let buf = WriteBuf::new(buf_vec.as_mut_slice());
        let enc = cancel_open_orders_response_codec::CancelOpenOrdersResponseEncoder::default()
            .wrap(buf, message_header_codec::ENCODED_LENGTH);
        let mut header = enc.header(0);
        let enc = header.parent().unwrap();
        let responses_enc = cancel_open_orders_response_codec::encoder::ResponsesEncoder::default();
        let mut responses_enc = enc.responses_encoder(0, responses_enc);
        let enc = responses_enc.parent().unwrap();
        let encoded_len = enc.get_limit();
        buf_vec.truncate(encoded_len);
        buf_vec
    }

    fn encode_ws_api_envelope(request_id: &str, result: &[u8]) -> Vec<u8> {
        let mut buf_vec = vec![0u8; 512 + result.len()];
        let buf = WriteBuf::new(buf_vec.as_mut_slice());
        let enc = web_socket_response_codec::WebSocketResponseEncoder::default()
            .wrap(buf, message_header_codec::ENCODED_LENGTH);
        let mut header = enc.header(0);
        let mut enc = header.parent().unwrap();
        enc.sbe_schema_id_version_deprecated(BoolEnum::False);
        enc.status(200);

        let rate_limits_enc = web_socket_response_codec::encoder::RateLimitsEncoder::default();
        let mut rate_limits_enc = enc.rate_limits_encoder(0, rate_limits_enc);
        let mut enc = rate_limits_enc.parent().unwrap();
        enc.id(request_id);
        enc.result(result);
        let encoded_len = enc.get_limit();
        buf_vec.truncate(encoded_len);
        buf_vec
    }

    fn encode_direct_template_header(template_id: u16) -> Vec<u8> {
        let mut buf_vec = vec![0u8; message_header_codec::ENCODED_LENGTH];
        buf_vec[0..2].copy_from_slice(&0u16.to_le_bytes());
        buf_vec[2..4].copy_from_slice(&template_id.to_le_bytes());
        buf_vec[4..6].copy_from_slice(&crate::spot::sbe::spot::SBE_SCHEMA_ID.to_le_bytes());
        buf_vec[6..8].copy_from_slice(&crate::spot::sbe::spot::SBE_SCHEMA_VERSION.to_le_bytes());
        buf_vec
    }

    fn create_test_handler() -> BinanceSpotWsTradingHandler {
        create_test_handler_with_rx().0
    }

    fn create_test_handler_with_rx() -> (
        BinanceSpotWsTradingHandler,
        tokio::sync::mpsc::UnboundedReceiver<BinanceSpotWsTradingMessage>,
    ) {
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (_raw_tx, raw_rx) = tokio::sync::mpsc::unbounded_channel();
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            BinanceSpotWsTradingHandler::new(
                Arc::new(AtomicBool::new(false)),
                cmd_rx,
                raw_rx,
                out_tx,
                Arc::new(SigningCredential::new(
                    "test_key".to_string(),
                    "test_secret".to_string(),
                )),
            ),
            out_rx,
        )
    }

    #[rstest]
    fn test_spot_sbe_ws_api_urls_match_generated_schema_version() {
        let schema_query = format!(
            "sbeSchemaId={}&sbeSchemaVersion={}",
            crate::spot::sbe::spot::SBE_SCHEMA_ID,
            crate::spot::sbe::spot::SBE_SCHEMA_VERSION
        );
        for url in [
            BINANCE_SPOT_SBE_WS_API_URL,
            BINANCE_SPOT_SBE_WS_API_TESTNET_URL,
            BINANCE_SPOT_SBE_WS_API_DEMO_URL,
        ] {
            assert!(
                url.contains(&schema_query),
                "SBE WS API URL must match generated codec schema: {url}"
            );
        }
    }

    #[rstest]
    #[case::microseconds_converted_to_ms(1_700_000_000_000_000_i64, 1_700_000_000_000_i64)]
    #[case::zero(0_i64, 0_i64)]
    #[case::negative(-1_000_i64, -1_i64)]
    fn test_parse_server_shutdown_event_time_ms(
        #[case] event_time_us: i64,
        #[case] expected_ms: i64,
    ) {
        let mut buf = vec![0u8; message_header_codec::ENCODED_LENGTH];
        buf.extend_from_slice(&event_time_us.to_le_bytes());
        assert_eq!(parse_server_shutdown_event_time_ms(&buf), expected_ms);
    }

    #[rstest]
    fn test_parse_server_shutdown_event_time_ms_short_buffer_returns_zero() {
        let buf = vec![0u8; message_header_codec::ENCODED_LENGTH + 4];
        assert_eq!(parse_server_shutdown_event_time_ms(&buf), 0);
    }

    #[rstest]
    fn test_classify_user_data_event_server_shutdown_emits_variant() {
        let event = serde_json::json!({"e": "serverShutdown", "E": 1_700_000_000_000_i64});
        let msg = classify_user_data_event(&event).expect("expected ServerShutdown");
        match msg {
            BinanceSpotWsTradingMessage::ServerShutdown { event_time } => {
                assert_eq!(event_time, 1_700_000_000_000);
            }
            other => panic!("expected ServerShutdown variant, was {other:?}"),
        }
    }

    #[rstest]
    fn test_classify_user_data_event_server_shutdown_missing_event_time_defaults_to_zero() {
        let event = serde_json::json!({"e": "serverShutdown"});
        let msg = classify_user_data_event(&event).expect("expected ServerShutdown");
        match msg {
            BinanceSpotWsTradingMessage::ServerShutdown { event_time } => {
                assert_eq!(event_time, 0);
            }
            other => panic!("expected ServerShutdown variant, was {other:?}"),
        }
    }

    #[rstest]
    fn test_classify_user_data_event_unknown_returns_none() {
        let event = serde_json::json!({"e": "somethingElse"});
        assert!(classify_user_data_event(&event).is_none());
    }

    #[rstest]
    fn test_decode_ws_api_response_template_606_emits_list_status() {
        let mut handler = create_test_handler();
        let msg = handler
            .decode_ws_api_response(&encode_list_status())
            .expect("template 606 should decode");

        match msg {
            BinanceSpotWsTradingMessage::ListStatus(list_status) => {
                assert_eq!(list_status.order_list_id, 42);
                assert_eq!(list_status.orders.len(), 2);
            }
            BinanceSpotWsTradingMessage::Error(err) => {
                panic!("template 606 must not emit Error: {err}");
            }
            other => panic!("expected ListStatus variant, was {other:?}"),
        }
    }

    #[rstest]
    fn test_decode_ws_api_response_cancel_all_envelope_template_312_emits_order_list_result() {
        let mut handler = create_test_handler();
        handler.pending_requests.insert(
            "req-cancel-all".to_string(),
            BinanceSpotWsTradingRequestMeta::CancelAllOrders,
        );
        let inner = encode_cancel_order_list_response();
        let envelope = encode_ws_api_envelope("req-cancel-all", &inner);

        let msg = handler
            .decode_ws_api_response(&envelope)
            .expect("template 312 cancel-all result should decode");

        match msg {
            BinanceSpotWsTradingMessage::AllOrdersCanceled { request_id, result } => {
                assert_eq!(request_id, "req-cancel-all");
                match result {
                    BinanceSpotCancelAllResult::OrderList(response) => {
                        assert_eq!(response.template_id, 312);
                        assert_eq!(response.order_list_id, 42);
                        assert_eq!(response.list_client_order_id, "list-client-1");
                        assert_eq!(response.symbol, "BTCUSDT");
                        assert_eq!(response.orders.len(), 1);
                        assert_eq!(response.orders[0].client_order_id, "target-client");
                        assert_eq!(response.order_reports.len(), 1);
                        assert_eq!(response.order_reports[0].status, "Canceled");
                    }
                    other => panic!("expected order-list cancel result, was {other:?}"),
                }
            }
            BinanceSpotWsTradingMessage::FatalError { reason } => {
                panic!("template 312 cancel-all result must not be fatal: {reason}");
            }
            other => panic!("expected AllOrdersCanceled variant, was {other:?}"),
        }
    }

    #[rstest]
    fn test_decode_ws_api_response_cancel_all_envelope_template_306_emits_orders_result() {
        let mut handler = create_test_handler();
        handler.pending_requests.insert(
            "req-cancel-all".to_string(),
            BinanceSpotWsTradingRequestMeta::CancelAllOrders,
        );
        let inner = encode_cancel_open_orders_response();
        let envelope = encode_ws_api_envelope("req-cancel-all", &inner);

        let msg = handler
            .decode_ws_api_response(&envelope)
            .expect("template 306 cancel-all result should decode");

        match msg {
            BinanceSpotWsTradingMessage::AllOrdersCanceled { result, .. } => match result {
                BinanceSpotCancelAllResult::Orders(responses) => assert!(responses.is_empty()),
                other => panic!("expected standard orders cancel result, was {other:?}"),
            },
            other => panic!("expected AllOrdersCanceled variant, was {other:?}"),
        }
    }

    #[rstest]
    fn test_decode_ws_api_response_cancel_all_unexpected_inner_template_is_typed_error() {
        let mut handler = create_test_handler();
        handler.pending_requests.insert(
            "req-cancel-all".to_string(),
            BinanceSpotWsTradingRequestMeta::CancelAllOrders,
        );
        let inner = encode_direct_template_header(309);
        let envelope = encode_ws_api_envelope("req-cancel-all", &inner);

        let err = handler
            .decode_ws_api_response(&envelope)
            .expect_err("unexpected inner template should fail with typed error");

        match err {
            BinanceWsApiError::UnexpectedResponseTemplate {
                request_id,
                method,
                expected,
                actual,
            } => {
                assert_eq!(request_id, "req-cancel-all");
                assert_eq!(method, method::OPEN_ORDERS_CANCEL_ALL);
                assert_eq!(expected, CANCEL_ALL_RESPONSE_TEMPLATES);
                assert_eq!(actual, 309);
            }
            other => panic!("expected UnexpectedResponseTemplate, was {other:?}"),
        }
    }

    #[rstest]
    fn test_decode_ws_api_response_direct_template_312_is_typed_error() {
        let mut handler = create_test_handler();
        let err = handler
            .decode_ws_api_response(&encode_cancel_order_list_response())
            .expect_err("direct template 312 should not be accepted outside envelope");

        match err {
            BinanceWsApiError::UnsupportedDirectResponse { template_id, .. } => {
                assert_eq!(template_id, 312);
            }
            other => panic!("expected UnsupportedDirectResponse, was {other:?}"),
        }
    }

    #[rstest]
    fn test_decode_ws_api_response_unsupported_direct_template_is_typed_error() {
        let mut handler = create_test_handler();
        let err = handler
            .decode_ws_api_response(&encode_direct_template_header(309))
            .expect_err("unsupported direct template should fail before envelope parsing");

        match err {
            BinanceWsApiError::UnsupportedDirectResponse { template_id, .. } => {
                assert_eq!(template_id, 309);
            }
            other => panic!("expected UnsupportedDirectResponse, was {other:?}"),
        }
    }

    #[rstest]
    fn test_handle_binary_response_unsupported_direct_template_emits_fatal_error() {
        let (mut handler, mut out_rx) = create_test_handler_with_rx();

        handler.handle_binary_response(&encode_direct_template_header(309));

        match out_rx.try_recv().expect("fatal error message expected") {
            BinanceSpotWsTradingMessage::FatalError { reason } => {
                assert!(reason.contains("Unsupported direct SBE WebSocket API response"));
            }
            other => panic!("expected FatalError, was {other:?}"),
        }
    }
}
