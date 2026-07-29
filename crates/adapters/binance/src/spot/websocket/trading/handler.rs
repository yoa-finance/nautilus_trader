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
        BinanceSpotWsTradingCommand, BinanceSpotWsTradingMessage, BinanceSpotWsTradingRequest,
        BinanceSpotWsTradingRequestMeta, method,
    },
};
use crate::{
    common::credential::SigningCredential,
    spot::{
        enums::BinanceSpotUserDataEventType,
        http::{
            models::{
                BinanceCancelOrderResponse, BinanceSpotCancelAllItem, BinanceSpotCancelAllResult,
                BinanceSpotOrderListCancelResult,
            },
            parse,
        },
        sbe::{
            spot::{
                ReadBuf, SBE_SCHEMA_ID, SBE_SCHEMA_VERSION, cancel_open_orders_response_codec,
                cancel_order_list_response_codec, cancel_order_response_codec,
                cancel_replace_order_response_codec, cancel_replace_status,
                error_response_codec::ErrorResponseDecoder,
                message_header_codec, new_order_full_response_codec,
                web_socket_response_codec::{SBE_TEMPLATE_ID, WebSocketResponseDecoder},
                web_socket_session_logon_response_codec,
            },
            template_catalog,
        },
    },
};

const PLACE_ORDER_RESPONSE_TEMPLATES: &[u16] = &[new_order_full_response_codec::SBE_TEMPLATE_ID];
const CANCEL_ORDER_RESPONSE_TEMPLATES: &[u16] = &[cancel_order_response_codec::SBE_TEMPLATE_ID];
const CANCEL_REPLACE_RESPONSE_TEMPLATES: &[u16] =
    &[cancel_replace_order_response_codec::SBE_TEMPLATE_ID];
const CANCEL_ALL_RESPONSE_TEMPLATES: &[u16] = &[
    cancel_open_orders_response_codec::SBE_TEMPLATE_ID,
    cancel_order_list_response_codec::SBE_TEMPLATE_ID,
];
const SESSION_LOGON_RESPONSE_TEMPLATES: &[u16] =
    &[web_socket_session_logon_response_codec::SBE_TEMPLATE_ID];
const USER_DATA_STREAM_SUBSCRIBE_RESPONSE_TEMPLATE_ID: u16 = 503;
const SUBSCRIBE_USER_DATA_RESPONSE_TEMPLATES: &[u16] =
    &[USER_DATA_STREAM_SUBSCRIBE_RESPONSE_TEMPLATE_ID];

#[derive(Debug, Clone, Copy)]
struct SbeFrameHeader {
    block_length: u16,
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
                        log::debug!("Handler received reconnection signal");

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
        self.pending_requests.insert(
            id.clone(),
            BinanceSpotWsTradingRequestMeta::CancelAllOrders { symbol },
        );
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
                    let rejection = self.create_rejection(id_str, code as i32, msg, &meta);
                    self.emit(rejection);
                    return;
                }

                // Success response
                match meta {
                    BinanceSpotWsTradingRequestMeta::SessionLogon => {
                        log::debug!("Session authenticated");
                        self.emit(BinanceSpotWsTradingMessage::Authenticated);
                    }
                    BinanceSpotWsTradingRequestMeta::SubscribeUserData => {
                        let subscription_id = json
                            .get("result")
                            .and_then(|r| r.get("subscriptionId"))
                            .map(|v| v.to_string())
                            .unwrap_or_default();
                        log::debug!("User data stream subscribed: id={subscription_id}");
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
        log_sbe_frame("top_level", &top_header, None, data.len());

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
                return self.decode_direct_ws_api_response(data, top_header);
            }
            _ => {
                if let Some(template) = template_catalog::spot_api_template(top_header.template_id)
                {
                    return Ok(BinanceSpotWsTradingMessage::ProtocolAnomaly {
                        template_id: Some(top_header.template_id),
                        reason: format!(
                            "Unsupported official top-level SBE template_id={} name={} context={:?} support={:?} bytes={}",
                            top_header.template_id,
                            template.name,
                            template.context,
                            template.support,
                            data.len(),
                        ),
                    });
                }

                return Ok(BinanceSpotWsTradingMessage::ProtocolAnomaly {
                    template_id: Some(top_header.template_id),
                    reason: format!(
                        "Unknown top-level SBE template_id={} schema_id={} version={} block_length={} bytes={}",
                        top_header.template_id,
                        top_header.schema_id,
                        top_header.version,
                        top_header.block_length,
                        data.len(),
                    ),
                });
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
            return Ok(self.create_rejection(request_id, code, msg, &meta));
        }

        let inner_header = read_sbe_frame_header(&result_data, Some(&request_id))?;
        log_sbe_frame(
            "websocket_response_result",
            &inner_header,
            Some(&request_id),
            result_data.len(),
        );
        let inner_template_id = inner_header.template_id;

        match meta {
            BinanceSpotWsTradingRequestMeta::SessionLogon => {
                expect_response_template(
                    &request_id,
                    &meta,
                    SESSION_LOGON_RESPONSE_TEMPLATES,
                    inner_template_id,
                )?;
                log::info!("Session authenticated (SBE response)");
                return Ok(BinanceSpotWsTradingMessage::Authenticated);
            }
            BinanceSpotWsTradingRequestMeta::SubscribeUserData => {
                expect_response_template(
                    &request_id,
                    &meta,
                    SUBSCRIBE_USER_DATA_RESPONSE_TEMPLATES,
                    inner_template_id,
                )?;
                log::info!("User data stream subscribed (SBE response)");
                return Ok(BinanceSpotWsTradingMessage::UserDataSubscribed {
                    subscription_id: request_id,
                });
            }
            _ => {}
        }

        // Decode the inner payload based on request type.
        match meta {
            BinanceSpotWsTradingRequestMeta::PlaceOrder => {
                expect_response_template(
                    &request_id,
                    &meta,
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
                    &meta,
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
                    &meta,
                    CANCEL_REPLACE_RESPONSE_TEMPLATES,
                    inner_template_id,
                )?;
                let (cancel_response, new_order_response) =
                    decode_cancel_replace_order_response(&request_id, &result_data)?;
                Ok(BinanceSpotWsTradingMessage::CancelReplaceAccepted {
                    request_id,
                    cancel_response,
                    new_order_response,
                })
            }
            BinanceSpotWsTradingRequestMeta::CancelAllOrders { ref symbol } => {
                match inner_template_id {
                    cancel_open_orders_response_codec::SBE_TEMPLATE_ID => {
                        let result = parse::decode_cancel_open_orders(&result_data)?;
                        Ok(BinanceSpotWsTradingMessage::AllOrdersCanceled {
                            request_id,
                            symbol: Some(symbol.clone()),
                            result,
                        })
                    }
                    cancel_order_list_response_codec::SBE_TEMPLATE_ID => {
                        let response = parse::decode_cancel_order_list_response(&result_data)?;
                        validate_order_list_cancel_result(&request_id, &response)?;
                        Ok(BinanceSpotWsTradingMessage::AllOrdersCanceled {
                            request_id,
                            symbol: Some(symbol.clone()),
                            result: BinanceSpotCancelAllResult::from_order_list(response),
                        })
                    }
                    actual => Err(unexpected_response_template(
                        &request_id,
                        &meta,
                        CANCEL_ALL_RESPONSE_TEMPLATES,
                        actual,
                    )),
                }
            }
            BinanceSpotWsTradingRequestMeta::SessionLogon
            | BinanceSpotWsTradingRequestMeta::SubscribeUserData => unreachable!(
                "session and subscription responses return before inner payload dispatch"
            ),
        }
    }

    fn decode_direct_ws_api_response(
        &mut self,
        data: &[u8],
        header: SbeFrameHeader,
    ) -> Result<BinanceSpotWsTradingMessage, BinanceWsApiError> {
        log_sbe_frame("direct_response", &header, None, data.len());

        match header.template_id {
            cancel_order_list_response_codec::SBE_TEMPLATE_ID => {
                let response = parse::decode_cancel_order_list_response(data)?;
                let Some(request_id) =
                    self.take_unique_cancel_all_request_for_symbol(&response.symbol)
                else {
                    return Ok(BinanceSpotWsTradingMessage::ProtocolAnomaly {
                        template_id: Some(header.template_id),
                        reason: format!(
                            "Direct SBE CancelOrderListResponse could not be deterministically correlated: symbol={} order_list_id={} pending_cancel_all_count={}",
                            response.symbol,
                            response.order_list_id,
                            self.count_cancel_all_requests_for_symbol(&response.symbol),
                        ),
                    });
                };
                let symbol = response.symbol.clone();
                validate_order_list_cancel_result(&request_id, &response)?;
                Ok(BinanceSpotWsTradingMessage::AllOrdersCanceled {
                    request_id,
                    symbol: Some(symbol),
                    result: BinanceSpotCancelAllResult::from_order_list(response),
                })
            }
            cancel_open_orders_response_codec::SBE_TEMPLATE_ID => {
                let result = parse::decode_cancel_open_orders(data)?;
                let symbol = cancel_open_orders_symbol(&result);
                let Some(request_id) = symbol
                    .as_deref()
                    .and_then(|symbol| self.take_unique_cancel_all_request_for_symbol(symbol))
                else {
                    return Ok(BinanceSpotWsTradingMessage::ProtocolAnomaly {
                        template_id: Some(header.template_id),
                        reason: format!(
                            "Direct SBE CancelOpenOrdersResponse could not be deterministically correlated: symbol={} response_count={} pending_cancel_all_count={}",
                            symbol.unwrap_or_else(|| "<unknown>".to_string()),
                            result.items.len(),
                            self.count_cancel_all_requests(),
                        ),
                    });
                };
                Ok(BinanceSpotWsTradingMessage::AllOrdersCanceled {
                    request_id,
                    symbol,
                    result,
                })
            }
            _ => {
                let template_name = template_catalog::spot_api_template(header.template_id)
                    .map_or("<unknown>", |template| template.name);
                Ok(BinanceSpotWsTradingMessage::ProtocolAnomaly {
                    template_id: Some(header.template_id),
                    reason: format!(
                        "Unsupported direct SBE WebSocket API response template {} ({template_name}): request/response payload is outside WebSocketResponse envelope and has no deterministic handler",
                        header.template_id,
                    ),
                })
            }
        }
    }

    fn count_cancel_all_requests(&self) -> usize {
        self.pending_requests
            .values()
            .filter(|meta| {
                matches!(
                    meta,
                    BinanceSpotWsTradingRequestMeta::CancelAllOrders { .. }
                )
            })
            .count()
    }

    fn count_cancel_all_requests_for_symbol(&self, symbol: &str) -> usize {
        self.pending_requests
            .values()
            .filter(|meta| {
                matches!(
                    meta,
                    BinanceSpotWsTradingRequestMeta::CancelAllOrders { symbol: pending_symbol }
                        if pending_symbol.as_str() == symbol
                )
            })
            .count()
    }

    fn take_unique_cancel_all_request_for_symbol(&mut self, symbol: &str) -> Option<String> {
        let request_ids: Vec<String> = self
            .pending_requests
            .iter()
            .filter_map(|(request_id, meta)| match meta {
                BinanceSpotWsTradingRequestMeta::CancelAllOrders {
                    symbol: pending_symbol,
                } if pending_symbol.as_str() == symbol => Some(request_id.clone()),
                _ => None,
            })
            .collect();

        if request_ids.len() != 1 {
            return None;
        }

        let request_id = request_ids.into_iter().next().expect("one request id");
        self.pending_requests.remove(&request_id);
        Some(request_id)
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
        meta: &BinanceSpotWsTradingRequestMeta,
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
            BinanceSpotWsTradingRequestMeta::CancelAllOrders { .. } => {
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
        block_length: buf.get_u16_at(0),
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
    meta: &BinanceSpotWsTradingRequestMeta,
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
    meta: &BinanceSpotWsTradingRequestMeta,
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

fn request_method(meta: &BinanceSpotWsTradingRequestMeta) -> &'static str {
    match meta {
        BinanceSpotWsTradingRequestMeta::PlaceOrder => method::ORDER_PLACE,
        BinanceSpotWsTradingRequestMeta::CancelOrder => method::ORDER_CANCEL,
        BinanceSpotWsTradingRequestMeta::CancelReplaceOrder => method::ORDER_CANCEL_REPLACE,
        BinanceSpotWsTradingRequestMeta::CancelAllOrders { .. } => method::OPEN_ORDERS_CANCEL_ALL,
        BinanceSpotWsTradingRequestMeta::SessionLogon => method::SESSION_LOGON,
        BinanceSpotWsTradingRequestMeta::SubscribeUserData => "userDataStream.subscribe",
    }
}

fn cancel_open_orders_symbol(result: &BinanceSpotCancelAllResult) -> Option<String> {
    let mut symbol: Option<&str> = None;
    for item in &result.items {
        let response_symbol = match item {
            BinanceSpotCancelAllItem::Order(response) => response.symbol.as_str(),
            BinanceSpotCancelAllItem::OrderList(response) => response.symbol.as_str(),
        };
        if let Some(existing) = symbol {
            if existing != response_symbol {
                return None;
            }
        } else if !response_symbol.is_empty() {
            symbol = Some(response_symbol);
        }
    }
    symbol.map(ToOwned::to_owned)
}

fn decode_cancel_replace_order_response(
    request_id: &str,
    data: &[u8],
) -> Result<
    (
        BinanceCancelOrderResponse,
        crate::spot::http::models::BinanceNewOrderResponse,
    ),
    BinanceWsApiError,
> {
    let header = read_sbe_frame_header(data, Some(request_id))?;
    if header.template_id != cancel_replace_order_response_codec::SBE_TEMPLATE_ID {
        return Err(unexpected_response_template(
            request_id,
            &BinanceSpotWsTradingRequestMeta::CancelReplaceOrder,
            CANCEL_REPLACE_RESPONSE_TEMPLATES,
            header.template_id,
        ));
    }

    if data.len() < message_header_codec::ENCODED_LENGTH + header.block_length as usize {
        return Err(BinanceWsApiError::ProtocolViolation {
            request_id: Some(request_id.to_string()),
            msg: format!(
                "CancelReplaceOrderResponse fixed block too short: expected at least {}, actual={}",
                message_header_codec::ENCODED_LENGTH + header.block_length as usize,
                data.len()
            ),
        });
    }

    let buf = ReadBuf::new(data);
    let mut decoder =
        cancel_replace_order_response_codec::CancelReplaceOrderResponseDecoder::default().wrap(
            buf,
            message_header_codec::ENCODED_LENGTH,
            header.block_length,
            header.version,
        );

    let cancel_result = decoder.cancel_result();
    let new_order_result = decoder.new_order_result();
    if cancel_result != cancel_replace_status::CancelReplaceStatus::Success
        || new_order_result != cancel_replace_status::CancelReplaceStatus::Success
    {
        return Err(BinanceWsApiError::ProtocolViolation {
            request_id: Some(request_id.to_string()),
            msg: format!(
                "CancelReplaceOrderResponse has non-success status: cancel_result={cancel_result}, new_order_result={new_order_result}"
            ),
        });
    }

    let cancel_coords = decoder.cancel_response_decoder();
    let cancel_response_bytes = decoder.cancel_response_slice(cancel_coords);
    let cancel_header = read_sbe_frame_header(cancel_response_bytes, Some(request_id))?;
    if cancel_header.template_id != cancel_order_response_codec::SBE_TEMPLATE_ID {
        return Err(unexpected_response_template(
            request_id,
            &BinanceSpotWsTradingRequestMeta::CancelOrder,
            CANCEL_ORDER_RESPONSE_TEMPLATES,
            cancel_header.template_id,
        ));
    }
    let cancel_response = parse::decode_cancel_order(cancel_response_bytes)?;

    let new_order_coords = decoder.new_order_response_decoder();
    let new_order_response_bytes = decoder.new_order_response_slice(new_order_coords);
    let new_order_header = read_sbe_frame_header(new_order_response_bytes, Some(request_id))?;
    if new_order_header.template_id != new_order_full_response_codec::SBE_TEMPLATE_ID {
        return Err(unexpected_response_template(
            request_id,
            &BinanceSpotWsTradingRequestMeta::PlaceOrder,
            PLACE_ORDER_RESPONSE_TEMPLATES,
            new_order_header.template_id,
        ));
    }
    let new_order_response = parse::decode_new_order_full(new_order_response_bytes)?;

    Ok((cancel_response, new_order_response))
}

fn log_sbe_frame(location: &str, header: &SbeFrameHeader, request_id: Option<&str>, bytes: usize) {
    log::debug!(
        "Binance WS SBE frame location={} request_id={} template_id={} schema_id={} version={} block_length={} bytes={}",
        location,
        request_id.unwrap_or("<none>"),
        header.template_id,
        header.schema_id,
        header.version,
        header.block_length,
        bytes,
    );
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
    template_catalog::is_spot_api_response_template(template_id)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

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
}
