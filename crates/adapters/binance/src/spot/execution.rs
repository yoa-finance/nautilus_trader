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

//! Live execution client implementation for the Binance Spot adapter.

use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    cache::fifo::FifoCache,
    clients::ExecutionClient,
    live::{get_runtime, runner::get_exec_event_sender},
    messages::execution::{
        BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GenerateOrderStatusReportsBuilder,
        GeneratePositionStatusReports, GeneratePositionStatusReportsBuilder, ModifyOrder,
        QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{
    MUTEX_POISONED, UUID4, UnixNanos,
    datetime::mins_to_nanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{LiquiditySide, OmsType, OrderSide, OrderType},
    events::{
        AccountState, OrderAccepted, OrderCancelRejected, OrderCanceled, OrderEventAny,
        OrderFilled, OrderModifyRejected, OrderRejected, OrderUpdated,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TradeId, Venue, VenueOrderId,
    },
    instruments::Instrument,
    orders::Order,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use rust_decimal::Decimal;
use tokio::task::JoinHandle;
use ustr::Ustr;

use super::websocket::trading::{
    client::BinanceSpotWsTradingClient,
    messages::BinanceSpotWsTradingMessage,
    parse::{
        parse_spot_account_position, parse_spot_exec_report_to_fill,
        parse_spot_exec_report_to_order_status,
    },
    user_data::{BinanceSpotExecutionReport, BinanceSpotExecutionType, BinanceSpotListStatusMsg},
};
use crate::{
    common::{
        consts::{
            BINANCE_GTX_ORDER_REJECT_CODE, BINANCE_NAUTILUS_SPOT_BROKER_ID,
            BINANCE_NEW_ORDER_REJECTED_CODE, BINANCE_SPOT_POST_ONLY_REJECT_MSG,
            BINANCE_STATUS_UNKNOWN_CODE, BINANCE_UNEXPECTED_RESPONSE_CODE, BINANCE_VENUE,
        },
        credential::resolve_credentials,
        dispatch::{
            OrderIdentity, PendingOperation, PendingRequest, WsDispatchState,
            ensure_accepted_emitted,
        },
        encoder::{decode_broker_id, encode_binance_client_order_id},
        enums::{BinanceSide, BinanceTimeInForce},
        parse::{
            parse_required_decimal, parse_required_price_at_precision,
            parse_required_quantity_at_precision,
        },
    },
    config::BinanceExecClientConfig,
    spot::{
        enums::{
            BinanceCancelReplaceMode, BinanceOrderResponseType, BinanceSpotOrderType,
            order_type_to_binance_spot, time_in_force_to_binance_spot,
        },
        http::{
            client::BinanceSpotHttpClient,
            error::BinanceSpotHttpError,
            models::{BatchCancelResult, BinanceOrderListResponse},
            query::{
                BatchCancelItem, CancelOrderParams, CancelReplaceOrderParams,
                NewOrderListOcoParams, NewOrderParams,
            },
        },
        sbe::spot::list_order_status::ListOrderStatus,
    },
};

const OCO_CONTINGENCY_TYPE_PARAM: &str = "contingency_type";
const OCO_CONTINGENCY_TYPE_VALUE: &str = "OCO";

/// Live execution client for Binance Spot trading.
///
/// Implements the [`ExecutionClient`] trait for order management on Binance Spot
/// and Spot Margin markets. Uses WebSocket API as the primary transport for order
/// operations (lowest latency), with HTTP API fallback when the WS connection is
/// unavailable. The WebSocket User Data Stream provides real-time execution events.
#[derive(Debug)]
pub struct BinanceSpotExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: BinanceExecClientConfig,
    emitter: ExecutionEventEmitter,
    dispatch_state: Arc<WsDispatchState>,
    http_client: BinanceSpotHttpClient,
    ws_trading_client: Option<BinanceSpotWsTradingClient>,
    ws_trading_handle: Mutex<Option<JoinHandle<()>>>,
    ws_authenticated: Arc<tokio::sync::Notify>,
    pending_tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl BinanceSpotExecutionClient {
    /// Creates a new [`BinanceSpotExecutionClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client fails to initialize or credentials are missing.
    pub fn new(core: ExecutionClientCore, config: BinanceExecClientConfig) -> anyhow::Result<Self> {
        let (api_key, api_secret) = resolve_credentials(
            config.api_key.clone(),
            config.api_secret.clone(),
            config.environment,
            config.product_type,
        )?;

        let clock = get_atomic_clock_realtime();

        let http_client = BinanceSpotHttpClient::new(
            config.environment,
            clock,
            Some(api_key.clone()),
            Some(api_secret.clone()),
            config.base_url_http.clone(),
            None, // recv_window
            None, // timeout_secs
            None, // proxy_url
        )
        .context("failed to construct Binance Spot HTTP client")?;
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );

        let ws_trading_client = if config.use_ws_trading {
            Some(BinanceSpotWsTradingClient::new(
                config.base_url_ws_trading.clone(),
                api_key,
                api_secret,
                None, // heartbeat
                config.transport_backend,
            ))
        } else {
            None
        };

        Ok(Self {
            core,
            clock,
            config,
            emitter,
            dispatch_state: Arc::new(WsDispatchState::default()),
            http_client,
            ws_trading_client,
            ws_trading_handle: Mutex::new(None),
            ws_authenticated: Arc::new(tokio::sync::Notify::new()),
            pending_tasks: Mutex::new(Vec::new()),
        })
    }

    async fn refresh_account_state(&self) -> anyhow::Result<AccountState> {
        self.http_client
            .request_account_state(self.core.account_id)
            .await
    }

    fn update_account_state(&self) {
        let http_client = self.http_client.clone();
        let account_id = self.core.account_id;
        let emitter = self.emitter.clone();
        let clock = self.clock;

        self.spawn_task("query_account", async move {
            let account_state = http_client.request_account_state(account_id).await?;
            let ts_now = clock.get_time_ns();
            emitter.emit_account_state(
                account_state.balances.clone(),
                account_state.margins.clone(),
                account_state.is_reported,
                ts_now,
            );
            Ok(())
        });
    }

    /// Returns whether the WS trading client is connected and active.
    fn ws_trading_active(&self) -> bool {
        self.ws_trading_client
            .as_ref()
            .is_some_and(|c| c.is_active())
    }

    fn submit_order_internal(&self, cmd: &SubmitOrder) -> anyhow::Result<()> {
        let order = self
            .core
            .cache()
            .order(&cmd.client_order_id)
            .map(|o| o.clone())
            .ok_or_else(|| anyhow::anyhow!("Order not found: {}", cmd.client_order_id))?;

        let event_emitter = self.emitter.clone();
        let trader_id = self.core.trader_id;
        let account_id = self.core.account_id;
        let client_order_id = order.client_order_id();
        let strategy_id = order.strategy_id();
        let instrument_id = order.instrument_id();
        let order_side = order.order_side();
        let order_type = order.order_type();
        let quantity = order.quantity();
        let time_in_force = order.time_in_force();
        let price = order.price();
        let trigger_price = order.trigger_price();
        let is_post_only = order.is_post_only();
        let is_quote_quantity = order.is_quote_quantity();
        let display_qty = order.display_qty();
        let clock = self.clock;
        let ts_init = self.clock.get_time_ns();

        // Register identity for tracked/external dispatch routing
        self.dispatch_state.order_identities.insert(
            client_order_id,
            OrderIdentity {
                instrument_id,
                strategy_id,
                order_side,
                order_type,
                price,
                quantity,
            },
        );

        if self.ws_trading_active() {
            let ws_client = self.ws_trading_client.as_ref().unwrap().clone();
            let dispatch_state = self.dispatch_state.clone();
            let params =
                build_new_order_params(&order, client_order_id, is_post_only, is_quote_quantity)?;

            // Pre-register before sending to avoid response racing the insert
            let request_id = ws_client.next_request_id();
            dispatch_state.pending_requests.insert(
                request_id.clone(),
                PendingRequest {
                    client_order_id,
                    venue_order_id: None,
                    operation: PendingOperation::Place,
                },
            );

            self.spawn_task("submit_order_ws", async move {
                if let Err(e) = ws_client
                    .place_order_with_id(request_id.clone(), params)
                    .await
                {
                    dispatch_state.pending_requests.remove(&request_id);
                    log::error!(
                        "WS submit request failed for {client_order_id}, awaiting reconciliation: {e}"
                    );
                    anyhow::bail!("WS submit order failed: {e}");
                }
                Ok(())
            });
        } else {
            let http_client = self.http_client.clone();
            let dispatch_state = self.dispatch_state.clone();
            log::debug!("WS trading not active, falling back to HTTP for submit_order");

            self.spawn_task("submit_order_http", async move {
                let result = http_client
                    .submit_order(
                        account_id,
                        instrument_id,
                        client_order_id,
                        order_side,
                        order_type,
                        quantity,
                        time_in_force,
                        price,
                        trigger_price,
                        is_post_only,
                        is_quote_quantity,
                        display_qty,
                    )
                    .await;

                match result {
                    Ok(report) => {
                        dispatch_state.insert_accepted(client_order_id);
                        let accepted = OrderAccepted::new(
                            trader_id,
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            report.venue_order_id,
                            account_id,
                            UUID4::new(),
                            ts_init,
                            ts_init,
                            false,
                        );
                        event_emitter.send_order_event(OrderEventAny::Accepted(accepted));
                    }
                    Err(e) => {
                        if is_ambiguous_submit_error(&e) {
                            log::error!(
                                "Ambiguous submit failure for {client_order_id}, awaiting reconciliation: {e}"
                            );
                        } else if is_structured_venue_rejection(&e)
                            || is_local_command_failure(&e)
                        {
                            let due_post_only = e
                                .downcast_ref::<BinanceSpotHttpError>()
                                .is_some_and(is_spot_post_only_rejection);
                            dispatch_state.cleanup_terminal(client_order_id);
                            let rejected = OrderRejected::new(
                                trader_id,
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                account_id,
                                format!("submit-order-error: {e}").into(),
                                UUID4::new(),
                                ts_init,
                                clock.get_time_ns(),
                                false,
                                due_post_only,
                            );
                            event_emitter.send_order_event(OrderEventAny::Rejected(rejected));
                        } else {
                            log::error!(
                                "Ambiguous submit failure for {client_order_id}, awaiting reconciliation: {e}"
                            );
                        }
                        return Err(e);
                    }
                }
                Ok(())
            });
        }

        Ok(())
    }

    fn cancel_order_internal(&self, cmd: &CancelOrder) -> anyhow::Result<()> {
        let event_emitter = self.emitter.clone();
        let trader_id = self.core.trader_id;
        let account_id = self.core.account_id;
        let clock = self.clock;
        let command = cmd.clone();

        if self.ws_trading_active() {
            let ws_client = self.ws_trading_client.as_ref().unwrap().clone();
            let dispatch_state = self.dispatch_state.clone();
            let params = build_cancel_order_params(&command)?;

            // Pre-register before sending to avoid response racing the insert
            let request_id = ws_client.next_request_id();
            dispatch_state.pending_requests.insert(
                request_id.clone(),
                PendingRequest {
                    client_order_id: command.client_order_id,
                    venue_order_id: command.venue_order_id,
                    operation: PendingOperation::Cancel,
                },
            );

            self.spawn_task("cancel_order_ws", async move {
                if let Err(e) = ws_client
                    .cancel_order_with_id(request_id.clone(), params)
                    .await
                {
                    dispatch_state.pending_requests.remove(&request_id);
                    log::error!(
                        "WS cancel request failed for {}, awaiting reconciliation: {e}",
                        command.client_order_id
                    );
                    anyhow::bail!("WS cancel order failed: {e}");
                }
                Ok(())
            });
        } else {
            let http_client = self.http_client.clone();
            let dispatch_state = self.dispatch_state.clone();
            log::debug!("WS trading not active, falling back to HTTP for cancel_order");

            self.spawn_task("cancel_order_http", async move {
                let result = http_client
                    .cancel_order(
                        command.instrument_id,
                        command.venue_order_id,
                        Some(command.client_order_id),
                    )
                    .await;

                match result {
                    Ok(venue_order_id) => {
                        dispatch_state.cleanup_terminal(command.client_order_id);
                        let ts_now = clock.get_time_ns();
                        let canceled_event = OrderCanceled::new(
                            trader_id,
                            command.strategy_id,
                            command.instrument_id,
                            command.client_order_id,
                            UUID4::new(),
                            ts_now,
                            ts_now,
                            false,
                            Some(venue_order_id),
                            Some(account_id),
                        );
                        event_emitter.send_order_event(OrderEventAny::Canceled(canceled_event));
                    }
                    Err(e) => {
                        if is_structured_venue_rejection(&e) {
                            let ts_now = clock.get_time_ns();
                            let rejected_event = OrderCancelRejected::new(
                                trader_id,
                                command.strategy_id,
                                command.instrument_id,
                                command.client_order_id,
                                format!("cancel-order-error: {e}").into(),
                                UUID4::new(),
                                ts_now,
                                ts_now,
                                false,
                                command.venue_order_id,
                                Some(account_id),
                            );
                            event_emitter
                                .send_order_event(OrderEventAny::CancelRejected(rejected_event));
                        } else if is_local_command_failure(&e) {
                            log::warn!(
                                "Cancel command failed local validation for {}: {e}",
                                command.client_order_id
                            );
                        } else {
                            log::error!(
                                "Ambiguous cancel failure for {}, awaiting reconciliation: {e}",
                                command.client_order_id
                            );
                        }
                        return Err(e);
                    }
                }
                Ok(())
            });
        }

        Ok(())
    }

    fn spawn_task<F>(&self, description: &'static str, fut: F)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        crate::common::execution::spawn_task(&self.pending_tasks, description, fut);
    }

    fn abort_pending_tasks(&self) {
        crate::common::execution::abort_pending_tasks(&self.pending_tasks);
    }
}

#[async_trait(?Send)]
impl ExecutionClient for BinanceSpotExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        *BINANCE_VENUE
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }

        // Load instruments if not already done
        if !self.core.instruments_initialized() {
            let instruments = self
                .http_client
                .request_instruments()
                .await
                .context("failed to request Binance Spot instruments")?;

            if instruments.is_empty() {
                log::warn!("No instruments returned for Binance Spot");
            } else {
                log::info!("Loaded {} Spot instruments", instruments.len());
                self.http_client.cache_instruments(instruments);
            }

            self.core.set_instruments_initialized();
        }

        // Request initial account state
        let account_state = self
            .refresh_account_state()
            .await
            .context("failed to request Binance account state")?;

        if !account_state.balances.is_empty() {
            log::info!(
                "Received account state with {} balance(s)",
                account_state.balances.len()
            );
        }

        self.emitter.send_account_state(account_state);

        // Wait for account to be registered in cache before completing connect
        crate::common::execution::await_account_registered(&self.core, self.core.account_id, 30.0)
            .await?;

        // Connect WS trading client (primary order transport)
        if let Some(ref mut ws_trading) = self.ws_trading_client {
            match ws_trading.connect().await {
                Ok(()) => {
                    log::info!("Connected to Binance Spot WS trading API");

                    let ws_trading_clone = ws_trading.clone();
                    let emitter = self.emitter.clone();
                    let account_id = self.core.account_id;
                    let clock = self.clock;
                    let http_client = self.http_client.clone();
                    let dispatch_state = self.dispatch_state.clone();
                    let ws_authenticated = self.ws_authenticated.clone();
                    let seen_trade_ids = std::sync::Arc::new(Mutex::new(FifoCache::new()));

                    let handle = get_runtime().spawn(async move {
                        loop {
                            match ws_trading_clone.recv().await {
                                Some(msg) => {
                                    dispatch_ws_trading_message(
                                        msg,
                                        &emitter,
                                        &http_client,
                                        account_id,
                                        clock,
                                        &dispatch_state,
                                        &ws_authenticated,
                                        &seen_trade_ids,
                                    );
                                }
                                None => {
                                    log::warn!("WS trading dispatch loop ended");
                                    break;
                                }
                            }
                        }
                    });

                    *self.ws_trading_handle.lock().expect(MUTEX_POISONED) = Some(handle);

                    // Block until session is authenticated before signaling connected
                    if let Err(e) = ws_trading.session_logon().await {
                        log::error!("WS session logon failed: {e}");
                    } else {
                        let auth_result = tokio::time::timeout(
                            Duration::from_secs(10),
                            self.ws_authenticated.notified(),
                        )
                        .await;

                        if auth_result.is_err() {
                            log::error!(
                                "WS session authentication timed out, \
                                 order operations will use HTTP fallback"
                            );

                            if let Some(handle) =
                                self.ws_trading_handle.lock().expect(MUTEX_POISONED).take()
                            {
                                handle.abort();
                            }
                            ws_trading.disconnect().await;
                            self.ws_trading_client = None;
                        } else if let Err(e) = ws_trading.subscribe_user_data().await {
                            log::error!("WS user data subscribe failed: {e}");
                        }
                    }
                }
                Err(e) => {
                    log::error!(
                        "Failed to connect WS trading API: {e}. \
                         Order operations will use HTTP fallback"
                    );
                }
            }
        }

        self.core.set_connected();
        log::info!("Connected: client_id={}", self.core.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.core.is_disconnected() {
            return Ok(());
        }

        // Abort WS trading task and disconnect
        if let Some(handle) = self.ws_trading_handle.lock().expect(MUTEX_POISONED).take() {
            handle.abort();
        }

        if let Some(ref mut ws_trading) = self.ws_trading_client {
            ws_trading.disconnect().await;
        }

        self.abort_pending_tasks();

        self.core.set_disconnected();
        log::info!("Disconnected: client_id={}", self.core.client_id);
        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        self.update_account_state();
        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        log::debug!("query_order: client_order_id={}", cmd.client_order_id);

        let http_client = self.http_client.clone();
        let command = cmd;
        let event_emitter = self.emitter.clone();
        let account_id = self.core.account_id;

        self.spawn_task("query_order", async move {
            let result = http_client
                .request_order_status_report(
                    account_id,
                    command.instrument_id,
                    command.venue_order_id,
                    Some(command.client_order_id),
                )
                .await;

            match result {
                Ok(Some(report)) => {
                    event_emitter.send_order_status_report(report);
                }
                Ok(None) => log::debug!(
                    "No order status report returned: client_order_id={}",
                    command.client_order_id
                ),
                Err(e) => log::warn!("Failed to query order status: {e}"),
            }

            Ok(())
        });

        Ok(())
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event);
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }

        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();

        // Spawn instrument bootstrap task
        let http_client = self.http_client.clone();

        get_runtime().spawn(async move {
            match http_client.request_instruments().await {
                Ok(instruments) => {
                    if instruments.is_empty() {
                        log::warn!("No instruments returned for Binance Spot");
                    } else {
                        http_client.cache_instruments(instruments);
                        log::info!("Instruments initialized");
                    }
                }
                Err(e) => {
                    log::error!("Failed to request Binance Spot instruments: {e}");
                }
            }
        });

        log::info!(
            "Started: client_id={}, account_id={}, account_type={:?}, environment={:?}, product_type={:?}",
            self.core.client_id,
            self.core.account_id,
            self.core.account_type,
            self.config.environment,
            self.config.product_type,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }

        // Abort WS trading task
        if let Some(handle) = self.ws_trading_handle.lock().expect(MUTEX_POISONED).take() {
            handle.abort();
        }

        self.core.set_stopped();
        self.core.set_disconnected();
        self.abort_pending_tasks();
        log::info!("Stopped: client_id={}", self.core.client_id);
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let Some(instrument_id) = cmd.instrument_id else {
            log::warn!("generate_order_status_report requires instrument_id: {cmd:?}");
            return Ok(None);
        };

        // Convert ClientOrderId to VenueOrderId if provided (API naming quirk)
        let venue_order_id = cmd
            .venue_order_id
            .as_ref()
            .map(|id| VenueOrderId::new(id.inner()));

        self.http_client
            .request_order_status_report(
                self.core.account_id,
                instrument_id,
                venue_order_id,
                cmd.client_order_id,
            )
            .await
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let start_dt = cmd.start.map(|nanos| nanos.to_datetime_utc());
        let end_dt = cmd.end.map(|nanos| nanos.to_datetime_utc());

        let reports = self
            .http_client
            .request_order_status_reports(
                self.core.account_id,
                cmd.instrument_id,
                start_dt,
                end_dt,
                cmd.open_only,
                None, // limit
            )
            .await?;

        Ok(reports)
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let Some(instrument_id) = cmd.instrument_id else {
            log::warn!("generate_fill_reports requires instrument_id for Binance Spot");
            return Ok(Vec::new());
        };

        // Convert ClientOrderId to VenueOrderId if provided (API naming quirk)
        let venue_order_id = cmd
            .venue_order_id
            .as_ref()
            .map(|id| VenueOrderId::new(id.inner()));

        let start_dt = cmd.start.map(|nanos| nanos.to_datetime_utc());
        let end_dt = cmd.end.map(|nanos| nanos.to_datetime_utc());

        let reports = self
            .http_client
            .request_fill_reports(
                self.core.account_id,
                instrument_id,
                venue_order_id,
                start_dt,
                end_dt,
                None, // limit
            )
            .await?;

        Ok(reports)
    }

    async fn generate_position_status_reports(
        &self,
        _cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        // Spot trading doesn't have positions in the traditional sense
        // Returns empty for spot, could be extended for margin positions
        Ok(Vec::new())
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        log::info!("Generating ExecutionMassStatus (lookback_mins={lookback_mins:?})");

        let ts_now = self.clock.get_time_ns();

        let start = lookback_mins.map(|mins| {
            let lookback_ns = mins_to_nanos(mins);
            UnixNanos::from(ts_now.as_u64().saturating_sub(lookback_ns))
        });

        // Binance requires instrument_id for historical orders (open_only=false).
        // Use open_only=true for mass status to get all open orders across instruments.
        let order_cmd = GenerateOrderStatusReportsBuilder::default()
            .ts_init(ts_now)
            .open_only(true)
            .start(start)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let position_cmd = GeneratePositionStatusReportsBuilder::default()
            .ts_init(ts_now)
            .start(start)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let (order_reports, position_reports) = tokio::try_join!(
            self.generate_order_status_reports(&order_cmd),
            self.generate_position_status_reports(&position_cmd),
        )?;

        // Note: Fill reports require instrument_id for Binance, so we skip them in mass status
        // They would need to be fetched per-instrument if needed

        log::info!("Received {} OrderStatusReports", order_reports.len());
        log::info!("Received {} PositionReports", position_reports.len());

        let mut mass_status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            *BINANCE_VENUE,
            ts_now,
            None,
        );

        mass_status.add_order_reports(order_reports);
        mass_status.add_position_reports(position_reports);

        Ok(Some(mass_status))
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self
            .core
            .cache()
            .order(&cmd.client_order_id)
            .map(|o| o.clone())
            .ok_or_else(|| anyhow::anyhow!("Order not found: {}", cmd.client_order_id))?;

        if order.is_closed() {
            let client_order_id = order.client_order_id();
            log::warn!("Cannot submit closed order {client_order_id}");
            return Ok(());
        }

        log::debug!("OrderSubmitted client_order_id={}", order.client_order_id());
        self.emitter.emit_order_submitted(&order);

        self.submit_order_internal(&cmd)
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        log::debug!(
            "Binance Spot submit_order_list entered child_count={}",
            cmd.order_list.client_order_ids.len()
        );
        let orders = cmd
            .order_list
            .client_order_ids
            .iter()
            .map(|client_order_id| {
                self.core
                    .cache()
                    .order(client_order_id)
                    .map(|o| o.clone())
                    .ok_or_else(|| anyhow::anyhow!("Order not found: {client_order_id}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        for order in &orders {
            if order.is_closed() {
                let client_order_id = order.client_order_id();
                let reason = format!("Cannot submit closed order list child {client_order_id}");
                log::warn!("Binance Spot submit_order_list validation_failed: {reason}");
                emit_order_list_denied(&self.emitter, &orders, &reason);
                return Ok(());
            }
        }

        let params = match build_oco_order_list_params(cmd.params.as_ref(), &orders) {
            Ok(params) => params,
            Err(err) => {
                let reason = err.to_string();
                log::warn!("Binance Spot submit_order_list validation_failed: {reason}");
                emit_order_list_denied(&self.emitter, &orders, &reason);
                return Ok(());
            }
        };

        for order in &orders {
            log::debug!(
                "Binance Spot submit_order_list child_submitted_emitted client_order_id={}",
                order.client_order_id()
            );
            self.emitter.emit_order_submitted(order);

            self.dispatch_state.order_identities.insert(
                order.client_order_id(),
                OrderIdentity {
                    instrument_id: order.instrument_id(),
                    strategy_id: order.strategy_id(),
                    order_side: order.order_side(),
                    order_type: order.order_type(),
                    price: order.price(),
                    quantity: order.quantity(),
                },
            );
        }

        let event_emitter = self.emitter.clone();
        let http_client = self.http_client.clone();
        let dispatch_state = self.dispatch_state.clone();
        let clock = self.clock;
        let mut orders_by_client_id = AHashMap::new();
        for order in orders {
            orders_by_client_id.insert(order.client_order_id(), order);
        }

        self.spawn_task("submit_oco_order_list_http", async move {
            match http_client.submit_oco_order_list(&params).await {
                Ok(response) => {
                    log::debug!(
                        "Binance Spot submit_order_list http_result=ok order_list_id={}",
                        response.order_list_id
                    );
                    for acceptance in oco_child_acceptances(&response) {
                        let Some(order) = orders_by_client_id.get(&acceptance.client_order_id)
                        else {
                            log::warn!(
                                "No cached OCO child order for accepted client_order_id={}",
                                acceptance.client_order_id
                            );
                            continue;
                        };

                        let ts_event = acceptance
                            .transact_time
                            .map(|millis| UnixNanos::from_millis(millis as u64))
                            .unwrap_or_else(|| clock.get_time_ns());
                        event_emitter.emit_order_accepted(
                            order,
                            acceptance.venue_order_id,
                            ts_event,
                        );
                        dispatch_state.insert_accepted(acceptance.client_order_id);
                    }
                }
                Err(err) => {
                    let reason = err.to_string();
                    log::warn!("Binance Spot submit_order_list http_result=err reason={reason}");
                    let due_post_only = is_spot_post_only_rejection(&err);
                    let ts_event = clock.get_time_ns();
                    for order in orders_by_client_id.values() {
                        let client_order_id = order.client_order_id();
                        dispatch_state.cleanup_terminal(client_order_id);
                        event_emitter.emit_order_rejected(order, &reason, ts_event, due_post_only);
                    }
                }
            }
            Ok(())
        });

        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        // Binance Spot uses cancel-replace for order modification, which requires
        // the full order specification (side, type, time_in_force). Since ModifyOrder
        // doesn't include these fields, we need to look up the original order from cache.
        let order = self
            .core
            .cache()
            .order(&cmd.client_order_id)
            .map(|o| o.clone());

        let Some(order) = order else {
            log::warn!(
                "Cannot modify order {}: not found in cache",
                cmd.client_order_id
            );
            let ts_init = self.clock.get_time_ns();
            let rejected_event = OrderModifyRejected::new(
                self.core.trader_id,
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                "Order not found in cache for modify".into(),
                UUID4::new(),
                ts_init, // no venue timestamp, rejected locally
                ts_init,
                false,
                cmd.venue_order_id,
                Some(self.core.account_id),
            );

            self.emitter
                .send_order_event(OrderEventAny::ModifyRejected(rejected_event));
            return Ok(());
        };

        let event_emitter = self.emitter.clone();
        let trader_id = self.core.trader_id;
        let account_id = self.core.account_id;
        let clock = self.clock;

        let order_side = order.order_side();
        let order_type = order.order_type();
        let time_in_force = order.time_in_force();
        let quantity = cmd.quantity.unwrap_or_else(|| order.quantity());

        if self.ws_trading_active() {
            let command = cmd;
            let ws_client = self.ws_trading_client.as_ref().unwrap().clone();
            let dispatch_state = self.dispatch_state.clone();
            let params = build_cancel_replace_params(&command, &order, quantity)?;

            // Pre-register before sending to avoid response racing the insert
            let request_id = ws_client.next_request_id();
            dispatch_state.pending_requests.insert(
                request_id.clone(),
                PendingRequest {
                    client_order_id: command.client_order_id,
                    venue_order_id: command.venue_order_id,
                    operation: PendingOperation::Modify,
                },
            );

            self.spawn_task("modify_order_ws", async move {
                if let Err(e) = ws_client
                    .cancel_replace_order_with_id(request_id.clone(), params)
                    .await
                {
                    dispatch_state.pending_requests.remove(&request_id);
                    log::error!(
                        "WS modify request failed for {}, awaiting reconciliation: {e}",
                        command.client_order_id
                    );
                    anyhow::bail!("WS modify order failed: {e}");
                }
                Ok(())
            });
        } else {
            let command = cmd;
            let http_client = self.http_client.clone();
            log::debug!("WS trading not active, falling back to HTTP for modify_order");

            self.spawn_task("modify_order_http", async move {
                let result = match command.venue_order_id {
                    Some(venue_order_id) => {
                        http_client
                            .modify_order(
                                account_id,
                                command.instrument_id,
                                venue_order_id,
                                command.client_order_id,
                                order_side,
                                order_type,
                                quantity,
                                time_in_force,
                                command.price,
                            )
                            .await
                    }
                    None => Err(anyhow::anyhow!(BinanceSpotHttpError::ValidationError(
                        "venue_order_id required for modify".to_string()
                    ))),
                };

                match result {
                    Ok(report) => {
                        let ts_now = clock.get_time_ns();
                        let updated_event = OrderUpdated::new(
                            trader_id,
                            command.strategy_id,
                            command.instrument_id,
                            command.client_order_id,
                            report.quantity,
                            UUID4::new(),
                            ts_now,
                            ts_now,
                            false,
                            Some(report.venue_order_id),
                            Some(account_id),
                            report.price,
                            None,  // trigger_price
                            None,  // protection_price
                            false, // is_quote_quantity
                        );
                        event_emitter.send_order_event(OrderEventAny::Updated(updated_event));
                    }
                    Err(e) => {
                        if is_structured_venue_rejection(&e) || is_local_command_failure(&e) {
                            let ts_now = clock.get_time_ns();
                            let rejected_event = OrderModifyRejected::new(
                                trader_id,
                                command.strategy_id,
                                command.instrument_id,
                                command.client_order_id,
                                format!("modify-order-error: {e}").into(),
                                UUID4::new(),
                                ts_now,
                                ts_now,
                                false,
                                command.venue_order_id,
                                Some(account_id),
                            );
                            event_emitter
                                .send_order_event(OrderEventAny::ModifyRejected(rejected_event));
                        } else {
                            log::error!(
                                "Ambiguous modify failure for {}, awaiting reconciliation: {e}",
                                command.client_order_id
                            );
                        }
                        return Err(e);
                    }
                }
                Ok(())
            });
        }

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        self.cancel_order_internal(&cmd)
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let event_emitter = self.emitter.clone();
        let trader_id = self.core.trader_id;
        let account_id = self.core.account_id;
        let clock = self.clock;

        if self.ws_trading_active() {
            let ws_client = self.ws_trading_client.as_ref().unwrap().clone();
            let symbol = cmd.instrument_id.symbol.to_string();

            self.spawn_task("cancel_all_orders_ws", async move {
                if let Err(e) = ws_client.cancel_all_orders(symbol).await {
                    log::error!("WS cancel_all_orders failed: {e}");
                }
                // Individual cancel confirmations dispatched via WS trading message loop
                Ok(())
            });

            return Ok(());
        }

        log::debug!("WS trading not active, falling back to HTTP for cancel_all_orders");
        let http_client = self.http_client.clone();

        // Build strategy lookup from cache before spawning (cache is not Send)
        let strategy_lookup: AHashMap<ClientOrderId, StrategyId> = {
            let cache = self.core.cache();
            cache
                .orders_open(None, Some(&cmd.instrument_id), None, None, None)
                .into_iter()
                .map(|order| (order.client_order_id(), order.strategy_id()))
                .collect()
        };

        let command = cmd;
        self.spawn_task("cancel_all_orders_http", async move {
            let canceled_orders = http_client.cancel_all_orders(command.instrument_id).await?;

            for (venue_order_id, client_order_id) in canceled_orders {
                let strategy_id = strategy_lookup
                    .get(&client_order_id)
                    .copied()
                    .unwrap_or(command.strategy_id);

                let canceled_event = OrderCanceled::new(
                    trader_id,
                    strategy_id,
                    command.instrument_id,
                    client_order_id,
                    UUID4::new(),
                    command.ts_init,
                    clock.get_time_ns(),
                    false,
                    Some(venue_order_id),
                    Some(account_id),
                );

                event_emitter.send_order_event(OrderEventAny::Canceled(canceled_event));
            }

            Ok(())
        });

        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        const BATCH_SIZE: usize = 5;

        if cmd.cancels.is_empty() {
            return Ok(());
        }

        let http_client = self.http_client.clone();
        let command = cmd;

        let event_emitter = self.emitter.clone();
        let trader_id = self.core.trader_id;
        let account_id = self.core.account_id;
        let clock = self.clock;

        self.spawn_task("batch_cancel_orders", async move {
            for chunk in command.cancels.chunks(BATCH_SIZE) {
                let batch_items: Vec<BatchCancelItem> = chunk
                    .iter()
                    .map(|cancel| {
                        if let Some(venue_order_id) = cancel.venue_order_id {
                            let order_id = venue_order_id.inner().parse::<i64>().unwrap_or(0);
                            if order_id != 0 {
                                Ok(BatchCancelItem::by_order_id(
                                    command.instrument_id.symbol.to_string(),
                                    order_id,
                                ))
                            } else {
                                let client_order_id = encode_binance_client_order_id(
                                    &cancel.client_order_id,
                                    BINANCE_NAUTILUS_SPOT_BROKER_ID,
                                )?;
                                Ok(BatchCancelItem::by_client_order_id(
                                    command.instrument_id.symbol.to_string(),
                                    client_order_id,
                                ))
                            }
                        } else {
                            let client_order_id = encode_binance_client_order_id(
                                &cancel.client_order_id,
                                BINANCE_NAUTILUS_SPOT_BROKER_ID,
                            )?;
                            Ok(BatchCancelItem::by_client_order_id(
                                command.instrument_id.symbol.to_string(),
                                client_order_id,
                            ))
                        }
                    })
                    .collect::<anyhow::Result<_>>()?;

                match http_client.batch_cancel_orders(&batch_items).await {
                    Ok(results) => {
                        for (i, result) in results.iter().enumerate() {
                            let cancel = &chunk[i];

                            match result {
                                BatchCancelResult::Success(success) => {
                                    let venue_order_id =
                                        VenueOrderId::new(success.order_id.to_string());
                                    let canceled_event = OrderCanceled::new(
                                        trader_id,
                                        cancel.strategy_id,
                                        cancel.instrument_id,
                                        cancel.client_order_id,
                                        UUID4::new(),
                                        cancel.ts_init,
                                        clock.get_time_ns(),
                                        false,
                                        Some(venue_order_id),
                                        Some(account_id),
                                    );

                                    event_emitter
                                        .send_order_event(OrderEventAny::Canceled(canceled_event));
                                }
                                BatchCancelResult::Error(error) => {
                                    let rejected_event = OrderCancelRejected::new(
                                        trader_id,
                                        cancel.strategy_id,
                                        cancel.instrument_id,
                                        cancel.client_order_id,
                                        format!(
                                            "batch-cancel-error: code={}, msg={}",
                                            error.code, error.msg
                                        )
                                        .into(),
                                        UUID4::new(),
                                        clock.get_time_ns(),
                                        cancel.ts_init,
                                        false,
                                        cancel.venue_order_id,
                                        Some(account_id),
                                    );

                                    event_emitter.send_order_event(OrderEventAny::CancelRejected(
                                        rejected_event,
                                    ));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if is_local_http_command_failure(&e) {
                            log::warn!(
                                "Batch cancel command failed local validation for {} orders: {e}",
                                chunk.len()
                            );
                        } else {
                            log::error!(
                                "Ambiguous batch cancel failure for {} orders, awaiting reconciliation: {e}",
                                chunk.len()
                            );
                        }
                    }
                }
            }

            Ok(())
        });

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OcoChildAcceptance {
    client_order_id: ClientOrderId,
    venue_order_id: VenueOrderId,
    transact_time: Option<i64>,
}

fn oco_child_acceptances(response: &BinanceOrderListResponse) -> Vec<OcoChildAcceptance> {
    let mut seen = AHashSet::new();
    let mut acceptances =
        Vec::with_capacity(response.order_reports.len().max(response.orders.len()));

    for report in &response.order_reports {
        let client_order_id = ClientOrderId::from(decode_broker_id(
            &report.client_order_id,
            BINANCE_NAUTILUS_SPOT_BROKER_ID,
        ));
        seen.insert(client_order_id);
        acceptances.push(OcoChildAcceptance {
            client_order_id,
            venue_order_id: VenueOrderId::new(report.order_id.to_string()),
            transact_time: report.transact_time.or(response.transaction_time),
        });
    }

    for order_ref in &response.orders {
        let client_order_id = ClientOrderId::from(decode_broker_id(
            &order_ref.client_order_id,
            BINANCE_NAUTILUS_SPOT_BROKER_ID,
        ));
        if !seen.insert(client_order_id) {
            continue;
        }

        acceptances.push(OcoChildAcceptance {
            client_order_id,
            venue_order_id: VenueOrderId::new(order_ref.order_id.to_string()),
            transact_time: response.transaction_time,
        });
    }

    acceptances
}

#[expect(clippy::too_many_arguments)]
fn dispatch_ws_trading_message(
    msg: BinanceSpotWsTradingMessage,
    emitter: &ExecutionEventEmitter,
    http_client: &BinanceSpotHttpClient,
    account_id: AccountId,
    clock: &'static AtomicTime,
    dispatch_state: &WsDispatchState,
    ws_authenticated: &tokio::sync::Notify,
    seen_trade_ids: &std::sync::Arc<Mutex<FifoCache<(Ustr, i64), 10_000>>>,
) {
    match msg {
        BinanceSpotWsTradingMessage::OrderAccepted {
            request_id,
            response,
        } => {
            dispatch_state.pending_requests.remove(&request_id);
            log::debug!(
                "WS order accepted: request_id={request_id}, order_id={}",
                response.order_id
            );
            // OrderAccepted event is synthesized from UDS executionReport (New)
        }
        BinanceSpotWsTradingMessage::OrderRejected {
            request_id,
            code,
            msg,
        } => {
            log::debug!("WS order rejected: request_id={request_id}, code={code}, msg={msg}");
            if let Some((_, pending)) = dispatch_state.pending_requests.remove(&request_id) {
                let code_i64 = i64::from(code);
                if matches!(
                    code_i64,
                    BINANCE_UNEXPECTED_RESPONSE_CODE | BINANCE_STATUS_UNKNOWN_CODE
                ) {
                    log::error!(
                        "Ambiguous WS submit failure for {}, awaiting reconciliation: code={code}, msg={msg}",
                        pending.client_order_id,
                    );
                    return;
                }

                // Clone to drop the DashMap read guard before cleanup_terminal
                let identity = dispatch_state
                    .order_identities
                    .get(&pending.client_order_id)
                    .map(|r| r.clone());

                if let Some(identity) = identity {
                    let due_post_only = code_i64 == BINANCE_GTX_ORDER_REJECT_CODE
                        || (code_i64 == BINANCE_NEW_ORDER_REJECTED_CODE
                            && msg == BINANCE_SPOT_POST_ONLY_REJECT_MSG);
                    let ts_now = clock.get_time_ns();
                    let rejected = OrderRejected::new(
                        emitter.trader_id(),
                        identity.strategy_id,
                        identity.instrument_id,
                        pending.client_order_id,
                        account_id,
                        Ustr::from(&format!("code={code}: {msg}")),
                        UUID4::new(),
                        ts_now,
                        ts_now,
                        false,
                        due_post_only,
                    );
                    dispatch_state.cleanup_terminal(pending.client_order_id);
                    emitter.send_order_event(OrderEventAny::Rejected(rejected));
                } else {
                    log::warn!(
                        "No order identity for {}, cannot emit OrderRejected",
                        pending.client_order_id
                    );
                }
            } else {
                log::warn!("No pending request for {request_id}, cannot emit OrderRejected");
            }
        }
        BinanceSpotWsTradingMessage::OrderCanceled {
            request_id,
            response,
        } => {
            dispatch_state.pending_requests.remove(&request_id);
            log::debug!(
                "WS order canceled: request_id={request_id}, order_id={}",
                response.order_id
            );
            // OrderCanceled event is synthesized from UDS executionReport (Canceled)
        }
        BinanceSpotWsTradingMessage::CancelRejected {
            request_id,
            code,
            msg,
        } => {
            log::warn!("WS cancel rejected: request_id={request_id}, code={code}, msg={msg}");
            if let Some((_, pending)) = dispatch_state.pending_requests.remove(&request_id)
                && let Some(identity) = dispatch_state
                    .order_identities
                    .get(&pending.client_order_id)
            {
                let ts_now = clock.get_time_ns();
                let rejected = OrderCancelRejected::new(
                    emitter.trader_id(),
                    identity.strategy_id,
                    identity.instrument_id,
                    pending.client_order_id,
                    Ustr::from(&format!("code={code}: {msg}")),
                    UUID4::new(),
                    ts_now,
                    ts_now,
                    false,
                    pending.venue_order_id,
                    Some(account_id),
                );
                emitter.send_order_event(OrderEventAny::CancelRejected(rejected));
            }
        }
        BinanceSpotWsTradingMessage::CancelReplaceAccepted {
            request_id,
            cancel_response,
            new_order_response,
        } => {
            if let Some((_, pending)) = dispatch_state.pending_requests.remove(&request_id)
                && matches!(pending.operation, PendingOperation::Modify)
            {
                dispatch_state.insert_pending_update(pending.client_order_id);
            }
            log::debug!(
                "WS cancel-replace accepted: request_id={request_id}, \
                 canceled_id={}, new_id={}",
                cancel_response.order_id,
                new_order_response.order_id,
            );
            // OrderUpdated event is synthesized from UDS executionReport (Replaced)
        }
        BinanceSpotWsTradingMessage::CancelReplaceRejected {
            request_id,
            code,
            msg,
        } => {
            log::warn!(
                "WS cancel-replace rejected: request_id={request_id}, code={code}, msg={msg}"
            );

            if let Some((_, pending)) = dispatch_state.pending_requests.remove(&request_id)
                && let Some(identity) = dispatch_state
                    .order_identities
                    .get(&pending.client_order_id)
            {
                let ts_now = clock.get_time_ns();
                let rejected = OrderModifyRejected::new(
                    emitter.trader_id(),
                    identity.strategy_id,
                    identity.instrument_id,
                    pending.client_order_id,
                    Ustr::from(&format!("code={code}: {msg}")),
                    UUID4::new(),
                    ts_now,
                    ts_now,
                    false,
                    pending.venue_order_id,
                    Some(account_id),
                );
                emitter.send_order_event(OrderEventAny::ModifyRejected(rejected));
            }
        }
        BinanceSpotWsTradingMessage::RequestFailed { request_id, msg } => {
            dispatch_state.pending_requests.remove(&request_id);
            log::error!(
                "WS trading request failed without structured venue response: request_id={request_id}, {msg}"
            );
        }
        BinanceSpotWsTradingMessage::AllOrdersCanceled {
            request_id,
            responses,
        } => {
            dispatch_state.pending_requests.remove(&request_id);
            log::debug!(
                "WS all orders canceled: request_id={request_id}, count={}",
                responses.len()
            );
            // Individual OrderCanceled events arrive via UDS executionReport
        }
        BinanceSpotWsTradingMessage::UserDataSubscribed { subscription_id } => {
            log::info!("User data stream subscribed: id={subscription_id}");
        }
        BinanceSpotWsTradingMessage::ExecutionReport(report) => {
            let ts_init = clock.get_time_ns();
            dispatch_execution_report(
                &report,
                emitter,
                http_client,
                account_id,
                dispatch_state,
                seen_trade_ids,
                ts_init,
            );
        }
        BinanceSpotWsTradingMessage::AccountPosition(position) => {
            let ts_init = clock.get_time_ns();
            let state = parse_spot_account_position(&position, account_id, ts_init);
            emitter.send_account_state(state);
        }
        BinanceSpotWsTradingMessage::BalanceUpdate(update) => {
            log::info!(
                "Balance update: asset={}, delta={}",
                update.asset,
                update.delta,
            );
            let http_client = http_client.clone();
            let emitter = emitter.clone();

            get_runtime().spawn(async move {
                match http_client.request_account_state(account_id).await {
                    Ok(state) => emitter.send_account_state(state),
                    Err(e) => {
                        log::error!("Failed to refresh account state after balance update: {e}");
                    }
                }
            });
        }
        BinanceSpotWsTradingMessage::ListStatus(list_status) => {
            dispatch_list_status(&list_status, emitter, account_id, dispatch_state, clock);
        }
        BinanceSpotWsTradingMessage::Connected => {
            log::info!("WS trading API connected");
        }
        BinanceSpotWsTradingMessage::Authenticated => {
            log::info!("WS trading API authenticated");
            ws_authenticated.notify_one();
        }
        BinanceSpotWsTradingMessage::Reconnected => {
            log::info!("WS trading API reconnected");
        }
        BinanceSpotWsTradingMessage::ServerShutdown { event_time } => {
            log::warn!(
                "WS trading API server shutdown notice (event_time={event_time}); reconnect expected within ~10 minutes"
            );
        }
        BinanceSpotWsTradingMessage::Error(err) => {
            log::error!("WS trading API error: {err}");
        }
    }
}

fn dispatch_list_status(
    list_status: &BinanceSpotListStatusMsg,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    dispatch_state: &WsDispatchState,
    clock: &'static AtomicTime,
) {
    log::debug!(
        "WS list status: symbol={}, order_list_id={}, list_client_order_id={}, status_type={:?}, order_status={:?}, child_count={}",
        list_status.symbol,
        list_status.order_list_id,
        list_status.list_client_order_id,
        list_status.list_status_type,
        list_status.list_order_status,
        list_status.orders.len(),
    );

    let ts_event = UnixNanos::from_millis(list_status.event_time as u64);
    let ts_init = clock.get_time_ns();
    let is_rejected = list_status.list_order_status == ListOrderStatus::Reject
        || !list_status.reject_reason.is_empty();
    let reject_reason = if list_status.reject_reason.is_empty() {
        "Order list rejected by venue"
    } else {
        list_status.reject_reason.as_str()
    };

    for child in &list_status.orders {
        let client_order_id = ClientOrderId::from(decode_broker_id(
            &child.client_order_id,
            BINANCE_NAUTILUS_SPOT_BROKER_ID,
        ));

        let Some(identity) = dispatch_state
            .order_identities
            .get(&client_order_id)
            .map(|entry| entry.clone())
        else {
            log::debug!(
                "Ignoring untracked list status child client_order_id={client_order_id}, order_id={}",
                child.order_id
            );
            continue;
        };

        if is_rejected {
            if !dispatch_state.has_emitted_accepted(&client_order_id)
                && !dispatch_state.has_filled(&client_order_id)
            {
                emitter.emit_order_rejected_event(
                    identity.strategy_id,
                    identity.instrument_id,
                    client_order_id,
                    reject_reason,
                    ts_init,
                    false,
                );
                dispatch_state.cleanup_terminal(client_order_id);
            }
            continue;
        }

        if dispatch_state.has_emitted_accepted(&client_order_id) {
            continue;
        }

        let venue_order_id = VenueOrderId::new(child.order_id.to_string());
        dispatch_state.insert_accepted(client_order_id);
        let accepted = OrderAccepted::new(
            emitter.trader_id(),
            identity.strategy_id,
            identity.instrument_id,
            client_order_id,
            venue_order_id,
            account_id,
            UUID4::new(),
            ts_event,
            ts_init,
            false,
        );
        emitter.send_order_event(OrderEventAny::Accepted(accepted));
    }
}

fn build_new_order_params(
    order: &impl Order,
    client_order_id: ClientOrderId,
    is_post_only: bool,
    is_quote_quantity: bool,
) -> anyhow::Result<NewOrderParams> {
    let binance_side = BinanceSide::try_from(order.order_side())?;
    let binance_order_type = order_type_to_binance_spot(order.order_type(), is_post_only)?;

    let requires_trigger = matches!(
        order.order_type(),
        OrderType::StopMarket
            | OrderType::StopLimit
            | OrderType::MarketIfTouched
            | OrderType::LimitIfTouched
    );

    if requires_trigger && order.trigger_price().is_none() {
        anyhow::bail!("Conditional orders require a trigger price");
    }

    let supports_tif = matches!(
        binance_order_type,
        BinanceSpotOrderType::Limit
            | BinanceSpotOrderType::StopLossLimit
            | BinanceSpotOrderType::TakeProfitLimit
    );
    let binance_tif = if supports_tif {
        Some(time_in_force_to_binance_spot(order.time_in_force())?)
    } else {
        None
    };

    let qty_str = order.quantity().to_string();
    let (base_qty, quote_qty) = if is_quote_quantity {
        (None, Some(qty_str))
    } else {
        (Some(qty_str), None)
    };

    let client_id_str =
        encode_binance_client_order_id(&client_order_id, BINANCE_NAUTILUS_SPOT_BROKER_ID)?;

    Ok(NewOrderParams {
        symbol: order.instrument_id().symbol.to_string(),
        side: binance_side,
        order_type: binance_order_type,
        time_in_force: binance_tif,
        quantity: base_qty,
        quote_order_qty: quote_qty,
        price: order.price().map(|p| p.to_string()),
        new_client_order_id: Some(client_id_str),
        stop_price: order.trigger_price().map(|p| p.to_string()),
        trailing_delta: None,
        iceberg_qty: order.display_qty().map(|q| q.to_string()),
        new_order_resp_type: Some(BinanceOrderResponseType::Full),
        self_trade_prevention_mode: None,
        strategy_id: None,
        strategy_type: None,
    })
}

fn emit_order_list_denied(
    emitter: &ExecutionEventEmitter,
    orders: &[nautilus_model::orders::OrderAny],
    reason: &str,
) {
    for order in orders {
        log::debug!(
            "Binance Spot submit_order_list child_denied_emitted client_order_id={}",
            order.client_order_id()
        );
        emitter.emit_order_denied(order, reason);
    }
}

fn build_oco_order_list_params(
    command_params: Option<&nautilus_core::params::Params>,
    orders: &[nautilus_model::orders::OrderAny],
) -> anyhow::Result<NewOrderListOcoParams> {
    if !has_oco_contingency_param(command_params) {
        anyhow::bail!(
            "Binance Spot submit_order_list only supports params.{OCO_CONTINGENCY_TYPE_PARAM}=\
             {OCO_CONTINGENCY_TYPE_VALUE}"
        );
    }

    if orders.len() != 2 {
        anyhow::bail!(
            "Binance Spot OCO order list requires exactly 2 child orders, received {}",
            orders.len()
        );
    }

    let first = &orders[0];
    let second = &orders[1];
    if first.instrument_id() != second.instrument_id() {
        anyhow::bail!("Binance Spot OCO child orders must use the same instrument");
    }
    if first.order_side() != second.order_side() {
        anyhow::bail!("Binance Spot OCO child orders must use the same side");
    }
    if first.quantity() != second.quantity() {
        anyhow::bail!("Binance Spot OCO child orders must use the same quantity");
    }
    if first.is_quote_quantity() || second.is_quote_quantity() {
        anyhow::bail!("Binance Spot OCO does not support quoteOrderQty child orders");
    }

    let target = orders
        .iter()
        .find(|order| order.order_type() == OrderType::Limit && order.is_post_only())
        .ok_or_else(|| anyhow::anyhow!("Binance Spot OCO requires one post-only LIMIT target"))?;
    let stop = orders
        .iter()
        .find(|order| {
            matches!(
                order.order_type(),
                OrderType::StopMarket | OrderType::StopLimit
            )
        })
        .ok_or_else(|| {
            anyhow::anyhow!("Binance Spot OCO requires one STOP_LOSS or STOP_LOSS_LIMIT child")
        })?;

    if target.client_order_id() == stop.client_order_id() {
        anyhow::bail!("Binance Spot OCO child orders must be distinct");
    }

    let target_price = target
        .price()
        .ok_or_else(|| anyhow::anyhow!("Binance Spot OCO target LIMIT_MAKER requires price"))?
        .to_string();
    let stop_trigger_price = stop
        .trigger_price()
        .ok_or_else(|| anyhow::anyhow!("Binance Spot OCO stop child requires trigger price"))?
        .to_string();
    let stop_limit_price = match stop.order_type() {
        OrderType::StopMarket => None,
        OrderType::StopLimit => Some(
            stop.price()
                .ok_or_else(|| {
                    anyhow::anyhow!("Binance Spot OCO STOP_LOSS_LIMIT child requires price")
                })?
                .to_string(),
        ),
        _ => unreachable!("stop order type filtered above"),
    };
    let stop_time_in_force = if stop.order_type() == OrderType::StopLimit {
        Some(time_in_force_to_binance_spot(stop.time_in_force())?)
    } else {
        None
    };

    let side = BinanceSide::try_from(first.order_side())?;
    let quantity = first.quantity().to_string();
    let target_client_id =
        encode_binance_client_order_id(&target.client_order_id(), BINANCE_NAUTILUS_SPOT_BROKER_ID)?;
    let stop_client_id =
        encode_binance_client_order_id(&stop.client_order_id(), BINANCE_NAUTILUS_SPOT_BROKER_ID)?;
    let stop_type = match stop.order_type() {
        OrderType::StopMarket => BinanceSpotOrderType::StopLoss,
        OrderType::StopLimit => BinanceSpotOrderType::StopLossLimit,
        _ => unreachable!("stop order type filtered above"),
    };

    let mut params = NewOrderListOcoParams {
        symbol: first.instrument_id().symbol.to_string(),
        side,
        quantity,
        list_client_order_id: None,
        above_type: BinanceSpotOrderType::LimitMaker,
        above_client_order_id: None,
        above_price: None,
        above_stop_price: None,
        above_time_in_force: None,
        below_type: BinanceSpotOrderType::StopLoss,
        below_client_order_id: None,
        below_price: None,
        below_stop_price: None,
        below_time_in_force: None,
        new_order_resp_type: Some(BinanceOrderResponseType::Full),
        self_trade_prevention_mode: None,
    };

    match first.order_side() {
        OrderSide::Sell => {
            params.above_type = BinanceSpotOrderType::LimitMaker;
            params.above_client_order_id = Some(target_client_id);
            params.above_price = Some(target_price);

            params.below_type = stop_type;
            params.below_client_order_id = Some(stop_client_id);
            params.below_price = stop_limit_price;
            params.below_stop_price = Some(stop_trigger_price);
            params.below_time_in_force = stop_time_in_force;
        }
        OrderSide::Buy => {
            params.above_type = stop_type;
            params.above_client_order_id = Some(stop_client_id);
            params.above_price = stop_limit_price;
            params.above_stop_price = Some(stop_trigger_price);
            params.above_time_in_force = stop_time_in_force;

            params.below_type = BinanceSpotOrderType::LimitMaker;
            params.below_client_order_id = Some(target_client_id);
            params.below_price = Some(target_price);
        }
        side => anyhow::bail!("Unsupported Binance Spot OCO order side: {side:?}"),
    }

    Ok(params)
}

fn has_oco_contingency_param(params: Option<&nautilus_core::params::Params>) -> bool {
    params
        .and_then(|params| params.get_str(OCO_CONTINGENCY_TYPE_PARAM))
        .is_some_and(|value| value.eq_ignore_ascii_case(OCO_CONTINGENCY_TYPE_VALUE))
}

fn build_cancel_order_params(cmd: &CancelOrder) -> anyhow::Result<CancelOrderParams> {
    let order_id = cmd
        .venue_order_id
        .and_then(|id| id.inner().parse::<i64>().ok());

    if let Some(order_id) = order_id {
        Ok(CancelOrderParams::by_order_id(
            cmd.instrument_id.symbol.to_string(),
            order_id,
        ))
    } else {
        let client_id_str =
            encode_binance_client_order_id(&cmd.client_order_id, BINANCE_NAUTILUS_SPOT_BROKER_ID)?;
        Ok(CancelOrderParams::by_client_order_id(
            cmd.instrument_id.symbol.to_string(),
            client_id_str,
        ))
    }
}

fn build_cancel_replace_params(
    cmd: &ModifyOrder,
    order: &impl Order,
    quantity: Quantity,
) -> anyhow::Result<CancelReplaceOrderParams> {
    let binance_side = BinanceSide::try_from(order.order_side())?;
    let binance_order_type = order_type_to_binance_spot(order.order_type(), false)?;
    let binance_tif = time_in_force_to_binance_spot(order.time_in_force())?;

    let cancel_order_id: Option<i64> = cmd
        .venue_order_id
        .map(|id| {
            id.inner()
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("Invalid venue order ID: {id}"))
        })
        .transpose()?;

    let client_id_str =
        encode_binance_client_order_id(&cmd.client_order_id, BINANCE_NAUTILUS_SPOT_BROKER_ID)?;

    Ok(CancelReplaceOrderParams {
        symbol: cmd.instrument_id.symbol.to_string(),
        side: binance_side,
        order_type: binance_order_type,
        cancel_replace_mode: BinanceCancelReplaceMode::StopOnFailure,
        time_in_force: Some(binance_tif),
        quantity: Some(quantity.to_string()),
        quote_order_qty: None,
        price: cmd.price.map(|p| p.to_string()),
        cancel_order_id,
        cancel_orig_client_order_id: if cancel_order_id.is_none() {
            Some(client_id_str.clone())
        } else {
            None
        },
        new_client_order_id: Some(client_id_str),
        stop_price: None,
        trailing_delta: None,
        iceberg_qty: None,
        new_order_resp_type: Some(BinanceOrderResponseType::Full),
        self_trade_prevention_mode: None,
    })
}

/// Dispatches a Spot execution report with tracked/untracked routing.
///
/// Tracked orders (with registered identity) produce proper order events.
/// Untracked orders fall back to execution reports for reconciliation.
fn dispatch_execution_report(
    report: &BinanceSpotExecutionReport,
    emitter: &ExecutionEventEmitter,
    http_client: &BinanceSpotHttpClient,
    account_id: AccountId,
    dispatch_state: &WsDispatchState,
    seen_trade_ids: &std::sync::Arc<Mutex<FifoCache<(Ustr, i64), 10_000>>>,
    ts_init: UnixNanos,
) {
    let symbol = report.symbol;
    let instrument_id = InstrumentId::new(symbol.into(), *BINANCE_VENUE);
    let (price_precision, size_precision) = http_client
        .get_instrument(&symbol)
        .map_or((8, 8), |i| (i.price_precision(), i.size_precision()));

    let client_order_id = ClientOrderId::new(decode_broker_id(
        &report.client_order_id,
        BINANCE_NAUTILUS_SPOT_BROKER_ID,
    ));

    let identity = dispatch_state
        .order_identities
        .get(&client_order_id)
        .map(|r| r.clone());

    if let Some(identity) = identity {
        dispatch_tracked_execution_report(
            report,
            emitter,
            account_id,
            dispatch_state,
            seen_trade_ids,
            client_order_id,
            &identity,
            instrument_id,
            price_precision,
            size_precision,
            ts_init,
        );
    } else {
        dispatch_untracked_execution_report(
            report,
            emitter,
            http_client,
            account_id,
            seen_trade_ids,
            instrument_id,
            price_precision,
            size_precision,
            ts_init,
        );
    }
}

/// Dispatches a tracked execution report as proper order events.
#[expect(clippy::too_many_arguments)]
fn dispatch_tracked_execution_report(
    report: &BinanceSpotExecutionReport,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    state: &WsDispatchState,
    seen_trade_ids: &std::sync::Arc<Mutex<FifoCache<(Ustr, i64), 10_000>>>,
    client_order_id: ClientOrderId,
    identity: &OrderIdentity,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) {
    let venue_order_id = VenueOrderId::new(report.order_id.to_string());
    let ts_event = UnixNanos::from_millis(report.event_time as u64);

    match report.execution_type {
        BinanceSpotExecutionType::New => {
            if state.has_filled(&client_order_id) {
                log::debug!("Skipping New for already-filled {client_order_id}");
                return;
            }

            if state.has_emitted_accepted(&client_order_id) {
                if !state.remove_pending_update(&client_order_id) {
                    log::debug!("Skipping duplicate New for already-accepted {client_order_id}");
                    return;
                }

                let Some(quantity) = parse_spot_execution_report_quantity(
                    report,
                    &report.original_qty,
                    size_precision,
                    "original_qty",
                ) else {
                    return;
                };

                let Some((price, trigger)) =
                    parse_order_update_prices(report, identity.order_type, price_precision)
                else {
                    return;
                };

                let updated = OrderUpdated::new(
                    emitter.trader_id(),
                    identity.strategy_id,
                    identity.instrument_id,
                    client_order_id,
                    quantity,
                    UUID4::new(),
                    ts_event,
                    ts_init,
                    false,
                    Some(venue_order_id),
                    Some(account_id),
                    price,
                    trigger,
                    None,  // protection_price
                    false, // is_quote_quantity
                );
                emitter.send_order_event(OrderEventAny::Updated(updated));
                return;
            }
            state.insert_accepted(client_order_id);
            let accepted = OrderAccepted::new(
                emitter.trader_id(),
                identity.strategy_id,
                identity.instrument_id,
                client_order_id,
                venue_order_id,
                account_id,
                UUID4::new(),
                ts_event,
                ts_init,
                false,
            );
            emitter.send_order_event(OrderEventAny::Accepted(accepted));
        }
        BinanceSpotExecutionType::Trade => {
            let dedup_key = (report.symbol, report.trade_id);
            let mut guard = seen_trade_ids.lock().expect(MUTEX_POISONED);
            let is_duplicate = guard.contains(&dedup_key);
            guard.add(dedup_key);
            drop(guard);

            if is_duplicate {
                log::debug!(
                    "Duplicate trade_id={} for {}, skipping",
                    report.trade_id,
                    report.symbol
                );
                return;
            }

            ensure_accepted_emitted(
                client_order_id,
                account_id,
                venue_order_id,
                identity,
                emitter,
                state,
                ts_init,
            );

            let Some(last_qty) = parse_spot_execution_report_quantity(
                report,
                &report.last_filled_qty,
                size_precision,
                "last_filled_qty",
            ) else {
                return;
            };
            let Some(last_px) = parse_spot_execution_report_price(
                report,
                &report.last_filled_price,
                price_precision,
                "last_filled_price",
            ) else {
                return;
            };
            let Some(commission) =
                parse_spot_execution_report_decimal(report, &report.commission, "commission")
            else {
                return;
            };
            let commission_currency = report
                .commission_asset
                .as_ref()
                .map_or_else(Currency::USDT, |a| {
                    Currency::get_or_create_crypto(a.as_str())
                });
            let commission_money = match Money::from_decimal(commission, commission_currency) {
                Ok(money) => money,
                Err(e) => {
                    log::warn!(
                        "Failed to build Spot commission money for symbol={}, order_id={}, \
                        trade_id={}: {e}",
                        report.symbol,
                        report.order_id,
                        report.trade_id,
                    );
                    return;
                }
            };

            let liquidity_side = if report.is_maker {
                LiquiditySide::Maker
            } else {
                LiquiditySide::Taker
            };

            let filled = OrderFilled::new(
                emitter.trader_id(),
                identity.strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                account_id,
                TradeId::new(report.trade_id.to_string()),
                identity.order_side,
                identity.order_type,
                last_qty,
                last_px,
                commission_currency,
                liquidity_side,
                UUID4::new(),
                ts_event,
                ts_init,
                false,
                None,
                Some(commission_money),
            );

            state.insert_filled(client_order_id);
            emitter.send_order_event(OrderEventAny::Filled(filled));

            let cumulative_qty = parse_spot_execution_report_decimal(
                report,
                &report.cumulative_filled_qty,
                "cumulative_filled_qty",
            );
            let original_qty =
                parse_spot_execution_report_decimal(report, &report.original_qty, "original_qty");
            if let (Some(original_qty), Some(cumulative_qty)) = (original_qty, cumulative_qty)
                && original_qty <= cumulative_qty
            {
                state.cleanup_terminal(client_order_id);
            }
        }
        BinanceSpotExecutionType::Replaced => {
            // Cancel-replace succeeded: the old order is being replaced.
            // The replacement NEW event follows with the new price/qty.
            log::debug!(
                "Order replaced: client_order_id={client_order_id}, venue_order_id={venue_order_id}"
            );
        }
        BinanceSpotExecutionType::Canceled
        | BinanceSpotExecutionType::Expired
        | BinanceSpotExecutionType::TradePrevention => {
            ensure_accepted_emitted(
                client_order_id,
                account_id,
                venue_order_id,
                identity,
                emitter,
                state,
                ts_init,
            );
            let canceled = OrderCanceled::new(
                emitter.trader_id(),
                identity.strategy_id,
                identity.instrument_id,
                client_order_id,
                UUID4::new(),
                ts_event,
                ts_init,
                false,
                Some(venue_order_id),
                Some(account_id),
            );
            state.cleanup_terminal(client_order_id);
            emitter.send_order_event(OrderEventAny::Canceled(canceled));
        }
        BinanceSpotExecutionType::Rejected => {
            let reason = if report.reject_reason.is_empty() {
                Ustr::from("Order rejected by venue")
            } else {
                Ustr::from(&report.reject_reason)
            };
            let due_post_only = report.time_in_force == BinanceTimeInForce::Gtx
                || (report.order_type == "LIMIT_MAKER"
                    && (report.reject_reason.is_empty() || report.reject_reason == "NONE"));
            state.cleanup_terminal(client_order_id);
            emitter.emit_order_rejected_event(
                identity.strategy_id,
                identity.instrument_id,
                client_order_id,
                reason.as_str(),
                ts_init,
                due_post_only,
            );
        }
    }
}

fn parse_order_update_prices(
    report: &BinanceSpotExecutionReport,
    order_type: OrderType,
    price_precision: u8,
) -> Option<(Option<Price>, Option<Price>)> {
    let price = if order_type_accepts_update_price(order_type) {
        Some(parse_spot_execution_report_price(
            report,
            &report.price,
            price_precision,
            "price",
        )?)
    } else {
        None
    };

    let trigger = if order_type_accepts_update_trigger(order_type) {
        let stop_price =
            parse_spot_execution_report_decimal(report, &report.stop_price, "stop_price")?;
        if stop_price > Decimal::ZERO {
            Some(parse_spot_execution_report_price(
                report,
                &report.stop_price,
                price_precision,
                "stop_price",
            )?)
        } else {
            None
        }
    } else {
        None
    };

    Some((price, trigger))
}

fn order_type_accepts_update_price(order_type: OrderType) -> bool {
    matches!(
        order_type,
        OrderType::Limit
            | OrderType::StopLimit
            | OrderType::MarketToLimit
            | OrderType::LimitIfTouched
            | OrderType::TrailingStopLimit
    )
}

fn order_type_accepts_update_trigger(order_type: OrderType) -> bool {
    matches!(
        order_type,
        OrderType::StopMarket
            | OrderType::StopLimit
            | OrderType::MarketIfTouched
            | OrderType::LimitIfTouched
            | OrderType::TrailingStopMarket
            | OrderType::TrailingStopLimit
    )
}

fn parse_spot_execution_report_quantity(
    report: &BinanceSpotExecutionReport,
    raw: &str,
    precision: u8,
    field: &str,
) -> Option<Quantity> {
    match parse_required_quantity_at_precision(raw, precision, field) {
        Ok(value) => Some(value),
        Err(e) => {
            warn_invalid_spot_execution_report_field(report, field, &e);
            None
        }
    }
}

fn parse_spot_execution_report_price(
    report: &BinanceSpotExecutionReport,
    raw: &str,
    precision: u8,
    field: &str,
) -> Option<Price> {
    match parse_required_price_at_precision(raw, precision, field) {
        Ok(value) => Some(value),
        Err(e) => {
            warn_invalid_spot_execution_report_field(report, field, &e);
            None
        }
    }
}

fn parse_spot_execution_report_decimal(
    report: &BinanceSpotExecutionReport,
    raw: &str,
    field: &str,
) -> Option<Decimal> {
    match parse_required_decimal(raw, field) {
        Ok(value) => Some(value),
        Err(e) => {
            warn_invalid_spot_execution_report_field(report, field, &e);
            None
        }
    }
}

fn warn_invalid_spot_execution_report_field(
    report: &BinanceSpotExecutionReport,
    field: &str,
    error: &anyhow::Error,
) {
    log::warn!(
        "Failed to parse Spot execution report {field} for symbol={}, order_id={}, \
        trade_id={}, client_order_id={}: {error}",
        report.symbol,
        report.order_id,
        report.trade_id,
        report.client_order_id,
    );
}

/// Dispatches an untracked execution report as execution reports for reconciliation.
#[expect(clippy::too_many_arguments)]
fn dispatch_untracked_execution_report(
    report: &BinanceSpotExecutionReport,
    emitter: &ExecutionEventEmitter,
    _http_client: &BinanceSpotHttpClient,
    account_id: AccountId,
    seen_trade_ids: &std::sync::Arc<Mutex<FifoCache<(Ustr, i64), 10_000>>>,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) {
    match report.execution_type {
        BinanceSpotExecutionType::Trade => {
            let dedup_key = (report.symbol, report.trade_id);
            let mut guard = seen_trade_ids.lock().expect(MUTEX_POISONED);
            let is_duplicate = guard.contains(&dedup_key);
            guard.add(dedup_key);
            drop(guard);

            if is_duplicate {
                log::debug!(
                    "Duplicate trade_id={} for {}, skipping",
                    report.trade_id,
                    report.symbol
                );
                return;
            }

            match parse_spot_exec_report_to_order_status(
                report,
                instrument_id,
                price_precision,
                size_precision,
                account_id,
                ts_init,
            ) {
                Ok(status) => emitter.send_order_status_report(status),
                Err(e) => log::error!("Failed to parse order status report: {e}"),
            }

            match parse_spot_exec_report_to_fill(
                report,
                instrument_id,
                price_precision,
                size_precision,
                account_id,
                ts_init,
            ) {
                Ok(fill) => emitter.send_fill_report(fill),
                Err(e) => log::error!("Failed to parse fill report: {e}"),
            }
        }
        BinanceSpotExecutionType::New
        | BinanceSpotExecutionType::Canceled
        | BinanceSpotExecutionType::Replaced
        | BinanceSpotExecutionType::Rejected
        | BinanceSpotExecutionType::Expired
        | BinanceSpotExecutionType::TradePrevention => {
            match parse_spot_exec_report_to_order_status(
                report,
                instrument_id,
                price_precision,
                size_precision,
                account_id,
                ts_init,
            ) {
                Ok(status) => emitter.send_order_status_report(status),
                Err(e) => log::error!("Failed to parse order status report: {e}"),
            }
        }
    }
}

// Checks for GTX (-5022) and spot LIMIT_MAKER (-2010 + specific message)
fn is_spot_post_only_rejection(error: &BinanceSpotHttpError) -> bool {
    match error {
        BinanceSpotHttpError::BinanceError { code, message } => {
            *code == BINANCE_GTX_ORDER_REJECT_CODE
                || (*code == BINANCE_NEW_ORDER_REJECTED_CODE
                    && message == BINANCE_SPOT_POST_ONLY_REJECT_MSG)
        }
        _ => false,
    }
}

fn is_structured_venue_rejection(err: &anyhow::Error) -> bool {
    err.downcast_ref::<BinanceSpotHttpError>()
        .is_some_and(|be| matches!(be, BinanceSpotHttpError::BinanceError { .. }))
}

fn is_ambiguous_submit_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<BinanceSpotHttpError>()
        .is_some_and(|be| {
            matches!(
                be,
                BinanceSpotHttpError::BinanceError {
                    code: BINANCE_UNEXPECTED_RESPONSE_CODE | BINANCE_STATUS_UNKNOWN_CODE,
                    ..
                }
            )
        })
}

fn is_local_command_failure(err: &anyhow::Error) -> bool {
    err.downcast_ref::<BinanceSpotHttpError>()
        .is_some_and(is_local_http_command_failure)
}

fn is_local_http_command_failure(err: &BinanceSpotHttpError) -> bool {
    matches!(
        err,
        BinanceSpotHttpError::MissingCredentials | BinanceSpotHttpError::ValidationError(_)
    )
}

#[cfg(test)]
mod tests {
    use nautilus_common::messages::ExecutionEvent;
    use nautilus_core::{params::Params, time::get_atomic_clock_realtime};
    use nautilus_model::{
        enums::{AccountType, LiquiditySide, OrderSide, OrderType},
        identifiers::{StrategyId, TraderId},
        orders::{OrderAny, OrderTestBuilder},
        types::{Price, Quantity},
    };
    use rstest::rstest;
    use serde_json::Value;

    use super::*;
    use crate::{
        common::{encoder::encode_broker_id, enums::BinanceEnvironment},
        spot::{
            http::models::{BinanceOrderListOrder, BinanceOrderListOrderReport},
            sbe::spot::{contingency_type::ContingencyType, list_status_type::ListStatusType},
            websocket::trading::user_data::BinanceSpotListStatusOrder,
        },
    };

    #[rstest]
    fn test_build_oco_order_list_params_maps_sell_target_above_stop_below() {
        let params = oco_order_list_params();
        let orders = vec![
            test_order(
                OrderType::Limit,
                "TARGET",
                OrderSide::Sell,
                "0.001",
                Some("120.00"),
                None,
                true,
            ),
            test_order(
                OrderType::StopMarket,
                "STOP",
                OrderSide::Sell,
                "0.001",
                None,
                Some("95.00"),
                false,
            ),
        ];

        let request = build_oco_order_list_params(Some(&params), &orders).unwrap();

        assert_eq!(request.side, BinanceSide::Sell);
        assert_eq!(request.quantity, "0.001");
        assert_eq!(request.above_type, BinanceSpotOrderType::LimitMaker);
        assert_eq!(request.above_price.as_deref(), Some("120.00"));
        assert!(request.above_stop_price.is_none());
        assert_eq!(request.below_type, BinanceSpotOrderType::StopLoss);
        assert_eq!(request.below_stop_price.as_deref(), Some("95.00"));
        assert!(request.below_price.is_none());
        assert!(request.above_client_order_id.is_some());
        assert!(request.below_client_order_id.is_some());
    }

    #[rstest]
    fn test_build_oco_order_list_params_maps_buy_stop_above_target_below() {
        let params = oco_order_list_params();
        let orders = vec![
            test_order(
                OrderType::Limit,
                "TARGET",
                OrderSide::Buy,
                "0.001",
                Some("95.00"),
                None,
                true,
            ),
            test_order(
                OrderType::StopMarket,
                "STOP",
                OrderSide::Buy,
                "0.001",
                None,
                Some("120.00"),
                false,
            ),
        ];

        let request = build_oco_order_list_params(Some(&params), &orders).unwrap();

        assert_eq!(request.side, BinanceSide::Buy);
        assert_eq!(request.above_type, BinanceSpotOrderType::StopLoss);
        assert_eq!(request.above_stop_price.as_deref(), Some("120.00"));
        assert!(request.above_price.is_none());
        assert_eq!(request.below_type, BinanceSpotOrderType::LimitMaker);
        assert_eq!(request.below_price.as_deref(), Some("95.00"));
        assert!(request.below_stop_price.is_none());
    }

    #[rstest]
    fn test_build_oco_order_list_params_rejects_missing_oco_param() {
        let orders = sell_oco_orders("0.001", "0.001");

        let error = build_oco_order_list_params(None, &orders).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("only supports params.contingency_type=OCO")
        );
    }

    #[rstest]
    fn test_build_oco_order_list_params_rejects_mismatched_quantity() {
        let params = oco_order_list_params();
        let orders = sell_oco_orders("0.001", "0.002");

        let error = build_oco_order_list_params(Some(&params), &orders).unwrap_err();

        assert!(error.to_string().contains("same quantity"));
    }

    #[rstest]
    fn test_build_oco_order_list_params_rejects_non_post_only_target() {
        let params = oco_order_list_params();
        let orders = vec![
            test_order(
                OrderType::Limit,
                "TARGET",
                OrderSide::Sell,
                "0.001",
                Some("120.00"),
                None,
                false,
            ),
            test_order(
                OrderType::StopMarket,
                "STOP",
                OrderSide::Sell,
                "0.001",
                None,
                Some("95.00"),
                false,
            ),
        ];

        let error = build_oco_order_list_params(Some(&params), &orders).unwrap_err();

        assert!(error.to_string().contains("post-only LIMIT target"));
    }

    #[rstest]
    fn test_build_new_order_params_rejects_invalid_client_order_id_before_http() {
        let order = test_order(
            OrderType::Limit,
            "order.with.dot",
            OrderSide::Buy,
            "0.001",
            Some("95.00"),
            None,
            false,
        );

        let error =
            build_new_order_params(&order, order.client_order_id(), false, false).unwrap_err();

        assert!(error.to_string().contains("Binance client order id"));
    }

    #[rstest]
    fn test_build_oco_order_list_params_rejects_too_long_child_client_order_id_before_http() {
        let params = oco_order_list_params();
        let orders = vec![
            test_order(
                OrderType::Limit,
                "G-r8dc659b0e9f9-n_exit_plan-target-1-2",
                OrderSide::Sell,
                "0.001",
                Some("120.00"),
                None,
                true,
            ),
            test_order(
                OrderType::StopMarket,
                "G-r8dc659b0e9f9-n_exit_plan-stop-3",
                OrderSide::Sell,
                "0.001",
                None,
                Some("95.00"),
                false,
            ),
        ];

        let error = build_oco_order_list_params(Some(&params), &orders).unwrap_err();

        assert!(error.to_string().contains("Binance client order id"));
    }

    #[rstest]
    fn test_build_oco_order_list_params_accepts_max_raw_child_client_order_ids() {
        let params = oco_order_list_params();
        let orders = vec![
            test_order(
                OrderType::Limit,
                "abcdefghijklmnopqrstuvwx",
                OrderSide::Sell,
                "0.001",
                Some("120.00"),
                None,
                true,
            ),
            test_order(
                OrderType::StopMarket,
                "abcdefghijklmnopqrstuvw1",
                OrderSide::Sell,
                "0.001",
                None,
                Some("95.00"),
                false,
            ),
        ];

        let request = build_oco_order_list_params(Some(&params), &orders).unwrap();

        assert_eq!(
            request.above_client_order_id.as_deref().map(str::len),
            Some(36)
        );
        assert_eq!(
            request.below_client_order_id.as_deref().map(str::len),
            Some(36)
        );
    }

    #[rstest]
    fn test_dispatch_ws_trading_message_emits_cancel_rejected_and_clears_pending_request() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let dispatch_state = create_tracked_dispatch_state(
            ClientOrderId::from("TEST"),
            InstrumentId::from("BTCUSDT.BINANCE"),
        );
        let ws_authenticated = tokio::sync::Notify::new();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        dispatch_state.pending_requests.insert(
            "req-cancel".to_string(),
            PendingRequest {
                client_order_id: ClientOrderId::from("TEST"),
                venue_order_id: Some(VenueOrderId::from("12345")),
                operation: PendingOperation::Cancel,
            },
        );

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::CancelRejected {
                request_id: "req-cancel".to_string(),
                code: -2011,
                msg: "Unknown order sent".to_string(),
            },
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            clock,
            &dispatch_state,
            &ws_authenticated,
            &seen_trade_ids,
        );

        assert!(dispatch_state.pending_requests.get("req-cancel").is_none());

        match rx
            .try_recv()
            .expect("Cancel rejection event should be emitted")
        {
            ExecutionEvent::Order(OrderEventAny::CancelRejected(event)) => {
                assert_eq!(event.client_order_id, ClientOrderId::from("TEST"));
                assert_eq!(event.account_id, Some(AccountId::from("BINANCE-001")));
                assert!(event.reason.as_str().contains("code=-2011"));
            }
            other => panic!("Expected CancelRejected event, was {other:?}"),
        }
    }

    #[rstest]
    #[case(
        BINANCE_UNEXPECTED_RESPONSE_CODE,
        "An unexpected response was received from the message bus"
    )]
    #[case(
        BINANCE_STATUS_UNKNOWN_CODE,
        "Timeout waiting for response from backend server"
    )]
    fn test_dispatch_ws_trading_message_unknown_status_keeps_order_registered(
        #[case] code: i64,
        #[case] msg: &str,
    ) {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let client_order_id = ClientOrderId::from("TEST");
        let dispatch_state =
            create_tracked_dispatch_state(client_order_id, InstrumentId::from("BTCUSDT.BINANCE"));
        let ws_authenticated = tokio::sync::Notify::new();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        dispatch_state.pending_requests.insert(
            "req-submit".to_string(),
            PendingRequest {
                client_order_id,
                venue_order_id: None,
                operation: PendingOperation::Place,
            },
        );

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::OrderRejected {
                request_id: "req-submit".to_string(),
                code: code as i32,
                msg: msg.to_string(),
            },
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            clock,
            &dispatch_state,
            &ws_authenticated,
            &seen_trade_ids,
        );

        assert!(dispatch_state.pending_requests.get("req-submit").is_none());
        assert!(
            dispatch_state
                .order_identities
                .get(&client_order_id)
                .is_some()
        );
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_dispatch_ws_trading_message_definite_submit_rejection_emits_order_rejected() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let client_order_id = ClientOrderId::from("TEST");
        let dispatch_state =
            create_tracked_dispatch_state(client_order_id, InstrumentId::from("BTCUSDT.BINANCE"));
        let ws_authenticated = tokio::sync::Notify::new();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        dispatch_state.pending_requests.insert(
            "req-submit".to_string(),
            PendingRequest {
                client_order_id,
                venue_order_id: None,
                operation: PendingOperation::Place,
            },
        );

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::OrderRejected {
                request_id: "req-submit".to_string(),
                code: BINANCE_NEW_ORDER_REJECTED_CODE as i32,
                msg: BINANCE_SPOT_POST_ONLY_REJECT_MSG.to_string(),
            },
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            clock,
            &dispatch_state,
            &ws_authenticated,
            &seen_trade_ids,
        );

        assert!(dispatch_state.pending_requests.get("req-submit").is_none());
        assert!(
            dispatch_state
                .order_identities
                .get(&client_order_id)
                .is_none()
        );

        match rx
            .try_recv()
            .expect("OrderRejected event should be emitted")
        {
            ExecutionEvent::Order(OrderEventAny::Rejected(event)) => {
                assert_eq!(event.client_order_id, client_order_id);
                assert_eq!(event.account_id, AccountId::from("BINANCE-001"));
                assert!(event.reason.as_str().contains("code=-2010"));
                assert!(event.due_post_only);
            }
            other => panic!("Expected OrderRejected event, was {other:?}"),
        }
    }

    #[rstest]
    fn test_dispatch_ws_trading_message_emits_modify_rejected_and_clears_pending_request() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let dispatch_state = create_tracked_dispatch_state(
            ClientOrderId::from("TEST"),
            InstrumentId::from("BTCUSDT.BINANCE"),
        );
        let ws_authenticated = tokio::sync::Notify::new();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        dispatch_state.pending_requests.insert(
            "req-modify".to_string(),
            PendingRequest {
                client_order_id: ClientOrderId::from("TEST"),
                venue_order_id: Some(VenueOrderId::from("12345")),
                operation: PendingOperation::Modify,
            },
        );

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::CancelReplaceRejected {
                request_id: "req-modify".to_string(),
                code: -2021,
                msg: "Order cancel-replace partially failed".to_string(),
            },
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            clock,
            &dispatch_state,
            &ws_authenticated,
            &seen_trade_ids,
        );

        assert!(dispatch_state.pending_requests.get("req-modify").is_none());

        match rx
            .try_recv()
            .expect("Modify rejection event should be emitted")
        {
            ExecutionEvent::Order(OrderEventAny::ModifyRejected(event)) => {
                assert_eq!(event.client_order_id, ClientOrderId::from("TEST"));
                assert_eq!(event.account_id, Some(AccountId::from("BINANCE-001")));
                assert!(event.reason.as_str().contains("code=-2021"));
            }
            other => panic!("Expected ModifyRejected event, was {other:?}"),
        }
    }

    fn oco_order_list_params() -> Params {
        let mut params = Params::new();
        params.insert(
            OCO_CONTINGENCY_TYPE_PARAM.to_string(),
            Value::String(OCO_CONTINGENCY_TYPE_VALUE.to_string()),
        );
        params
    }

    fn sell_oco_orders(target_qty: &str, stop_qty: &str) -> Vec<OrderAny> {
        vec![
            test_order(
                OrderType::Limit,
                "TARGET",
                OrderSide::Sell,
                target_qty,
                Some("120.00"),
                None,
                true,
            ),
            test_order(
                OrderType::StopMarket,
                "STOP",
                OrderSide::Sell,
                stop_qty,
                None,
                Some("95.00"),
                false,
            ),
        ]
    }

    fn test_order(
        order_type: OrderType,
        client_order_id: &str,
        side: OrderSide,
        quantity: &str,
        price: Option<&str>,
        trigger_price: Option<&str>,
        post_only: bool,
    ) -> OrderAny {
        let mut builder = OrderTestBuilder::new(order_type);
        builder
            .instrument_id(InstrumentId::from("BTCUSDT.BINANCE"))
            .client_order_id(ClientOrderId::from(client_order_id))
            .side(side)
            .quantity(Quantity::from(quantity))
            .post_only(post_only);

        if let Some(price) = price {
            builder.price(Price::from(price));
        }
        if let Some(trigger_price) = trigger_price {
            builder.trigger_price(Price::from(trigger_price));
        }

        builder.build()
    }

    fn create_test_emitter(
        clock: &'static AtomicTime,
    ) -> (
        ExecutionEventEmitter,
        tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    ) {
        let mut emitter = ExecutionEventEmitter::new(
            clock,
            TraderId::from("TESTER-001"),
            AccountId::from("BINANCE-001"),
            AccountType::Cash,
            None,
        );
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(tx);
        (emitter, rx)
    }

    #[rstest]
    fn test_emit_order_list_denied_emits_denied_for_each_child() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let orders = sell_oco_orders("0.001", "0.001");
        let reason = "Binance Spot OCO requires one post-only LIMIT target";

        emit_order_list_denied(&emitter, &orders, reason);

        for expected_id in [ClientOrderId::from("TARGET"), ClientOrderId::from("STOP")] {
            match rx.try_recv().expect("OrderDenied event expected") {
                ExecutionEvent::Order(OrderEventAny::Denied(event)) => {
                    assert_eq!(event.client_order_id, expected_id);
                    assert_eq!(event.reason.as_str(), reason);
                }
                other => panic!("Expected OrderDenied event, was {other:?}"),
            }
        }
        assert!(rx.try_recv().is_err());
    }

    fn create_test_http_client(clock: &'static AtomicTime) -> BinanceSpotHttpClient {
        BinanceSpotHttpClient::new(
            BinanceEnvironment::Live,
            clock,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("Test HTTP client should be created")
    }

    fn create_tracked_dispatch_state(
        client_order_id: ClientOrderId,
        instrument_id: InstrumentId,
    ) -> WsDispatchState {
        create_tracked_dispatch_state_with_order_type(
            client_order_id,
            instrument_id,
            OrderType::Limit,
        )
    }

    fn create_tracked_dispatch_state_with_order_type(
        client_order_id: ClientOrderId,
        instrument_id: InstrumentId,
        order_type: OrderType,
    ) -> WsDispatchState {
        let dispatch_state = WsDispatchState::default();
        dispatch_state.order_identities.insert(
            client_order_id,
            OrderIdentity {
                instrument_id,
                strategy_id: StrategyId::from("TEST-STRATEGY"),
                order_side: OrderSide::Buy,
                order_type,
                price: None,
                quantity: Quantity::from("1"),
            },
        );
        dispatch_state
    }

    fn execution_report_new(
        client_order_id: ClientOrderId,
        order_type: &str,
        price: &str,
        stop_price: &str,
    ) -> BinanceSpotExecutionReport {
        let encoded = encode_broker_id(&client_order_id, BINANCE_NAUTILUS_SPOT_BROKER_ID);
        serde_json::from_str(&format!(
            r#"{{
                "e":"executionReport","E":1709654400000,"s":"BTCUSDT",
                "c":"{encoded}","S":"SELL","o":"{order_type}","f":"GTC",
                "q":"0.00100000","p":"{price}","P":"{stop_price}",
                "x":"NEW","X":"NEW","r":"NONE","i":12345678,
                "l":"0.00000000","z":"0.00000000","L":"0.00000000",
                "n":"0","N":null,"T":1709654400000,"t":-1,"w":true,"m":false,
                "O":1709654400000,"Z":"0.00000000","C":""
            }}"#,
        ))
        .expect("valid executionReport NEW fixture")
    }

    #[rstest]
    fn test_oco_child_acceptances_merge_partial_order_reports_with_orders() {
        let target_id = ClientOrderId::from("TARGET");
        let stop_id = ClientOrderId::from("STOP");
        let target_wire = encode_broker_id(&target_id, BINANCE_NAUTILUS_SPOT_BROKER_ID);
        let stop_wire = encode_broker_id(&stop_id, BINANCE_NAUTILUS_SPOT_BROKER_ID);
        let response = BinanceOrderListResponse {
            order_list_id: 42,
            contingency_type: "OCO".to_string(),
            list_status_type: "EXEC_STARTED".to_string(),
            list_order_status: "EXECUTING".to_string(),
            list_client_order_id: "LIST".to_string(),
            transaction_time: Some(1_709_654_400_124),
            symbol: "BTCUSDT".to_string(),
            orders: vec![
                BinanceOrderListOrder {
                    symbol: "BTCUSDT".to_string(),
                    order_id: 1001,
                    client_order_id: target_wire.clone(),
                },
                BinanceOrderListOrder {
                    symbol: "BTCUSDT".to_string(),
                    order_id: 1002,
                    client_order_id: stop_wire,
                },
            ],
            order_reports: vec![BinanceOrderListOrderReport {
                symbol: "BTCUSDT".to_string(),
                order_id: 1001,
                order_list_id: 42,
                client_order_id: target_wire,
                transact_time: Some(1_709_654_400_125),
                price: None,
                orig_qty: None,
                executed_qty: None,
                cummulative_quote_qty: None,
                status: None,
                order_type: None,
                side: None,
                time_in_force: None,
                stop_price: None,
                working_time: None,
                self_trade_prevention_mode: None,
            }],
        };

        let acceptances = oco_child_acceptances(&response);

        assert_eq!(acceptances.len(), 2);
        assert_eq!(acceptances[0].client_order_id, target_id);
        assert_eq!(acceptances[0].venue_order_id, VenueOrderId::new("1001"));
        assert_eq!(acceptances[0].transact_time, Some(1_709_654_400_125));
        assert_eq!(acceptances[1].client_order_id, stop_id);
        assert_eq!(acceptances[1].venue_order_id, VenueOrderId::new("1002"));
        assert_eq!(acceptances[1].transact_time, Some(1_709_654_400_124));
    }

    #[rstest]
    fn test_dispatch_list_status_emits_missing_accepted_once() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let client_order_id = ClientOrderId::from("TARGET");
        let dispatch_state =
            create_tracked_dispatch_state(client_order_id, InstrumentId::from("BTCUSDT.BINANCE"));
        let msg = BinanceSpotListStatusMsg {
            event_time: 1_709_654_400_123,
            transact_time: 1_709_654_400_124,
            order_list_id: 42,
            contingency_type: ContingencyType::Oco,
            list_status_type: ListStatusType::ExecStarted,
            list_order_status: ListOrderStatus::Executing,
            subscription_id: None,
            symbol: Ustr::from("BTCUSDT"),
            list_client_order_id: "LIST".to_string(),
            reject_reason: String::new(),
            orders: vec![BinanceSpotListStatusOrder {
                order_id: 1001,
                symbol: Ustr::from("BTCUSDT"),
                client_order_id: encode_broker_id(
                    &client_order_id,
                    BINANCE_NAUTILUS_SPOT_BROKER_ID,
                ),
            }],
        };

        dispatch_list_status(
            &msg,
            &emitter,
            AccountId::from("BINANCE-001"),
            &dispatch_state,
            clock,
        );
        dispatch_list_status(
            &msg,
            &emitter,
            AccountId::from("BINANCE-001"),
            &dispatch_state,
            clock,
        );

        match rx.try_recv().expect("OrderAccepted event expected") {
            ExecutionEvent::Order(OrderEventAny::Accepted(event)) => {
                assert_eq!(event.client_order_id, client_order_id);
                assert_eq!(event.venue_order_id, VenueOrderId::new("1001"));
                assert_eq!(event.account_id, AccountId::from("BINANCE-001"));
            }
            other => panic!("Expected OrderAccepted event, was {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_dispatch_list_status_reject_emits_order_rejected() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let client_order_id = ClientOrderId::from("TARGET");
        let dispatch_state =
            create_tracked_dispatch_state(client_order_id, InstrumentId::from("BTCUSDT.BINANCE"));
        let msg = BinanceSpotListStatusMsg {
            event_time: 1_709_654_400_123,
            transact_time: 1_709_654_400_124,
            order_list_id: 42,
            contingency_type: ContingencyType::Oco,
            list_status_type: ListStatusType::Response,
            list_order_status: ListOrderStatus::Reject,
            subscription_id: None,
            symbol: Ustr::from("BTCUSDT"),
            list_client_order_id: "LIST".to_string(),
            reject_reason: "INSUFFICIENT_BALANCE".to_string(),
            orders: vec![BinanceSpotListStatusOrder {
                order_id: 1001,
                symbol: Ustr::from("BTCUSDT"),
                client_order_id: encode_broker_id(
                    &client_order_id,
                    BINANCE_NAUTILUS_SPOT_BROKER_ID,
                ),
            }],
        };

        dispatch_list_status(
            &msg,
            &emitter,
            AccountId::from("BINANCE-001"),
            &dispatch_state,
            clock,
        );

        match rx.try_recv().expect("OrderRejected event expected") {
            ExecutionEvent::Order(OrderEventAny::Rejected(event)) => {
                assert_eq!(event.client_order_id, client_order_id);
                assert_eq!(event.account_id, AccountId::from("BINANCE-001"));
                assert_eq!(event.reason.as_str(), "INSUFFICIENT_BALANCE");
            }
            other => panic!("Expected OrderRejected event, was {other:?}"),
        }
        assert!(
            dispatch_state
                .order_identities
                .get(&client_order_id)
                .is_none()
        );
    }

    #[rstest]
    fn test_duplicate_new_after_list_status_accepted_is_ignored() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let client_order_id = ClientOrderId::from("STOP");
        let dispatch_state = create_tracked_dispatch_state_with_order_type(
            client_order_id,
            InstrumentId::from("BTCUSDT.BINANCE"),
            OrderType::StopMarket,
        );
        let ws_authenticated = tokio::sync::Notify::new();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));
        let list_status = BinanceSpotListStatusMsg {
            event_time: 1_709_654_400_123,
            transact_time: 1_709_654_400_124,
            order_list_id: 42,
            contingency_type: ContingencyType::Oco,
            list_status_type: ListStatusType::ExecStarted,
            list_order_status: ListOrderStatus::Executing,
            subscription_id: None,
            symbol: Ustr::from("BTCUSDT"),
            list_client_order_id: "LIST".to_string(),
            reject_reason: String::new(),
            orders: vec![BinanceSpotListStatusOrder {
                order_id: 1001,
                symbol: Ustr::from("BTCUSDT"),
                client_order_id: encode_broker_id(
                    &client_order_id,
                    BINANCE_NAUTILUS_SPOT_BROKER_ID,
                ),
            }],
        };

        dispatch_list_status(
            &list_status,
            &emitter,
            AccountId::from("BINANCE-001"),
            &dispatch_state,
            clock,
        );

        match rx.try_recv().expect("OrderAccepted event expected") {
            ExecutionEvent::Order(OrderEventAny::Accepted(event)) => {
                assert_eq!(event.client_order_id, client_order_id);
            }
            other => panic!("Expected OrderAccepted event, was {other:?}"),
        }

        let report =
            execution_report_new(client_order_id, "STOP_LOSS", "0.00000000", "90000.00000000");
        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::ExecutionReport(Box::new(report)),
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            clock,
            &dispatch_state,
            &ws_authenticated,
            &seen_trade_ids,
        );

        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_pending_modify_new_emits_limit_order_updated() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let client_order_id = ClientOrderId::from("LIMIT");
        let dispatch_state =
            create_tracked_dispatch_state(client_order_id, InstrumentId::from("BTCUSDT.BINANCE"));
        let ws_authenticated = tokio::sync::Notify::new();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        dispatch_state.insert_accepted(client_order_id);
        dispatch_state.insert_pending_update(client_order_id);
        let report = execution_report_new(client_order_id, "LIMIT", "90123.45000000", "0.00000000");

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::ExecutionReport(Box::new(report)),
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            clock,
            &dispatch_state,
            &ws_authenticated,
            &seen_trade_ids,
        );

        match rx.try_recv().expect("OrderUpdated event expected") {
            ExecutionEvent::Order(OrderEventAny::Updated(event)) => {
                assert_eq!(event.client_order_id, client_order_id);
                assert_eq!(event.price, Some(Price::from("90123.45")));
                assert_eq!(event.trigger_price, None);
            }
            other => panic!("Expected OrderUpdated event, was {other:?}"),
        }
    }

    #[rstest]
    fn test_pending_modify_new_emits_valid_stop_market_update_shape() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let client_order_id = ClientOrderId::from("STOP");
        let dispatch_state = create_tracked_dispatch_state_with_order_type(
            client_order_id,
            InstrumentId::from("BTCUSDT.BINANCE"),
            OrderType::StopMarket,
        );
        let ws_authenticated = tokio::sync::Notify::new();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        dispatch_state.insert_accepted(client_order_id);
        dispatch_state.insert_pending_update(client_order_id);
        let report =
            execution_report_new(client_order_id, "STOP_LOSS", "0.00000000", "90000.00000000");

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::ExecutionReport(Box::new(report)),
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            clock,
            &dispatch_state,
            &ws_authenticated,
            &seen_trade_ids,
        );

        match rx.try_recv().expect("OrderUpdated event expected") {
            ExecutionEvent::Order(OrderEventAny::Updated(event)) => {
                assert_eq!(event.client_order_id, client_order_id);
                assert_eq!(event.price, None);
                assert_eq!(event.trigger_price, Some(Price::from("90000")));
            }
            other => panic!("Expected OrderUpdated event, was {other:?}"),
        }
    }

    #[rstest]
    #[case::gtx(
        BinanceSpotHttpError::BinanceError {
            code: BINANCE_GTX_ORDER_REJECT_CODE,
            message: "Order would immediately trigger.".to_string(),
        },
        true,
    )]
    #[case::spot_post_only(
        BinanceSpotHttpError::BinanceError {
            code: BINANCE_NEW_ORDER_REJECTED_CODE,
            message: BINANCE_SPOT_POST_ONLY_REJECT_MSG.to_string(),
        },
        true,
    )]
    #[case::new_order_rejected_other_message(
        BinanceSpotHttpError::BinanceError {
            code: BINANCE_NEW_ORDER_REJECTED_CODE,
            message: "Insufficient balance.".to_string(),
        },
        false,
    )]
    #[case::unrelated_code(
        BinanceSpotHttpError::BinanceError {
            code: -2011,
            message: "Unknown order sent.".to_string(),
        },
        false,
    )]
    #[case::non_binance_error(
        BinanceSpotHttpError::NetworkError("connection reset".to_string()),
        false,
    )]
    fn test_is_spot_post_only_rejection(
        #[case] error: BinanceSpotHttpError,
        #[case] expected: bool,
    ) {
        assert_eq!(is_spot_post_only_rejection(&error), expected);
    }

    #[rstest]
    #[case(BINANCE_UNEXPECTED_RESPONSE_CODE)]
    #[case(BINANCE_STATUS_UNKNOWN_CODE)]
    fn test_unknown_status_submit_error_is_ambiguous(#[case] code: i64) {
        let err = anyhow::Error::new(BinanceSpotHttpError::BinanceError {
            code,
            message: "test error".to_string(),
        });
        assert!(is_ambiguous_submit_error(&err));
        assert!(is_structured_venue_rejection(&err));
    }

    #[rstest]
    fn test_other_structured_submit_error_is_not_ambiguous() {
        let err = anyhow::Error::new(BinanceSpotHttpError::BinanceError {
            code: BINANCE_GTX_ORDER_REJECT_CODE,
            message: "test error".to_string(),
        });
        assert!(!is_ambiguous_submit_error(&err));
        assert!(is_structured_venue_rejection(&err));
    }

    #[rstest]
    fn test_dispatch_tracked_execution_report_trade_dedup() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let client_order_id = ClientOrderId::from("x-TD67BGP9-T0000000000000");
        let dispatch_state = create_tracked_dispatch_state(
            ClientOrderId::from("O-20200101-000000-000-000-0"),
            InstrumentId::from("ETHUSDT.BINANCE"),
        );
        let ws_authenticated = tokio::sync::Notify::new();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        let trade_json = crate::common::testing::load_fixture_string(
            "spot/user_data_json/execution_report_trade.json",
        );
        let report: BinanceSpotExecutionReport = serde_json::from_str(&trade_json).unwrap();

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::ExecutionReport(Box::new(report.clone())),
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            clock,
            &dispatch_state,
            &ws_authenticated,
            &seen_trade_ids,
        );
        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::ExecutionReport(Box::new(report)),
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            clock,
            &dispatch_state,
            &ws_authenticated,
            &seen_trade_ids,
        );

        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }

        let fills: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, ExecutionEvent::Order(OrderEventAny::Filled(_))))
            .collect();
        assert_eq!(fills.len(), 1, "duplicate trade should be deduped");

        match fills[0] {
            ExecutionEvent::Order(OrderEventAny::Filled(fill)) => {
                assert_eq!(
                    fill.client_order_id,
                    ClientOrderId::from("O-20200101-000000-000-000-0"),
                );
                assert_eq!(fill.trade_id, TradeId::new("98765432"));
                assert_eq!(fill.liquidity_side, LiquiditySide::Maker);
            }
            _ => unreachable!(),
        }
        let _ = client_order_id;
    }

    #[rstest]
    fn test_dispatch_tracked_execution_report_invalid_fill_qty_skips_filled_event() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let dispatch_state = create_tracked_dispatch_state(
            ClientOrderId::from("O-20200101-000000-000-000-0"),
            InstrumentId::from("ETHUSDT.BINANCE"),
        );
        let ws_authenticated = tokio::sync::Notify::new();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        let trade_json = crate::common::testing::load_fixture_string(
            "spot/user_data_json/execution_report_trade.json",
        );
        let mut report: BinanceSpotExecutionReport = serde_json::from_str(&trade_json).unwrap();
        report.last_filled_qty = "not-a-number".to_string();

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::ExecutionReport(Box::new(report)),
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            clock,
            &dispatch_state,
            &ws_authenticated,
            &seen_trade_ids,
        );

        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }

        assert!(
            events
                .iter()
                .all(|e| !matches!(e, ExecutionEvent::Order(OrderEventAny::Filled(_)))),
            "invalid fill quantity must not emit OrderFilled",
        );
    }

    #[rstest]
    fn test_dispatch_tracked_execution_report_rejected_gtx_sets_post_only() {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let http_client = create_test_http_client(clock);
        let client_order_id = ClientOrderId::from("O-20200101-000000-000-000-1");
        let dispatch_state =
            create_tracked_dispatch_state(client_order_id, InstrumentId::from("ETHUSDT.BINANCE"));
        let ws_authenticated = tokio::sync::Notify::new();
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));

        let encoded = encode_broker_id(&client_order_id, BINANCE_NAUTILUS_SPOT_BROKER_ID);
        let report_json = format!(
            r#"{{
                "e":"executionReport","E":1709654400000,"s":"ETHUSDT",
                "c":"{encoded}","S":"BUY","o":"LIMIT","f":"GTX",
                "q":"1.00000000","p":"2500.00000000","P":"0.00000000",
                "x":"REJECTED","X":"REJECTED","r":"NONE","i":12345678,
                "l":"0.00000000","z":"0.00000000","L":"0.00000000",
                "n":"0","N":null,"T":1709654400000,"t":-1,"w":false,"m":false,
                "O":1709654400000,"Z":"0.00000000","C":""
            }}"#,
        );
        let report: BinanceSpotExecutionReport = serde_json::from_str(&report_json).unwrap();

        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::ExecutionReport(Box::new(report)),
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            clock,
            &dispatch_state,
            &ws_authenticated,
            &seen_trade_ids,
        );

        match rx.try_recv().expect("OrderRejected event expected") {
            ExecutionEvent::Order(OrderEventAny::Rejected(event)) => {
                assert_eq!(event.client_order_id, client_order_id);
                assert_eq!(event.account_id, AccountId::from("BINANCE-001"));
                assert!(event.due_post_only);
            }
            other => panic!("Expected OrderRejected event, was {other:?}"),
        }
    }
}
