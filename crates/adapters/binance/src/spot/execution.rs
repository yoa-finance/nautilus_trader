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
    clients::{ExecutionClient, ExecutionClientCapabilities},
    live::{get_runtime, runner::get_exec_event_sender},
    messages::{
        execution::{
            BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
            GenerateOrderStatusReport, GenerateOrderStatusReports,
            GenerateOrderStatusReportsBuilder, GeneratePositionStatusReports,
            GeneratePositionStatusReportsBuilder, ModifyOrder, QueryAccount, QueryOrder,
            SubmitOrder, SubmitOrderList,
        },
        system::ShutdownSystem,
    },
    msgbus::{self, MessagingSwitchboard},
};
use nautilus_core::{
    MUTEX_POISONED, UUID4, UnixNanos,
    datetime::mins_to_nanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{
        LiquiditySide, OmsType, OrderListType, OrderSide, OrderStatus, OrderType, TimeInForce,
    },
    events::{
        AccountState, OrderAccepted, OrderCancelRejected, OrderCanceled, OrderEventAny,
        OrderExpired, OrderFilled, OrderModifyRejected, OrderRejected, OrderUpdated,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TradeId, Venue, VenueOrderId,
    },
    instruments::Instrument,
    orders::Order,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};

/// Returns the exact technical operations supported by the Binance Spot execution adapter.
#[must_use]
pub fn binance_spot_execution_capabilities() -> ExecutionClientCapabilities {
    ExecutionClientCapabilities {
        order_types: vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::StopMarket,
            OrderType::StopLimit,
            OrderType::MarketIfTouched,
            OrderType::LimitIfTouched,
        ],
        order_list_types: vec![OrderListType::Oco, OrderListType::Opoco],
        time_in_force: vec![TimeInForce::Gtc, TimeInForce::Ioc, TimeInForce::Fok],
        submit_order: true,
        submit_order_list: true,
        modify_order: true,
        cancel_order: true,
        batch_cancel_orders: true,
        cancel_all_orders: true,
        ..ExecutionClientCapabilities::default()
    }
}
use rust_decimal::Decimal;
use tokio::task::JoinHandle;
use ustr::Ustr;

use super::{
    http::models::{
        BinanceSpotCancelAllItem, BinanceSpotCancelAllResult, BinanceSpotOrderListCancelResult,
    },
    websocket::trading::{
        client::BinanceSpotWsTradingClient,
        messages::BinanceSpotWsTradingMessage,
        parse::{
            parse_spot_account_position, parse_spot_exec_report_to_fill,
            parse_spot_exec_report_to_order_status,
        },
        user_data::{
            BinanceSpotExecutionReport, BinanceSpotExecutionType, BinanceSpotListStatusMsg,
        },
    },
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
            emit_order_accepted_once, ensure_accepted_emitted,
        },
        encoder::{decode_client_order_id, encode_binance_client_order_id},
        enums::{BinanceSide, BinanceTimeInForce},
        parse::{
            parse_required_decimal, parse_required_price_at_precision,
            parse_required_quantity_at_precision,
        },
        time::{unix_nanos_from_micros, unix_nanos_from_millis},
    },
    config::BinanceExecClientConfig,
    spot::{
        enums::{
            BinanceCancelReplaceMode, BinanceOrderResponseType, BinanceSpotOrderType,
            order_type_to_binance_spot, time_in_force_to_binance_spot,
        },
        http::{
            BinanceSpotHttpResult,
            client::BinanceSpotHttpClient,
            error::BinanceSpotHttpError,
            models::{BatchCancelResult, BinanceOrderListResponse},
            query::{
                BatchCancelItem, CancelOrderListParams, CancelOrderParams,
                CancelReplaceOrderParams, NewOcoOrderListParams, NewOpocoOrderListParams,
            },
        },
        sbe::spot::list_order_status::ListOrderStatus,
    },
};

const ORDER_LIST_CANCEL_PARAM: &str = "order_list_cancel";
const ORDER_LIST_CLIENT_ORDER_ID_PARAM: &str = "list_client_order_id";
const ORDER_LIST_ID_PARAM: &str = "order_list_id";
const BINANCE_SPOT_SUBMIT_ACK_TIMEOUT_MS: u64 = 5_000;

/// Live execution client for Binance Spot trading.
///
/// Implements the [`ExecutionClient`] trait for order management on Binance Spot
/// and Spot Margin markets. New order submit uses the HTTP FULL response as the
/// venue acknowledgement source. The WebSocket User Data Stream provides real-time
/// execution events.
#[derive(Debug)]
pub struct BinanceSpotExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: BinanceExecClientConfig,
    emitter: ExecutionEventEmitter,
    dispatch_state: Arc<WsDispatchState>,
    seen_trade_ids: Arc<Mutex<FifoCache<(Ustr, i64), 10_000>>>,
    http_client: BinanceSpotHttpClient,
    ws_trading_client: Option<BinanceSpotWsTradingClient>,
    ws_trading_handle: Mutex<Option<JoinHandle<()>>>,
    ws_authenticated: Arc<tokio::sync::Notify>,
    ws_user_data_subscribed: Arc<tokio::sync::Notify>,
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
            seen_trade_ids: Arc::new(Mutex::new(FifoCache::new())),
            http_client,
            ws_trading_client,
            ws_trading_handle: Mutex::new(None),
            ws_authenticated: Arc::new(tokio::sync::Notify::new()),
            ws_user_data_subscribed: Arc::new(tokio::sync::Notify::new()),
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
        let dispatch_running = self
            .ws_trading_handle
            .lock()
            .expect(MUTEX_POISONED)
            .as_ref()
            .is_some_and(|handle| !handle.is_finished());

        self.ws_trading_client
            .as_ref()
            .is_some_and(|client| client.is_active())
            && dispatch_running
    }

    fn submit_order_internal(&self, cmd: &SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;

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

        let http_client = self.http_client.clone();
        let dispatch_state = self.dispatch_state.clone();
        let seen_trade_ids = self.seen_trade_ids.clone();

        self.spawn_task("submit_order_http", async move {
            let result = tokio::time::timeout(
                Duration::from_millis(BINANCE_SPOT_SUBMIT_ACK_TIMEOUT_MS),
                http_client.submit_order_with_fills(
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
                ),
            )
            .await;

            let response = match result {
                Ok(Ok(response)) => response,
                Ok(Err(e)) => {
                    let reason = format!("submit-order-error: {e}");
                    if is_definite_submit_failure(&e) {
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
                            reason.into(),
                            UUID4::new(),
                            ts_init,
                            clock.get_time_ns(),
                            false,
                            due_post_only,
                        );
                        event_emitter.send_order_event(OrderEventAny::Rejected(rejected));
                        return Err(e);
                    }

                    if reconcile_ambiguous_submit(
                        &http_client,
                        &event_emitter,
                        &dispatch_state,
                        &seen_trade_ids,
                        account_id,
                        instrument_id,
                        client_order_id,
                        strategy_id,
                        ts_init,
                        &reason,
                    )
                    .await
                    {
                        return Ok(());
                    }
                    return Err(e);
                }
                Err(_) => {
                    let reason = format!(
                        "submit-order-ack-timeout: no venue ack within {BINANCE_SPOT_SUBMIT_ACK_TIMEOUT_MS}ms"
                    );
                    if reconcile_ambiguous_submit(
                        &http_client,
                        &event_emitter,
                        &dispatch_state,
                        &seen_trade_ids,
                        account_id,
                        instrument_id,
                        client_order_id,
                        strategy_id,
                        ts_init,
                        &reason,
                    )
                    .await
                    {
                        return Ok(());
                    }
                    anyhow::bail!("{reason}");
                }
            };

            let report = response.status;
            if dispatch_state.insert_accepted(client_order_id, report.venue_order_id) {
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
            let fills = retain_unseen_fill_reports(&report, response.fills, &seen_trade_ids);
            event_emitter.send_order_with_fills(report, fills);
            Ok(())
        });

        Ok(())
    }

    fn cancel_order_internal(&self, cmd: &CancelOrder) -> anyhow::Result<()> {
        let event_emitter = self.emitter.clone();
        let trader_id = self.core.trader_id;
        let account_id = self.core.account_id;
        let clock = self.clock;
        let command = cmd.clone();

        if let Some(order_list_cancel_params) = build_cancel_order_list_params(&command)? {
            let http_client = self.http_client.clone();
            let dispatch_state = self.dispatch_state.clone();
            self.spawn_task("cancel_order_list_http", async move {
                match http_client
                    .cancel_order_list(&order_list_cancel_params)
                    .await
                {
                    Ok(response) => {
                        dispatch_http_order_list_cancel_result(
                            &response,
                            &event_emitter,
                            account_id,
                            &dispatch_state,
                            clock,
                        );
                    }
                    Err(e) => {
                        let error = anyhow::anyhow!(e);
                        if is_structured_venue_rejection(&error)
                            || is_local_command_failure(&error)
                        {
                            let ts_now = clock.get_time_ns();
                            let rejected_event = OrderCancelRejected::new(
                                trader_id,
                                command.strategy_id,
                                command.instrument_id,
                                command.client_order_id,
                                format!("cancel-order-list-error: {error}").into(),
                                UUID4::new(),
                                ts_now,
                                ts_now,
                                false,
                                command.venue_order_id,
                                Some(account_id),
                            );
                            event_emitter
                                .send_order_event(OrderEventAny::CancelRejected(rejected_event));
                        } else {
                            log::error!(
                                "Ambiguous order-list cancel failure for {}, awaiting reconciliation: {error}",
                                command.client_order_id
                            );
                        }
                        return Err(error);
                    }
                }
                Ok(())
            });
            return Ok(());
        }

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
                    log::warn!(
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
                            log::warn!(
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

    async fn enter_http_only_execution_mode(
        &mut self,
        mut ws_trading: BinanceSpotWsTradingClient,
        reason: &str,
    ) {
        log::error!(
            "{reason}; entering Spot HTTP-only execution mode. Order commands use HTTP responses; execution reconciliation requires explicit queries until WS trading is re-enabled"
        );

        if let Some(handle) = self.ws_trading_handle.lock().expect(MUTEX_POISONED).take() {
            handle.abort();
        }
        ws_trading.disconnect().await;
        self.ws_trading_client = Some(ws_trading);
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

    fn capabilities(&self) -> ExecutionClientCapabilities {
        binance_spot_execution_capabilities()
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
                log::debug!("Loaded {} Spot instruments", instruments.len());
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
            log::debug!(
                "Received account state with {} balance(s)",
                account_state.balances.len()
            );
        }

        self.emitter.send_account_state(account_state);

        // Wait for account to be registered in cache before completing connect
        crate::common::execution::await_account_registered(&self.core, self.core.account_id, 30.0)
            .await?;

        // Connect WS trading client (primary order transport)
        if let Some(mut ws_trading) = self.ws_trading_client.take() {
            match ws_trading.connect().await {
                Ok(()) => {
                    log::debug!("Connected to Binance Spot WS trading API");

                    let ws_trading_clone = ws_trading.clone();
                    let emitter = self.emitter.clone();
                    let account_id = self.core.account_id;
                    let clock = self.clock;
                    let http_client = self.http_client.clone();
                    let dispatch_state = self.dispatch_state.clone();
                    let treat_expired_as_canceled = self.config.treat_expired_as_canceled;
                    let ws_authenticated = self.ws_authenticated.clone();
                    let ws_user_data_subscribed = self.ws_user_data_subscribed.clone();
                    let (ws_setup_error_tx, mut ws_setup_error_rx) =
                        tokio::sync::mpsc::unbounded_channel();
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
                                        treat_expired_as_canceled,
                                        clock,
                                        &dispatch_state,
                                        &ws_authenticated,
                                        &ws_user_data_subscribed,
                                        &ws_setup_error_tx,
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

                    if let Err(e) = ws_trading.session_logon().await {
                        let reason = format!("WS session logon failed: {e}");
                        self.enter_http_only_execution_mode(ws_trading, &reason)
                            .await;
                    } else {
                        let auth_result = wait_for_ws_setup_response(
                            Duration::from_secs(10),
                            self.ws_authenticated.notified(),
                            &mut ws_setup_error_rx,
                            "WS session authentication timed out",
                        )
                        .await;

                        if let Err(e) = auth_result {
                            self.enter_http_only_execution_mode(ws_trading, &e.to_string())
                                .await;
                        } else if let Err(e) = ws_trading.subscribe_user_data().await {
                            let reason = format!("WS user data subscribe failed: {e}");
                            self.enter_http_only_execution_mode(ws_trading, &reason)
                                .await;
                        } else {
                            let subscribe_result = wait_for_ws_setup_response(
                                Duration::from_secs(10),
                                self.ws_user_data_subscribed.notified(),
                                &mut ws_setup_error_rx,
                                "WS user data subscription timed out",
                            )
                            .await;

                            if let Err(e) = subscribe_result {
                                self.enter_http_only_execution_mode(ws_trading, &e.to_string())
                                    .await;
                            } else {
                                self.ws_trading_client = Some(ws_trading);
                            }
                        }
                    }
                }
                Err(e) => {
                    let reason = format!("Failed to connect WS trading API: {e}");
                    self.enter_http_only_execution_mode(ws_trading, &reason)
                        .await;
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
        let treat_expired_as_canceled = self.config.treat_expired_as_canceled;

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
                Ok(Some(mut report)) => {
                    normalize_spot_order_status_report(&mut report, treat_expired_as_canceled);
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
                        log::debug!("Instruments initialized");
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

        let report = self
            .http_client
            .request_order_status_report(
                self.core.account_id,
                instrument_id,
                venue_order_id,
                cmd.client_order_id,
            )
            .await?;

        Ok(report.map(|mut report| {
            normalize_spot_order_status_report(&mut report, self.config.treat_expired_as_canceled);
            report
        }))
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let start_dt = cmd.start.map(|nanos| nanos.to_datetime_utc());
        let end_dt = cmd.end.map(|nanos| nanos.to_datetime_utc());

        let mut reports = self
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

        normalize_spot_order_status_reports(&mut reports, self.config.treat_expired_as_canceled);

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
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;

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
        log::info!(
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

        let params = match build_spot_order_list_params(
            cmd.order_list.order_list_type,
            cmd.params.as_ref(),
            &orders,
        ) {
            Ok(params) => params,
            Err(err) => {
                let reason = err.to_string();
                log::warn!("Binance Spot submit_order_list validation_failed: {reason}");
                emit_order_list_denied(&self.emitter, &orders, &reason);
                return Ok(());
            }
        };

        for order in &orders {
            log::info!(
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

        let task_name = params.task_name();
        self.spawn_task(task_name, async move {
            match params.submit(&http_client).await {
                Ok(response) => {
                    log::info!(
                        "Binance Spot submit_order_list http_result=ok order_list_id={}",
                        response.order_list_id
                    );
                    for acceptance in oco_child_acceptances(&response) {
                        let Some(order) = orders_by_client_id.get(&acceptance.client_order_id)
                        else {
                            log::warn!(
                                "No cached order-list child order for accepted client_order_id={}",
                                acceptance.client_order_id
                            );
                            continue;
                        };

                        let ts_event = acceptance
                            .transact_time
                            .and_then(|millis| {
                                checked_unix_nanos_from_millis(
                                    millis,
                                    "acceptance.transact_time",
                                    "spot_http_order_list_acceptance",
                                )
                            })
                            .unwrap_or_else(|| clock.get_time_ns());
                        if dispatch_state
                            .insert_accepted(acceptance.client_order_id, acceptance.venue_order_id)
                        {
                            event_emitter.emit_order_accepted(
                                order,
                                acceptance.venue_order_id,
                                ts_event,
                            );
                        }
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
                    log::warn!(
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
                            log::warn!(
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
            self.dispatch_state.mark_cancel_all_started(&symbol);

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
                            log::warn!(
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
        let client_order_id = match decode_client_order_id(
            &report.client_order_id,
            BINANCE_NAUTILUS_SPOT_BROKER_ID,
            "order_report.client_order_id",
            "spot_http_order_list_acceptance",
        ) {
            Ok(client_order_id) => client_order_id,
            Err(e) => {
                log::error!(
                    "Skipping malformed Spot OCO order report client_order_id for order_id={}: {e}",
                    report.order_id
                );
                continue;
            }
        };
        seen.insert(client_order_id);
        acceptances.push(OcoChildAcceptance {
            client_order_id,
            venue_order_id: VenueOrderId::new(report.order_id.to_string()),
            transact_time: report.transact_time.or(response.transaction_time),
        });
    }

    for order_ref in &response.orders {
        let client_order_id = match decode_client_order_id(
            &order_ref.client_order_id,
            BINANCE_NAUTILUS_SPOT_BROKER_ID,
            "order.client_order_id",
            "spot_http_order_list_acceptance",
        ) {
            Ok(client_order_id) => client_order_id,
            Err(e) => {
                log::error!(
                    "Skipping malformed Spot OCO order reference client_order_id for order_id={}: {e}",
                    order_ref.order_id
                );
                continue;
            }
        };
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

fn normalize_spot_order_status_report(
    report: &mut OrderStatusReport,
    treat_expired_as_canceled: bool,
) {
    if treat_expired_as_canceled && report.order_status == OrderStatus::Expired {
        report.order_status = OrderStatus::Canceled;
    }
}

fn normalize_spot_order_status_reports(
    reports: &mut [OrderStatusReport],
    treat_expired_as_canceled: bool,
) {
    for report in reports {
        normalize_spot_order_status_report(report, treat_expired_as_canceled);
    }
}

async fn wait_for_ws_setup_response(
    timeout: Duration,
    success: impl Future<Output = ()>,
    setup_errors: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    timeout_message: &'static str,
) -> anyhow::Result<()> {
    tokio::pin!(success);

    let result = tokio::time::timeout(timeout, async {
        tokio::select! {
            () = &mut success => Ok(()),
            err = setup_errors.recv() => {
                anyhow::bail!(
                    "{}",
                    err.unwrap_or_else(|| "WS setup error channel closed".to_string()),
                )
            }
        }
    })
    .await;

    result.map_err(|_| anyhow::anyhow!(timeout_message))?
}

#[expect(clippy::too_many_arguments)]
fn dispatch_ws_trading_message(
    msg: BinanceSpotWsTradingMessage,
    emitter: &ExecutionEventEmitter,
    http_client: &BinanceSpotHttpClient,
    account_id: AccountId,
    treat_expired_as_canceled: bool,
    clock: &'static AtomicTime,
    dispatch_state: &WsDispatchState,
    ws_authenticated: &tokio::sync::Notify,
    ws_user_data_subscribed: &tokio::sync::Notify,
    ws_setup_error_tx: &tokio::sync::mpsc::UnboundedSender<String>,
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
                    log::warn!(
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
            symbol,
            result,
        } => {
            dispatch_state.pending_requests.remove(&request_id);
            let terminal_symbol = cancel_all_terminal_symbol(symbol.as_deref(), &result);
            let mut order_count = 0usize;
            let mut order_list_count = 0usize;
            for item in &result.items {
                match item {
                    BinanceSpotCancelAllItem::Order(_) => {
                        order_count += 1;
                    }
                    BinanceSpotCancelAllItem::OrderList(response) => {
                        order_list_count += 1;
                        dispatch_order_list_cancel_result(
                            &request_id,
                            response,
                            emitter,
                            account_id,
                            dispatch_state,
                            clock,
                        );
                    }
                }
            }
            log::debug!(
                "WS all orders canceled: request_id={request_id}, order_count={order_count}, order_list_count={order_list_count}"
            );
            if let Some(symbol) = terminal_symbol {
                dispatch_state.complete_cancel_all(&symbol);
            } else {
                log::warn!(
                    "WS all orders canceled without symbol; deferred live exit close orders cannot be released deterministically"
                );
            }
        }
        BinanceSpotWsTradingMessage::UserDataSubscribed { subscription_id } => {
            log::debug!("User data stream subscribed: id={subscription_id}");
            ws_user_data_subscribed.notify_one();
        }
        BinanceSpotWsTradingMessage::ExecutionReport(report) => {
            let ts_init = clock.get_time_ns();
            dispatch_execution_report(
                &report,
                emitter,
                http_client,
                account_id,
                treat_expired_as_canceled,
                dispatch_state,
                seen_trade_ids,
                ts_init,
            );
        }
        BinanceSpotWsTradingMessage::AccountPosition(position) => {
            let ts_init = clock.get_time_ns();
            match parse_spot_account_position(&position, account_id, ts_init) {
                Ok(state) => emitter.send_account_state(state),
                Err(e) => log::error!("Failed to parse account position: {e}"),
            }
        }
        BinanceSpotWsTradingMessage::BalanceUpdate(update) => {
            log::debug!(
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
            log::debug!("WS trading API connected");
        }
        BinanceSpotWsTradingMessage::Authenticated => {
            log::debug!("WS trading API authenticated");
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
            let _ = ws_setup_error_tx.send(err);
        }
        BinanceSpotWsTradingMessage::ProtocolAnomaly {
            template_id,
            reason,
        } => {
            log::error!(
                "binance_ws_sbe_protocol_anomaly template_id={}: {reason}",
                template_id.map_or_else(|| "<unknown>".to_string(), |value| value.to_string())
            );
        }
        BinanceSpotWsTradingMessage::FatalError { reason } => {
            log::error!("WS trading API fatal error: {reason}");
            publish_adapter_fatal_shutdown(dispatch_state, emitter, clock, reason);
        }
    }
}

fn dispatch_order_list_cancel_result(
    request_id: &str,
    response: &BinanceSpotOrderListCancelResult,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    dispatch_state: &WsDispatchState,
    clock: &'static AtomicTime,
) {
    log::debug!(
        "WS order-list cancel result: request_id={request_id}, template_id={}, order_list_id={}, list_client_order_id={}, symbol={}, child_count={}, report_count={}",
        response.template_id,
        response.order_list_id,
        response.list_client_order_id,
        response.symbol,
        response.orders.len(),
        response.order_reports.len(),
    );

    let response_ts_event = checked_unix_nanos_from_micros(
        response.transaction_time,
        "transaction_time",
        "ws_order_list_cancel_response",
    )
    .unwrap_or_else(|| clock.get_time_ns());
    let ts_init = clock.get_time_ns();

    for report in &response.order_reports {
        let client_order_id = match decode_client_order_id(
            &report.orig_client_order_id,
            BINANCE_NAUTILUS_SPOT_BROKER_ID,
            "order_report.orig_client_order_id",
            "ws_order_list_cancel_report",
        ) {
            Ok(client_order_id) => client_order_id,
            Err(e) => {
                log::error!(
                    "Skipping malformed WS order-list cancel report client_order_id for order_id={}: {e}",
                    report.order_id
                );
                continue;
            }
        };

        let Some(identity) = dispatch_state
            .order_identities
            .get(&client_order_id)
            .map(|entry| entry.clone())
        else {
            log::debug!(
                "Ignoring untracked order-list cancel report client_order_id={client_order_id}, order_id={}",
                report.order_id
            );
            continue;
        };

        let venue_order_id = VenueOrderId::new(report.order_id.to_string());
        let ts_event = checked_unix_nanos_from_micros(
            report.transact_time,
            "order_report.transact_time",
            "ws_order_list_cancel_report",
        )
        .unwrap_or(response_ts_event);
        ensure_accepted_emitted(
            client_order_id,
            account_id,
            venue_order_id,
            &identity,
            emitter,
            dispatch_state,
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
        dispatch_state.cleanup_terminal(client_order_id);
        emitter.send_order_event(OrderEventAny::Canceled(canceled));
    }
}

fn dispatch_http_order_list_cancel_result(
    response: &BinanceOrderListResponse,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    dispatch_state: &WsDispatchState,
    clock: &'static AtomicTime,
) {
    log::debug!(
        "HTTP order-list cancel result: order_list_id={}, list_client_order_id={}, symbol={}, child_count={}, report_count={}",
        response.order_list_id,
        response.list_client_order_id,
        response.symbol,
        response.orders.len(),
        response.order_reports.len(),
    );

    let ts_init = clock.get_time_ns();
    let canceled_children = if response.order_reports.is_empty() {
        response
            .orders
            .iter()
            .map(|order| {
                (
                    order.client_order_id.as_str(),
                    order.order_id,
                    response.transaction_time,
                )
            })
            .collect::<Vec<_>>()
    } else {
        response
            .order_reports
            .iter()
            .map(|report| {
                (
                    report.client_order_id.as_str(),
                    report.order_id,
                    report.transact_time.or(response.transaction_time),
                )
            })
            .collect::<Vec<_>>()
    };

    for (venue_client_order_id, venue_order_id, transact_time) in canceled_children {
        let client_order_id = match decode_client_order_id(
            venue_client_order_id,
            BINANCE_NAUTILUS_SPOT_BROKER_ID,
            "order_report.client_order_id",
            "http_order_list_cancel_report",
        ) {
            Ok(client_order_id) => client_order_id,
            Err(e) => {
                log::error!(
                    "Skipping malformed HTTP order-list cancel report client_order_id for order_id={venue_order_id}: {e}"
                );
                continue;
            }
        };

        let Some(identity) = dispatch_state
            .order_identities
            .get(&client_order_id)
            .map(|entry| entry.clone())
        else {
            log::debug!(
                "Ignoring untracked HTTP order-list cancel report client_order_id={client_order_id}, order_id={venue_order_id}"
            );
            continue;
        };

        let venue_order_id = VenueOrderId::new(venue_order_id.to_string());
        let ts_event = transact_time
            .and_then(|millis| {
                checked_unix_nanos_from_millis(
                    millis,
                    "order_report.transact_time",
                    "http_order_list_cancel_report",
                )
            })
            .unwrap_or_else(|| clock.get_time_ns());

        ensure_accepted_emitted(
            client_order_id,
            account_id,
            venue_order_id,
            &identity,
            emitter,
            dispatch_state,
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
        dispatch_state.cleanup_terminal(client_order_id);
        emitter.send_order_event(OrderEventAny::Canceled(canceled));
    }
}

fn checked_unix_nanos_from_micros(value: i64, field: &str, context: &str) -> Option<UnixNanos> {
    unix_nanos_from_micros(value, field, context)
        .inspect_err(|e| log::warn!("binance_timestamp_fallback {e}"))
        .ok()
}

fn checked_unix_nanos_from_millis(value: i64, field: &str, context: &str) -> Option<UnixNanos> {
    unix_nanos_from_millis(value, field, context)
        .inspect_err(|e| log::warn!("binance_timestamp_fallback {e}"))
        .ok()
}

#[expect(clippy::too_many_arguments)]
async fn reconcile_ambiguous_submit(
    http_client: &BinanceSpotHttpClient,
    event_emitter: &ExecutionEventEmitter,
    dispatch_state: &WsDispatchState,
    seen_trade_ids: &Arc<Mutex<FifoCache<(Ustr, i64), 10_000>>>,
    account_id: AccountId,
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    strategy_id: StrategyId,
    ts_init: UnixNanos,
    reason: &str,
) -> bool {
    log::error!(
        "Ambiguous submit failure for {client_order_id}, attempting order status reconciliation: {reason}"
    );

    let report = match http_client
        .request_order_status_report(account_id, instrument_id, None, Some(client_order_id))
        .await
    {
        Ok(Some(report)) => report,
        Ok(None) => {
            log::warn!(
                "Ambiguous submit reconciliation found no venue order for {client_order_id}; leaving order pending for recovery"
            );
            return false;
        }
        Err(err) => {
            log::error!(
                "Ambiguous submit reconciliation failed for {client_order_id}; leaving order pending for recovery: {err}"
            );
            return false;
        }
    };

    let venue_order_id = report.venue_order_id;
    if dispatch_state.insert_accepted(client_order_id, venue_order_id) {
        let accepted = OrderAccepted::new(
            event_emitter.trader_id(),
            strategy_id,
            instrument_id,
            client_order_id,
            venue_order_id,
            account_id,
            UUID4::new(),
            report.ts_accepted,
            ts_init,
            false,
        );
        event_emitter.send_order_event(OrderEventAny::Accepted(accepted));
    }

    let fills = match http_client
        .request_fill_reports(
            account_id,
            instrument_id,
            Some(venue_order_id),
            None,
            None,
            None,
        )
        .await
    {
        Ok(fills) => retain_unseen_fill_reports(&report, fills, seen_trade_ids),
        Err(err) => {
            log::warn!(
                "Ambiguous submit fill reconciliation failed for {client_order_id}; emitting status without fills: {err}"
            );
            Vec::new()
        }
    };
    event_emitter.send_order_with_fills(report, fills);
    true
}

fn cancel_all_terminal_symbol(
    symbol: Option<&str>,
    result: &BinanceSpotCancelAllResult,
) -> Option<String> {
    if let Some(symbol) = symbol.filter(|value| !value.is_empty()) {
        return Some(symbol.to_string());
    }

    let mut symbols = result.items.iter().filter_map(|item| match item {
        BinanceSpotCancelAllItem::Order(response) => {
            (!response.symbol.is_empty()).then_some(response.symbol.as_str())
        }
        BinanceSpotCancelAllItem::OrderList(response) => {
            (!response.symbol.is_empty()).then_some(response.symbol.as_str())
        }
    });
    let first = symbols.next()?;
    symbols
        .all(|symbol| symbol == first)
        .then(|| first.to_string())
}

fn publish_adapter_fatal_shutdown(
    dispatch_state: &WsDispatchState,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    reason: String,
) {
    if !dispatch_state.set_adapter_fatal_reason(reason.clone()) {
        return;
    }

    let command = ShutdownSystem::new(
        emitter.trader_id(),
        Ustr::from("binance_spot_execution_client"),
        Some(reason),
        UUID4::new(),
        clock.get_time_ns(),
        None,
    );
    msgbus::publish_any(
        MessagingSwitchboard::shutdown_system_topic(),
        command.as_any(),
    );
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

    let ts_init = clock.get_time_ns();
    let ts_event = checked_unix_nanos_from_millis(
        list_status.event_time,
        "list_status.event_time",
        "spot_json_list_status",
    )
    .unwrap_or(ts_init);
    let is_rejected = list_status.list_order_status == ListOrderStatus::Reject
        || !list_status.reject_reason.is_empty();
    let reject_reason = if list_status.reject_reason.is_empty() {
        "Order list rejected by venue"
    } else {
        list_status.reject_reason.as_str()
    };

    for child in &list_status.orders {
        let client_order_id = match decode_client_order_id(
            &child.client_order_id,
            BINANCE_NAUTILUS_SPOT_BROKER_ID,
            "list_status.child.client_order_id",
            "spot_json_list_status",
        ) {
            Ok(client_order_id) => client_order_id,
            Err(e) => {
                log::error!(
                    "Skipping malformed Spot list status child client_order_id for order_id={}: {e}",
                    child.order_id
                );
                continue;
            }
        };

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

        let venue_order_id = VenueOrderId::new(child.order_id.to_string());
        emit_order_accepted_once(
            client_order_id,
            account_id,
            venue_order_id,
            &identity,
            emitter,
            dispatch_state,
            ts_event,
            ts_init,
        );
    }
}

fn emit_order_list_denied(
    emitter: &ExecutionEventEmitter,
    orders: &[nautilus_model::orders::OrderAny],
    reason: &str,
) {
    for order in orders {
        log::info!(
            "Binance Spot submit_order_list child_denied_emitted client_order_id={}",
            order.client_order_id()
        );
        emitter.emit_order_denied(order, reason);
    }
}

#[derive(Debug)]
enum BinanceSpotOrderListParams {
    Oco(NewOcoOrderListParams),
    Opoco(NewOpocoOrderListParams),
}

impl BinanceSpotOrderListParams {
    const fn task_name(&self) -> &'static str {
        match self {
            Self::Oco(_) => "submit_oco_order_list_http",
            Self::Opoco(_) => "submit_opoco_order_list_http",
        }
    }

    async fn submit(
        self,
        http_client: &BinanceSpotHttpClient,
    ) -> BinanceSpotHttpResult<BinanceOrderListResponse> {
        match self {
            Self::Oco(params) => http_client.submit_oco_order_list(&params).await,
            Self::Opoco(params) => http_client.submit_opoco_order_list(&params).await,
        }
    }
}

fn build_spot_order_list_params(
    order_list_type: OrderListType,
    command_params: Option<&nautilus_core::params::Params>,
    orders: &[nautilus_model::orders::OrderAny],
) -> anyhow::Result<BinanceSpotOrderListParams> {
    match order_list_type {
        OrderListType::Oco => {
            build_oco_order_list_params(command_params, orders).map(BinanceSpotOrderListParams::Oco)
        }
        OrderListType::Opoco => build_opoco_order_list_params(command_params, orders)
            .map(BinanceSpotOrderListParams::Opoco),
        OrderListType::Standard => {
            anyhow::bail!("Binance Spot submit_order_list requires OCO or OPOCO order_list_type")
        }
    }
}

fn build_oco_order_list_params(
    command_params: Option<&nautilus_core::params::Params>,
    orders: &[nautilus_model::orders::OrderAny],
) -> anyhow::Result<NewOcoOrderListParams> {
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

    let mut params = NewOcoOrderListParams {
        symbol: first.instrument_id().symbol.to_string(),
        side,
        quantity,
        list_client_order_id: None,
        above_type: BinanceSpotOrderType::LimitMaker,
        above_client_order_id: None,
        above_iceberg_qty: None,
        above_price: None,
        above_stop_price: None,
        above_time_in_force: None,
        below_type: BinanceSpotOrderType::StopLoss,
        below_client_order_id: None,
        below_iceberg_qty: None,
        below_price: None,
        below_stop_price: None,
        below_time_in_force: None,
        new_order_resp_type: Some(BinanceOrderResponseType::Full),
        self_trade_prevention_mode: None,
    };
    if let Some(list_client_order_id) = order_list_client_order_id(command_params) {
        params.list_client_order_id = Some(encode_binance_client_order_id(
            &ClientOrderId::new(list_client_order_id),
            BINANCE_NAUTILUS_SPOT_BROKER_ID,
        )?);
    }

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

fn build_opoco_order_list_params(
    command_params: Option<&nautilus_core::params::Params>,
    orders: &[nautilus_model::orders::OrderAny],
) -> anyhow::Result<NewOpocoOrderListParams> {
    if orders.len() != 3 {
        anyhow::bail!(
            "Binance Spot OPOCO order list requires exactly 3 child orders, received {}",
            orders.len()
        );
    }

    let first = &orders[0];
    if orders
        .iter()
        .any(|order| order.instrument_id() != first.instrument_id())
    {
        anyhow::bail!("Binance Spot OPOCO child orders must use the same instrument");
    }
    if orders
        .iter()
        .any(|order| order.quantity() != first.quantity())
    {
        anyhow::bail!("Binance Spot OPOCO child orders must use the same quantity");
    }
    if orders.iter().any(|order| order.is_quote_quantity()) {
        anyhow::bail!("Binance Spot OPOCO does not support quoteOrderQty child orders");
    }

    let buy_count = orders
        .iter()
        .filter(|order| order.order_side() == OrderSide::Buy)
        .count();
    let sell_count = orders
        .iter()
        .filter(|order| order.order_side() == OrderSide::Sell)
        .count();
    if buy_count + sell_count != orders.len() {
        anyhow::bail!("Binance Spot OPOCO child orders must use BUY or SELL sides");
    }
    let (working_side, pending_side) = match (buy_count, sell_count) {
        (1, 2) => (OrderSide::Buy, OrderSide::Sell),
        (2, 1) => (OrderSide::Sell, OrderSide::Buy),
        _ => anyhow::bail!(
            "Binance Spot OPOCO requires exactly one working-side child and two pending-side children"
        ),
    };
    let working = orders
        .iter()
        .find(|order| order.order_side() == working_side)
        .ok_or_else(|| anyhow::anyhow!("Binance Spot OPOCO requires one working order"))?;
    if working.order_type() != OrderType::Limit {
        anyhow::bail!("Binance Spot OPOCO requires one LIMIT or LIMIT_MAKER working order");
    }
    let pending: Vec<_> = orders
        .iter()
        .filter(|order| order.client_order_id() != working.client_order_id())
        .collect();
    if pending.len() != 2
        || pending
            .iter()
            .any(|order| order.order_side() != pending_side)
    {
        anyhow::bail!("Binance Spot OPOCO pending child orders must use the opposite working side");
    }
    let target = pending
        .iter()
        .copied()
        .find(|order| order.order_type() == OrderType::Limit && order.is_post_only())
        .ok_or_else(|| anyhow::anyhow!("Binance Spot OPOCO requires one post-only LIMIT target"))?;
    let stop = pending
        .iter()
        .copied()
        .find(|order| {
            matches!(
                order.order_type(),
                OrderType::StopMarket | OrderType::StopLimit
            )
        })
        .ok_or_else(|| {
            anyhow::anyhow!("Binance Spot OPOCO requires one STOP_LOSS or STOP_LOSS_LIMIT child")
        })?;
    if working.client_order_id() == target.client_order_id()
        || working.client_order_id() == stop.client_order_id()
        || target.client_order_id() == stop.client_order_id()
    {
        anyhow::bail!("Binance Spot OPOCO child orders must be distinct");
    }

    let working_price = working
        .price()
        .ok_or_else(|| anyhow::anyhow!("Binance Spot OPOCO working order requires price"))?
        .to_string();
    let target_price = target
        .price()
        .ok_or_else(|| anyhow::anyhow!("Binance Spot OPOCO target LIMIT_MAKER requires price"))?
        .to_string();
    let stop_trigger_price = stop
        .trigger_price()
        .ok_or_else(|| anyhow::anyhow!("Binance Spot OPOCO stop child requires trigger price"))?
        .to_string();
    let stop_limit_price = match stop.order_type() {
        OrderType::StopMarket => None,
        OrderType::StopLimit => Some(
            stop.price()
                .ok_or_else(|| {
                    anyhow::anyhow!("Binance Spot OPOCO STOP_LOSS_LIMIT child requires price")
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

    let working_type = if working.is_post_only() {
        BinanceSpotOrderType::LimitMaker
    } else {
        BinanceSpotOrderType::Limit
    };
    let working_time_in_force = if working_type == BinanceSpotOrderType::Limit {
        Some(time_in_force_to_binance_spot(working.time_in_force())?)
    } else {
        None
    };
    let stop_type = match stop.order_type() {
        OrderType::StopMarket => BinanceSpotOrderType::StopLoss,
        OrderType::StopLimit => BinanceSpotOrderType::StopLossLimit,
        _ => unreachable!("stop order type filtered above"),
    };
    let working_client_id = encode_binance_client_order_id(
        &working.client_order_id(),
        BINANCE_NAUTILUS_SPOT_BROKER_ID,
    )?;
    let target_client_id =
        encode_binance_client_order_id(&target.client_order_id(), BINANCE_NAUTILUS_SPOT_BROKER_ID)?;
    let stop_client_id =
        encode_binance_client_order_id(&stop.client_order_id(), BINANCE_NAUTILUS_SPOT_BROKER_ID)?;

    let mut params = NewOpocoOrderListParams {
        symbol: first.instrument_id().symbol.to_string(),
        list_client_order_id: None,
        working_type,
        working_side: BinanceSide::try_from(working.order_side())?,
        working_client_order_id: Some(working_client_id),
        working_price,
        working_quantity: working.quantity().to_string(),
        working_time_in_force,
        pending_side: BinanceSide::try_from(pending_side)?,
        pending_above_type: BinanceSpotOrderType::LimitMaker,
        pending_above_client_order_id: None,
        pending_above_price: None,
        pending_above_stop_price: None,
        pending_above_time_in_force: None,
        pending_below_type: None,
        pending_below_client_order_id: None,
        pending_below_price: None,
        pending_below_stop_price: None,
        pending_below_time_in_force: None,
        new_order_resp_type: Some(BinanceOrderResponseType::Full),
        self_trade_prevention_mode: None,
    };
    if let Some(list_client_order_id) = order_list_client_order_id(command_params) {
        params.list_client_order_id = Some(encode_binance_client_order_id(
            &ClientOrderId::new(list_client_order_id),
            BINANCE_NAUTILUS_SPOT_BROKER_ID,
        )?);
    }

    match pending_side {
        OrderSide::Sell => {
            params.pending_above_type = BinanceSpotOrderType::LimitMaker;
            params.pending_above_client_order_id = Some(target_client_id);
            params.pending_above_price = Some(target_price);

            params.pending_below_type = Some(stop_type);
            params.pending_below_client_order_id = Some(stop_client_id);
            params.pending_below_price = stop_limit_price;
            params.pending_below_stop_price = Some(stop_trigger_price);
            params.pending_below_time_in_force = stop_time_in_force;
        }
        OrderSide::Buy => {
            params.pending_above_type = stop_type;
            params.pending_above_client_order_id = Some(stop_client_id);
            params.pending_above_price = stop_limit_price;
            params.pending_above_stop_price = Some(stop_trigger_price);
            params.pending_above_time_in_force = stop_time_in_force;

            params.pending_below_type = Some(BinanceSpotOrderType::LimitMaker);
            params.pending_below_client_order_id = Some(target_client_id);
            params.pending_below_price = Some(target_price);
        }
        side => anyhow::bail!("Unsupported Binance Spot OPOCO pending order side: {side:?}"),
    }

    Ok(params)
}

fn order_list_client_order_id(params: Option<&nautilus_core::params::Params>) -> Option<&str> {
    params.and_then(|params| params.get_str(ORDER_LIST_CLIENT_ORDER_ID_PARAM))
}

fn build_cancel_order_list_params(
    cmd: &CancelOrder,
) -> anyhow::Result<Option<CancelOrderListParams>> {
    let Some(params) = cmd.params.as_ref() else {
        return Ok(None);
    };
    if !params.get_bool(ORDER_LIST_CANCEL_PARAM).unwrap_or(false) {
        return Ok(None);
    }

    let symbol = cmd.instrument_id.symbol.to_string();
    let mut cancel_params = if let Some(order_list_id) = params.get_i64(ORDER_LIST_ID_PARAM) {
        CancelOrderListParams::by_order_list_id(symbol, order_list_id)
    } else if let Some(list_client_order_id) = params.get_str(ORDER_LIST_CLIENT_ORDER_ID_PARAM) {
        let encoded_list_client_order_id = encode_binance_client_order_id(
            &ClientOrderId::new(list_client_order_id),
            BINANCE_NAUTILUS_SPOT_BROKER_ID,
        )?;
        CancelOrderListParams::by_list_client_order_id(symbol, encoded_list_client_order_id)
    } else {
        anyhow::bail!(
            "order-list cancel requires params.{ORDER_LIST_ID_PARAM} or params.{ORDER_LIST_CLIENT_ORDER_ID_PARAM}"
        );
    };

    if let Some(new_client_order_id) = params.get_str("new_client_order_id") {
        cancel_params.new_client_order_id = Some(new_client_order_id.to_string());
    }

    Ok(Some(cancel_params))
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
#[expect(clippy::too_many_arguments)]
fn dispatch_execution_report(
    report: &BinanceSpotExecutionReport,
    emitter: &ExecutionEventEmitter,
    http_client: &BinanceSpotHttpClient,
    account_id: AccountId,
    treat_expired_as_canceled: bool,
    dispatch_state: &WsDispatchState,
    seen_trade_ids: &std::sync::Arc<Mutex<FifoCache<(Ustr, i64), 10_000>>>,
    ts_init: UnixNanos,
) {
    let symbol = report.symbol;
    let instrument_id = InstrumentId::new(symbol.into(), *BINANCE_VENUE);
    let (price_precision, size_precision) = http_client
        .get_instrument(&symbol)
        .map_or((8, 8), |i| (i.price_precision(), i.size_precision()));
    let venue_order_id = VenueOrderId::new(report.order_id.to_string());

    let decoded_client_order_id = match decode_client_order_id(
        &report.client_order_id,
        BINANCE_NAUTILUS_SPOT_BROKER_ID,
        "execution_report.client_order_id",
        "spot_json_execution_report_dispatch",
    ) {
        Ok(client_order_id) => Some(client_order_id),
        Err(e) => {
            log::error!(
                "Malformed Spot execution report client_order_id for symbol={}, order_id={}, execution_type={:?}, status={:?}: {e}",
                report.symbol,
                report.order_id,
                report.execution_type,
                report.order_status,
            );
            None
        }
    };

    let tracked_identity = decoded_client_order_id
        .and_then(|client_order_id| {
            dispatch_state
                .order_identities
                .get(&client_order_id)
                .map(|entry| (client_order_id, entry.clone()))
        })
        .or_else(|| {
            let fallback = dispatch_state.identity_for_venue_order(&venue_order_id);
            if fallback.is_some() && decoded_client_order_id.is_none() {
                log::warn!(
                    "Routing Spot execution report by venue_order_id fallback: symbol={}, order_id={}, raw_client_order_id={:?}",
                    report.symbol,
                    report.order_id,
                    report.client_order_id,
                );
            }
            fallback
        });

    if let Some((client_order_id, identity)) = tracked_identity {
        dispatch_tracked_execution_report(
            report,
            emitter,
            account_id,
            treat_expired_as_canceled,
            dispatch_state,
            seen_trade_ids,
            client_order_id,
            &identity,
            instrument_id,
            price_precision,
            size_precision,
            ts_init,
        );
    } else if decoded_client_order_id.is_some() {
        dispatch_untracked_execution_report(
            report,
            emitter,
            http_client,
            account_id,
            treat_expired_as_canceled,
            seen_trade_ids,
            instrument_id,
            price_precision,
            size_precision,
            ts_init,
        );
    } else {
        log::error!(
            "Skipping Spot execution report with invalid client_order_id and no venue_order_id fallback: symbol={}, order_id={}, execution_type={:?}, status={:?}",
            report.symbol,
            report.order_id,
            report.execution_type,
            report.order_status,
        );
    }
}

/// Dispatches a tracked execution report as proper order events.
#[expect(clippy::too_many_arguments)]
fn dispatch_tracked_execution_report(
    report: &BinanceSpotExecutionReport,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    treat_expired_as_canceled: bool,
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
    let ts_event = checked_unix_nanos_from_millis(
        report.event_time,
        "execution_report.event_time",
        "spot_json_execution_report_dispatch",
    )
    .unwrap_or(ts_init);

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
            emit_order_accepted_once(
                client_order_id,
                account_id,
                venue_order_id,
                identity,
                emitter,
                state,
                ts_event,
                ts_init,
            );
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
        BinanceSpotExecutionType::Canceled | BinanceSpotExecutionType::TradePrevention => {
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
        BinanceSpotExecutionType::Expired => {
            ensure_accepted_emitted(
                client_order_id,
                account_id,
                venue_order_id,
                identity,
                emitter,
                state,
                ts_init,
            );
            state.cleanup_terminal(client_order_id);

            if treat_expired_as_canceled {
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
                emitter.send_order_event(OrderEventAny::Canceled(canceled));
            } else {
                let expired = OrderExpired::new(
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
                emitter.send_order_event(OrderEventAny::Expired(expired));
            }
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
    treat_expired_as_canceled: bool,
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
                treat_expired_as_canceled,
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
                treat_expired_as_canceled,
                ts_init,
            ) {
                Ok(status) => emitter.send_order_status_report(status),
                Err(e) => log::error!("Failed to parse order status report: {e}"),
            }
        }
    }
}

fn retain_unseen_fill_reports(
    report: &OrderStatusReport,
    fills: Vec<FillReport>,
    seen_trade_ids: &Arc<Mutex<FifoCache<(Ustr, i64), 10_000>>>,
) -> Vec<FillReport> {
    if fills.is_empty() {
        return fills;
    }

    let symbol = Ustr::from(report.instrument_id.symbol.as_str());
    let mut guard = seen_trade_ids.lock().expect(MUTEX_POISONED);

    fills
        .into_iter()
        .filter(|fill| {
            let Ok(trade_id) = fill.trade_id.as_str().parse::<i64>() else {
                return true;
            };

            let dedup_key = (symbol, trade_id);
            let is_duplicate = guard.contains(&dedup_key);
            guard.add(dedup_key);

            if is_duplicate {
                log::debug!("Duplicate trade_id={trade_id} for {symbol}, skipping");
            }

            !is_duplicate
        })
        .collect()
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

fn is_definite_submit_failure(err: &anyhow::Error) -> bool {
    is_structured_venue_rejection(err) || is_local_command_failure(err)
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
    use nautilus_core::time::get_atomic_clock_realtime;
    use nautilus_model::{
        enums::{AccountType, LiquiditySide, OrderSide},
        identifiers::{StrategyId, TraderId},
    };
    use rstest::rstest;

    use super::*;
    use crate::common::{encoder::encode_broker_id, enums::BinanceEnvironment};

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
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
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
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
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
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
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
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
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
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
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
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
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
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
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
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
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
        let dispatch_state = WsDispatchState::default();
        dispatch_state.order_identities.insert(
            client_order_id,
            OrderIdentity {
                instrument_id,
                strategy_id: StrategyId::from("TEST-STRATEGY"),
                order_side: OrderSide::Buy,
                order_type: OrderType::Limit,
                price: None,
                quantity: Quantity::from("1"),
            },
        );
        dispatch_state
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
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
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
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
            &seen_trade_ids,
        );
        dispatch_ws_trading_message(
            BinanceSpotWsTradingMessage::ExecutionReport(Box::new(report)),
            &emitter,
            &http_client,
            AccountId::from("BINANCE-001"),
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
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
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
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
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
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
    #[case::as_expired(false, OrderStatus::Expired)]
    #[case::as_canceled(true, OrderStatus::Canceled)]
    fn test_normalize_spot_order_status_report_expired_respects_config(
        #[case] treat_expired_as_canceled: bool,
        #[case] expected: OrderStatus,
    ) {
        let clock = get_atomic_clock_realtime();
        let json = crate::common::testing::load_fixture_string(
            "spot/user_data_json/execution_report_expired.json",
        );
        let msg: BinanceSpotExecutionReport = serde_json::from_str(&json).unwrap();
        let mut report = parse_spot_exec_report_to_order_status(
            &msg,
            InstrumentId::from("ETHUSDT.BINANCE"),
            2,
            5,
            AccountId::from("BINANCE-001"),
            false,
            clock.get_time_ns(),
        )
        .unwrap();
        let mut reports = vec![report.clone()];

        normalize_spot_order_status_report(&mut report, treat_expired_as_canceled);
        normalize_spot_order_status_reports(&mut reports, treat_expired_as_canceled);

        assert_eq!(report.order_status, expected);
        assert_eq!(reports[0].order_status, expected);
    }

    #[rstest]
    #[case::as_expired(false)]
    #[case::as_canceled(true)]
    fn test_dispatch_tracked_execution_report_expired_respects_config(
        #[case] treat_expired_as_canceled: bool,
    ) {
        let clock = get_atomic_clock_realtime();
        let (emitter, mut rx) = create_test_emitter(clock);
        let client_order_id = ClientOrderId::from("O-20200101-000000-000-000-0");
        let instrument_id = InstrumentId::from("ETHUSDT.BINANCE");
        let dispatch_state = WsDispatchState::default();
        dispatch_state.insert_accepted(client_order_id, VenueOrderId::from("12345678"));
        let seen_trade_ids = Arc::new(Mutex::new(FifoCache::new()));
        let identity = OrderIdentity {
            instrument_id,
            strategy_id: StrategyId::from("TEST-STRATEGY"),
            order_side: OrderSide::Buy,
            order_type: OrderType::Limit,
            price: None,
            quantity: Quantity::from("1"),
        };

        let json = crate::common::testing::load_fixture_string(
            "spot/user_data_json/execution_report_expired.json",
        );
        let report: BinanceSpotExecutionReport = serde_json::from_str(&json).unwrap();

        dispatch_tracked_execution_report(
            &report,
            &emitter,
            AccountId::from("BINANCE-001"),
            treat_expired_as_canceled,
            &dispatch_state,
            &seen_trade_ids,
            client_order_id,
            &identity,
            instrument_id,
            2,
            5,
            clock.get_time_ns(),
        );

        let event = rx.try_recv().expect("terminal order event expected");
        match (treat_expired_as_canceled, event) {
            (true, ExecutionEvent::Order(OrderEventAny::Canceled(event))) => {
                assert_eq!(event.client_order_id, client_order_id);
            }
            (false, ExecutionEvent::Order(OrderEventAny::Expired(event))) => {
                assert_eq!(event.client_order_id, client_order_id);
            }
            (_, other) => panic!("Expected terminal expired/canceled event, was {other:?}"),
        }
        assert!(rx.try_recv().is_err());
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
        let ws_user_data_subscribed = tokio::sync::Notify::new();
        let (ws_setup_error_tx, _ws_setup_error_rx) = tokio::sync::mpsc::unbounded_channel();
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
            false,
            clock,
            &dispatch_state,
            &ws_authenticated,
            &ws_user_data_subscribed,
            &ws_setup_error_tx,
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
