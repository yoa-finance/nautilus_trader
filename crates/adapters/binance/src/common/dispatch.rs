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

//! WebSocket dispatch state for tracked/external order routing.
//!
//! Orders submitted through this client have their identity registered in
//! [`WsDispatchState`]. When user data stream messages arrive, the dispatch
//! function checks for a registered identity:
//! - Tracked orders produce proper order events (OrderAccepted, OrderFilled, etc.).
//! - Untracked orders fall back to execution reports for reconciliation.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use nautilus_common::cache::fifo::FifoCache;
use nautilus_core::{MUTEX_POISONED, UUID4, UnixNanos};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    enums::{OrderSide, OrderType},
    events::{OrderAccepted, OrderEventAny},
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, VenueOrderId},
    types::{Price, Quantity},
};
use tokio::sync::Notify;

/// The type of operation a pending WS API request represents.
#[derive(Debug, Clone, Copy)]
pub enum PendingOperation {
    Place,
    Cancel,
    Modify,
}

/// A pending WS API request awaiting a response.
///
/// Stored in [`WsDispatchState::pending_requests`] after the WS client
/// returns a request ID. When the venue responds (accepted or rejected),
/// the pending request is removed and used to emit the correct order event.
#[derive(Debug, Clone)]
pub struct PendingRequest {
    pub client_order_id: ClientOrderId,
    pub venue_order_id: Option<VenueOrderId>,
    pub operation: PendingOperation,
}

/// Order identity context stored at submission time.
///
/// Provides the strategy and instrument metadata needed to construct proper
/// order events without accessing the cache from the async dispatch task.
#[derive(Debug, Clone)]
pub struct OrderIdentity {
    pub instrument_id: InstrumentId,
    pub strategy_id: StrategyId,
    pub order_side: OrderSide,
    pub order_type: OrderType,
    pub price: Option<Price>,
    pub quantity: Quantity,
}

/// Tracks order lifecycle state for dispatch routing.
///
/// Orders with a registered identity (submitted through this client) produce
/// proper order events. Orders without identity (external or pre-existing)
/// fall back to execution reports for reconciliation.
#[derive(Debug)]
pub struct WsDispatchState {
    pub order_identities: DashMap<ClientOrderId, OrderIdentity>,
    pub venue_order_identities: DashMap<VenueOrderId, ClientOrderId>,
    pub pending_requests: DashMap<String, PendingRequest>,
    pending_new_orders: DashMap<ClientOrderId, ()>,
    live_exit_cancel_gates: Mutex<HashMap<String, CancelAllGate>>,
    emitted_accepted: Mutex<FifoCache<ClientOrderId, 10_000>>,
    emitted_accepted_events: Box<Mutex<FifoCache<AcceptedOrderKey, 10_000>>>,
    pending_updates: Mutex<FifoCache<ClientOrderId, 10_000>>,
    filled_orders: Mutex<FifoCache<ClientOrderId, 10_000>>,
    adapter_fatal_reason: Mutex<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AcceptedOrderKey {
    client_order_id: ClientOrderId,
    venue_order_id: VenueOrderId,
}

#[derive(Debug)]
struct CancelAllGate {
    notify: Arc<Notify>,
    pending_count: usize,
}

impl Default for WsDispatchState {
    fn default() -> Self {
        Self {
            order_identities: DashMap::new(),
            venue_order_identities: DashMap::new(),
            pending_requests: DashMap::new(),
            pending_new_orders: DashMap::new(),
            live_exit_cancel_gates: Mutex::new(HashMap::new()),
            emitted_accepted: Mutex::new(FifoCache::new()),
            emitted_accepted_events: Box::new(Mutex::new(FifoCache::new())),
            pending_updates: Mutex::new(FifoCache::new()),
            filled_orders: Mutex::new(FifoCache::new()),
            adapter_fatal_reason: Mutex::new(None),
        }
    }
}

#[expect(clippy::missing_panics_doc, reason = "mutex poisoning is not expected")]
impl WsDispatchState {
    pub fn has_emitted_accepted(&self, cid: &ClientOrderId) -> bool {
        self.emitted_accepted
            .lock()
            .expect(MUTEX_POISONED)
            .contains(cid)
    }

    /// Marks an order as having emitted an OrderAccepted event.
    pub fn insert_accepted(&self, cid: ClientOrderId, venue_order_id: VenueOrderId) -> bool {
        self.venue_order_identities.insert(venue_order_id, cid);

        let key = AcceptedOrderKey {
            client_order_id: cid,
            venue_order_id,
        };
        {
            let mut emitted_accepted_events =
                self.emitted_accepted_events.lock().expect(MUTEX_POISONED);
            if emitted_accepted_events.contains(&key) {
                return false;
            }
            emitted_accepted_events.add(key);
        }
        let mut emitted_accepted = self.emitted_accepted.lock().expect(MUTEX_POISONED);
        let was_emitted_for_client = emitted_accepted.contains(&cid);
        emitted_accepted.add(cid);
        !was_emitted_for_client
    }

    pub fn identity_for_venue_order(
        &self,
        venue_order_id: &VenueOrderId,
    ) -> Option<(ClientOrderId, OrderIdentity)> {
        let client_order_id = *self.venue_order_identities.get(venue_order_id)?;
        let identity = self
            .order_identities
            .get(&client_order_id)
            .map(|entry| entry.clone())?;
        Some((client_order_id, identity))
    }

    pub fn insert_pending_update(&self, cid: ClientOrderId) {
        self.pending_updates.lock().expect(MUTEX_POISONED).add(cid);
    }

    pub fn mark_pending_new(&self, cid: ClientOrderId) {
        self.pending_new_orders.insert(cid, ());
    }

    pub fn is_pending_new(&self, cid: &ClientOrderId) -> bool {
        self.pending_new_orders.contains_key(cid)
    }

    pub fn clear_pending_new(&self, cid: &ClientOrderId) {
        self.pending_new_orders.remove(cid);
    }

    pub fn remove_pending_update(&self, cid: &ClientOrderId) -> bool {
        let mut pending_updates = self.pending_updates.lock().expect(MUTEX_POISONED);
        let existed = pending_updates.contains(cid);
        pending_updates.remove(cid);
        existed
    }

    pub fn has_filled(&self, cid: &ClientOrderId) -> bool {
        self.filled_orders
            .lock()
            .expect(MUTEX_POISONED)
            .contains(cid)
    }

    /// Marks an order as having received a fill.
    pub fn insert_filled(&self, cid: ClientOrderId) {
        self.filled_orders.lock().expect(MUTEX_POISONED).add(cid);
    }

    pub fn set_adapter_fatal_reason(&self, reason: String) -> bool {
        let mut fatal_reason = self.adapter_fatal_reason.lock().expect(MUTEX_POISONED);
        if fatal_reason.is_some() {
            return false;
        }
        *fatal_reason = Some(reason);
        true
    }

    pub fn adapter_fatal_reason(&self) -> Option<String> {
        self.adapter_fatal_reason
            .lock()
            .expect(MUTEX_POISONED)
            .clone()
    }

    pub fn mark_cancel_all_started(&self, symbol: &str) {
        let mut gates = self.live_exit_cancel_gates.lock().expect(MUTEX_POISONED);
        let gate = gates
            .entry(symbol.to_string())
            .or_insert_with(|| CancelAllGate {
                notify: Arc::new(Notify::new()),
                pending_count: 0,
            });
        gate.pending_count += 1;
    }

    pub fn cancel_all_gate(&self, symbol: &str) -> Option<Arc<Notify>> {
        self.live_exit_cancel_gates
            .lock()
            .expect(MUTEX_POISONED)
            .get(symbol)
            .and_then(|gate| (gate.pending_count > 0).then(|| gate.notify.clone()))
    }

    pub fn complete_cancel_all(&self, symbol: &str) {
        let notify = {
            let mut gates = self.live_exit_cancel_gates.lock().expect(MUTEX_POISONED);
            let Some(gate) = gates.get_mut(symbol) else {
                return;
            };
            gate.pending_count = gate.pending_count.saturating_sub(1);
            if gate.pending_count > 0 {
                return;
            }
            gates.remove(symbol).map(|gate| gate.notify)
        };

        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    /// Removes all tracking state for a terminal order.
    pub fn cleanup_terminal(&self, cid: ClientOrderId) {
        self.order_identities.remove(&cid);
        self.venue_order_identities
            .retain(|_, mapped_cid| mapped_cid != &cid);
        self.pending_updates
            .lock()
            .expect(MUTEX_POISONED)
            .remove(&cid);
        self.pending_new_orders.remove(&cid);
        self.filled_orders
            .lock()
            .expect(MUTEX_POISONED)
            .remove(&cid);
    }
}

/// Synthesizes and emits OrderAccepted if one has not yet been emitted.
///
/// Handles fast-filling orders that skip the New state on Binance.
#[expect(clippy::too_many_arguments)]
pub fn emit_order_accepted_once(
    client_order_id: ClientOrderId,
    account_id: AccountId,
    venue_order_id: VenueOrderId,
    identity: &OrderIdentity,
    emitter: &ExecutionEventEmitter,
    state: &WsDispatchState,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> bool {
    if !state.insert_accepted(client_order_id, venue_order_id) {
        return false;
    }
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
    true
}

/// Synthesizes and emits OrderAccepted if one has not yet been emitted.
///
/// Handles fast-filling orders that skip the New state on Binance.
pub fn ensure_accepted_emitted(
    client_order_id: ClientOrderId,
    account_id: AccountId,
    venue_order_id: VenueOrderId,
    identity: &OrderIdentity,
    emitter: &ExecutionEventEmitter,
    state: &WsDispatchState,
    ts_init: UnixNanos,
) {
    emit_order_accepted_once(
        client_order_id,
        account_id,
        venue_order_id,
        identity,
        emitter,
        state,
        ts_init,
        ts_init,
    );
}
