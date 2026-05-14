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

//! Benchmarks focusing on the `CacheIndex` set intersections that power order
//! queries.  These benches isolate the cost of building the result sets for
//! various filter combinations without measuring deserialization or I/O.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use nautilus_common::cache::Cache;
use nautilus_model::{
    identifiers::{InstrumentId, PositionId, Venue},
    orders::stubs::create_order_list_sample,
};

/// Populate a `Cache` with the synthetic 100 k order universe used across
/// cache benchmarks (5 venues × 100 instruments × 200 orders).
fn build_populated_cache() -> Cache {
    let orders = create_order_list_sample(5, 100, 200);
    let mut cache = Cache::default();
    for order in orders {
        cache.add_order(order, None, None, false).unwrap();
    }
    cache
}

fn build_single_instrument_historical_order_cache() -> (Cache, InstrumentId) {
    let instrument = InstrumentId::from("SYMBOL-0.VENUE-0");
    let orders = create_order_list_sample(1, 1, 100_000);
    let mut cache = Cache::default();
    for order in orders {
        cache.add_order(order, None, None, false).unwrap();
    }

    (cache, instrument)
}

fn build_single_position_historical_order_cache() -> (Cache, PositionId) {
    let position_id = PositionId::from("P-HISTORICAL");
    let orders = create_order_list_sample(1, 1, 100_000);
    let mut cache = Cache::default();
    for order in orders {
        cache
            .add_order(order, Some(position_id), None, false)
            .unwrap();
    }

    (cache, position_id)
}

fn bench_set_intersections(c: &mut Criterion) {
    let cache = build_populated_cache();

    // Pre-create filter values so we don’t allocate in the hot loop
    let venue = Venue::from("VENUE-1");
    let instrument = InstrumentId::from("SYMBOL-1.1");

    let mut group = c.benchmark_group("Cache set intersections");

    // No filters → full set
    group.bench_function("all orders", |b| {
        b.iter(|| {
            black_box(cache.client_order_ids(None, None, None, None));
        });
    });

    // Venue only
    group.bench_function("venue filter", |b| {
        b.iter(|| {
            black_box(cache.client_order_ids(Some(&venue), None, None, None));
        });
    });

    // Instrument only
    group.bench_function("instrument filter", |b| {
        b.iter(|| {
            black_box(cache.client_order_ids(None, Some(&instrument), None, None));
        });
    });

    // Venue + instrument
    group.bench_function("venue + instrument filter", |b| {
        b.iter(|| {
            black_box(cache.client_order_ids(Some(&venue), Some(&instrument), None, None));
        });
    });

    group.finish();
}

fn bench_state_scoped_queries(c: &mut Criterion) {
    let (cache, instrument) = build_single_instrument_historical_order_cache();
    let (position_cache, position_id) = build_single_position_historical_order_cache();

    let mut group = c.benchmark_group("Cache state scoped queries");

    group.bench_function("open orders over 100k historical orders", |b| {
        b.iter(|| {
            black_box(cache.orders_open(None, Some(black_box(&instrument)), None, None, None));
        });
    });

    group.bench_function(
        "open passive reduce-only orders over 100k historical position orders",
        |b| {
            b.iter(|| {
                black_box(
                    position_cache
                        .open_passive_reduce_only_orders_for_position(black_box(&position_id)),
                );
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_set_intersections, bench_state_scoped_queries);
criterion_main!(benches);
