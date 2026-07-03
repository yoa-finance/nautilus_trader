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

//! Timestamp conversion helpers for external Binance venue timestamps.

use nautilus_core::nanos::UnixNanos;

/// Venue timestamp unit declared by the Binance transport/schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceTimestampUnit {
    /// Milliseconds since Unix epoch.
    Milliseconds,
    /// Microseconds since Unix epoch.
    Microseconds,
}

impl BinanceTimestampUnit {
    const fn multiplier(self) -> u64 {
        match self {
            Self::Milliseconds => 1_000_000,
            Self::Microseconds => 1_000,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Milliseconds => "milliseconds",
            Self::Microseconds => "microseconds",
        }
    }
}

/// Converts a Binance venue timestamp into [`UnixNanos`] without panicking.
///
/// # Errors
///
/// Returns an error when the raw timestamp is negative or the conversion to nanoseconds overflows.
pub fn unix_nanos_from_timestamp(
    value: i64,
    unit: BinanceTimestampUnit,
    field: &str,
    context: &str,
) -> anyhow::Result<UnixNanos> {
    if value < 0 {
        anyhow::bail!(
            "invalid Binance timestamp context={context} field={field} unit={} raw_value={value} reason=negative",
            unit.name(),
        );
    }

    let raw_value = value as u64;
    let nanos = raw_value
        .checked_mul(unit.multiplier())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "invalid Binance timestamp context={context} field={field} unit={} raw_value={value} reason=unix_nanos_overflow",
                unit.name(),
            )
        })?;

    Ok(UnixNanos::from(nanos))
}

/// Converts a Binance millisecond timestamp into [`UnixNanos`] without panicking.
pub fn unix_nanos_from_millis(value: i64, field: &str, context: &str) -> anyhow::Result<UnixNanos> {
    unix_nanos_from_timestamp(value, BinanceTimestampUnit::Milliseconds, field, context)
}

/// Converts a Binance microsecond timestamp into [`UnixNanos`] without panicking.
pub fn unix_nanos_from_micros(value: i64, field: &str, context: &str) -> anyhow::Result<UnixNanos> {
    unix_nanos_from_timestamp(value, BinanceTimestampUnit::Microseconds, field, context)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_unix_nanos_from_micros_valid() {
        let nanos = unix_nanos_from_micros(
            1_700_000_000_000_000,
            "transact_time",
            "spot_sbe_new_order_response",
        )
        .unwrap();

        assert_eq!(nanos, UnixNanos::from(1_700_000_000_000_000_000u64));
    }

    #[rstest]
    fn test_unix_nanos_from_millis_valid() {
        let nanos = unix_nanos_from_millis(
            1_700_000_000_000,
            "event_time",
            "spot_json_execution_report",
        )
        .unwrap();

        assert_eq!(nanos, UnixNanos::from(1_700_000_000_000_000_000u64));
    }

    #[rstest]
    fn test_unix_nanos_from_timestamp_rejects_negative() {
        let err = unix_nanos_from_micros(-1, "time", "spot_sbe_order").unwrap_err();
        let message = err.to_string();

        assert!(message.contains("context=spot_sbe_order"));
        assert!(message.contains("field=time"));
        assert!(message.contains("unit=microseconds"));
        assert!(message.contains("raw_value=-1"));
        assert!(message.contains("reason=negative"));
    }

    #[rstest]
    fn test_unix_nanos_from_timestamp_rejects_overflow() {
        let err = unix_nanos_from_micros(
            (u64::MAX / 1_000 + 1) as i64,
            "transact_time",
            "spot_sbe_new_order_response",
        )
        .unwrap_err();
        let message = err.to_string();

        assert!(message.contains("context=spot_sbe_new_order_response"));
        assert!(message.contains("field=transact_time"));
        assert!(message.contains("unit=microseconds"));
        assert!(message.contains("reason=unix_nanos_overflow"));
    }

    #[rstest]
    fn test_unix_nanos_from_timestamp_rejects_nanosecond_like_microsecond_value() {
        let err = unix_nanos_from_micros(
            1_783_095_241_795_039_412,
            "transact_time",
            "spot_sbe_new_order_response",
        )
        .unwrap_err();
        let message = err.to_string();

        assert!(message.contains("unit=microseconds"));
        assert!(message.contains("raw_value=1783095241795039412"));
        assert!(message.contains("reason=unix_nanos_overflow"));
    }

    #[rstest]
    fn test_spot_adapter_uses_checked_timestamp_helpers() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let files = [
            "common/parse.rs",
            "spot/execution.rs",
            "spot/http/models.rs",
            "spot/websocket/public_json/parse.rs",
            "spot/websocket/streams/parse.rs",
            "spot/websocket/trading/parse.rs",
        ];
        let forbidden = [
            concat!("UnixNanos", "::", "from_micros("),
            concat!("UnixNanos", "::", "from_millis("),
            "as u64 * 1_000",
        ];
        let mut offenders = Vec::new();

        for file in files {
            let path = root.join(file);
            let source = std::fs::read_to_string(&path).unwrap();
            for pattern in forbidden {
                if source.contains(pattern) {
                    offenders.push(format!("{file}: {pattern}"));
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "unchecked Binance timestamp conversions found: {offenders:?}"
        );
    }
}
