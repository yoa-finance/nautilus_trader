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

//! Value conversions between Nautilus domain types and Binance Spot venue types.

use nautilus_model::enums::{TrailingOffsetType, TriggerType};
use rust_decimal::{Decimal, prelude::ToPrimitive};

/// Converts a Nautilus trailing offset into Binance Spot `trailingDelta` BIPS.
///
/// # Errors
///
/// Returns an error unless the offset uses whole, positive basis points and the
/// trigger follows Binance Spot's last-trade-price semantics.
pub(crate) fn trailing_offset_to_delta(
    offset: Decimal,
    offset_type: TrailingOffsetType,
    trigger_type: TriggerType,
) -> anyhow::Result<i64> {
    if offset_type != TrailingOffsetType::BasisPoints {
        anyhow::bail!(
            "Binance Spot trailing stops only support TrailingOffsetType::BasisPoints, received {offset_type:?}"
        );
    }

    if !matches!(trigger_type, TriggerType::Default | TriggerType::LastPrice) {
        anyhow::bail!(
            "Binance Spot trailing stops only support last-trade-price triggers, received {trigger_type:?}"
        );
    }

    if offset <= Decimal::ZERO {
        anyhow::bail!("Binance Spot trailingDelta must be positive, received {offset}");
    }

    if !offset.fract().is_zero() {
        anyhow::bail!("Binance Spot trailingDelta must be whole BIPS, received {offset}");
    }

    offset.to_i64().ok_or_else(|| {
        anyhow::anyhow!("Binance Spot trailingDelta is outside the i64 range: {offset}")
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::valid(
        Decimal::from(250),
        TrailingOffsetType::BasisPoints,
        TriggerType::LastPrice,
        Ok(250)
    )]
    #[case::price_offset(
        Decimal::from(250),
        TrailingOffsetType::Price,
        TriggerType::LastPrice,
        Err("only support TrailingOffsetType::BasisPoints")
    )]
    #[case::fractional(
        Decimal::new(2505, 1),
        TrailingOffsetType::BasisPoints,
        TriggerType::LastPrice,
        Err("must be whole BIPS")
    )]
    #[case::zero(
        Decimal::ZERO,
        TrailingOffsetType::BasisPoints,
        TriggerType::LastPrice,
        Err("must be positive")
    )]
    #[case::bid_ask(
        Decimal::from(250),
        TrailingOffsetType::BasisPoints,
        TriggerType::BidAsk,
        Err("only support last-trade-price triggers")
    )]
    fn test_trailing_offset_to_delta(
        #[case] offset: Decimal,
        #[case] offset_type: TrailingOffsetType,
        #[case] trigger_type: TriggerType,
        #[case] expected: Result<i64, &str>,
    ) {
        let result = trailing_offset_to_delta(offset, offset_type, trigger_type);

        match expected {
            Ok(expected) => assert_eq!(result.unwrap(), expected),
            Err(expected) => assert!(result.unwrap_err().to_string().contains(expected)),
        }
    }
}
