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

//! Binance Spot SBE template catalog.
//!
//! The template IDs are taken from Binance's official Spot `spot_3_5.xml` and
//! stream `stream_1_0.xml` schemas. The catalog is intentionally separate from
//! business decoding so the ingress layer can distinguish official-but-unused
//! templates from truly unknown wire data.

/// Runtime handling status for a template.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateSupport {
    /// Fully decoded and routed by the adapter.
    Decoded,
    /// Known official template, but not used by the current adapter surface.
    KnownUnsupported,
}

/// Where an official template may appear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateContext {
    /// Spot REST or WebSocket API response payload.
    ApiResponse,
    /// Inline user data stream event.
    UserDataEvent,
    /// Nested messageData payload inside another SBE message.
    NestedMessageData,
    /// Spot market data stream event.
    MarketDataStream,
}

/// Official Binance SBE template metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemplateInfo {
    /// SBE template ID.
    pub id: u16,
    /// Official SBE message name.
    pub name: &'static str,
    /// Expected ingress context.
    pub context: TemplateContext,
    /// Runtime handling status.
    pub support: TemplateSupport,
}

/// Official Spot API schema 3:5 templates.
pub const SPOT_API_TEMPLATES: &[TemplateInfo] = &[
    TemplateInfo {
        id: 1,
        name: "PriceFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 2,
        name: "PercentPriceFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 3,
        name: "PercentPriceBySideFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 4,
        name: "LotSizeFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 5,
        name: "MinNotionalFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 6,
        name: "NotionalFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 7,
        name: "IcebergPartsFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 8,
        name: "MarketLotSizeFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 9,
        name: "MaxNumOrdersFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 10,
        name: "MaxNumAlgoOrdersFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 11,
        name: "MaxNumIcebergOrdersFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 12,
        name: "MaxPositionFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 13,
        name: "TrailingDeltaFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 14,
        name: "TPlusSellFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 15,
        name: "ExchangeMaxNumOrdersFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 16,
        name: "ExchangeMaxNumAlgoOrdersFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 17,
        name: "ExchangeMaxNumIcebergOrdersFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 18,
        name: "MaxNumOrderListsFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 19,
        name: "ExchangeMaxNumOrderListsFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 20,
        name: "MaxNumOrderAmendsFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 21,
        name: "MaxAssetFilter",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 22,
        name: "PriceRangeExecutionRule",
        context: TemplateContext::NestedMessageData,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 50,
        name: "WebSocketResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 51,
        name: "WebSocketSessionLogonResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 52,
        name: "WebSocketSessionStatusResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 53,
        name: "WebSocketSessionLogoutResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 54,
        name: "WebSocketSessionSubscriptionsResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 100,
        name: "ErrorResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 101,
        name: "PingResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 102,
        name: "ServerTimeResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 103,
        name: "ExchangeInfoResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 104,
        name: "ExecutionRulesResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 105,
        name: "MyFiltersResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 200,
        name: "DepthResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 201,
        name: "TradesResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 202,
        name: "AggTradesResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 203,
        name: "KlinesResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 204,
        name: "AveragePriceResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 205,
        name: "Ticker24hSymbolFullResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 206,
        name: "Ticker24hFullResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 207,
        name: "Ticker24hSymbolMiniResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 208,
        name: "Ticker24hMiniResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 209,
        name: "TickerSymbolFullResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 210,
        name: "TickerFullResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 211,
        name: "TickerSymbolMiniResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 212,
        name: "TickerMiniResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 213,
        name: "PriceTickerSymbolResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 214,
        name: "PriceTickerResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 215,
        name: "BookTickerSymbolResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 216,
        name: "BookTickerResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 217,
        name: "ReferencePriceResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 218,
        name: "ReferencePriceCalculationResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 219,
        name: "BlockTradesResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 300,
        name: "NewOrderAckResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 301,
        name: "NewOrderResultResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 302,
        name: "NewOrderFullResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 303,
        name: "OrderTestResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 304,
        name: "OrderResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 305,
        name: "CancelOrderResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 306,
        name: "CancelOpenOrdersResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 307,
        name: "CancelReplaceOrderResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 308,
        name: "OrdersResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 309,
        name: "NewOrderListAckResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 310,
        name: "NewOrderListResultResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 311,
        name: "NewOrderListFullResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 312,
        name: "CancelOrderListResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 313,
        name: "OrderListResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 314,
        name: "OrderListsResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 315,
        name: "OrderTestWithCommissionsResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 316,
        name: "OrderAmendmentsResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 317,
        name: "OrderAmendKeepPriorityResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 400,
        name: "AccountResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 401,
        name: "AccountTradesResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 402,
        name: "AccountOrderRateLimitResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 403,
        name: "AccountPreventedMatchesResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 404,
        name: "AccountAllocationsResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 500,
        name: "UserDataStreamStartResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 501,
        name: "UserDataStreamPingResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 502,
        name: "UserDataStreamStopResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 503,
        name: "UserDataStreamSubscribeResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 504,
        name: "UserDataStreamUnsubscribeResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 505,
        name: "UserDataStreamSubscribeListenTokenResponse",
        context: TemplateContext::ApiResponse,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 600,
        name: "AllocationReportEvent",
        context: TemplateContext::UserDataEvent,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 601,
        name: "BalanceUpdateEvent",
        context: TemplateContext::UserDataEvent,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 602,
        name: "EventStreamTerminatedEvent",
        context: TemplateContext::UserDataEvent,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 603,
        name: "ExecutionReportEvent",
        context: TemplateContext::UserDataEvent,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 604,
        name: "ExternalLockUpdateEvent",
        context: TemplateContext::UserDataEvent,
        support: TemplateSupport::KnownUnsupported,
    },
    TemplateInfo {
        id: 606,
        name: "ListStatusEvent",
        context: TemplateContext::UserDataEvent,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 607,
        name: "OutboundAccountPositionEvent",
        context: TemplateContext::UserDataEvent,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 610,
        name: "ServerShutdownEvent",
        context: TemplateContext::UserDataEvent,
        support: TemplateSupport::Decoded,
    },
];

/// Official Spot market-data stream schema 1:0 templates.
pub const SPOT_STREAM_TEMPLATES: &[TemplateInfo] = &[
    TemplateInfo {
        id: 10000,
        name: "TradesStreamEvent",
        context: TemplateContext::MarketDataStream,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 10001,
        name: "BestBidAskStreamEvent",
        context: TemplateContext::MarketDataStream,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 10002,
        name: "DepthSnapshotStreamEvent",
        context: TemplateContext::MarketDataStream,
        support: TemplateSupport::Decoded,
    },
    TemplateInfo {
        id: 10003,
        name: "DepthDiffStreamEvent",
        context: TemplateContext::MarketDataStream,
        support: TemplateSupport::Decoded,
    },
];

/// Returns official Spot API template metadata for `template_id`.
pub fn spot_api_template(template_id: u16) -> Option<&'static TemplateInfo> {
    SPOT_API_TEMPLATES
        .iter()
        .find(|template| template.id == template_id)
}

/// Returns official Spot stream template metadata for `template_id`.
pub fn spot_stream_template(template_id: u16) -> Option<&'static TemplateInfo> {
    SPOT_STREAM_TEMPLATES
        .iter()
        .find(|template| template.id == template_id)
}

/// Returns true when a top-level SBE frame is an official API response template.
pub fn is_spot_api_response_template(template_id: u16) -> bool {
    spot_api_template(template_id)
        .is_some_and(|template| template.context == TemplateContext::ApiResponse)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    fn test_spot_api_catalog_covers_official_schema_3_5_templates() {
        let expected = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 50, 51,
            52, 53, 54, 100, 101, 102, 103, 104, 105, 200, 201, 202, 203, 204, 205, 206, 207, 208,
            209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 300, 301, 302, 303, 304, 305,
            306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 400, 401, 402, 403, 404,
            500, 501, 502, 503, 504, 505, 600, 601, 602, 603, 604, 606, 607, 610,
        ];
        let actual: Vec<u16> = SPOT_API_TEMPLATES
            .iter()
            .map(|template| template.id)
            .collect();

        assert_eq!(actual, expected);
    }

    #[rstest::rstest]
    fn test_spot_stream_catalog_covers_official_schema_1_0_templates() {
        let actual: Vec<u16> = SPOT_STREAM_TEMPLATES
            .iter()
            .map(|template| template.id)
            .collect();

        assert_eq!(actual, vec![10000, 10001, 10002, 10003]);
    }
}
