// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Telemetry view traits from the published `otel-arrow-dfe-pdata-views` crate.
//!
//! Re-exported flat so the normalization code names every trait from one path.

pub use otel_arrow_dfe_pdata_views::{
    views::{common::*, logs::*, metrics::*, resource::*, trace::*},
    SpanId, TraceId,
};
