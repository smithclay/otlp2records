//! Semantic telemetry views used at the normalization boundary.
//!
//! The traits in [`pdata`] are kept source-compatible with the logs portion of
//! `otap-df-pdata-views` from the OpenTelemetry Arrow repository. Upstream does
//! not currently publish that zero-dependency crate and its workspace requires
//! Rust 1.86, so the small interface surface is vendored here for crates.io
//! packaging compatibility.

pub(crate) mod otlp;
pub(crate) mod pdata;
