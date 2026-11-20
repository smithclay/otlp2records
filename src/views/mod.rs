//! Semantic telemetry views used at the normalization boundary.
//!
//! The traits in [`pdata`] come from `otel-arrow-dfe-pdata-views`, the
//! zero-dependency view crate published by the OpenTelemetry Arrow project.
//! [`otlp`] implements them over the Prost OTLP types.

pub(crate) mod otlp;
pub(crate) mod pdata;
