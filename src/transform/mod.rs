// src/transform/mod.rs
//! VRL transformation module for OTLP data
//!
//! This module provides the VRL runtime and custom functions for transforming
//! OTLP data into the target schema.

pub mod functions;
pub mod runtime;

pub use runtime::{
    VrlError, VrlTransformer, OTLP_EXP_HISTOGRAM_PROGRAM, OTLP_GAUGE_PROGRAM,
    OTLP_HISTOGRAM_PROGRAM, OTLP_LOGS_PROGRAM, OTLP_SUM_PROGRAM, OTLP_TRACES_PROGRAM,
};

// Only export init_programs for WASM target (used in worker startup)
#[cfg(target_arch = "wasm32")]
pub use runtime::init_programs;
