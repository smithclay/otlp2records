//! Production-supported transform observation API.

use std::time::{Duration, Instant};

/// Telemetry signal being transformed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransformSignal {
    Logs,
    Traces,
    Metrics,
}

/// Timed transform phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransformPhase {
    ProtobufDecode,
    JsonDecode,
    JsonlDecode,
    RowCount,
    BuilderInit,
    /// Broad log resource group build timing. Includes nested narrow timings.
    ResourceLogsBuild,
    /// Broad trace resource group build timing. Includes nested narrow timings.
    ResourceSpansBuild,
    ResourceContextBuild,
    ResourceAttributesJson,
    /// Broad log scope group build timing. Includes nested narrow timings.
    ScopeLogsBuild,
    /// Broad trace scope group build timing. Includes nested narrow timings.
    ScopeSpansBuild,
    ScopeContextBuild,
    ScopeAttributesJson,
    /// Broad log record build timing. Includes nested narrow timings.
    LogRecordBuild,
    /// Broad trace span build timing. Includes nested narrow timings.
    SpanBuild,
    ArrowAppend,
    BodyAppend,
    ResourceAttributesAppend,
    ScopeAttributesAppend,
    LogAttributesJson,
    SpanAttributesJson,
    MetricAttributesJson,
    EventsJson,
    LinksJson,
    ExemplarsJson,
    MetricArrayJson,
    MetricsCapacity,
    ArrowFinalize,
}

/// Counted transform event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransformCounter {
    OutputRows,
    ResourceContextDuplicateHit,
    ResourceContextDuplicateMiss,
    ScopeContextDuplicateHit,
    ScopeContextDuplicateMiss,
    ResourceAttributesRowCopies,
    ResourceAttributesRowCopyBytes,
    ScopeAttributesRowCopies,
    ScopeAttributesRowCopyBytes,
}

/// A measured transform phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransformPhaseTiming {
    pub signal: TransformSignal,
    pub phase: TransformPhase,
    pub elapsed: Duration,
}

/// A counted transform event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransformCounterValue {
    pub signal: TransformSignal,
    pub counter: TransformCounter,
    pub value: u64,
}

/// Optional sink for transform timing observations.
///
/// Default transform entrypoints do not require an observer. Production callers that need phase
/// timings can use the `*_with_observer` functions and provide a lightweight sink.
pub trait TransformObserver {
    fn on_phase(&mut self, timing: TransformPhaseTiming);

    fn on_counter(&mut self, _counter: TransformCounterValue) {}
}

pub(crate) fn observe_phase(
    observer: &mut Option<&mut dyn TransformObserver>,
    signal: TransformSignal,
    phase: TransformPhase,
    elapsed: Duration,
) {
    if let Some(observer) = observer.as_deref_mut() {
        observer.on_phase(TransformPhaseTiming {
            signal,
            phase,
            elapsed,
        });
    }
}

pub(crate) fn observe_counter(
    observer: &mut Option<&mut dyn TransformObserver>,
    signal: TransformSignal,
    counter: TransformCounter,
    value: u64,
) {
    if value == 0 {
        return;
    }
    if let Some(observer) = observer.as_deref_mut() {
        observer.on_counter(TransformCounterValue {
            signal,
            counter,
            value,
        });
    }
}

pub(crate) fn measure_phase<T, F>(
    observer: &mut Option<&mut dyn TransformObserver>,
    signal: TransformSignal,
    phase: TransformPhase,
    f: F,
) -> T
where
    F: FnOnce() -> T,
{
    if observer.is_some() {
        let start = Instant::now();
        let value = f();
        observe_phase(observer, signal, phase, start.elapsed());
        value
    } else {
        f()
    }
}

pub(crate) fn measure_result<T, E, F>(
    observer: &mut Option<&mut dyn TransformObserver>,
    signal: TransformSignal,
    phase: TransformPhase,
    f: F,
) -> std::result::Result<T, E>
where
    F: FnOnce() -> std::result::Result<T, E>,
{
    if observer.is_some() {
        let start = Instant::now();
        let value = f();
        observe_phase(observer, signal, phase, start.elapsed());
        value
    } else {
        f()
    }
}
