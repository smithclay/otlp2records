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
///
/// Marked `#[non_exhaustive]` because the phase set evolves as the transform
/// pipelines grow (e.g. OTAP star added per-resource/scope/metric variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
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
    /// Broad metric resource group build timing. Includes nested narrow timings.
    ResourceMetricsBuild,
    ResourceContextBuild,
    ResourceAttributesJson,
    /// Broad log scope group build timing. Includes nested narrow timings.
    ScopeLogsBuild,
    /// Broad trace scope group build timing. Includes nested narrow timings.
    ScopeSpansBuild,
    /// Broad metric scope group build timing. Includes nested narrow timings.
    ScopeMetricsBuild,
    ScopeContextBuild,
    ScopeAttributesJson,
    /// Broad log record build timing. Includes nested narrow timings.
    LogRecordBuild,
    /// Broad trace span build timing. Includes nested narrow timings.
    SpanBuild,
    /// Broad metric build timing (per `Metric` proto). Includes nested narrow timings.
    MetricBuild,
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
///
/// Marked `#[non_exhaustive]` so new counters can be added without it being a
/// major-version break for callers that match on this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
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

/// Begin timing a phase by inverting the closure-based `measure_*` helpers:
/// returns `Some(Instant::now())` when an observer is attached, `None`
/// otherwise. Pair with [`finish_phase`] at the end of the work block. Useful
/// when the work spans multiple borrows of `observer` (e.g. nested broad
/// phases around inner loops that themselves emit observations).
pub(crate) fn phase_start(observer: &Option<&mut dyn TransformObserver>) -> Option<Instant> {
    observer.is_some().then(Instant::now)
}

/// Emit a phase timing previously started by [`phase_start`]. No-op when
/// `start` is `None` (observer was disabled).
pub(crate) fn finish_phase(
    observer: &mut Option<&mut dyn TransformObserver>,
    signal: TransformSignal,
    phase: TransformPhase,
    start: Option<Instant>,
) {
    if let Some(start) = start {
        observe_phase(observer, signal, phase, start.elapsed());
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
