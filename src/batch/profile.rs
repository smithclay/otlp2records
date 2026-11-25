//! Production-supported transform observation API.

use std::time::{Duration, Instant};

use super::context::ContextDuplicateTracker;
use crate::views::pdata::{InstrumentationScopeView, ResourceView};

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
/// timings pass a lightweight sink to [`crate::Transformer::observer`].
pub trait TransformObserver {
    fn on_phase(&mut self, timing: TransformPhaseTiming);

    fn on_counter(&mut self, _counter: TransformCounterValue) {}
}

/// What one transform reports: phase timings and counters for its signal to
/// an optional observer, and, only when observed, how often resource and
/// scope contexts repeat. Every internal step takes one of these instead of
/// the observer, signal and duplicate tracker separately.
pub(crate) struct Observation<'o> {
    observer: Option<&'o mut dyn TransformObserver>,
    signal: TransformSignal,
    duplicates: Option<ContextDuplicateTracker>,
}

impl<'o> Observation<'o> {
    pub(crate) fn new(
        signal: TransformSignal,
        observer: Option<&'o mut dyn TransformObserver>,
    ) -> Self {
        let duplicates = observer.is_some().then(ContextDuplicateTracker::default);
        Self {
            observer,
            signal,
            duplicates,
        }
    }

    /// A transform nobody observes.
    pub(crate) fn none(signal: TransformSignal) -> Self {
        Self::new(signal, None)
    }

    /// Runs `f`, timing it as `phase` when observed.
    pub(crate) fn phase<T>(&mut self, phase: TransformPhase, f: impl FnOnce() -> T) -> T {
        if self.observer.is_none() {
            return f();
        }
        let start = Instant::now();
        let value = f();
        self.record(phase, start.elapsed());
        value
    }

    /// Begins timing a phase whose work also reports observations of its own
    /// (so it cannot be one closure); [`Self::finish`] ends it.
    pub(crate) fn start(&self) -> Option<Instant> {
        self.observer.is_some().then(Instant::now)
    }

    /// Ends a phase begun by [`Self::start`].
    pub(crate) fn finish(&mut self, phase: TransformPhase, start: Option<Instant>) {
        if let Some(start) = start {
            self.record(phase, start.elapsed());
        }
    }

    /// Reports that `phase` took `elapsed`.
    pub(crate) fn record(&mut self, phase: TransformPhase, elapsed: Duration) {
        if let Some(observer) = self.observer.as_deref_mut() {
            observer.on_phase(TransformPhaseTiming {
                signal: self.signal,
                phase,
                elapsed,
            });
        }
    }

    /// Adds `value` to `counter`.
    pub(crate) fn count(&mut self, counter: TransformCounter, value: u64) {
        if value == 0 {
            return;
        }
        if let Some(observer) = self.observer.as_deref_mut() {
            observer.on_counter(TransformCounterValue {
                signal: self.signal,
                counter,
                value,
            });
        }
    }

    /// Counts whether `resource`'s context repeats an earlier one.
    pub(crate) fn track_resource<R: ResourceView>(&mut self, resource: &R) {
        if let Some(duplicates) = self.duplicates.as_mut() {
            let counter = duplicates.resource(resource);
            self.count(counter, 1);
        }
    }

    /// Counts whether `scope`'s context repeats an earlier one.
    pub(crate) fn track_scope<S: InstrumentationScopeView>(&mut self, scope: &S) {
        if let Some(duplicates) = self.duplicates.as_mut() {
            let counter = duplicates.scope(scope);
            self.count(counter, 1);
        }
    }
}
