//! Trace request-to-RecordBatch conversion.

use std::time::Instant;

use arrow_array::{
    builder::{Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder},
    RecordBatch,
};
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;

use crate::{schema::traces_schema, Result};

use super::{
    context::{ContextDuplicateTracker, ResourceContext, ScopeContext},
    json::{append_attrs_json, append_span_events_json, append_span_links_json},
    profile::{
        measure_phase, measure_result, observe_counter, observe_phase, TransformCounter,
        TransformObserver, TransformPhase, TransformSignal,
    },
    util::{
        append_empty_as_null, append_hex_or_null, append_opt_n, append_required_service_name_n,
        array, record_batch, string_builder_bytes, u64_to_i64,
    },
};

pub fn transform_traces_protobuf(bytes: &[u8]) -> Result<RecordBatch> {
    let request = ExportTraceServiceRequest::decode(bytes)?;
    transform_traces_request(request, bytes.len())
}

pub fn transform_traces_protobuf_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<RecordBatch> {
    let request = measure_result(
        observer,
        TransformSignal::Traces,
        TransformPhase::ProtobufDecode,
        || ExportTraceServiceRequest::decode(bytes),
    )?;
    transform_traces_request_observed(request, bytes.len(), observer)
}

pub fn transform_traces_request(
    request: ExportTraceServiceRequest,
    input_bytes: usize,
) -> Result<RecordBatch> {
    let mut observer = None;
    transform_traces_request_observed(request, input_bytes, &mut observer)
}

pub fn transform_traces_request_observed(
    request: ExportTraceServiceRequest,
    input_bytes: usize,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<RecordBatch> {
    let rows = measure_phase(
        observer,
        TransformSignal::Traces,
        TransformPhase::RowCount,
        || {
            request
                .resource_spans
                .iter()
                .map(|rs| {
                    rs.scope_spans
                        .iter()
                        .map(|ss| ss.spans.len())
                        .sum::<usize>()
                })
                .sum()
        },
    );
    observe_counter(
        observer,
        TransformSignal::Traces,
        TransformCounter::OutputRows,
        rows as u64,
    );
    let mut builders = measure_phase(
        observer,
        TransformSignal::Traces,
        TransformPhase::BuilderInit,
        || TraceBuilders::with_capacity(rows, input_bytes),
    );
    let mut duplicates = observer.is_some().then(ContextDuplicateTracker::default);

    for resource_spans in request.resource_spans {
        append_resource_spans_observed(
            resource_spans,
            &mut builders,
            observer,
            duplicates.as_mut(),
        )?;
    }

    measure_result(
        observer,
        TransformSignal::Traces,
        TransformPhase::ArrowFinalize,
        || builders.finish(),
    )
}

fn append_resource_spans_observed(
    resource_spans: ResourceSpans,
    builders: &mut TraceBuilders,
    observer: &mut Option<&mut dyn TransformObserver>,
    mut duplicates: Option<&mut ContextDuplicateTracker>,
) -> Result<()> {
    let resource_phase_start = phase_start(observer);
    let ResourceSpans {
        resource,
        scope_spans,
        ..
    } = resource_spans;
    let resource_attrs = resource
        .as_ref()
        .map(|r| r.attributes.as_slice())
        .unwrap_or(&[]);
    let resource = ResourceContext::from_attrs_observed(
        resource_attrs,
        TransformSignal::Traces,
        observer,
        duplicates.as_deref_mut(),
    );

    for scope_spans in scope_spans {
        append_scope_spans_observed(
            scope_spans,
            builders,
            &resource,
            observer,
            duplicates.as_deref_mut(),
        )?;
    }

    finish_phase(
        observer,
        TransformSignal::Traces,
        TransformPhase::ResourceSpansBuild,
        resource_phase_start,
    );
    Ok(())
}

fn append_scope_spans_observed(
    scope_spans: ScopeSpans,
    builders: &mut TraceBuilders,
    resource: &ResourceContext,
    observer: &mut Option<&mut dyn TransformObserver>,
    duplicates: Option<&mut ContextDuplicateTracker>,
) -> Result<()> {
    let scope_phase_start = phase_start(observer);
    let ScopeSpans { scope, spans, .. } = scope_spans;
    let scope_name = scope.as_ref().map(|s| s.name.as_str());
    let scope_version = scope.as_ref().map(|s| s.version.as_str());
    let scope_attrs = scope
        .as_ref()
        .map(|s| s.attributes.as_slice())
        .unwrap_or(&[]);
    let scope = ScopeContext::new_observed(
        scope_name,
        scope_version,
        scope_attrs,
        TransformSignal::Traces,
        observer,
        duplicates,
    );
    let span_count = spans.len();
    if span_count > 0 {
        let context_phase_start = phase_start(observer);
        builders.append_context_observed(span_count, resource, &scope, observer);
        finish_phase(
            observer,
            TransformSignal::Traces,
            TransformPhase::SpanBuild,
            context_phase_start,
        );
    }

    for span in spans {
        append_span_observed(span, builders, observer)?;
    }

    finish_phase(
        observer,
        TransformSignal::Traces,
        TransformPhase::ScopeSpansBuild,
        scope_phase_start,
    );
    Ok(())
}

fn append_span_observed(
    span: Span,
    builders: &mut TraceBuilders,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<()> {
    let phase_start = phase_start(observer);
    builders.append_observed(&span, observer)?;
    finish_phase(
        observer,
        TransformSignal::Traces,
        TransformPhase::SpanBuild,
        phase_start,
    );
    Ok(())
}

fn phase_start(observer: &Option<&mut dyn TransformObserver>) -> Option<Instant> {
    observer.is_some().then(Instant::now)
}

fn finish_phase(
    observer: &mut Option<&mut dyn TransformObserver>,
    signal: TransformSignal,
    phase: TransformPhase,
    start: Option<Instant>,
) {
    if let Some(start) = start {
        observe_phase(observer, signal, phase, start.elapsed());
    }
}

struct TraceBuilders {
    timestamp: TimestampMicrosecondBuilder,
    end_timestamp: Int64Builder,
    duration: Int64Builder,
    trace_id: StringBuilder,
    span_id: StringBuilder,
    parent_span_id: StringBuilder,
    trace_state: StringBuilder,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    span_name: StringBuilder,
    span_kind: Int32Builder,
    status_code: Int32Builder,
    status_message: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    span_attributes: StringBuilder,
    events_json: StringBuilder,
    links_json: StringBuilder,
    dropped_attributes_count: Int32Builder,
    dropped_events_count: Int32Builder,
    dropped_links_count: Int32Builder,
    flags: Int32Builder,
    json_scratch: String,
}

impl TraceBuilders {
    fn with_capacity(rows: usize, input_bytes: usize) -> Self {
        Self {
            timestamp: TimestampMicrosecondBuilder::with_capacity(rows),
            end_timestamp: Int64Builder::with_capacity(rows),
            duration: Int64Builder::with_capacity(rows),
            trace_id: string_builder_bytes(rows, rows.saturating_mul(32)),
            span_id: string_builder_bytes(rows, rows.saturating_mul(16)),
            parent_span_id: string_builder_bytes(rows, rows.saturating_mul(16)),
            trace_state: string_builder_bytes(rows, rows.saturating_mul(16)),
            service_name: string_builder_bytes(rows, rows.saturating_mul(24)),
            service_namespace: string_builder_bytes(rows, rows.saturating_mul(16)),
            service_instance_id: string_builder_bytes(rows, rows.saturating_mul(32)),
            span_name: string_builder_bytes(rows, rows.saturating_mul(48)),
            span_kind: Int32Builder::with_capacity(rows),
            status_code: Int32Builder::with_capacity(rows),
            status_message: string_builder_bytes(rows, rows.saturating_mul(16)),
            resource_attributes: string_builder_bytes(rows, input_bytes),
            scope_name: string_builder_bytes(rows, rows.saturating_mul(16)),
            scope_version: string_builder_bytes(rows, rows.saturating_mul(8)),
            scope_attributes: string_builder_bytes(rows, input_bytes / 4),
            span_attributes: string_builder_bytes(rows, input_bytes),
            events_json: string_builder_bytes(rows, input_bytes),
            links_json: string_builder_bytes(rows, input_bytes / 2),
            dropped_attributes_count: Int32Builder::with_capacity(rows),
            dropped_events_count: Int32Builder::with_capacity(rows),
            dropped_links_count: Int32Builder::with_capacity(rows),
            flags: Int32Builder::with_capacity(rows),
            json_scratch: String::new(),
        }
    }

    fn append_observed(
        &mut self,
        span: &Span,
        observer: &mut Option<&mut dyn TransformObserver>,
    ) -> Result<()> {
        measure_result(
            observer,
            TransformSignal::Traces,
            TransformPhase::ArrowAppend,
            || {
                let start = u64_to_i64(span.start_time_unix_nano, "span.start_time_unix_nano")?;
                let end = u64_to_i64(span.end_time_unix_nano, "span.end_time_unix_nano")?;
                self.timestamp.append_value(start / 1_000);
                self.end_timestamp.append_value(end / 1_000_000);
                self.duration
                    .append_value(end.saturating_sub(start) / 1_000_000);
                append_hex_or_null(&mut self.trace_id, &span.trace_id);
                append_hex_or_null(&mut self.span_id, &span.span_id);
                append_hex_or_null(&mut self.parent_span_id, &span.parent_span_id);
                append_empty_as_null(&mut self.trace_state, &span.trace_state);
                self.span_name.append_value(&span.name);
                self.span_kind.append_value(span.kind);
                let (status_code, status_message) = span
                    .status
                    .as_ref()
                    .map(|status| (status.code, status.message.as_str()))
                    .unwrap_or((0, ""));
                self.status_code.append_value(status_code);
                append_empty_as_null(&mut self.status_message, status_message);
                Ok::<(), crate::Error>(())
            },
        )?;

        measure_result(
            observer,
            TransformSignal::Traces,
            TransformPhase::SpanAttributesJson,
            || {
                append_attrs_json(
                    &mut self.span_attributes,
                    &span.attributes,
                    &mut self.json_scratch,
                )
            },
        )?;
        measure_result(
            observer,
            TransformSignal::Traces,
            TransformPhase::EventsJson,
            || append_span_events_json(&mut self.events_json, &span.events, &mut self.json_scratch),
        )?;
        measure_result(
            observer,
            TransformSignal::Traces,
            TransformPhase::LinksJson,
            || append_span_links_json(&mut self.links_json, &span.links, &mut self.json_scratch),
        )?;
        measure_phase(
            observer,
            TransformSignal::Traces,
            TransformPhase::ArrowAppend,
            || {
                self.dropped_attributes_count
                    .append_value(span.dropped_attributes_count as i32);
                self.dropped_events_count
                    .append_value(span.dropped_events_count as i32);
                self.dropped_links_count
                    .append_value(span.dropped_links_count as i32);
                self.flags.append_value(span.flags as i32);
            },
        );
        Ok(())
    }

    fn append_context_observed(
        &mut self,
        rows: usize,
        resource: &ResourceContext,
        scope: &ScopeContext,
        observer: &mut Option<&mut dyn TransformObserver>,
    ) {
        if rows == 0 {
            return;
        }

        measure_phase(
            observer,
            TransformSignal::Traces,
            TransformPhase::ArrowAppend,
            || {
                append_required_service_name_n(
                    &mut self.service_name,
                    resource.service_name.as_deref(),
                    rows,
                );
                append_opt_n(
                    &mut self.service_namespace,
                    resource.service_namespace.as_deref(),
                    rows,
                );
                append_opt_n(
                    &mut self.service_instance_id,
                    resource.service_instance_id.as_deref(),
                    rows,
                );
            },
        );

        measure_phase(
            observer,
            TransformSignal::Traces,
            TransformPhase::ResourceAttributesAppend,
            || {
                append_opt_n(
                    &mut self.resource_attributes,
                    resource.attributes_json.as_deref(),
                    rows,
                );
            },
        );
        if let Some(json) = resource.attributes_json.as_deref() {
            observe_counter(
                observer,
                TransformSignal::Traces,
                TransformCounter::ResourceAttributesRowCopies,
                rows as u64,
            );
            observe_counter(
                observer,
                TransformSignal::Traces,
                TransformCounter::ResourceAttributesRowCopyBytes,
                (json.len() as u64).saturating_mul(rows as u64),
            );
        }

        measure_phase(
            observer,
            TransformSignal::Traces,
            TransformPhase::ArrowAppend,
            || {
                append_opt_n(&mut self.scope_name, scope.name.as_deref(), rows);
                append_opt_n(&mut self.scope_version, scope.version.as_deref(), rows);
            },
        );

        measure_phase(
            observer,
            TransformSignal::Traces,
            TransformPhase::ScopeAttributesAppend,
            || {
                append_opt_n(
                    &mut self.scope_attributes,
                    scope.attributes_json.as_deref(),
                    rows,
                )
            },
        );
        if let Some(json) = scope.attributes_json.as_deref() {
            observe_counter(
                observer,
                TransformSignal::Traces,
                TransformCounter::ScopeAttributesRowCopies,
                rows as u64,
            );
            observe_counter(
                observer,
                TransformSignal::Traces,
                TransformCounter::ScopeAttributesRowCopyBytes,
                (json.len() as u64).saturating_mul(rows as u64),
            );
        }
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            traces_schema(),
            vec![
                array(self.timestamp.finish()),
                array(self.end_timestamp.finish()),
                array(self.duration.finish()),
                array(self.trace_id.finish()),
                array(self.span_id.finish()),
                array(self.parent_span_id.finish()),
                array(self.trace_state.finish()),
                array(self.service_name.finish()),
                array(self.service_namespace.finish()),
                array(self.service_instance_id.finish()),
                array(self.span_name.finish()),
                array(self.span_kind.finish()),
                array(self.status_code.finish()),
                array(self.status_message.finish()),
                array(self.resource_attributes.finish()),
                array(self.scope_name.finish()),
                array(self.scope_version.finish()),
                array(self.scope_attributes.finish()),
                array(self.span_attributes.finish()),
                array(self.events_json.finish()),
                array(self.links_json.finish()),
                array(self.dropped_attributes_count.finish()),
                array(self.dropped_events_count.finish()),
                array(self.dropped_links_count.finish()),
                array(self.flags.finish()),
            ],
        )
    }
}
