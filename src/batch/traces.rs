//! Trace request-to-RecordBatch conversion.

use arrow_array::{
    builder::{
        ArrayBuilder, DurationNanosecondBuilder, FixedSizeBinaryBuilder, Int32Builder,
        StringBuilder, TimestampNanosecondBuilder, UInt32Builder,
    },
    RecordBatch,
};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;

use crate::{
    options::{in_range, Plan},
    schema::traces_schema_arc,
    views::otlp::Obj,
    views::pdata::{ResourceSpansView, ScopeSpansView, SpanView, StatusView, TracesView},
    Result,
};

use super::{
    context::{ResourceContext, ScopeContext},
    profile::{Observation, TransformCounter, TransformPhase, TransformSignal},
    util::{
        append_empty_as_null, append_fixed_or_null, append_fixed_required, append_opt_n,
        append_required_service_name_n, append_required_ts_ns, array, string_builder_bytes,
        u64_to_i64, JsonColumn,
    },
    view_json,
};

pub fn transform_traces_protobuf_observed(
    bytes: &[u8],
    obs: &mut Observation<'_>,
    plan: &Plan,
) -> Result<RecordBatch> {
    let request = obs.phase(TransformPhase::ProtobufDecode, || {
        ExportTraceServiceRequest::decode(bytes)
    })?;
    transform_traces_request_observed(request, bytes.len(), obs, plan)
}

pub fn transform_traces_request_observed(
    request: ExportTraceServiceRequest,
    input_bytes: usize,
    obs: &mut Observation<'_>,
    plan: &Plan,
) -> Result<RecordBatch> {
    transform_traces_view_observed(&Obj::new(&request), input_bytes, obs, plan)
}

pub(crate) fn transform_traces_view<V: TracesView>(
    view: &V,
    input_bytes: usize,
    plan: &Plan,
) -> Result<RecordBatch> {
    transform_traces_view_observed(
        view,
        input_bytes,
        &mut Observation::none(TransformSignal::Traces),
        plan,
    )
}

pub(crate) fn transform_traces_view_observed<V: TracesView>(
    view: &V,
    input_bytes: usize,
    obs: &mut Observation<'_>,
    plan: &Plan,
) -> Result<RecordBatch> {
    let rows = obs.phase(TransformPhase::RowCount, || {
        view.resources()
            .map(|resource| {
                resource
                    .scopes()
                    .map(|scope| scope.spans().count())
                    .sum::<usize>()
            })
            .sum()
    });
    obs.count(TransformCounter::OutputRows, rows as u64);
    let mut builders = obs.phase(TransformPhase::BuilderInit, || {
        TraceBuilders::with_capacity(rows, input_bytes, plan.clone())
    });

    for resource_spans in view.resources() {
        append_resource_spans_observed(&resource_spans, &mut builders, obs)?;
    }

    obs.phase(TransformPhase::ArrowFinalize, || builders.finish())
}

fn append_resource_spans_observed<R: ResourceSpansView>(
    resource_spans: &R,
    builders: &mut TraceBuilders,
    obs: &mut Observation<'_>,
) -> Result<()> {
    let resource_phase_start = obs.start();
    let resource_view = resource_spans.resource();
    let Some(resource) = ResourceContext::from_view(
        resource_view.as_ref(),
        obs,
        |service| builders.plan.keeps_service(service),
        builders.resource_attributes.built(),
    ) else {
        return Ok(());
    };

    for scope_spans in resource_spans.scopes() {
        append_scope_spans_observed(&scope_spans, builders, &resource, obs)?;
    }

    obs.finish(TransformPhase::ResourceSpansBuild, resource_phase_start);
    Ok(())
}

fn append_scope_spans_observed<S: ScopeSpansView>(
    scope_spans: &S,
    builders: &mut TraceBuilders,
    resource: &ResourceContext,
    obs: &mut Observation<'_>,
) -> Result<()> {
    let scope_phase_start = obs.start();
    let scope_view = scope_spans.scope();
    let scope =
        ScopeContext::from_view(scope_view.as_ref(), obs, builders.scope_attributes.built());
    // The context columns are separate builders, so they can be appended once
    // the scope's kept spans are counted.
    let mut span_count = 0;
    for span in scope_spans.spans() {
        if builders.keeps(&span) {
            append_span_observed(&span, builders, obs)?;
            span_count += 1;
        }
    }
    if span_count > 0 {
        let context_phase_start = obs.start();
        builders.append_context_observed(span_count, resource, &scope, obs);
        obs.finish(TransformPhase::SpanBuild, context_phase_start);
    }

    obs.finish(TransformPhase::ScopeSpansBuild, scope_phase_start);
    Ok(())
}

fn append_span_observed<S: SpanView>(
    span: &S,
    builders: &mut TraceBuilders,
    obs: &mut Observation<'_>,
) -> Result<()> {
    let phase_start = obs.start();
    builders.append_observed(span, obs)?;
    obs.finish(TransformPhase::SpanBuild, phase_start);
    Ok(())
}

struct TraceBuilders {
    plan: Plan,
    start_time_range: Option<(i64, i64)>,
    start_time_unix_nano: TimestampNanosecondBuilder,
    duration_time_unix_nano: DurationNanosecondBuilder,
    trace_id: FixedSizeBinaryBuilder,
    span_id: FixedSizeBinaryBuilder,
    parent_span_id: FixedSizeBinaryBuilder,
    trace_state: StringBuilder,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    name: StringBuilder,
    kind: Int32Builder,
    status_code: Int32Builder,
    status_status_message: StringBuilder,
    resource_attributes: JsonColumn,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: JsonColumn,
    span_attributes: JsonColumn,
    events_json: JsonColumn,
    links_json: JsonColumn,
    dropped_attributes_count: UInt32Builder,
    dropped_events_count: UInt32Builder,
    dropped_links_count: UInt32Builder,
    flags: UInt32Builder,
    json_scratch: String,
}

impl TraceBuilders {
    fn with_capacity(rows: usize, input_bytes: usize, plan: Plan) -> Self {
        let schema = traces_schema_arc();
        let json = |name, bytes| JsonColumn::new(&plan, &schema, name, rows, bytes);
        Self {
            start_time_range: plan.time_range(&schema, "start_time_unix_nano"),
            resource_attributes: json("resource_attributes", input_bytes),
            scope_attributes: json("scope_attributes", input_bytes / 4),
            span_attributes: json("span_attributes", input_bytes),
            events_json: json("events_json", input_bytes),
            links_json: json("links_json", input_bytes / 2),
            start_time_unix_nano: TimestampNanosecondBuilder::with_capacity(rows),
            duration_time_unix_nano: DurationNanosecondBuilder::with_capacity(rows),
            trace_id: FixedSizeBinaryBuilder::new(16),
            span_id: FixedSizeBinaryBuilder::new(8),
            parent_span_id: FixedSizeBinaryBuilder::new(8),
            trace_state: string_builder_bytes(rows, rows.saturating_mul(16)),
            service_name: string_builder_bytes(rows, rows.saturating_mul(24)),
            service_namespace: string_builder_bytes(rows, rows.saturating_mul(16)),
            service_instance_id: string_builder_bytes(rows, rows.saturating_mul(32)),
            name: string_builder_bytes(rows, rows.saturating_mul(48)),
            kind: Int32Builder::with_capacity(rows),
            status_code: Int32Builder::with_capacity(rows),
            status_status_message: string_builder_bytes(rows, rows.saturating_mul(16)),
            scope_name: string_builder_bytes(rows, rows.saturating_mul(16)),
            scope_version: string_builder_bytes(rows, rows.saturating_mul(8)),
            dropped_attributes_count: UInt32Builder::with_capacity(rows),
            dropped_events_count: UInt32Builder::with_capacity(rows),
            dropped_links_count: UInt32Builder::with_capacity(rows),
            flags: UInt32Builder::with_capacity(rows),
            json_scratch: String::new(),
            plan,
        }
    }

    /// Whether the span passes the start time range (the service name was
    /// checked for its whole resource). A missing start time is emitted as 0.
    fn keeps<S: SpanView>(&self, span: &S) -> bool {
        in_range(
            self.start_time_range,
            Some(span.start_time_unix_nano().unwrap_or_default()),
        )
    }

    fn append_observed<S: SpanView>(&mut self, span: &S, obs: &mut Observation<'_>) -> Result<()> {
        obs.phase(TransformPhase::ArrowAppend, || {
            let start_time = span.start_time_unix_nano().unwrap_or_default();
            append_required_ts_ns(
                &mut self.start_time_unix_nano,
                start_time,
                "span.start_time_unix_nano",
            )?;
            let duration = span
                .end_time_unix_nano()
                .unwrap_or_default()
                .saturating_sub(start_time);
            self.duration_time_unix_nano
                .append_value(u64_to_i64(duration, "span.duration_time_unix_nano")?);
            append_fixed_required(
                &mut self.trace_id,
                span.trace_id().map_or(&[][..], |value| value.as_slice()),
                16,
                "span.trace_id",
            )?;
            append_fixed_required(
                &mut self.span_id,
                span.span_id().map_or(&[][..], |value| value.as_slice()),
                8,
                "span.span_id",
            )?;
            append_fixed_or_null(
                &mut self.parent_span_id,
                span.parent_span_id()
                    .map_or(&[][..], |value| value.as_slice()),
                8,
            )?;
            let trace_state = String::from_utf8_lossy(span.trace_state().unwrap_or_default());
            append_empty_as_null(&mut self.trace_state, &trace_state);
            let name = String::from_utf8_lossy(span.name().unwrap_or_default());
            self.name.append_value(name.as_ref());
            if span.kind() == 0 {
                self.kind.append_null();
            } else {
                self.kind.append_value(span.kind());
            }
            if let Some(status) = span.status() {
                self.status_code.append_value(status.status_code());
                let message = String::from_utf8_lossy(status.message().unwrap_or_default());
                append_empty_as_null(&mut self.status_status_message, &message);
            } else {
                self.status_code.append_null();
                self.status_status_message.append_null();
            }
            Ok::<(), crate::Error>(())
        })?;

        obs.phase(TransformPhase::SpanAttributesJson, || {
            self.span_attributes.append(|column| {
                view_json::append_attributes(column, span.attributes(), &mut self.json_scratch)
            })
        })?;
        obs.phase(TransformPhase::EventsJson, || {
            self.events_json.append(|column| {
                view_json::append_events(column, span.events(), &mut self.json_scratch)
            })
        })?;
        obs.phase(TransformPhase::LinksJson, || {
            self.links_json.append(|column| {
                view_json::append_links(column, span.links(), &mut self.json_scratch)
            })
        })?;
        obs.phase(TransformPhase::ArrowAppend, || {
            self.dropped_attributes_count
                .append_value(span.dropped_attributes_count());
            self.dropped_events_count
                .append_value(span.dropped_events_count());
            self.dropped_links_count
                .append_value(span.dropped_links_count());
            self.flags.append_value(span.flags().unwrap_or_default());
        });
        Ok(())
    }

    fn append_context_observed(
        &mut self,
        rows: usize,
        resource: &ResourceContext,
        scope: &ScopeContext,
        obs: &mut Observation<'_>,
    ) {
        if rows == 0 {
            return;
        }

        obs.phase(TransformPhase::ArrowAppend, || {
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
        });

        obs.phase(TransformPhase::ResourceAttributesAppend, || {
            self.resource_attributes
                .append_opt_n(resource.attributes_json.as_deref(), rows);
        });
        if let Some(json) = resource.attributes_json.as_deref() {
            obs.count(TransformCounter::ResourceAttributesRowCopies, rows as u64);
            obs.count(
                TransformCounter::ResourceAttributesRowCopyBytes,
                (json.len() as u64).saturating_mul(rows as u64),
            );
        }

        obs.phase(TransformPhase::ArrowAppend, || {
            append_opt_n(&mut self.scope_name, scope.name.as_deref(), rows);
            append_opt_n(&mut self.scope_version, scope.version.as_deref(), rows);
        });

        obs.phase(TransformPhase::ScopeAttributesAppend, || {
            self.scope_attributes
                .append_opt_n(scope.attributes_json.as_deref(), rows)
        });
        if let Some(json) = scope.attributes_json.as_deref() {
            obs.count(TransformCounter::ScopeAttributesRowCopies, rows as u64);
            obs.count(
                TransformCounter::ScopeAttributesRowCopyBytes,
                (json.len() as u64).saturating_mul(rows as u64),
            );
        }
    }

    fn finish(mut self) -> Result<RecordBatch> {
        // Every kept span appends its start time, whatever the plan builds.
        let rows = self.start_time_unix_nano.len();
        self.plan.finish(
            traces_schema_arc(),
            vec![
                array(self.start_time_unix_nano.finish()),
                array(self.duration_time_unix_nano.finish()),
                array(self.trace_id.finish()),
                array(self.span_id.finish()),
                array(self.parent_span_id.finish()),
                array(self.trace_state.finish()),
                array(self.service_name.finish()),
                array(self.service_namespace.finish()),
                array(self.service_instance_id.finish()),
                array(self.name.finish()),
                array(self.kind.finish()),
                array(self.status_code.finish()),
                array(self.status_status_message.finish()),
                self.resource_attributes.finish(),
                array(self.scope_name.finish()),
                array(self.scope_version.finish()),
                self.scope_attributes.finish(),
                self.span_attributes.finish(),
                self.events_json.finish(),
                self.links_json.finish(),
                array(self.dropped_attributes_count.finish()),
                array(self.dropped_events_count.finish()),
                array(self.dropped_links_count.finish()),
                array(self.flags.finish()),
            ],
            rows,
        )
    }
}
