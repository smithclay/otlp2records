//! Log request-to-RecordBatch conversion.

use arrow_array::{
    builder::{
        ArrayBuilder, FixedSizeBinaryBuilder, Int32Builder, StringBuilder,
        TimestampNanosecondBuilder, UInt32Builder,
    },
    RecordBatch,
};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;

use crate::{
    options::{in_range, Plan},
    schema::logs_schema_arc,
    views::otlp::Obj,
    views::pdata::{LogRecordView, LogsDataView, ResourceLogsView, ScopeLogsView},
    DecodeError, Error, Result,
};

use super::{
    context::{ContextDuplicateTracker, ResourceContext, ScopeContext},
    profile::{
        finish_phase, measure_phase, measure_result, observe_counter, phase_start,
        TransformCounter, TransformObserver, TransformPhase, TransformSignal,
    },
    util::{
        append_fixed_or_null, append_opt_n, append_required_service_name_n, array,
        string_builder_bytes, u64_to_i64, JsonColumn,
    },
    view_json,
};

pub fn transform_logs_protobuf_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
    plan: &Plan,
) -> Result<RecordBatch> {
    let request = measure_result(
        observer,
        TransformSignal::Logs,
        TransformPhase::ProtobufDecode,
        || ExportLogsServiceRequest::decode(bytes),
    )?;
    transform_logs_request_observed(request, bytes.len(), observer, plan)
}

#[cfg(test)]
fn transform_logs_request(
    request: ExportLogsServiceRequest,
    input_bytes: usize,
) -> Result<RecordBatch> {
    let mut observer = None;
    let plan = Plan::all(&logs_schema_arc());
    transform_logs_request_observed(request, input_bytes, &mut observer, &plan)
}

pub fn transform_logs_request_observed(
    request: ExportLogsServiceRequest,
    input_bytes: usize,
    observer: &mut Option<&mut dyn TransformObserver>,
    plan: &Plan,
) -> Result<RecordBatch> {
    validate_log_ids(&request)?;
    transform_logs_view_observed(&Obj::new(&request), input_bytes, observer, plan)
}

/// Rejects log identifiers whose byte width is non-canonical. OTLP `trace_id`
/// is 16 bytes (or empty) and `span_id` is 8 bytes (or empty); the fixed-width
/// Arrow output cannot represent any other length. Empty and all-zero (but
/// correctly sized) identifiers normalize to null, but a non-empty identifier
/// of the wrong length signals corruption and is rejected here, restoring the
/// strict check that existed before identifiers were routed through the
/// semantic views' `parse_id`.
fn validate_log_ids(request: &ExportLogsServiceRequest) -> Result<()> {
    for resource in &request.resource_logs {
        for scope in &resource.scope_logs {
            for record in &scope.log_records {
                check_id_width(&record.trace_id, 16, "log.trace_id")?;
                check_id_width(&record.span_id, 8, "log.span_id")?;
            }
        }
    }
    Ok(())
}

fn check_id_width(id: &[u8], width: usize, field: &str) -> Result<()> {
    if !id.is_empty() && id.len() != width {
        return Err(Error::Decode(DecodeError::Unsupported(format!(
            "{field} fixed binary length mismatch: expected {width}, got {}",
            id.len()
        ))));
    }
    Ok(())
}

fn transform_logs_view_observed<V: LogsDataView>(
    view: &V,
    input_bytes: usize,
    observer: &mut Option<&mut dyn TransformObserver>,
    plan: &Plan,
) -> Result<RecordBatch> {
    let rows = measure_phase(
        observer,
        TransformSignal::Logs,
        TransformPhase::RowCount,
        || {
            view.resources()
                .map(|resource| {
                    resource
                        .scopes()
                        .map(|scope| scope.log_records().count())
                        .sum::<usize>()
                })
                .sum()
        },
    );
    observe_counter(
        observer,
        TransformSignal::Logs,
        TransformCounter::OutputRows,
        rows as u64,
    );
    let mut builders = measure_phase(
        observer,
        TransformSignal::Logs,
        TransformPhase::BuilderInit,
        || LogBuilders::with_capacity(rows, input_bytes, plan.clone()),
    );
    let mut duplicates = observer.is_some().then(ContextDuplicateTracker::default);

    for resource_logs in view.resources() {
        append_resource_logs_observed(resource_logs, &mut builders, observer, duplicates.as_mut())?;
    }

    measure_result(
        observer,
        TransformSignal::Logs,
        TransformPhase::ArrowFinalize,
        || builders.finish(),
    )
}

pub(crate) fn transform_logs_view<V: LogsDataView>(
    view: &V,
    input_bytes: usize,
    plan: &Plan,
) -> Result<RecordBatch> {
    let mut observer = None;
    transform_logs_view_observed(view, input_bytes, &mut observer, plan)
}

fn append_resource_logs_observed<R: ResourceLogsView>(
    resource_logs: R,
    builders: &mut LogBuilders,
    observer: &mut Option<&mut dyn TransformObserver>,
    mut duplicates: Option<&mut ContextDuplicateTracker>,
) -> Result<()> {
    let resource_phase_start = phase_start(observer);
    let resource_view = resource_logs.resource();
    let Some(resource) = ResourceContext::from_view_observed(
        resource_view.as_ref(),
        TransformSignal::Logs,
        observer,
        duplicates.as_deref_mut(),
        |service| builders.plan.keeps_service(service),
        builders.resource_attributes.built(),
    ) else {
        return Ok(());
    };

    for scope_logs in resource_logs.scopes() {
        append_scope_logs_observed(
            scope_logs,
            builders,
            &resource,
            observer,
            duplicates.as_deref_mut(),
        )?;
    }

    finish_phase(
        observer,
        TransformSignal::Logs,
        TransformPhase::ResourceLogsBuild,
        resource_phase_start,
    );
    Ok(())
}

fn append_scope_logs_observed<S: ScopeLogsView>(
    scope_logs: S,
    builders: &mut LogBuilders,
    resource: &ResourceContext,
    observer: &mut Option<&mut dyn TransformObserver>,
    duplicates: Option<&mut ContextDuplicateTracker>,
) -> Result<()> {
    let scope_phase_start = phase_start(observer);
    let scope_view = scope_logs.scope();
    let scope = ScopeContext::from_view_observed(
        scope_view.as_ref(),
        TransformSignal::Logs,
        observer,
        duplicates,
        builders.scope_attributes.built(),
    );
    // The context columns are separate builders, so they can be appended once
    // the scope's kept records are counted.
    let mut record_count = 0;
    for record in scope_logs.log_records() {
        if builders.keeps(&record) {
            append_log_record_observed(record, builders, observer)?;
            record_count += 1;
        }
    }
    if record_count > 0 {
        let context_phase_start = phase_start(observer);
        builders.append_context_observed(record_count, resource, &scope, observer);
        finish_phase(
            observer,
            TransformSignal::Logs,
            TransformPhase::LogRecordBuild,
            context_phase_start,
        );
    }

    finish_phase(
        observer,
        TransformSignal::Logs,
        TransformPhase::ScopeLogsBuild,
        scope_phase_start,
    );
    Ok(())
}

fn append_log_record_observed<R: LogRecordView>(
    record: R,
    builders: &mut LogBuilders,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<()> {
    let phase_start = phase_start(observer);
    builders.append_observed(&record, observer)?;
    finish_phase(
        observer,
        TransformSignal::Logs,
        TransformPhase::LogRecordBuild,
        phase_start,
    );
    Ok(())
}

struct LogBuilders {
    plan: Plan,
    time_range: Option<(i64, i64)>,
    observed_time_range: Option<(i64, i64)>,
    time_unix_nano: TimestampNanosecondBuilder,
    observed_time_unix_nano: TimestampNanosecondBuilder,
    trace_id: FixedSizeBinaryBuilder,
    span_id: FixedSizeBinaryBuilder,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    severity_number: Int32Builder,
    severity_text: StringBuilder,
    event_name: StringBuilder,
    body: JsonColumn,
    resource_attributes: JsonColumn,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: JsonColumn,
    log_attributes: JsonColumn,
    dropped_attributes_count: UInt32Builder,
    flags: UInt32Builder,
    json_scratch: String,
}

impl LogBuilders {
    fn with_capacity(rows: usize, input_bytes: usize, plan: Plan) -> Self {
        let schema = logs_schema_arc();
        let json = |name, bytes| JsonColumn::new(&plan, &schema, name, rows, bytes);
        Self {
            time_range: plan.time_range(&schema, "time_unix_nano"),
            observed_time_range: plan.time_range(&schema, "observed_time_unix_nano"),
            body: json("body", input_bytes),
            resource_attributes: json("resource_attributes", input_bytes),
            scope_attributes: json("scope_attributes", input_bytes / 4),
            log_attributes: json("log_attributes", input_bytes),
            time_unix_nano: TimestampNanosecondBuilder::with_capacity(rows),
            observed_time_unix_nano: TimestampNanosecondBuilder::with_capacity(rows),
            trace_id: FixedSizeBinaryBuilder::new(16),
            span_id: FixedSizeBinaryBuilder::new(8),
            service_name: string_builder_bytes(rows, rows.saturating_mul(24)),
            service_namespace: string_builder_bytes(rows, rows.saturating_mul(16)),
            service_instance_id: string_builder_bytes(rows, rows.saturating_mul(32)),
            severity_number: Int32Builder::with_capacity(rows),
            severity_text: string_builder_bytes(rows, rows.saturating_mul(8)),
            event_name: string_builder_bytes(rows, rows.saturating_mul(16)),
            scope_name: string_builder_bytes(rows, rows.saturating_mul(16)),
            scope_version: string_builder_bytes(rows, rows.saturating_mul(8)),
            dropped_attributes_count: UInt32Builder::with_capacity(rows),
            flags: UInt32Builder::with_capacity(rows),
            json_scratch: String::new(),
            plan,
        }
    }

    /// Whether the record passes the time ranges (the service name was
    /// checked for its whole resource).
    fn keeps(&self, record: &impl LogRecordView) -> bool {
        in_range(self.time_range, record.time_unix_nano())
            && in_range(self.observed_time_range, record.observed_time_unix_nano())
    }

    fn append_observed(
        &mut self,
        record: &impl LogRecordView,
        observer: &mut Option<&mut dyn TransformObserver>,
    ) -> Result<()> {
        measure_result(
            observer,
            TransformSignal::Logs,
            TransformPhase::ArrowAppend,
            || {
                match record.time_unix_nano() {
                    Some(value) => self
                        .time_unix_nano
                        .append_value(u64_to_i64(value, "log.time_unix_nano")?),
                    None => self.time_unix_nano.append_null(),
                }
                match record.observed_time_unix_nano() {
                    Some(value) => self
                        .observed_time_unix_nano
                        .append_value(u64_to_i64(value, "log.observed_time_unix_nano")?),
                    None => self.observed_time_unix_nano.append_null(),
                }
                append_fixed_or_null(
                    &mut self.trace_id,
                    record.trace_id().map(|id| id.as_slice()).unwrap_or(&[]),
                    16,
                )?;
                append_fixed_or_null(
                    &mut self.span_id,
                    record.span_id().map(|id| id.as_slice()).unwrap_or(&[]),
                    8,
                )?;
                match record.severity_number() {
                    Some(value) => self.severity_number.append_value(value),
                    None => self.severity_number.append_null(),
                }
                append_view_text(&mut self.severity_text, record.severity_text());
                append_view_text(&mut self.event_name, record.event_name());
                self.dropped_attributes_count
                    .append_value(record.dropped_attributes_count());
                self.flags.append_value(record.flags().unwrap_or_default());
                Ok::<(), crate::Error>(())
            },
        )?;

        measure_result(
            observer,
            TransformSignal::Logs,
            TransformPhase::BodyAppend,
            || {
                self.body
                    .append(|body| view_json::append_body(body, record.body()))
            },
        )?;

        measure_result(
            observer,
            TransformSignal::Logs,
            TransformPhase::LogAttributesJson,
            || {
                self.log_attributes.append(|attributes| {
                    view_json::append_attributes(
                        attributes,
                        record.attributes(),
                        &mut self.json_scratch,
                    )
                })
            },
        )?;
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
            TransformSignal::Logs,
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
            TransformSignal::Logs,
            TransformPhase::ResourceAttributesAppend,
            || {
                self.resource_attributes
                    .append_opt_n(resource.attributes_json.as_deref(), rows);
            },
        );
        if let Some(json) = resource.attributes_json.as_deref() {
            observe_counter(
                observer,
                TransformSignal::Logs,
                TransformCounter::ResourceAttributesRowCopies,
                rows as u64,
            );
            observe_counter(
                observer,
                TransformSignal::Logs,
                TransformCounter::ResourceAttributesRowCopyBytes,
                (json.len() as u64).saturating_mul(rows as u64),
            );
        }

        measure_phase(
            observer,
            TransformSignal::Logs,
            TransformPhase::ArrowAppend,
            || {
                append_opt_n(&mut self.scope_name, scope.name.as_deref(), rows);
                append_opt_n(&mut self.scope_version, scope.version.as_deref(), rows);
            },
        );

        measure_phase(
            observer,
            TransformSignal::Logs,
            TransformPhase::ScopeAttributesAppend,
            || {
                self.scope_attributes
                    .append_opt_n(scope.attributes_json.as_deref(), rows)
            },
        );
        if let Some(json) = scope.attributes_json.as_deref() {
            observe_counter(
                observer,
                TransformSignal::Logs,
                TransformCounter::ScopeAttributesRowCopies,
                rows as u64,
            );
            observe_counter(
                observer,
                TransformSignal::Logs,
                TransformCounter::ScopeAttributesRowCopyBytes,
                (json.len() as u64).saturating_mul(rows as u64),
            );
        }
    }

    fn finish(mut self) -> Result<RecordBatch> {
        // Every kept record appends its time, whatever the plan builds.
        let rows = self.time_unix_nano.len();
        self.plan.finish(
            logs_schema_arc(),
            vec![
                array(self.time_unix_nano.finish()),
                array(self.observed_time_unix_nano.finish()),
                array(self.trace_id.finish()),
                array(self.span_id.finish()),
                array(self.service_name.finish()),
                array(self.service_namespace.finish()),
                array(self.service_instance_id.finish()),
                array(self.severity_number.finish()),
                array(self.severity_text.finish()),
                array(self.event_name.finish()),
                self.body.finish(),
                self.resource_attributes.finish(),
                array(self.scope_name.finish()),
                array(self.scope_version.finish()),
                self.scope_attributes.finish(),
                self.log_attributes.finish(),
                array(self.dropped_attributes_count.finish()),
                array(self.flags.finish()),
            ],
            rows,
        )
    }
}

fn append_view_text(builder: &mut StringBuilder, value: Option<&[u8]>) {
    match value {
        Some(value) if !value.is_empty() => {
            builder.append_value(String::from_utf8_lossy(value).as_ref())
        }
        _ => builder.append_null(),
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

    use super::*;

    fn request_with_trace_id(trace_id: Vec<u8>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        trace_id,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn rejects_wrong_length_trace_id() {
        let error = transform_logs_request(request_with_trace_id(vec![0u8; 5]), 0).unwrap_err();
        assert!(
            error.to_string().contains("length mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn nulls_all_zero_trace_id() {
        // An all-zero but correctly sized id stays accepted and normalizes to
        // null rather than erroring.
        let batch = transform_logs_request(request_with_trace_id(vec![0u8; 16]), 0).unwrap();
        let trace_id = batch.column_by_name("trace_id").expect("trace_id column");
        assert!(trace_id.is_null(0));
    }
}
