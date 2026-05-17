//! Log request-to-RecordBatch conversion.

use std::time::Instant;

use arrow_array::{
    builder::{Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder},
    RecordBatch,
};
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
};
use prost::Message;

use crate::{schema::logs_schema, Result};

use super::{
    context::{ContextDuplicateTracker, ResourceContext, ScopeContext},
    json::{append_attrs_json, append_log_body},
    profile::{
        measure_phase, measure_result, observe_counter, observe_phase, TransformCounter,
        TransformObserver, TransformPhase, TransformSignal,
    },
    util::{
        append_hex_or_null, append_opt, append_required_service_name, array, record_batch,
        string_builder_bytes, u64_to_i64,
    },
};

pub fn transform_logs_protobuf(bytes: &[u8]) -> Result<RecordBatch> {
    let request = ExportLogsServiceRequest::decode(bytes)?;
    transform_logs_request(request, bytes.len())
}

pub fn transform_logs_protobuf_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<RecordBatch> {
    let request = measure_result(
        observer,
        TransformSignal::Logs,
        TransformPhase::ProtobufDecode,
        || ExportLogsServiceRequest::decode(bytes),
    )?;
    transform_logs_request_observed(request, bytes.len(), observer)
}

pub fn transform_logs_request(
    request: ExportLogsServiceRequest,
    input_bytes: usize,
) -> Result<RecordBatch> {
    let mut observer = None;
    transform_logs_request_observed(request, input_bytes, &mut observer)
}

pub fn transform_logs_request_observed(
    request: ExportLogsServiceRequest,
    input_bytes: usize,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<RecordBatch> {
    let rows = measure_phase(
        observer,
        TransformSignal::Logs,
        TransformPhase::RowCount,
        || {
            request
                .resource_logs
                .iter()
                .map(|rl| {
                    rl.scope_logs
                        .iter()
                        .map(|sl| sl.log_records.len())
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
        || LogBuilders::with_capacity(rows, input_bytes),
    );
    let mut duplicates = observer.is_some().then(ContextDuplicateTracker::default);

    for resource_logs in request.resource_logs {
        append_resource_logs_observed(resource_logs, &mut builders, observer, duplicates.as_mut())?;
    }

    measure_result(
        observer,
        TransformSignal::Logs,
        TransformPhase::ArrowFinalize,
        || builders.finish(),
    )
}

fn append_resource_logs_observed(
    resource_logs: ResourceLogs,
    builders: &mut LogBuilders,
    observer: &mut Option<&mut dyn TransformObserver>,
    mut duplicates: Option<&mut ContextDuplicateTracker>,
) -> Result<()> {
    let phase_start = phase_start(observer);
    let ResourceLogs {
        resource,
        scope_logs,
        ..
    } = resource_logs;
    let resource_attrs = resource
        .as_ref()
        .map(|r| r.attributes.as_slice())
        .unwrap_or(&[]);
    let resource = ResourceContext::from_attrs_observed(
        resource_attrs,
        TransformSignal::Logs,
        observer,
        duplicates.as_deref_mut(),
    );

    for scope_logs in scope_logs {
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
        phase_start,
    );
    Ok(())
}

fn append_scope_logs_observed(
    scope_logs: ScopeLogs,
    builders: &mut LogBuilders,
    resource: &ResourceContext,
    observer: &mut Option<&mut dyn TransformObserver>,
    duplicates: Option<&mut ContextDuplicateTracker>,
) -> Result<()> {
    let phase_start = phase_start(observer);
    let ScopeLogs {
        scope, log_records, ..
    } = scope_logs;
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
        TransformSignal::Logs,
        observer,
        duplicates,
    );

    for record in log_records {
        append_log_record_observed(record, builders, resource, &scope, observer)?;
    }

    finish_phase(
        observer,
        TransformSignal::Logs,
        TransformPhase::ScopeLogsBuild,
        phase_start,
    );
    Ok(())
}

fn append_log_record_observed(
    record: LogRecord,
    builders: &mut LogBuilders,
    resource: &ResourceContext,
    scope: &ScopeContext,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<()> {
    let phase_start = phase_start(observer);
    builders.append_observed(&record, resource, scope, observer)?;
    finish_phase(
        observer,
        TransformSignal::Logs,
        TransformPhase::LogRecordBuild,
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

struct LogBuilders {
    timestamp: TimestampMicrosecondBuilder,
    observed_timestamp: Int64Builder,
    trace_id: StringBuilder,
    span_id: StringBuilder,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    severity_number: Int32Builder,
    severity_text: StringBuilder,
    body: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    log_attributes: StringBuilder,
    json_scratch: String,
}

impl LogBuilders {
    fn with_capacity(rows: usize, input_bytes: usize) -> Self {
        Self {
            timestamp: TimestampMicrosecondBuilder::with_capacity(rows),
            observed_timestamp: Int64Builder::with_capacity(rows),
            trace_id: string_builder_bytes(rows, rows.saturating_mul(32)),
            span_id: string_builder_bytes(rows, rows.saturating_mul(16)),
            service_name: string_builder_bytes(rows, rows.saturating_mul(24)),
            service_namespace: string_builder_bytes(rows, rows.saturating_mul(16)),
            service_instance_id: string_builder_bytes(rows, rows.saturating_mul(32)),
            severity_number: Int32Builder::with_capacity(rows),
            severity_text: string_builder_bytes(rows, rows.saturating_mul(8)),
            body: string_builder_bytes(rows, input_bytes),
            resource_attributes: string_builder_bytes(rows, input_bytes),
            scope_name: string_builder_bytes(rows, rows.saturating_mul(16)),
            scope_version: string_builder_bytes(rows, rows.saturating_mul(8)),
            scope_attributes: string_builder_bytes(rows, input_bytes / 4),
            log_attributes: string_builder_bytes(rows, input_bytes),
            json_scratch: String::new(),
        }
    }

    fn append_observed(
        &mut self,
        record: &LogRecord,
        resource: &ResourceContext,
        scope: &ScopeContext,
        observer: &mut Option<&mut dyn TransformObserver>,
    ) -> Result<()> {
        measure_result(
            observer,
            TransformSignal::Logs,
            TransformPhase::ArrowAppend,
            || {
                self.timestamp
                    .append_value(u64_to_i64(record.time_unix_nano, "log.time_unix_nano")? / 1_000);
                self.observed_timestamp.append_value(
                    u64_to_i64(
                        record.observed_time_unix_nano,
                        "log.observed_time_unix_nano",
                    )? / 1_000,
                );
                append_hex_or_null(&mut self.trace_id, &record.trace_id);
                append_hex_or_null(&mut self.span_id, &record.span_id);
                append_required_service_name(
                    &mut self.service_name,
                    resource.service_name.as_deref(),
                );
                append_opt(
                    &mut self.service_namespace,
                    resource.service_namespace.as_deref(),
                );
                append_opt(
                    &mut self.service_instance_id,
                    resource.service_instance_id.as_deref(),
                );
                self.severity_number.append_value(record.severity_number);
                self.severity_text.append_value(&record.severity_text);
                Ok::<(), crate::Error>(())
            },
        )?;

        measure_result(
            observer,
            TransformSignal::Logs,
            TransformPhase::BodyAppend,
            || append_log_body(&mut self.body, record.body.as_ref()),
        )?;

        measure_phase(
            observer,
            TransformSignal::Logs,
            TransformPhase::ResourceAttributesAppend,
            || {
                append_opt(
                    &mut self.resource_attributes,
                    resource.attributes_json.as_deref(),
                );
            },
        );
        if let Some(json) = resource.attributes_json.as_deref() {
            observe_counter(
                observer,
                TransformSignal::Logs,
                TransformCounter::ResourceAttributesRowCopies,
                1,
            );
            observe_counter(
                observer,
                TransformSignal::Logs,
                TransformCounter::ResourceAttributesRowCopyBytes,
                json.len() as u64,
            );
        }

        measure_phase(
            observer,
            TransformSignal::Logs,
            TransformPhase::ArrowAppend,
            || {
                append_opt(&mut self.scope_name, scope.name.as_deref());
                append_opt(&mut self.scope_version, scope.version.as_deref());
            },
        );

        measure_phase(
            observer,
            TransformSignal::Logs,
            TransformPhase::ScopeAttributesAppend,
            || append_opt(&mut self.scope_attributes, scope.attributes_json.as_deref()),
        );
        if let Some(json) = scope.attributes_json.as_deref() {
            observe_counter(
                observer,
                TransformSignal::Logs,
                TransformCounter::ScopeAttributesRowCopies,
                1,
            );
            observe_counter(
                observer,
                TransformSignal::Logs,
                TransformCounter::ScopeAttributesRowCopyBytes,
                json.len() as u64,
            );
        }

        measure_result(
            observer,
            TransformSignal::Logs,
            TransformPhase::LogAttributesJson,
            || {
                append_attrs_json(
                    &mut self.log_attributes,
                    &record.attributes,
                    &mut self.json_scratch,
                )
            },
        )?;
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            logs_schema(),
            vec![
                array(self.timestamp.finish()),
                array(self.observed_timestamp.finish()),
                array(self.trace_id.finish()),
                array(self.span_id.finish()),
                array(self.service_name.finish()),
                array(self.service_namespace.finish()),
                array(self.service_instance_id.finish()),
                array(self.severity_number.finish()),
                array(self.severity_text.finish()),
                array(self.body.finish()),
                array(self.resource_attributes.finish()),
                array(self.scope_name.finish()),
                array(self.scope_version.finish()),
                array(self.scope_attributes.finish()),
                array(self.log_attributes.finish()),
            ],
        )
    }
}
