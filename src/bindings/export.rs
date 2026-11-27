use std::panic::AssertUnwindSafe;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch, StructArray};

use crate::{ArrowProfile, MetricBatches};

pub(crate) fn ffi_boundary<T>(on_panic: T, body: impl FnOnce() -> T) -> T {
    #[allow(clippy::disallowed_methods)]
    std::panic::catch_unwind(AssertUnwindSafe(body)).unwrap_or(on_panic)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExportError {
    Unrepresentable,
    SchemaNotExportable,
}

pub(crate) struct ExportedBatch {
    array: FFI_ArrowArray,
    schema: FFI_ArrowSchema,
}

impl ExportedBatch {
    pub(crate) fn new(profile: ArrowProfile, batch: RecordBatch) -> Result<Self, ExportError> {
        let batch = profile
            .apply(batch)
            .map_err(|_| ExportError::Unrepresentable)?;
        let schema = FFI_ArrowSchema::try_from(batch.schema().as_ref())
            .map_err(|_| ExportError::SchemaNotExportable)?;
        let array = FFI_ArrowArray::new(&StructArray::from(batch).into_data());
        Ok(Self { array, schema })
    }

    /// Hands ownership of both structures to the caller.
    ///
    /// # Safety
    ///
    /// Both pointers must be valid for writes. Their previous contents are
    /// overwritten without being released.
    pub(crate) unsafe fn write_to(self, array: *mut FFI_ArrowArray, schema: *mut FFI_ArrowSchema) {
        std::ptr::write(array, self.array);
        std::ptr::write(schema, self.schema);
    }

    #[cfg(all(feature = "server", not(target_arch = "wasm32")))]
    pub(crate) fn lend<R>(
        batches: &mut [(crate::ffi::OtlpSignalType, Self)],
        f: impl FnOnce(&[crate::server_ffi::OtlpIngestBatch]) -> R,
    ) -> R {
        let borrowed: Vec<_> = batches
            .iter_mut()
            .map(|(signal_type, batch)| crate::server_ffi::OtlpIngestBatch {
                signal_type: *signal_type,
                array: &mut batch.array,
                schema: &mut batch.schema,
            })
            .collect();
        f(&borrowed)
    }

    fn into_c(self) -> OtlpArrowBatch {
        OtlpArrowBatch {
            array: self.array,
            schema: self.schema,
            present: 1,
        }
    }
}

/// One optional Arrow batch inside [`OtlpMetricsArrowBatches`].
///
/// If `present` is 0, `array` and `schema` must not be released. If it is
/// non-zero, the caller owns both and must call their release callbacks.
#[repr(C)]
pub struct OtlpArrowBatch {
    pub array: FFI_ArrowArray,
    pub schema: FFI_ArrowSchema,
    pub present: std::ffi::c_int,
}

impl OtlpArrowBatch {
    fn absent() -> Self {
        Self {
            array: FFI_ArrowArray::empty(),
            schema: FFI_ArrowSchema::empty(),
            present: 0,
        }
    }
}

/// Output batches for all normalized metric shapes.
#[repr(C)]
pub struct OtlpMetricsArrowBatches {
    pub gauge: OtlpArrowBatch,
    pub sum: OtlpArrowBatch,
    pub histogram: OtlpArrowBatch,
    pub exp_histogram: OtlpArrowBatch,
    pub skipped_summaries: u64,
    pub skipped_nan_values: u64,
    pub skipped_infinity_values: u64,
    pub skipped_missing_values: u64,
}

impl OtlpMetricsArrowBatches {
    pub(crate) fn absent() -> Self {
        Self {
            gauge: OtlpArrowBatch::absent(),
            sum: OtlpArrowBatch::absent(),
            histogram: OtlpArrowBatch::absent(),
            exp_histogram: OtlpArrowBatch::absent(),
            skipped_summaries: 0,
            skipped_nan_values: 0,
            skipped_infinity_values: 0,
            skipped_missing_values: 0,
        }
    }
}

pub(crate) struct ExportedMetrics {
    gauge: Option<ExportedBatch>,
    sum: Option<ExportedBatch>,
    histogram: Option<ExportedBatch>,
    exp_histogram: Option<ExportedBatch>,
    skipped: crate::SkippedMetrics,
}

impl ExportedMetrics {
    pub(crate) fn new(profile: ArrowProfile, batches: MetricBatches) -> Result<Self, ExportError> {
        let export = |batch: Option<RecordBatch>| {
            batch
                .map(|batch| ExportedBatch::new(profile, batch))
                .transpose()
        };
        Ok(Self {
            gauge: export(batches.gauge)?,
            sum: export(batches.sum)?,
            histogram: export(batches.histogram)?,
            exp_histogram: export(batches.exp_histogram)?,
            skipped: batches.skipped,
        })
    }

    /// Hands ownership of every present batch to the caller.
    ///
    /// # Safety
    ///
    /// `out` must be valid for writes. Its previous contents are overwritten
    /// without being released.
    pub(crate) unsafe fn write_to(self, out: *mut OtlpMetricsArrowBatches) {
        let present = |batch: Option<ExportedBatch>| {
            batch.map_or_else(OtlpArrowBatch::absent, ExportedBatch::into_c)
        };
        std::ptr::write(
            out,
            OtlpMetricsArrowBatches {
                gauge: present(self.gauge),
                sum: present(self.sum),
                histogram: present(self.histogram),
                exp_histogram: present(self.exp_histogram),
                skipped_summaries: self.skipped.summaries as u64,
                skipped_nan_values: self.skipped.nan_values as u64,
                skipped_infinity_values: self.skipped.infinity_values as u64,
                skipped_missing_values: self.skipped.missing_values as u64,
            },
        );
    }

    #[cfg(all(feature = "server", not(target_arch = "wasm32")))]
    pub(crate) fn into_shapes(
        self,
    ) -> impl Iterator<Item = (crate::ffi::OtlpSignalType, ExportedBatch)> {
        use crate::ffi::OtlpSignalType;
        [
            (OtlpSignalType::MetricsGauge, self.gauge),
            (OtlpSignalType::MetricsSum, self.sum),
            (OtlpSignalType::MetricsHistogram, self.histogram),
            (OtlpSignalType::MetricsExpHistogram, self.exp_histogram),
        ]
        .into_iter()
        .filter_map(|(signal, batch)| batch.map(|batch| (signal, batch)))
    }
}
