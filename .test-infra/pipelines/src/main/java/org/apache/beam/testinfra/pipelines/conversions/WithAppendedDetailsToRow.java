package org.apache.beam.testinfra.pipelines.conversions;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.dataflow.v1beta3.JobMetrics;
import com.google.dataflow.v1beta3.StageSummary;
import com.google.dataflow.v1beta3.WorkerDetails;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Optional;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.testinfra.pipelines.dataflow.JobMetricsWithAppendedDetails;
import org.apache.beam.testinfra.pipelines.dataflow.StageSummaryWithAppendedDetails;
import org.apache.beam.testinfra.pipelines.dataflow.WorkerDetailsWithAppendedDetails;
import org.apache.beam.testinfra.pipelines.schemas.DescriptorSchemaRegistry;
import org.apache.beam.testinfra.pipelines.schemas.GeneratedMessageV3RowBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;

public class WithAppendedDetailsToRow<T, EmbeddedT extends GeneratedMessageV3> extends PTransform<PCollection<T>, RowConversionResult<T>> {

  public static WithAppendedDetailsToRow<JobMetricsWithAppendedDetails, JobMetrics> jobMetricsWithAppendedDetailsToRow() {
    return new WithAppendedDetailsToRow<>(
        JobMetricsWithAppendedDetails.class,
        JobMetrics.getDescriptor(),
        new TupleTag<ConversionError<JobMetricsWithAppendedDetails>>(){},
        "job_metrics",
        element -> checkStateNotNull(element).getJobId(),
        element -> checkStateNotNull(element).getJobCreateTime(),
        element -> checkStateNotNull(element).getJobMetrics()
    );
  }

  public static WithAppendedDetailsToRow<StageSummaryWithAppendedDetails, StageSummary> stageSummaryWithAppendedDetailsToRow() {
    return new WithAppendedDetailsToRow<>(
        StageSummaryWithAppendedDetails.class,
        StageSummary.getDescriptor(),
        new TupleTag<ConversionError<StageSummaryWithAppendedDetails>>(){},
        "stage_summary",
        element -> checkStateNotNull(element).getJobId(),
        element -> checkStateNotNull(element).getJobCreateTime(),
        element -> checkStateNotNull(element).getStageSummary()
    );
  }

  public static WithAppendedDetailsToRow<WorkerDetailsWithAppendedDetails, WorkerDetails> workerDetailsWithAppendedDetailsToRow() {
    return new WithAppendedDetailsToRow<>(
        WorkerDetailsWithAppendedDetails.class,
        WorkerDetails.getDescriptor(),
        new TupleTag<ConversionError<WorkerDetailsWithAppendedDetails>>(){},
        "worker_details",
        element -> checkStateNotNull(element).getJobId(),
        element -> checkStateNotNull(element).getJobCreateTime(),
        element -> checkStateNotNull(element).getWorkerDetails()
    );
  }

  private static final TupleTag<Row> SUCCESS = new TupleTag<Row>(){};

  private static final DescriptorSchemaRegistry SCHEMA_REGISTRY = new DescriptorSchemaRegistry();
  private static final Field JOB_ID_FIELD = Field.of("job_id", FieldType.STRING);

  private static final Field JOB_CREATE_TIME = Field.of("job_create_time", FieldType.DATETIME);

  private final Class<T> containerClass;

  private final Descriptor descriptor;
  private final TupleTag<ConversionError<T>> failureTag;

  private final String embeddedFieldName;

  private final SerializableFunction<@NonNull T, @NonNull String> jobIdSupplier;

  private final SerializableFunction<@NonNull T, @NonNull Instant> jobCreateTimeSupplier;

  private final SerializableFunction<@NonNull T, @NonNull EmbeddedT> embeddedInstanceSupplier;

  private WithAppendedDetailsToRow(
      Class<T> containerClass,
      Descriptor descriptor,
      TupleTag<ConversionError<T>> failureTag,
      String embeddedFieldName,
      SerializableFunction<@NonNull T, @NonNull String> jobIdSupplier,
      SerializableFunction<@NonNull T, @NonNull Instant> jobCreateTimeSupplier,
      SerializableFunction<@NonNull T, @NonNull EmbeddedT> embeddedInstanceSupplier
  ) {
    this.containerClass = containerClass;
    this.descriptor = descriptor;
    this.failureTag = failureTag;
    this.embeddedFieldName = embeddedFieldName;
    this.jobIdSupplier = jobIdSupplier;
    this.jobCreateTimeSupplier = jobCreateTimeSupplier;
    this.embeddedInstanceSupplier = embeddedInstanceSupplier;
  }

  @Override
  public RowConversionResult<T> expand(PCollection<T> input) {
    SCHEMA_REGISTRY.build(descriptor);
    Schema embeddedSchema = checkStateNotNull(SCHEMA_REGISTRY.getSchema(descriptor));
    FieldType embeddedType = FieldType.row(embeddedSchema);
    Field embeddedField = Field.of(embeddedFieldName, embeddedType);
    Schema schema = Schema.of(JOB_ID_FIELD, JOB_CREATE_TIME, embeddedField);

    PCollectionTuple pct = input.apply(containerClass.getSimpleName() + " ToRowFn",
        ParDo.of(new ToRowFn<>(schema, this)).withOutputTags(
            SUCCESS,
            TupleTagList.of(failureTag)
        ));

    return new RowConversionResult<>(schema, SUCCESS, failureTag, pct);
  }

  private class ToRowFn<T, EmbeddedT extends GeneratedMessageV3> extends DoFn<T, Row> {
    private final @NonNull Schema schema;
    private final WithAppendedDetailsToRow<T, EmbeddedT> spec;

    private ToRowFn(@NonNull Schema schema, WithAppendedDetailsToRow<T, EmbeddedT> spec) {
      this.schema = schema;
      this.spec = spec;
    }

    @ProcessElement
    public void process(@Element T element, MultiOutputReceiver receiver) {
      String id = checkStateNotNull(spec.jobIdSupplier.apply(element));
      Instant createTime = checkStateNotNull(spec.jobCreateTimeSupplier.apply(element));
      EmbeddedT embeddedInstance = checkStateNotNull(spec.embeddedInstanceSupplier.apply(element));
      GeneratedMessageV3RowBuilder<EmbeddedT> builder = GeneratedMessageV3RowBuilder.of(
          SCHEMA_REGISTRY,
          embeddedInstance
      );
      try {
        Row embeddedRow = checkStateNotNull(builder.build());
        Row result = Row.withSchema(schema)
            .withFieldValue(JOB_ID_FIELD.getName(), id)
            .withFieldValue(JOB_CREATE_TIME.getName(), createTime)
            .withFieldValue(spec.embeddedFieldName, embeddedRow)
            .build();
        receiver.get(SUCCESS).output(result);
      } catch(RuntimeException e) {
        receiver.get(spec.failureTag).output(
            ConversionError.<T>builder()
                .setObservedTime(Instant.now())
                .setSource(element)
                .setMessage(Optional.ofNullable(e.getMessage()).orElse(""))
                .setStackTrace(Throwables.getStackTraceAsString(e))
                .build()
        );
      }
    }
  }
}
