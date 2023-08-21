/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.testinfra.pipelines.conversions;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.dataflow.v1beta3.JobMetrics;
import com.google.dataflow.v1beta3.StageSummary;
import com.google.dataflow.v1beta3.WorkerDetails;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Internal;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;

/**
 * Convenience methods for converted enriched types such as {@link JobMetricsWithAppendedDetails}
 * and {@link StageSummaryWithAppendedDetails} into {@link Row}s.
 */
@Internal
public class WithAppendedDetailsToRow<AppendedDetailsT, EmbeddedT extends GeneratedMessageV3>
    extends PTransform<
        PCollection<AppendedDetailsT>, RowConversionResult<AppendedDetailsT, ConversionError>> {

  public static WithAppendedDetailsToRow<JobMetricsWithAppendedDetails, JobMetrics>
      jobMetricsWithAppendedDetailsToRow() {
    return new WithAppendedDetailsToRow<>(
        JobMetricsWithAppendedDetails.class,
        JobMetrics.class,
        new TupleTag<ConversionError>() {},
        "job_metrics",
        clazz -> JobMetrics.getDescriptor(),
        element ->
            checkStateNotNull(
                    element,
                    "%s element null for jobId supplier in %s",
                    JobMetricsWithAppendedDetails.class,
                    WithAppendedDetailsToRow.class)
                .getJobId(),
        element ->
            checkStateNotNull(
                    element,
                    "%s element null for jobCreateTime supplier in %s",
                    JobMetricsWithAppendedDetails.class,
                    WithAppendedDetailsToRow.class)
                .getJobCreateTime(),
        element ->
            checkStateNotNull(
                    element,
                    "%s element null for %s supplier in %s",
                    JobMetricsWithAppendedDetails.class,
                    JobMetrics.class,
                    WithAppendedDetailsToRow.class)
                .getJobMetrics());
  }

  public static WithAppendedDetailsToRow<StageSummaryWithAppendedDetails, StageSummary>
      stageSummaryWithAppendedDetailsToRow() {
    return new WithAppendedDetailsToRow<>(
        StageSummaryWithAppendedDetails.class,
        StageSummary.class,
        new TupleTag<ConversionError>() {},
        "stage_summary",
        clazz -> StageSummary.getDescriptor(),
        element ->
            checkStateNotNull(
                    element,
                    "%s element null for jobId supplier in %s",
                    StageSummaryWithAppendedDetails.class,
                    WithAppendedDetailsToRow.class)
                .getJobId(),
        element ->
            checkStateNotNull(
                    element,
                    "%s element null for jobCreateTime supplier in %s",
                    StageSummaryWithAppendedDetails.class,
                    WithAppendedDetailsToRow.class)
                .getJobCreateTime(),
        element ->
            checkStateNotNull(
                    element,
                    "%s element null for %s supplier in %s",
                    StageSummaryWithAppendedDetails.class,
                    StageSummary.class,
                    WithAppendedDetailsToRow.class)
                .getStageSummary());
  }

  public static WithAppendedDetailsToRow<WorkerDetailsWithAppendedDetails, WorkerDetails>
      workerDetailsWithAppendedDetailsToRow() {
    return new WithAppendedDetailsToRow<>(
        WorkerDetailsWithAppendedDetails.class,
        WorkerDetails.class,
        new TupleTag<ConversionError>() {},
        "worker_details",
        clazz -> WorkerDetails.getDescriptor(),
        element ->
            checkStateNotNull(
                    element,
                    "%s element null for jobId supplier in %s",
                    WorkerDetailsWithAppendedDetails.class,
                    WithAppendedDetailsToRow.class)
                .getJobId(),
        element ->
            checkStateNotNull(
                    element,
                    "%s element null for jobCreateTime supplier in %s",
                    WorkerDetailsWithAppendedDetails.class,
                    WithAppendedDetailsToRow.class)
                .getJobCreateTime(),
        element ->
            checkStateNotNull(
                    element,
                    "%s element null for %s supplier in %s",
                    WorkerDetailsWithAppendedDetails.class,
                    WorkerDetails.class,
                    WithAppendedDetailsToRow.class)
                .getWorkerDetails());
  }

  private static final TupleTag<Row> SUCCESS = new TupleTag<Row>() {};

  static final Field JOB_ID_FIELD = Field.of("job_id", FieldType.STRING);

  static final Field JOB_CREATE_TIME = Field.of("job_create_time", FieldType.DATETIME);

  private final Class<AppendedDetailsT> containerClass;

  private final Class<EmbeddedT> embeddedTClass;

  private final TupleTag<ConversionError> failureTag;

  private final String embeddedFieldName;

  private final SerializableFunction<@NonNull Class<EmbeddedT>, @NonNull Descriptor>
      descriptorSupplier;

  private final SerializableFunction<@NonNull AppendedDetailsT, @NonNull String> jobIdSupplier;

  private final SerializableFunction<@NonNull AppendedDetailsT, @NonNull Instant>
      jobCreateTimeSupplier;

  private final SerializableFunction<@NonNull AppendedDetailsT, @NonNull EmbeddedT>
      embeddedInstanceSupplier;

  private WithAppendedDetailsToRow(
      Class<AppendedDetailsT> containerClass,
      Class<EmbeddedT> embeddedTClass,
      TupleTag<ConversionError> failureTag,
      String embeddedFieldName,
      SerializableFunction<@NonNull Class<EmbeddedT>, @NonNull Descriptor> descriptorSupplier,
      SerializableFunction<@NonNull AppendedDetailsT, @NonNull String> jobIdSupplier,
      SerializableFunction<@NonNull AppendedDetailsT, @NonNull Instant> jobCreateTimeSupplier,
      SerializableFunction<@NonNull AppendedDetailsT, @NonNull EmbeddedT>
          embeddedInstanceSupplier) {
    this.containerClass = containerClass;
    this.embeddedTClass = embeddedTClass;
    this.failureTag = failureTag;
    this.embeddedFieldName = embeddedFieldName;
    this.descriptorSupplier = descriptorSupplier;
    this.jobIdSupplier = jobIdSupplier;
    this.jobCreateTimeSupplier = jobCreateTimeSupplier;
    this.embeddedInstanceSupplier = embeddedInstanceSupplier;
  }

  @Override
  public RowConversionResult<AppendedDetailsT, ConversionError> expand(
      PCollection<AppendedDetailsT> input) {
    Descriptor descriptor = descriptorSupplier.apply(embeddedTClass);
    Schema embeddedSchema =
        checkStateNotNull(
            DescriptorSchemaRegistry.INSTANCE.getOrBuild(descriptor),
            "%s null from %s: %s",
            Schema.class,
            Descriptor.class,
            descriptor.getFullName());
    FieldType embeddedType = FieldType.row(embeddedSchema);
    Field embeddedField = Field.of(embeddedFieldName, embeddedType);
    Schema schema = Schema.of(JOB_ID_FIELD, JOB_CREATE_TIME, embeddedField);

    PCollectionTuple pct =
        input.apply(
            containerClass.getSimpleName() + " ToRowFn",
            ParDo.of(new ToRowFn<>(schema, this))
                .withOutputTags(SUCCESS, TupleTagList.of(failureTag)));

    return new RowConversionResult<>(schema, SUCCESS, failureTag, pct);
  }

  private static class ToRowFn<AppendedDetailsT, EmbeddedT extends GeneratedMessageV3>
      extends DoFn<AppendedDetailsT, Row> {
    private final @NonNull Schema schema;
    private final WithAppendedDetailsToRow<AppendedDetailsT, EmbeddedT> spec;

    private ToRowFn(
        @NonNull Schema schema, WithAppendedDetailsToRow<AppendedDetailsT, EmbeddedT> spec) {
      this.schema = schema;
      this.spec = spec;
    }

    @ProcessElement
    public void process(@Element @NonNull AppendedDetailsT element, MultiOutputReceiver receiver) {
      String id = spec.jobIdSupplier.apply(element);
      Instant createTime = spec.jobCreateTimeSupplier.apply(element);
      EmbeddedT embeddedInstance = spec.embeddedInstanceSupplier.apply(element);
      GeneratedMessageV3RowBuilder<EmbeddedT> builder =
          GeneratedMessageV3RowBuilder.of(embeddedInstance);
      try {
        Row embeddedRow =
            checkStateNotNull(
                builder.build(), "null Row from build of %s type", embeddedInstance.getClass());
        Row result =
            Row.withSchema(schema)
                .withFieldValue(JOB_ID_FIELD.getName(), id)
                .withFieldValue(JOB_CREATE_TIME.getName(), createTime)
                .withFieldValue(spec.embeddedFieldName, embeddedRow)
                .build();
        receiver.get(SUCCESS).output(result);
      } catch (IllegalStateException e) {
        receiver
            .get(spec.failureTag)
            .output(
                ConversionError.builder()
                    .setObservedTime(Instant.now())
                    .setMessage(Optional.ofNullable(e.getMessage()).orElse(""))
                    .setStackTrace(Throwables.getStackTraceAsString(e))
                    .build());
      }
    }
  }
}
