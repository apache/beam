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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Mutation;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.FailureMode;

import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})

@AutoService(SchemaTransformProvider.class)
public class SpannerWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        SpannerWriteSchemaTransformProvider.SpannerWriteSchemaTransformConfiguration> {

  @Override
  protected @UnknownKeyFor @NonNull @Initialized Class<SpannerWriteSchemaTransformConfiguration>
      configurationClass() {
    return SpannerWriteSchemaTransformConfiguration.class;
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      SpannerWriteSchemaTransformConfiguration configuration) {
    return new SpannerSchemaTransformWrite(configuration);
  }

  static class SpannerSchemaTransformWrite extends SchemaTransform implements Serializable {
    private final SpannerWriteSchemaTransformConfiguration configuration;

    SpannerSchemaTransformWrite(SpannerWriteSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    // A generic counter for PCollection of Row. Will be initialized with the given
    // name argument. Performs element-wise counter of the input PCollection.
    private static class ElementCounterFn extends DoFn<Row, Row> {

      private Counter spannerGenericElementCounter;
      private Long elementsInBundle = 0L;

      ElementCounterFn(String name) {
        this.spannerGenericElementCounter =
            Metrics.counter(SpannerSchemaTransformWrite.class, name);
      }

      @ProcessElement
      public void process(ProcessContext c) {
        this.elementsInBundle += 1;
        c.output(c.element());
      }

      @FinishBundle
      public void finish(FinishBundleContext c) {
        this.spannerGenericElementCounter.inc(this.elementsInBundle);
        this.elementsInBundle = 0L;
      }
    }

    private static class NoOutputDoFn<T> extends DoFn<T, Row> {
      @ProcessElement
      public void process(ProcessContext c) {}
    }

    @Override
    public PCollectionRowTuple expand(@NonNull PCollectionRowTuple input) {
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());
      FailureMode failureMode;
      if (handleErrors)
        failureMode = SpannerIO.FailureMode.REPORT_FAILURES;
      else
        failureMode = SpannerIO.FailureMode.FAIL_FAST;

      SpannerWriteResult result =
          input
              .get("input")
              .apply(
                  MapElements.via(
                      new SimpleFunction<Row, Mutation>(
                          row ->
                              MutationUtils.createMutationFromBeamRows(
                                  Mutation.newInsertOrUpdateBuilder(configuration.getTableId()),
                                  Objects.requireNonNull(row))) {}))
              .apply(
                  SpannerIO.write()
                      .withProjectId(configuration.getProjectId())
                      .withDatabaseId(configuration.getDatabaseId())
                      .withInstanceId(configuration.getInstanceId())
                      .withFailureMode(failureMode));

      PCollection<Row> postWrite =
        result
            .getFailedMutations()
            .apply("post-write", ParDo.of(new NoOutputDoFn<MutationGroup>()))
            .setRowSchema(Schema.of());

      if (!handleErrors) 
        return PCollectionRowTuple.of("post-write", postWrite);
    
      Schema inputSchema = input.get("input").getSchema();
      Schema failureSchema =
          Schema.of(
            Field.of("failed_row", FieldType.STRING),
            Field.of("error_message", FieldType.row(inputSchema)));

      PCollection<Row> failures =
          result
              .getFailedMutations()
              .apply(
                  FlatMapElements.into(TypeDescriptors.rows())
                      .via(
                          mtg ->
                              Objects.requireNonNull(mtg).attached().stream()
                                  .map(
                                      mutation ->
                                          Row.withSchema(failureSchema)
                                            .withFieldValue("error_message", String.format("%s operation failed at instance: %s, database: %s, table: %s",
                                              mutation.getOperation().toString(), configuration.getInstanceId(), configuration.getDatabaseId(), mutation.getTable()))
                                            //.withFieldValue("failed_row", Iterators.toString(mutation.getValues().iterator()))
                                            .withFieldValue("failed_row", MutationUtils.createRowFromMutation(inputSchema, mutation))
                                          .build())
                                          .collect(Collectors.toList())))
                                  
              .setRowSchema(failureSchema)
              .apply("error-count", ParDo.of(new ElementCounterFn("Spanner-write-error-counter")))
              .setRowSchema(failureSchema);

      return PCollectionRowTuple.of("post-write", postWrite).and(configuration.getErrorHandling().getOutput(), failures);
    }
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:spanner_write:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.singletonList("input");
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
      return Collections.singletonList("post-write");
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class SpannerWriteSchemaTransformConfiguration implements Serializable {

    @SchemaFieldDescription("Specifies the GCP project ID.")
    @Nullable
    public abstract String getProjectId();

    @SchemaFieldDescription("Specifies the Cloud Spanner instance.")
    @Nullable
    public abstract String getInstanceId();

    @SchemaFieldDescription("Specifies the Cloud Spanner database.")
    @Nullable
    public abstract String getDatabaseId();

    @SchemaFieldDescription("Specifies the Cloud Spanner table.")
    @Nullable
    public abstract String getTableId();

    @SchemaFieldDescription("Specifies Error Handling.")
    @Nullable
    public abstract ErrorHandling getErrorHandling();

    public static Builder builder() {
      return new AutoValue_SpannerWriteSchemaTransformProvider_SpannerWriteSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    @Nullable
    public abstract static class Builder {
      public abstract Builder setProjectId(String projectId);

      public abstract Builder setInstanceId(String instanceId);

      public abstract Builder setDatabaseId(String databaseId);

      public abstract Builder setTableId(String tableId);

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract SpannerWriteSchemaTransformConfiguration build();
    }
    public void validate() {
      String invalidConfigMessage = "Invalid Spanner Write configuration: ";

      checkArgument(
        !Strings.isNullOrEmpty(this.getProjectId()),
        invalidConfigMessage + "Project ID for a Spanner Write must be specified.");

      checkArgument(
        !Strings.isNullOrEmpty(this.getInstanceId()),
        invalidConfigMessage + "Instance ID for a Spanner Write must be specified.");
      
      checkArgument(
        !Strings.isNullOrEmpty(this.getDatabaseId()),
        invalidConfigMessage + "Database ID for a Spanner Write must be specified.");

      checkArgument(
        !Strings.isNullOrEmpty(this.getTableId()),
        invalidConfigMessage + "Table ID for a Spanner Write must be specified.");
    }
  }
}