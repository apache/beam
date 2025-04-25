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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Mutation;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.FailureMode;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.NonNull;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
/**
 * A provider for writing to Cloud Spanner using the Schema Transform Provider.
 *
 * <p>This provider enables writing to Cloud Spanner with support for error handling during the
 * write process. Configuration is managed through the {@link
 * SpannerWriteSchemaTransformConfiguration} class, allowing users to specify project, instance,
 * database, table, and error handling strategies.
 *
 * <p>The transformation uses the {@link SpannerIO} to perform the write operation and provides
 * options to handle failed mutations, either by throwing an error, or passing the failed mutation
 * further in the pipeline for dealing with accordingly.
 */
@AutoService(SchemaTransformProvider.class)
public class SpannerWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        SpannerWriteSchemaTransformProvider.SpannerWriteSchemaTransformConfiguration> {

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:spanner_write:v1";
  }

  @Override
  public String description() {
    return "Performs a bulk write to a Google Cloud Spanner table.\n"
        + "\n"
        + "Example configuration for performing a write to a single table: ::\n"
        + "\n"
        + "    - type: ReadFromSpanner\n"
        + "      config:\n"
        + "        project_id: 'my-project-id'\n"
        + "        instance_id: 'my-instance-id'\n"
        + "        database_id: 'my-database'\n"
        + "        table: 'my-table'\n"
        + "\n"
        + "Note: See <a href=\""
        + "https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.html\">"
        + "SpannerIO</a> for more advanced information.";
  }

  @Override
  protected Class<SpannerWriteSchemaTransformConfiguration> configurationClass() {
    return SpannerWriteSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(SpannerWriteSchemaTransformConfiguration configuration) {
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
      SpannerWriteResult result =
          input
              .get("input")
              .apply(
                  MapElements.into(TypeDescriptor.of(Mutation.class))
                      .via(
                          (Row row) ->
                              MutationUtils.createMutationFromBeamRows(
                                  Mutation.newInsertOrUpdateBuilder(configuration.getTableId()),
                                  Objects.requireNonNull(row))))
              .apply(
                  SpannerIO.write()
                      .withProjectId(configuration.getProjectId())
                      .withDatabaseId(configuration.getDatabaseId())
                      .withInstanceId(configuration.getInstanceId())
                      .withFailureMode(
                          handleErrors ? FailureMode.REPORT_FAILURES : FailureMode.FAIL_FAST));

      PCollection<Row> postWrite =
          result
              .getFailedMutations()
              .apply("post-write", ParDo.of(new NoOutputDoFn<MutationGroup>()))
              .setRowSchema(Schema.of());

      if (!handleErrors) {
        return PCollectionRowTuple.of("post-write", postWrite);
      }

      Schema inputSchema = input.get("input").getSchema();
      Schema failureSchema = ErrorHandling.errorSchema(inputSchema);

      PCollection<Row> failures =
          result
              .getFailedMutations()
              .apply(
                  FlatMapElements.into(TypeDescriptors.rows())
                      .via(
                          mtg ->
                              StreamSupport.stream(Objects.requireNonNull(mtg).spliterator(), false)
                                  .map(
                                      mutation ->
                                          Row.withSchema(failureSchema)
                                              .withFieldValue(
                                                  "error_message",
                                                  String.format(
                                                      "%s operation failed at instance: %s, database: %s, table: %s",
                                                      mutation.getOperation(),
                                                      configuration.getInstanceId(),
                                                      configuration.getDatabaseId(),
                                                      mutation.getTable()))
                                              .withFieldValue(
                                                  "failed_row",
                                                  MutationUtils.createRowFromMutation(
                                                      inputSchema, mutation))
                                              .build())
                                  .collect(Collectors.toList())))
              .setRowSchema(failureSchema)
              .apply("error-count", ParDo.of(new ElementCounterFn("Spanner-write-error-counter")))
              .setRowSchema(failureSchema);

      return PCollectionRowTuple.of("post-write", postWrite)
          .and(configuration.getErrorHandling().getOutput(), failures);
    }
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList("input");
  }

  @Override
  public List<String> outputCollectionNames() {
    return Arrays.asList("post-write", "errors");
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class SpannerWriteSchemaTransformConfiguration implements Serializable {

    @SchemaFieldDescription("Specifies the Cloud Spanner instance.")
    public abstract String getInstanceId();

    @SchemaFieldDescription("Specifies the Cloud Spanner database.")
    public abstract String getDatabaseId();

    @SchemaFieldDescription("Specifies the Cloud Spanner table.")
    public abstract String getTableId();

    @SchemaFieldDescription("Specifies the GCP project.")
    @Nullable
    public abstract String getProjectId();

    @SchemaFieldDescription("Whether and how to handle write errors.")
    @Nullable
    public abstract ErrorHandling getErrorHandling();

    public static Builder builder() {
      return new AutoValue_SpannerWriteSchemaTransformProvider_SpannerWriteSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
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
