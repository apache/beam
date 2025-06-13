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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Struct;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
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
import org.apache.beam.sdk.transforms.DoFn.FinishBundle;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/** A provider for reading from Cloud Spanner using a Schema Transform Provider. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
/**
 * A provider for reading from Cloud Spanner using a Schema Transform Provider.
 *
 * <p>This provider enables reading from Cloud Spanner using a specified SQL query or by directly
 * accessing a table and its columns. It supports configuration through the {@link
 * SpannerReadSchemaTransformConfiguration} class, allowing users to specify project, instance,
 * database, table, query, and columns.
 *
 * <p>The transformation leverages the {@link SpannerIO} to perform the read operation and maps the
 * results to Beam rows, preserving the schema.
 */
@AutoService(SchemaTransformProvider.class)
public class SpannerReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        SpannerReadSchemaTransformProvider.SpannerReadSchemaTransformConfiguration> {

  public static final TupleTag<Row> OUTPUT_TAG = new TupleTag<Row>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  public static final Schema ERROR_SCHEMA =
      Schema.builder().addStringField("error").addStringField("row").build();

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:spanner_read:v1";
  }

  @Override
  public String description() {
    return "Performs a Bulk read from Google Cloud Spanner using a specified SQL query or "
        + "by directly accessing a single table and its columns.\n"
        + "\n"
        + "Both Query and Read APIs are supported. See more information about "
        + "<a href=\"https://cloud.google.com/spanner/docs/reads\">reading from Cloud Spanner</a>.\n"
        + "\n"
        + "Example configuration for performing a read using a SQL query: ::\n"
        + "\n"
        + "    - type: ReadFromSpanner\n"
        + "      config:\n"
        + "        instance_id: 'my-instance-id'\n"
        + "        database_id: 'my-database'\n"
        + "        query: 'SELECT * FROM table'\n"
        + "\n"
        + "It is also possible to read a table by specifying a table name and a list of columns. For "
        + "example, the following configuration will perform a read on an entire table: ::\n"
        + "\n"
        + "    - type: ReadFromSpanner\n"
        + "      config:\n"
        + "        instance_id: 'my-instance-id'\n"
        + "        database_id: 'my-database'\n"
        + "        table: 'my-table'\n"
        + "        columns: ['col1', 'col2']\n"
        + "\n"
        + "Additionally, to read using a <a href=\"https://cloud.google.com/spanner/docs/secondary-indexes\">"
        + "Secondary Index</a>, specify the index name: ::"
        + "\n"
        + "    - type: ReadFromSpanner\n"
        + "      config:\n"
        + "        instance_id: 'my-instance-id'\n"
        + "        database_id: 'my-database'\n"
        + "        table: 'my-table'\n"
        + "        index: 'my-index'\n"
        + "        columns: ['col1', 'col2']\n"
        + "\n"
        + "#### Advanced Usage\n"
        + "\n"
        + "Reads by default use the <a href=\"https://cloud.google.com/spanner/docs/reads#read_data_in_parallel\">"
        + "PartitionQuery API</a> which enforces some limitations on the type of queries that can be used so that "
        + "the data can be read in parallel. If the query is not supported by the PartitionQuery API, then you "
        + "can specify a non-partitioned read by setting batching to false.\n"
        + "\n"
        + "For example: ::"
        + "\n"
        + "    - type: ReadFromSpanner\n"
        + "      config:\n"
        + "        batching: false\n"
        + "        ...\n"
        + "\n"
        + "Note: See <a href=\""
        + "https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.html\">"
        + "SpannerIO</a> for more advanced information.";
  }

  static class SpannerSchemaTransformRead extends SchemaTransform implements Serializable {
    private final SpannerReadSchemaTransformConfiguration configuration;

    SpannerSchemaTransformRead(SpannerReadSchemaTransformConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      checkNotNull(input, "Input to SpannerReadSchemaTransform cannot be null.");
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());
      SpannerIO.Read read =
          SpannerIO.readWithSchema()
              .withProjectId(configuration.getProjectId())
              .withInstanceId(configuration.getInstanceId())
              .withDatabaseId(configuration.getDatabaseId());

      if (!Strings.isNullOrEmpty(configuration.getQuery())) {
        read = read.withQuery(configuration.getQuery());
      } else {
        read = read.withTable(configuration.getTableId()).withColumns(configuration.getColumns());
      }
      if (!Strings.isNullOrEmpty(configuration.getIndex())) {
        read = read.withIndex(configuration.getIndex());
      }
      if (Boolean.FALSE.equals(configuration.getBatching())) {
        read = read.withBatching(false);
      }
      PCollection<Struct> spannerRows = input.getPipeline().apply(read);
      Schema schema = spannerRows.getSchema();

      PCollectionTuple outputTuple =
          spannerRows.apply(
              ParDo.of(
                      new ErrorFn("spanner-read-error-counter", ERROR_SCHEMA, schema, handleErrors))
                  .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      PCollectionRowTuple outputRows =
          PCollectionRowTuple.of("output", outputTuple.get(OUTPUT_TAG).setRowSchema(schema));

      // Error handling
      PCollection<Row> errorOutput = outputTuple.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA);
      if (handleErrors) {
        outputRows =
            outputRows.and(
                checkArgumentNotNull(configuration.getErrorHandling()).getOutput(), errorOutput);
      }

      return outputRows;
    }
  }

  public static class ErrorFn extends DoFn<Struct, Row> {
    private final Counter errorCounter;
    private Long errorsInBundle = 0L;
    private final boolean handleErrors;
    private final Schema errorSchema;
    private final Schema schema;

    public ErrorFn(String name, Schema errorSchema, Schema schema, boolean handleErrors) {
      this.errorCounter = Metrics.counter(SpannerReadSchemaTransformProvider.class, name);
      this.handleErrors = handleErrors;
      this.errorSchema = errorSchema;
      this.schema = schema;
    }

    @ProcessElement
    public void processElement(@DoFn.Element Struct struct, MultiOutputReceiver receiver) {
      Row mappedRow = null;
      try {
        mappedRow = StructUtils.structToBeamRow(struct, schema);
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(e);
        }
        errorsInBundle += 1;
        receiver
            .get(ERROR_TAG)
            .output(
                Row.withSchema(errorSchema).addValues(e.getMessage(), struct.toString()).build());
      }
      if (mappedRow != null) {
        receiver.get(OUTPUT_TAG).output(mappedRow);
      }
    }

    @FinishBundle
    public void finish(FinishBundleContext c) {
      errorCounter.inc(errorsInBundle);
      errorsInBundle = 0L;
    }
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Arrays.asList("output", "errors");
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class SpannerReadSchemaTransformConfiguration implements Serializable {
    @AutoValue.Builder
    @Nullable
    public abstract static class Builder {
      public abstract Builder setProjectId(String projectId);

      public abstract Builder setInstanceId(String instanceId);

      public abstract Builder setDatabaseId(String databaseId);

      public abstract Builder setTableId(String tableId);

      public abstract Builder setQuery(String query);

      public abstract Builder setColumns(List<String> columns);

      public abstract Builder setIndex(String index);

      public abstract Builder setBatching(Boolean batching);

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract SpannerReadSchemaTransformConfiguration build();
    }

    public void validate() {
      String invalidConfigMessage = "Invalid Cloud Spanner Read configuration: ";
      checkArgument(
          !Strings.isNullOrEmpty(this.getInstanceId()),
          invalidConfigMessage + "Instance ID must be specified.");
      checkArgument(
          !Strings.isNullOrEmpty(this.getDatabaseId()),
          invalidConfigMessage + "Database ID must be specified.");
      if (Strings.isNullOrEmpty(this.getQuery())) {
        checkArgument(
            !Strings.isNullOrEmpty(this.getTableId()),
            invalidConfigMessage + "Table name must be specified for table read.");
        checkArgument(
            this.getColumns() != null && !this.getColumns().isEmpty(),
            invalidConfigMessage + "Columns must be specified for table read.");
      } else {
        checkArgument(
            !Strings.isNullOrEmpty(this.getQuery()),
            invalidConfigMessage + "Query must be specified for query read.");
        checkArgument(
            Strings.isNullOrEmpty(this.getTableId()),
            invalidConfigMessage + "Table name should not be specified when using a query.");
        checkArgument(
            this.getColumns() == null || this.getColumns().isEmpty(),
            invalidConfigMessage + "Columns should not be specified when using a query.");
      }
    }

    public static Builder builder() {
      return new AutoValue_SpannerReadSchemaTransformProvider_SpannerReadSchemaTransformConfiguration
          .Builder();
    }

    @SchemaFieldDescription("Specifies the Cloud Spanner instance.")
    public abstract String getInstanceId();

    @SchemaFieldDescription("Specifies the Cloud Spanner database.")
    public abstract String getDatabaseId();

    @SchemaFieldDescription("Specifies the GCP project ID.")
    @Nullable
    public abstract String getProjectId();

    @SchemaFieldDescription("Specifies the Cloud Spanner table.")
    @Nullable
    public abstract String getTableId();

    @SchemaFieldDescription("Specifies the SQL query to execute.")
    @Nullable
    public abstract String getQuery();

    @SchemaFieldDescription(
        "Specifies the columns to read from the table. This parameter is required when table is specified.")
    @Nullable
    public abstract List<String> getColumns();

    @SchemaFieldDescription(
        "Specifies the Index to read from. This parameter can only be specified when using table.")
    @Nullable
    public abstract String getIndex();

    @SchemaFieldDescription(
        "Set to false to disable batching. Useful when using a query that is not compatible with the PartitionQuery API. Defaults to true.")
    @Nullable
    public abstract Boolean getBatching();

    @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
    @Nullable
    public abstract ErrorHandling getErrorHandling();
  }

  @Override
  protected Class<SpannerReadSchemaTransformConfiguration> configurationClass() {
    return SpannerReadSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(SpannerReadSchemaTransformConfiguration configuration) {
    return new SpannerSchemaTransformRead(configuration);
  }
}
