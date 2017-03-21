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
package org.apache.beam.sdk.io.gcp.bigquery;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.CountingOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TableRowJsonCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.AvroSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.FileIOChannelFactory;
import org.apache.beam.sdk.util.GcsIOChannelFactory;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.GcsUtil.GcsUtilFactory;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.Reshuffle;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s for reading and writing
 * <a href="https://developers.google.com/bigquery/">BigQuery</a> tables.
 *
 * <h3>Table References</h3>
 *
 * <p>A fully-qualified BigQuery table name consists of three components:
 * <ul>
 *   <li>{@code projectId}: the Cloud project id (defaults to
 *       {@link GcpOptions#getProject()}).
 *   <li>{@code datasetId}: the BigQuery dataset id, unique within a project.
 *   <li>{@code tableId}: a table id, unique within a dataset.
 * </ul>
 *
 * <p>BigQuery table references are stored as a {@link TableReference}, which comes
 * from the <a href="https://cloud.google.com/bigquery/client-libraries">
 * BigQuery Java Client API</a>.
 * Tables can be referred to as Strings, with or without the {@code projectId}.
 * A helper function is provided ({@link BigQueryIO#parseTableSpec(String)})
 * that parses the following string forms into a {@link TableReference}:
 *
 * <ul>
 *   <li>[{@code project_id}]:[{@code dataset_id}].[{@code table_id}]
 *   <li>[{@code dataset_id}].[{@code table_id}]
 * </ul>
 *
 * <h3>Reading</h3>
 *
 * <p>To read from a BigQuery table, apply a {@link BigQueryIO.Read} transformation.
 * This produces a {@link PCollection} of {@link TableRow TableRows} as output:
 * <pre>{@code
 * PCollection<TableRow> weatherData = pipeline.apply(
 *     BigQueryIO.read().from("clouddataflow-readonly:samples.weather_stations"));
 * }</pre>
 *
 * <p>See {@link TableRow} for more information on the {@link TableRow} object.
 *
 * <p>Users may provide a query to read from rather than reading all of a BigQuery table. If
 * specified, the result obtained by executing the specified query will be used as the data of the
 * input transform.
 *
 * <pre>{@code
 * PCollection<TableRow> meanTemperatureData = pipeline.apply(
 *     BigQueryIO.read().fromQuery("SELECT year, mean_temp FROM [samples.weather_stations]"));
 * }</pre>
 *
 * <p>When creating a BigQuery input transform, users should provide either a query or a table.
 * Pipeline construction will fail with a validation error if neither or both are specified.
 *
 * <h3>Writing</h3>
 *
 * <p>To write to a BigQuery table, apply a {@link BigQueryIO.Write} transformation.
 * This consumes either a {@link PCollection} of {@link TableRow TableRows} as input when using
 * {@link BigQueryIO#writeTableRows()} or of a user-defined type when using
 * {@link BigQueryIO#write()}. When using a user-defined type, a function must be provided to
 * turn this type into a {@link TableRow} using
 * {@link BigQueryIO.Write#withFormatFunction(SerializableFunction)}.
 * <pre>{@code
 * PCollection<TableRow> quotes = ...
 *
 * List<TableFieldSchema> fields = new ArrayList<>();
 * fields.add(new TableFieldSchema().setName("source").setType("STRING"));
 * fields.add(new TableFieldSchema().setName("quote").setType("STRING"));
 * TableSchema schema = new TableSchema().setFields(fields);
 *
 * quotes.apply(BigQueryIO.writeTableRows()
 *     .to("my-project:output.output_table")
 *     .withSchema(schema)
 *     .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
 * }</pre>
 *
 * <p>See {@link BigQueryIO.Write} for details on how to specify if a write should
 * append to an existing table, replace the table, or verify that the table is
 * empty. Note that the dataset being written to must already exist. Unbounded PCollections can only
 * be written using {@link Write.WriteDisposition#WRITE_EMPTY} or
 * {@link Write.WriteDisposition#WRITE_APPEND}.
 *
 * <h3>Sharding BigQuery output tables</h3>
 *
 * <p>A common use case is to dynamically generate BigQuery table names based on
 * the current window. To support this,
 * {@link BigQueryIO.Write#to(SerializableFunction)}
 * accepts a function mapping the current window to a tablespec. For example,
 * here's code that outputs daily tables to BigQuery:
 * <pre>{@code
 * PCollection<TableRow> quotes = ...
 * quotes.apply(Window.<TableRow>into(CalendarWindows.days(1)))
 *       .apply(BigQueryIO.Write
 *         .withSchema(schema)
 *         .to(new SerializableFunction<BoundedWindow, String>() {
 *           public String apply(BoundedWindow window) {
 *             // The cast below is safe because CalendarWindows.days(1) produces IntervalWindows.
 *             String dayString = DateTimeFormat.forPattern("yyyy_MM_dd")
 *                  .withZone(DateTimeZone.UTC)
 *                  .print(((IntervalWindow) window).start());
 *             return "my-project:output.output_table_" + dayString;
 *           }
 *         }));
 * }</pre>
 *
 * <p>Per-window tables are not yet supported in batch mode.
 *
 * <h3>Permissions</h3>
 *
 * <p>Permission requirements depend on the {@link PipelineRunner} that is used to execute the
 * Dataflow job. Please refer to the documentation of corresponding {@link PipelineRunner}s for
 * more details.
 *
 * <p>Please see <a href="https://cloud.google.com/bigquery/access-control">BigQuery Access Control
 * </a> for security and permission related information specific to BigQuery.
 */
public class BigQueryIO {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIO.class);

  /**
   * Singleton instance of the JSON factory used to read and write JSON
   * formatted rows.
   */
  private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

  /**
   * Project IDs must contain 6-63 lowercase letters, digits, or dashes.
   * IDs must start with a letter and may not end with a dash.
   * This regex isn't exact - this allows for patterns that would be rejected by
   * the service, but this is sufficient for basic parsing of table references.
   */
  private static final String PROJECT_ID_REGEXP = "[a-z][-a-z0-9:.]{4,61}[a-z0-9]";

  /**
   * Regular expression that matches Dataset IDs.
   */
  private static final String DATASET_REGEXP = "[-\\w.]{1,1024}";

  /**
   * Regular expression that matches Table IDs.
   */
  private static final String TABLE_REGEXP = "[-\\w$@]{1,1024}";

  /**
   * Matches table specifications in the form {@code "[project_id]:[dataset_id].[table_id]"} or
   * {@code "[dataset_id].[table_id]"}.
   */
  private static final String DATASET_TABLE_REGEXP =
      String.format("((?<PROJECT>%s):)?(?<DATASET>%s)\\.(?<TABLE>%s)", PROJECT_ID_REGEXP,
          DATASET_REGEXP, TABLE_REGEXP);

  private static final Pattern TABLE_SPEC = Pattern.compile(DATASET_TABLE_REGEXP);

  private static final String RESOURCE_NOT_FOUND_ERROR =
      "BigQuery %1$s not found for table \"%2$s\" . Please create the %1$s before pipeline"
          + " execution. If the %1$s is created by an earlier stage of the pipeline, this"
          + " validation can be disabled using #withoutValidation.";

  private static final String UNABLE_TO_CONFIRM_PRESENCE_OF_RESOURCE_ERROR =
      "Unable to confirm BigQuery %1$s presence for table \"%2$s\". If the %1$s is created by"
          + " an earlier stage of the pipeline, this validation can be disabled using"
          + " #withoutValidation.";

  /**
   * Parse a table specification in the form
   * {@code "[project_id]:[dataset_id].[table_id]"} or {@code "[dataset_id].[table_id]"}.
   *
   * <p>If the project id is omitted, the default project id is used.
   */
  public static TableReference parseTableSpec(String tableSpec) {
    Matcher match = TABLE_SPEC.matcher(tableSpec);
    if (!match.matches()) {
      throw new IllegalArgumentException(
          "Table reference is not in [project_id]:[dataset_id].[table_id] "
          + "format: " + tableSpec);
    }

    TableReference ref = new TableReference();
    ref.setProjectId(match.group("PROJECT"));

    return ref.setDatasetId(match.group("DATASET")).setTableId(match.group("TABLE"));
  }

  /**
   * Returns a canonical string representation of the {@link TableReference}.
   */
  public static String toTableSpec(TableReference ref) {
    StringBuilder sb = new StringBuilder();
    if (ref.getProjectId() != null) {
      sb.append(ref.getProjectId());
      sb.append(":");
    }

    sb.append(ref.getDatasetId()).append('.').append(ref.getTableId());
    return sb.toString();
  }

  @VisibleForTesting
  static class JsonSchemaToTableSchema
      implements SerializableFunction<String, TableSchema> {
    @Override
    public TableSchema apply(String from) {
      return fromJsonString(from, TableSchema.class);
    }
  }

  private static class TableSchemaToJsonSchema
      implements SerializableFunction<TableSchema, String> {
    @Override
    public String apply(TableSchema from) {
      return toJsonString(from);
    }
  }

  private static class JsonTableRefToTableRef
      implements SerializableFunction<String, TableReference> {
    @Override
    public TableReference apply(String from) {
      return fromJsonString(from, TableReference.class);
    }
  }

  private static class TableRefToTableSpec
      implements SerializableFunction<TableReference, String> {
    @Override
    public String apply(TableReference from) {
      return toTableSpec(from);
    }
  }

  private static class TableRefToJson
      implements SerializableFunction<TableReference, String> {
    @Override
    public String apply(TableReference from) {
      return toJsonString(from);
    }
  }

  private static class TableRefToProjectId
      implements SerializableFunction<TableReference, String> {
    @Override
    public String apply(TableReference from) {
      return from.getProjectId();
    }
  }

  @VisibleForTesting
  static class TableSpecToTableRef
      implements SerializableFunction<String, TableReference> {
    @Override
    public TableReference apply(String from) {
      return parseTableSpec(from);
    }
  }

  @VisibleForTesting
  static class BeamJobUuidToBigQueryJobUuid
      implements SerializableFunction<String, String> {
    @Override
    public String apply(String from) {
      return "beam_job_" + from;
    }
  }

  @VisibleForTesting
  static class CreatePerBeamJobUuid
      implements SerializableFunction<String, String> {
    private final String stepUuid;

    private CreatePerBeamJobUuid(String stepUuid) {
      this.stepUuid = stepUuid;
    }

    @Override
    public String apply(String jobUuid) {
      return stepUuid + "_" + jobUuid.replaceAll("-", "");
    }
  }

  @VisibleForTesting
  static class CreateJsonTableRefFromUuid
      implements SerializableFunction<String, TableReference> {
    private final String executingProject;

    private CreateJsonTableRefFromUuid(String executingProject) {
      this.executingProject = executingProject;
    }

    @Override
    public TableReference apply(String jobUuid) {
      String queryTempDatasetId = "temp_dataset_" + jobUuid;
      String queryTempTableId = "temp_table_" + jobUuid;
      TableReference queryTempTableRef = new TableReference()
          .setProjectId(executingProject)
          .setDatasetId(queryTempDatasetId)
          .setTableId(queryTempTableId);
      return queryTempTableRef;
    }
  }

  @Nullable
  private static ValueProvider<String> displayTable(
      @Nullable ValueProvider<TableReference> table) {
    if (table == null) {
      return null;
    }
    return NestedValueProvider.of(table, new TableRefToTableSpec());
  }


  /**
   * A formatting function that maps a TableRow to itself. This allows sending a
   * {@code PCollection<TableRow>} directly to BigQueryIO.Write.
   */
  private static final SerializableFunction<TableRow, TableRow> IDENTITY_FORMATTER =
      new SerializableFunction<TableRow, TableRow>() {
    @Override
    public TableRow apply(TableRow input) {
      return input;
    }
  };

  /**
   * A {@link PTransform} that reads from a BigQuery table and returns a
   * {@link PCollection} of {@link TableRow TableRows} containing each of the rows of the table.
   *
   * <p>Each {@link TableRow} contains values indexed by column name. Here is a
   * sample processing function that processes a "line" column from rows:
   * <pre>{@code
   * static class ExtractWordsFn extends DoFn<TableRow, String> {
   *   public void processElement(ProcessContext c) {
   *     // Get the "line" field of the TableRow object, split it into words, and emit them.
   *     TableRow row = c.element();
   *     String[] words = row.get("line").toString().split("[^a-zA-Z']+");
   *     for (String word : words) {
   *       if (!word.isEmpty()) {
   *         c.output(word);
   *       }
   *     }
   *   }
   * }}</pre>
   */
  public static Read read() {
    return new AutoValue_BigQueryIO_Read.Builder()
        .setValidate(true)
        .setBigQueryServices(new BigQueryServicesImpl())
        .build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<TableRow>> {
    @Nullable abstract ValueProvider<String> getJsonTableRef();
    @Nullable abstract ValueProvider<String> getQuery();
    abstract boolean getValidate();
    @Nullable abstract Boolean getFlattenResults();
    @Nullable abstract Boolean getUseLegacySql();
    abstract BigQueryServices getBigQueryServices();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setJsonTableRef(ValueProvider<String> jsonTableRef);
      abstract Builder setQuery(ValueProvider<String> query);
      abstract Builder setValidate(boolean validate);
      abstract Builder setFlattenResults(Boolean flattenResults);
      abstract Builder setUseLegacySql(Boolean useLegacySql);
      abstract Builder setBigQueryServices(BigQueryServices bigQueryServices);

      abstract Read build();
    }

    /** Ensures that methods of the from() / fromQuery() family are called at most once. */
    private void ensureFromNotCalledYet() {
      checkState(
          getJsonTableRef() == null && getQuery() == null, "from() or fromQuery() already called");
    }

    /**
     * Reads a BigQuery table specified as {@code "[project_id]:[dataset_id].[table_id]"} or
     * {@code "[dataset_id].[table_id]"} for tables within the current project.
     */
    public Read from(String tableSpec) {
      return from(StaticValueProvider.of(tableSpec));
    }

    /**
     * Same as {@code from(String)}, but with a {@link ValueProvider}.
     */
    public Read from(ValueProvider<String> tableSpec) {
      ensureFromNotCalledYet();
      return toBuilder()
          .setJsonTableRef(
              NestedValueProvider.of(
                  NestedValueProvider.of(tableSpec, new TableSpecToTableRef()),
                  new TableRefToJson())).build();
    }

    /**
     * Reads results received after executing the given query.
     *
     * <p>By default, the query results will be flattened -- see
     * "flattenResults" in the <a href="https://cloud.google.com/bigquery/docs/reference/v2/jobs">
     * Jobs documentation</a> for more information.  To disable flattening, use
     * {@link BigQueryIO.Read#withoutResultFlattening}.
     *
     * <p>By default, the query will use BigQuery's legacy SQL dialect. To use the BigQuery
     * Standard SQL dialect, use {@link BigQueryIO.Read#usingStandardSql}.
     */
    public Read fromQuery(String query) {
      return fromQuery(StaticValueProvider.of(query));
    }

    /**
     * Same as {@code fromQuery(String)}, but with a {@link ValueProvider}.
     */
    public Read fromQuery(ValueProvider<String> query) {
      ensureFromNotCalledYet();
      return toBuilder().setQuery(query).setFlattenResults(true).setUseLegacySql(true).build();
    }

    /**
     * Read from table specified by a {@link TableReference}.
     */
    public Read from(TableReference table) {
      return from(StaticValueProvider.of(toTableSpec(table)));
    }

    private static final String QUERY_VALIDATION_FAILURE_ERROR =
        "Validation of query \"%1$s\" failed. If the query depends on an earlier stage of the"
        + " pipeline, This validation can be disabled using #withoutValidation.";

    /**
     * Disable validation that the table exists or the query succeeds prior to pipeline
     * submission. Basic validation (such as ensuring that a query or table is specified) still
     * occurs.
     */
    public Read withoutValidation() {
      return toBuilder().setValidate(false).build();
    }

    /**
     * Disable <a href="https://cloud.google.com/bigquery/docs/reference/v2/jobs">
     * flattening of query results</a>.
     *
     * <p>Only valid when a query is used ({@link #fromQuery}). Setting this option when reading
     * from a table will cause an error during validation.
     */
    public Read withoutResultFlattening() {
      return toBuilder().setFlattenResults(false).build();
    }

    /**
     * Enables BigQuery's Standard SQL dialect when reading from a query.
     *
     * <p>Only valid when a query is used ({@link #fromQuery}). Setting this option when reading
     * from a table will cause an error during validation.
     */
    public Read usingStandardSql() {
      return toBuilder().setUseLegacySql(false).build();
    }

    @VisibleForTesting
    Read withTestServices(BigQueryServices testServices) {
      return toBuilder().setBigQueryServices(testServices).build();
    }

    @Override
    public void validate(PBegin input) {
      // Even if existence validation is disabled, we need to make sure that the BigQueryIO
      // read is properly specified.
      BigQueryOptions bqOptions = input.getPipeline().getOptions().as(BigQueryOptions.class);

      String tempLocation = bqOptions.getTempLocation();
      checkArgument(
          !Strings.isNullOrEmpty(tempLocation),
          "BigQueryIO.Read needs a GCS temp location to store temp files.");
      if (getBigQueryServices() == null) {
        try {
          GcsPath.fromUri(tempLocation);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              String.format(
                  "BigQuery temp location expected a valid 'gs://' path, but was given '%s'",
                  tempLocation),
              e);
        }
      }

      ValueProvider<TableReference> table = getTableWithDefaultProject(bqOptions);

      checkState(
          table == null || getQuery() == null,
          "Invalid BigQueryIO.Read: table reference and query may not both be set");
      checkState(
          table != null || getQuery() != null,
          "Invalid BigQueryIO.Read: one of table reference and query must be set");

      if (table != null) {
        checkState(
            getFlattenResults() == null,
            "Invalid BigQueryIO.Read: Specifies a table with a result flattening"
                + " preference, which only applies to queries");
        checkState(
            getUseLegacySql() == null,
            "Invalid BigQueryIO.Read: Specifies a table with a SQL dialect"
                + " preference, which only applies to queries");
      } else /* query != null */ {
        checkState(
            getFlattenResults() != null, "flattenResults should not be null if query is set");
        checkState(getUseLegacySql() != null, "useLegacySql should not be null if query is set");
      }

      // Note that a table or query check can fail if the table or dataset are created by
      // earlier stages of the pipeline or if a query depends on earlier stages of a pipeline.
      // For these cases the withoutValidation method can be used to disable the check.
      if (getValidate() && table != null) {
        checkState(table.isAccessible(), "Cannot call validate if table is dynamically set.");
        // Check for source table presence for early failure notification.
        DatasetService datasetService = getBigQueryServices().getDatasetService(bqOptions);
        verifyDatasetPresence(datasetService, table.get());
        verifyTablePresence(datasetService, table.get());
      } else if (getValidate() && getQuery() != null) {
        checkState(getQuery().isAccessible(), "Cannot call validate if query is dynamically set.");
        JobService jobService = getBigQueryServices().getJobService(bqOptions);
        try {
          jobService.dryRunQuery(
              bqOptions.getProject(),
              new JobConfigurationQuery()
                  .setQuery(getQuery().get())
                  .setFlattenResults(getFlattenResults())
                  .setUseLegacySql(getUseLegacySql()));
        } catch (Exception e) {
          throw new IllegalArgumentException(
              String.format(QUERY_VALIDATION_FAILURE_ERROR, getQuery().get()), e);
        }
      }
    }

    @Override
    public PCollection<TableRow> expand(PBegin input) {
      String stepUuid = randomUUIDString();
      BigQueryOptions bqOptions = input.getPipeline().getOptions().as(BigQueryOptions.class);
      ValueProvider<String> jobUuid = NestedValueProvider.of(
         StaticValueProvider.of(bqOptions.getJobName()), new CreatePerBeamJobUuid(stepUuid));
      final ValueProvider<String> jobIdToken = NestedValueProvider.of(
          jobUuid, new BeamJobUuidToBigQueryJobUuid());

      BoundedSource<TableRow> source;

      final String extractDestinationDir;
      String tempLocation = bqOptions.getTempLocation();
      try {
        IOChannelFactory factory = IOChannelUtils.getFactory(tempLocation);
        extractDestinationDir = factory.resolve(tempLocation, stepUuid);
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Failed to resolve extract destination directory in %s", tempLocation));
      }

      final String executingProject = bqOptions.getProject();
      if (getQuery() != null
          && (!getQuery().isAccessible() || !Strings.isNullOrEmpty(getQuery().get()))) {
        source =
            BigQueryQuerySource.create(
                jobIdToken,
                getQuery(),
                NestedValueProvider.of(
                    jobUuid, new CreateJsonTableRefFromUuid(executingProject)),
                getFlattenResults(),
                getUseLegacySql(),
                extractDestinationDir,
                getBigQueryServices());
      } else {
        ValueProvider<TableReference> inputTable = getTableWithDefaultProject(bqOptions);
        source = BigQueryTableSource.create(
            jobIdToken, inputTable, extractDestinationDir, getBigQueryServices(),
            StaticValueProvider.of(executingProject));
      }
      PassThroughThenCleanup.CleanupOperation cleanupOperation =
          new PassThroughThenCleanup.CleanupOperation() {
            @Override
            void cleanup(PipelineOptions options) throws Exception {
              BigQueryOptions bqOptions = options.as(BigQueryOptions.class);

              JobReference jobRef = new JobReference()
                  .setProjectId(executingProject)
                  .setJobId(getExtractJobId(jobIdToken));

              Job extractJob = getBigQueryServices().getJobService(bqOptions)
                  .getJob(jobRef);

              Collection<String> extractFiles = null;
              if (extractJob != null) {
                extractFiles = getExtractFilePaths(extractDestinationDir, extractJob);
              } else {
                IOChannelFactory factory = IOChannelUtils.getFactory(extractDestinationDir);
                Collection<String> dirMatch = factory.match(extractDestinationDir);
                if (!dirMatch.isEmpty()) {
                  extractFiles = factory.match(factory.resolve(extractDestinationDir, "*"));
                }
              }
              if (extractFiles != null && !extractFiles.isEmpty()) {
                new GcsUtilFactory().create(options).remove(extractFiles);
              }
            }};
      return input.getPipeline()
          .apply(org.apache.beam.sdk.io.Read.from(source))
          .setCoder(getDefaultOutputCoder())
          .apply(new PassThroughThenCleanup<TableRow>(cleanupOperation));
    }

    @Override
    protected Coder<TableRow> getDefaultOutputCoder() {
      return TableRowJsonCoder.of();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("table", displayTable(getTableProvider()))
            .withLabel("Table"))
          .addIfNotNull(DisplayData.item("query", getQuery())
            .withLabel("Query"))
          .addIfNotNull(DisplayData.item("flattenResults", getFlattenResults())
            .withLabel("Flatten Query Results"))
          .addIfNotNull(DisplayData.item("useLegacySql", getUseLegacySql())
            .withLabel("Use Legacy SQL Dialect"))
          .addIfNotDefault(DisplayData.item("validation", getValidate())
            .withLabel("Validation Enabled"),
              true);
    }

    /**
     * Returns the table to read, or {@code null} if reading from a query instead.
     *
     * <p>If the table's project is not specified, use the executing project.
     */
    @Nullable private ValueProvider<TableReference> getTableWithDefaultProject(
        BigQueryOptions bqOptions) {
      ValueProvider<TableReference> table = getTableProvider();
      if (table == null) {
        return table;
      }
      if (!table.isAccessible()) {
        LOG.info("Using a dynamic value for table input. This must contain a project"
            + " in the table reference: {}", table);
        return table;
      }
      if (Strings.isNullOrEmpty(table.get().getProjectId())) {
        // If user does not specify a project we assume the table to be located in
        // the default project.
        TableReference tableRef = table.get();
        tableRef.setProjectId(bqOptions.getProject());
        return NestedValueProvider.of(StaticValueProvider.of(
            toJsonString(tableRef)), new JsonTableRefToTableRef());
      }
      return table;
    }

    /**
     * Returns the table to read, or {@code null} if reading from a query instead.
     */
    @Nullable
    public ValueProvider<TableReference> getTableProvider() {
      return getJsonTableRef() == null
          ? null : NestedValueProvider.of(getJsonTableRef(), new JsonTableRefToTableRef());
    }
    /**
     * Returns the table to read, or {@code null} if reading from a query instead.
     */
    @Nullable
    public TableReference getTable() {
      ValueProvider<TableReference> provider = getTableProvider();
      return provider == null ? null : provider.get();
    }
  }

  /**
   * A {@link PTransform} that invokes {@link CleanupOperation} after the input {@link PCollection}
   * has been processed.
   */
  @VisibleForTesting
  static class PassThroughThenCleanup<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private CleanupOperation cleanupOperation;

    PassThroughThenCleanup(CleanupOperation cleanupOperation) {
      this.cleanupOperation = cleanupOperation;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      TupleTag<T> mainOutput = new TupleTag<>();
      TupleTag<Void> cleanupSignal = new TupleTag<>();
      PCollectionTuple outputs = input.apply(ParDo.of(new IdentityFn<T>())
          .withOutputTags(mainOutput, TupleTagList.of(cleanupSignal)));

      PCollectionView<Void> cleanupSignalView = outputs.get(cleanupSignal)
          .setCoder(VoidCoder.of())
          .apply(View.<Void>asSingleton().withDefaultValue(null));

      input.getPipeline()
          .apply("Create(CleanupOperation)", Create.of(cleanupOperation))
          .apply("Cleanup", ParDo.of(
              new DoFn<CleanupOperation, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c)
                    throws Exception {
                  c.element().cleanup(c.getPipelineOptions());
                }
              }).withSideInputs(cleanupSignalView));

      return outputs.get(mainOutput);
    }

    private static class IdentityFn<T> extends DoFn<T, T> {
      @ProcessElement
      public void processElement(ProcessContext c) {
        c.output(c.element());
      }
    }

    abstract static class CleanupOperation implements Serializable {
      abstract void cleanup(PipelineOptions options) throws Exception;
    }
  }

  /**
   * A {@link BigQuerySourceBase} for reading BigQuery tables.
   */
  @VisibleForTesting
  static class BigQueryTableSource extends BigQuerySourceBase {

    static BigQueryTableSource create(
        ValueProvider<String> jobIdToken,
        ValueProvider<TableReference> table,
        String extractDestinationDir,
        BigQueryServices bqServices,
        ValueProvider<String> executingProject) {
      return new BigQueryTableSource(
          jobIdToken, table, extractDestinationDir, bqServices, executingProject);
    }

    private final ValueProvider<String> jsonTable;
    private final AtomicReference<Long> tableSizeBytes;

    private BigQueryTableSource(
        ValueProvider<String> jobIdToken,
        ValueProvider<TableReference> table,
        String extractDestinationDir,
        BigQueryServices bqServices,
        ValueProvider<String> executingProject) {
      super(jobIdToken, extractDestinationDir, bqServices, executingProject);
      this.jsonTable = NestedValueProvider.of(checkNotNull(table, "table"), new TableRefToJson());
      this.tableSizeBytes = new AtomicReference<>();
    }

    @Override
    protected TableReference getTableToExtract(BigQueryOptions bqOptions) throws IOException {
      checkState(jsonTable.isAccessible());
      return JSON_FACTORY.fromString(jsonTable.get(), TableReference.class);
    }

    @Override
    public BoundedReader<TableRow> createReader(PipelineOptions options) throws IOException {
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
      checkState(jsonTable.isAccessible());
      TableReference tableRef = JSON_FACTORY.fromString(jsonTable.get(), TableReference.class);
      return new BigQueryReader(this, bqServices.getReaderFromTable(bqOptions, tableRef));
    }

    @Override
    public synchronized long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      if (tableSizeBytes.get() == null) {
        TableReference table = JSON_FACTORY.fromString(jsonTable.get(), TableReference.class);

        Long numBytes = bqServices.getDatasetService(options.as(BigQueryOptions.class))
            .getTable(table).getNumBytes();
        tableSizeBytes.compareAndSet(null, numBytes);
      }
      return tableSizeBytes.get();
    }

    @Override
    protected void cleanupTempResource(BigQueryOptions bqOptions) throws Exception {
      // Do nothing.
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("table", jsonTable));
    }
  }

  /**
   * A {@link BigQuerySourceBase} for querying BigQuery tables.
   */
  @VisibleForTesting
  static class BigQueryQuerySource extends BigQuerySourceBase {

    static BigQueryQuerySource create(
        ValueProvider<String> jobIdToken,
        ValueProvider<String> query,
        ValueProvider<TableReference> queryTempTableRef,
        Boolean flattenResults,
        Boolean useLegacySql,
        String extractDestinationDir,
        BigQueryServices bqServices) {
      return new BigQueryQuerySource(
          jobIdToken,
          query,
          queryTempTableRef,
          flattenResults,
          useLegacySql,
          extractDestinationDir,
          bqServices);
    }

    private final ValueProvider<String> query;
    private final ValueProvider<String> jsonQueryTempTable;
    private final Boolean flattenResults;
    private final Boolean useLegacySql;
    private transient AtomicReference<JobStatistics> dryRunJobStats;

    private BigQueryQuerySource(
        ValueProvider<String> jobIdToken,
        ValueProvider<String> query,
        ValueProvider<TableReference> queryTempTableRef,
        Boolean flattenResults,
        Boolean useLegacySql,
        String extractDestinationDir,
        BigQueryServices bqServices) {
      super(jobIdToken, extractDestinationDir, bqServices,
          NestedValueProvider.of(
              checkNotNull(queryTempTableRef, "queryTempTableRef"), new TableRefToProjectId()));
      this.query = checkNotNull(query, "query");
      this.jsonQueryTempTable = NestedValueProvider.of(
          queryTempTableRef, new TableRefToJson());
      this.flattenResults = checkNotNull(flattenResults, "flattenResults");
      this.useLegacySql = checkNotNull(useLegacySql, "useLegacySql");
      this.dryRunJobStats = new AtomicReference<>();
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
      return dryRunQueryIfNeeded(bqOptions).getTotalBytesProcessed();
    }

    @Override
    public BoundedReader<TableRow> createReader(PipelineOptions options) throws IOException {
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
      return new BigQueryReader(this, bqServices.getReaderFromQuery(
          bqOptions, executingProject.get(), createBasicQueryConfig()));
    }

    @Override
    protected TableReference getTableToExtract(BigQueryOptions bqOptions)
        throws IOException, InterruptedException {
      // 1. Find the location of the query.
      String location = null;
      List<TableReference> referencedTables =
          dryRunQueryIfNeeded(bqOptions).getQuery().getReferencedTables();
      DatasetService tableService = bqServices.getDatasetService(bqOptions);
      if (referencedTables != null && !referencedTables.isEmpty()) {
        TableReference queryTable = referencedTables.get(0);
        location = tableService.getTable(queryTable).getLocation();
      }

      // 2. Create the temporary dataset in the query location.
      TableReference tableToExtract =
          JSON_FACTORY.fromString(jsonQueryTempTable.get(), TableReference.class);
      tableService.createDataset(
          tableToExtract.getProjectId(),
          tableToExtract.getDatasetId(),
          location,
          "Dataset for BigQuery query job temporary table");

      // 3. Execute the query.
      String queryJobId = jobIdToken.get() + "-query";
      executeQuery(
          executingProject.get(),
          queryJobId,
          tableToExtract,
          bqServices.getJobService(bqOptions));
      return tableToExtract;
    }

    @Override
    protected void cleanupTempResource(BigQueryOptions bqOptions) throws Exception {
      checkState(jsonQueryTempTable.isAccessible());
      TableReference tableToRemove =
          JSON_FACTORY.fromString(jsonQueryTempTable.get(), TableReference.class);

      DatasetService tableService = bqServices.getDatasetService(bqOptions);
      tableService.deleteTable(tableToRemove);
      tableService.deleteDataset(tableToRemove.getProjectId(), tableToRemove.getDatasetId());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("query", query));
    }

    private synchronized JobStatistics dryRunQueryIfNeeded(BigQueryOptions bqOptions)
        throws InterruptedException, IOException {
      if (dryRunJobStats.get() == null) {
        JobStatistics jobStats = bqServices.getJobService(bqOptions).dryRunQuery(
            executingProject.get(), createBasicQueryConfig());
        dryRunJobStats.compareAndSet(null, jobStats);
      }
      return dryRunJobStats.get();
    }

    private void executeQuery(
        String executingProject,
        String jobId,
        TableReference destinationTable,
        JobService jobService) throws IOException, InterruptedException {
      JobReference jobRef = new JobReference()
          .setProjectId(executingProject)
          .setJobId(jobId);

      JobConfigurationQuery queryConfig = createBasicQueryConfig()
          .setAllowLargeResults(true)
          .setCreateDisposition("CREATE_IF_NEEDED")
          .setDestinationTable(destinationTable)
          .setPriority("BATCH")
          .setWriteDisposition("WRITE_EMPTY");

      jobService.startQueryJob(jobRef, queryConfig);
      Job job = jobService.pollJob(jobRef, JOB_POLL_MAX_RETRIES);
      if (parseStatus(job) != Status.SUCCEEDED) {
        throw new IOException(String.format(
            "Query job %s failed, status: %s.", jobId, statusToPrettyString(job.getStatus())));
      }
    }

    private JobConfigurationQuery createBasicQueryConfig() {
      return new JobConfigurationQuery()
          .setFlattenResults(flattenResults)
          .setQuery(query.get())
          .setUseLegacySql(useLegacySql);
    }

    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
      in.defaultReadObject();
      dryRunJobStats = new AtomicReference<>();
    }
  }

  /**
   * An abstract {@link BoundedSource} to read a table from BigQuery.
   *
   * <p>This source uses a BigQuery export job to take a snapshot of the table on GCS, and then
   * reads in parallel from each produced file. It is implemented by {@link BigQueryTableSource},
   * and {@link BigQueryQuerySource}, depending on the configuration of the read.
   * Specifically,
   * <ul>
   * <li>{@link BigQueryTableSource} is for reading BigQuery tables</li>
   * <li>{@link BigQueryQuerySource} is for querying BigQuery tables</li>
   * </ul>
   * ...
   */
  private abstract static class BigQuerySourceBase extends BoundedSource<TableRow> {
    // The maximum number of retries to poll a BigQuery job.
    protected static final int JOB_POLL_MAX_RETRIES = Integer.MAX_VALUE;

    protected final ValueProvider<String> jobIdToken;
    protected final String extractDestinationDir;
    protected final BigQueryServices bqServices;
    protected final ValueProvider<String> executingProject;

    private BigQuerySourceBase(
        ValueProvider<String> jobIdToken,
        String extractDestinationDir,
        BigQueryServices bqServices,
        ValueProvider<String> executingProject) {
      this.jobIdToken = checkNotNull(jobIdToken, "jobIdToken");
      this.extractDestinationDir = checkNotNull(extractDestinationDir, "extractDestinationDir");
      this.bqServices = checkNotNull(bqServices, "bqServices");
      this.executingProject = checkNotNull(executingProject, "executingProject");
    }

    @Override
    public List<BoundedSource<TableRow>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
      TableReference tableToExtract = getTableToExtract(bqOptions);
      JobService jobService = bqServices.getJobService(bqOptions);
      String extractJobId = getExtractJobId(jobIdToken);
      List<String> tempFiles = executeExtract(extractJobId, tableToExtract, jobService);

      TableSchema tableSchema = bqServices.getDatasetService(bqOptions)
          .getTable(tableToExtract).getSchema();

      cleanupTempResource(bqOptions);
      return createSources(tempFiles, tableSchema);
    }

    protected abstract TableReference getTableToExtract(BigQueryOptions bqOptions) throws Exception;

    protected abstract void cleanupTempResource(BigQueryOptions bqOptions) throws Exception;

    @Override
    public void validate() {
      // Do nothing, validation is done in BigQuery.Read.
    }

    @Override
    public Coder<TableRow> getDefaultOutputCoder() {
      return TableRowJsonCoder.of();
    }

    private List<String> executeExtract(
        String jobId, TableReference table, JobService jobService)
            throws InterruptedException, IOException {
      JobReference jobRef = new JobReference()
          .setProjectId(executingProject.get())
          .setJobId(jobId);

      String destinationUri = getExtractDestinationUri(extractDestinationDir);
      JobConfigurationExtract extract = new JobConfigurationExtract()
          .setSourceTable(table)
          .setDestinationFormat("AVRO")
          .setDestinationUris(ImmutableList.of(destinationUri));

      LOG.info("Starting BigQuery extract job: {}", jobId);
      jobService.startExtractJob(jobRef, extract);
      Job extractJob =
          jobService.pollJob(jobRef, JOB_POLL_MAX_RETRIES);
      if (parseStatus(extractJob) != Status.SUCCEEDED) {
        throw new IOException(String.format(
            "Extract job %s failed, status: %s.",
            extractJob.getJobReference().getJobId(), statusToPrettyString(extractJob.getStatus())));
      }

      List<String> tempFiles = getExtractFilePaths(extractDestinationDir, extractJob);
      return ImmutableList.copyOf(tempFiles);
    }

    private List<BoundedSource<TableRow>> createSources(
        List<String> files, TableSchema tableSchema) throws IOException, InterruptedException {
      final String jsonSchema = JSON_FACTORY.toString(tableSchema);

      SerializableFunction<GenericRecord, TableRow> function =
          new SerializableFunction<GenericRecord, TableRow>() {
            @Override
            public TableRow apply(GenericRecord input) {
              return BigQueryAvroUtils.convertGenericRecordToTableRow(
                  input, fromJsonString(jsonSchema, TableSchema.class));
            }};

      List<BoundedSource<TableRow>> avroSources = Lists.newArrayList();
      for (String fileName : files) {
        avroSources.add(new TransformingSource<>(
            AvroSource.from(fileName), function, getDefaultOutputCoder()));
      }
      return ImmutableList.copyOf(avroSources);
    }

    protected static class BigQueryReader extends BoundedSource.BoundedReader<TableRow> {
      private final BigQuerySourceBase source;
      private final BigQueryServices.BigQueryJsonReader reader;

      private BigQueryReader(
          BigQuerySourceBase source, BigQueryServices.BigQueryJsonReader reader) {
        this.source = source;
        this.reader = reader;
      }

      @Override
      public BoundedSource<TableRow> getCurrentSource() {
        return source;
      }

      @Override
      public boolean start() throws IOException {
        return reader.start();
      }

      @Override
      public boolean advance() throws IOException {
        return reader.advance();
      }

      @Override
      public TableRow getCurrent() throws NoSuchElementException {
        return reader.getCurrent();
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }
    }
  }

  /**
   * A {@link BoundedSource} that reads from {@code BoundedSource<T>}
   * and transforms elements to type {@code V}.
  */
  @VisibleForTesting
  static class TransformingSource<T, V> extends BoundedSource<V> {
    private final BoundedSource<T> boundedSource;
    private final SerializableFunction<T, V> function;
    private final Coder<V> outputCoder;

    TransformingSource(
        BoundedSource<T> boundedSource,
        SerializableFunction<T, V> function,
        Coder<V> outputCoder) {
      this.boundedSource = checkNotNull(boundedSource, "boundedSource");
      this.function = checkNotNull(function, "function");
      this.outputCoder = checkNotNull(outputCoder, "outputCoder");
    }

    @Override
    public List<? extends BoundedSource<V>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return Lists.transform(
          boundedSource.splitIntoBundles(desiredBundleSizeBytes, options),
          new Function<BoundedSource<T>, BoundedSource<V>>() {
            @Override
            public BoundedSource<V> apply(BoundedSource<T> input) {
              return new TransformingSource<>(input, function, outputCoder);
            }
          });
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return boundedSource.getEstimatedSizeBytes(options);
    }

    @Override
    public BoundedReader<V> createReader(PipelineOptions options) throws IOException {
      return new TransformingReader(boundedSource.createReader(options));
    }

    @Override
    public void validate() {
      boundedSource.validate();
    }

    @Override
    public Coder<V> getDefaultOutputCoder() {
      return outputCoder;
    }

    private class TransformingReader extends BoundedReader<V> {
      private final BoundedReader<T> boundedReader;

      private TransformingReader(BoundedReader<T> boundedReader) {
        this.boundedReader = checkNotNull(boundedReader, "boundedReader");
      }

      @Override
      public synchronized BoundedSource<V> getCurrentSource() {
        return new TransformingSource<>(boundedReader.getCurrentSource(), function, outputCoder);
      }

      @Override
      public boolean start() throws IOException {
        return boundedReader.start();
      }

      @Override
      public boolean advance() throws IOException {
        return boundedReader.advance();
      }

      @Override
      public V getCurrent() throws NoSuchElementException {
        T current = boundedReader.getCurrent();
        return function.apply(current);
      }

      @Override
      public void close() throws IOException {
        boundedReader.close();
      }

      @Override
      public synchronized BoundedSource<V> splitAtFraction(double fraction) {
        BoundedSource<T> split = boundedReader.splitAtFraction(fraction);
        return split == null ? null : new TransformingSource<>(split, function, outputCoder);
      }

      @Override
      public Double getFractionConsumed() {
        return boundedReader.getFractionConsumed();
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return boundedReader.getCurrentTimestamp();
      }
    }
  }

  private static String getExtractJobId(ValueProvider<String> jobIdToken) {
    return jobIdToken.get() + "-extract";
  }

  private static String getExtractDestinationUri(String extractDestinationDir) {
    return String.format("%s/%s", extractDestinationDir, "*.avro");
  }

  private static List<String> getExtractFilePaths(String extractDestinationDir, Job extractJob)
      throws IOException {
    JobStatistics jobStats = extractJob.getStatistics();
    List<Long> counts = jobStats.getExtract().getDestinationUriFileCounts();
    if (counts.size() != 1) {
      String errorMessage = (counts.size() == 0
          ? "No destination uri file count received."
          : String.format("More than one destination uri file count received. First two are %s, %s",
              counts.get(0), counts.get(1)));
      throw new RuntimeException(errorMessage);
    }
    long filesCount = counts.get(0);

    ImmutableList.Builder<String> paths = ImmutableList.builder();
    IOChannelFactory factory = IOChannelUtils.getFactory(extractDestinationDir);
    for (long i = 0; i < filesCount; ++i) {
      String filePath =
          factory.resolve(extractDestinationDir, String.format("%012d%s", i, ".avro"));
      paths.add(filePath);
    }
    return paths.build();
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link PTransform} that writes a {@link PCollection} to a BigQuery table. A formatting
   * function must be provided to convert each input element into a {@link TableRow} using
   * {@link Write#withFormatFunction(SerializableFunction)}.
   *
   * <p>In BigQuery, each table has an encosing dataset. The dataset being written must already
   * exist.
   *
   * <p>By default, tables will be created if they do not exist, which corresponds to a
   * {@link Write.CreateDisposition#CREATE_IF_NEEDED} disposition that matches the default of
   * BigQuery's Jobs API. A schema must be provided (via {@link Write#withSchema(TableSchema)}),
   * or else the transform may fail at runtime with an {@link IllegalArgumentException}.
   *
   * <p>By default, writes require an empty table, which corresponds to
   * a {@link Write.WriteDisposition#WRITE_EMPTY} disposition that matches the
   * default of BigQuery's Jobs API.
   *
   * <p>Here is a sample transform that produces TableRow values containing
   * "word" and "count" columns:
   * <pre>{@code
   * static class FormatCountsFn extends DoFn<KV<String, Long>, TableRow> {
   *   public void processElement(ProcessContext c) {
   *     TableRow row = new TableRow()
   *         .set("word", c.element().getKey())
   *         .set("count", c.element().getValue().intValue());
   *     c.output(row);
   *   }
   * }}</pre>
   */
  public static <T> Write<T> write() {
    return new AutoValue_BigQueryIO_Write.Builder<T>()
        .setValidate(true)
        .setBigQueryServices(new BigQueryServicesImpl())
        .setCreateDisposition(Write.CreateDisposition.CREATE_IF_NEEDED)
        .setWriteDisposition(Write.WriteDisposition.WRITE_EMPTY)
        .build();
  }

  /**
   * A {@link PTransform} that writes a {@link PCollection} containing {@link TableRow TableRows}
   * to a BigQuery table.
   */
  public static Write<TableRow> writeTableRows() {
    return BigQueryIO.<TableRow>write().withFormatFunction(IDENTITY_FORMATTER);
  }

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
    @VisibleForTesting
    // Maximum number of files in a single partition.
    static final int MAX_NUM_FILES = 10000;

    @VisibleForTesting
    // Maximum number of bytes in a single partition -- 11 TiB just under BQ's 12 TiB limit.
    static final long MAX_SIZE_BYTES = 11 * (1L << 40);

    // The maximum number of retry jobs.
    private static final int MAX_RETRY_JOBS = 3;

    // The maximum number of retries to poll the status of a job.
    // It sets to {@code Integer.MAX_VALUE} to block until the BigQuery job finishes.
    private static final int LOAD_JOB_POLL_MAX_RETRIES = Integer.MAX_VALUE;

    @Nullable abstract ValueProvider<String> getJsonTableRef();
    @Nullable abstract SerializableFunction<ValueInSingleWindow<T>, TableReference>
      getTableRefFunction();
    @Nullable abstract SerializableFunction<T, TableRow> getFormatFunction();
    /** Table schema. The schema is required only if the table does not exist. */
    @Nullable abstract ValueProvider<String> getJsonSchema();
    abstract CreateDisposition getCreateDisposition();
    abstract WriteDisposition getWriteDisposition();
    @Nullable abstract String getTableDescription();
    /** An option to indicate if table validation is desired. Default is true. */
    abstract boolean getValidate();
    abstract BigQueryServices getBigQueryServices();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setJsonTableRef(ValueProvider<String> jsonTableRef);
      abstract Builder<T> setTableRefFunction(
          SerializableFunction<ValueInSingleWindow<T>, TableReference> tableRefFunction);
      abstract Builder<T> setFormatFunction(SerializableFunction<T, TableRow> formatFunction);
      abstract Builder<T> setJsonSchema(ValueProvider<String> jsonSchema);
      abstract Builder<T> setCreateDisposition(CreateDisposition createDisposition);
      abstract Builder<T> setWriteDisposition(WriteDisposition writeDisposition);
      abstract Builder<T> setTableDescription(String tableDescription);
      abstract Builder<T> setValidate(boolean validate);
      abstract Builder<T> setBigQueryServices(BigQueryServices bigQueryServices);

      abstract Write<T> build();
    }

    /**
     * An enumeration type for the BigQuery create disposition strings.
     *
     * @see <a href="https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.createDisposition">
     * <code>configuration.query.createDisposition</code> in the BigQuery Jobs API</a>
     */
    public enum CreateDisposition {
      /**
       * Specifics that tables should not be created.
       *
       * <p>If the output table does not exist, the write fails.
       */
      CREATE_NEVER,

      /**
       * Specifies that tables should be created if needed. This is the default
       * behavior.
       *
       * <p>Requires that a table schema is provided via {@link BigQueryIO.Write#withSchema}.
       * This precondition is checked before starting a job. The schema is
       * not required to match an existing table's schema.
       *
       * <p>When this transformation is executed, if the output table does not
       * exist, the table is created from the provided schema. Note that even if
       * the table exists, it may be recreated if necessary when paired with a
       * {@link WriteDisposition#WRITE_TRUNCATE}.
       */
      CREATE_IF_NEEDED
    }

    /**
     * An enumeration type for the BigQuery write disposition strings.
     *
     * @see <a href="https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.writeDisposition">
     * <code>configuration.query.writeDisposition</code> in the BigQuery Jobs API</a>
     */
    public enum WriteDisposition {
      /**
       * Specifies that write should replace a table.
       *
       * <p>The replacement may occur in multiple steps - for instance by first
       * removing the existing table, then creating a replacement, then filling
       * it in. This is not an atomic operation, and external programs may
       * see the table in any of these intermediate steps.
       */
      WRITE_TRUNCATE,

      /**
       * Specifies that rows may be appended to an existing table.
       */
      WRITE_APPEND,

      /**
       * Specifies that the output table must be empty. This is the default
       * behavior.
       *
       * <p>If the output table is not empty, the write fails at runtime.
       *
       * <p>This check may occur long before data is written, and does not
       * guarantee exclusive access to the table. If two programs are run
       * concurrently, each specifying the same output table and
       * a {@link WriteDisposition} of {@link WriteDisposition#WRITE_EMPTY}, it is possible
       * for both to succeed.
       */
      WRITE_EMPTY
    }

    /** Ensures that methods of the to() family are called at most once. */
    private void ensureToNotCalledYet() {
      checkState(
          getJsonTableRef() == null && getTable() == null, "to() already called");
    }

    /**
     * Writes to the given table, specified in the format described in {@link #parseTableSpec}.
     */
    public Write<T> to(String tableSpec) {
      return to(StaticValueProvider.of(tableSpec));
    }

    /** Writes to the given table, specified as a {@link TableReference}. */
    public Write<T> to(TableReference table) {
      return to(StaticValueProvider.of(toTableSpec(table)));
    }

    /** Same as {@link #to(String)}, but with a {@link ValueProvider}. */
    public Write<T> to(ValueProvider<String> tableSpec) {
      ensureToNotCalledYet();
      return toBuilder()
          .setJsonTableRef(
              NestedValueProvider.of(
                  NestedValueProvider.of(tableSpec, new TableSpecToTableRef()),
                  new TableRefToJson()))
          .build();
    }

    /**
     * Writes to table specified by the specified table function. The table is a function of
     * {@link ValueInSingleWindow}, so can be determined by the value or by the window.
     */
    public Write<T> to(SerializableFunction<ValueInSingleWindow<T>, String> tableSpecFunction) {
      return toTableReference(new TranslateTableSpecFunction<T>(tableSpecFunction));
    }

    /**
     * Like {@link BigQueryIO.Write#to(SerializableFunction)}, but the function returns a
     * {@link TableReference} instead of a string table specification.
     */
    private Write<T> toTableReference(
        SerializableFunction<ValueInSingleWindow<T>, TableReference> tableRefFunction) {
      ensureToNotCalledYet();
      return toBuilder().setTableRefFunction(tableRefFunction).build();
    }

    /**
     * Formats the user's type into a {@link TableRow} to be written to BigQuery.
     */
    public Write<T> withFormatFunction(SerializableFunction<T, TableRow> formatFunction) {
      return toBuilder().setFormatFunction(formatFunction).build();
    }

    private static class TranslateTableSpecFunction<T> implements
        SerializableFunction<ValueInSingleWindow<T>, TableReference> {
      private SerializableFunction<ValueInSingleWindow<T>, String> tableSpecFunction;

      TranslateTableSpecFunction(
          SerializableFunction<ValueInSingleWindow<T>, String> tableSpecFunction) {
        this.tableSpecFunction = tableSpecFunction;
      }

      @Override
      public TableReference apply(ValueInSingleWindow<T> value) {
        return parseTableSpec(tableSpecFunction.apply(value));
      }
    }

    /**
     * Uses the specified schema for rows to be written.
     *
     * <p>The schema is <i>required</i> only if writing to a table that does not already
     * exist, and {@link CreateDisposition} is set to
     * {@link CreateDisposition#CREATE_IF_NEEDED}.
     */
    public Write<T> withSchema(TableSchema schema) {
      return toBuilder()
          .setJsonSchema(StaticValueProvider.of(toJsonString(schema)))
          .build();
    }

    /**
     * Use the specified schema for rows to be written.
     */
    public Write<T> withSchema(ValueProvider<TableSchema> schema) {
      return toBuilder()
          .setJsonSchema(NestedValueProvider.of(schema, new TableSchemaToJsonSchema()))
          .build();
    }

    /** Specifies whether the table should be created if it does not exist. */
    public Write<T> withCreateDisposition(CreateDisposition createDisposition) {
      return toBuilder().setCreateDisposition(createDisposition).build();
    }

    /** Specifies what to do with existing data in the table, in case the table already exists. */
    public Write<T> withWriteDisposition(WriteDisposition writeDisposition) {
      return toBuilder().setWriteDisposition(writeDisposition).build();
    }

    /** Specifies the table description. */
    public Write<T> withTableDescription(String tableDescription) {
      return toBuilder().setTableDescription(tableDescription).build();
    }

    /** Disables BigQuery table validation. */
    public Write<T> withoutValidation() {
      return toBuilder().setValidate(false).build();
    }

    @VisibleForTesting
    Write<T> withTestServices(BigQueryServices testServices) {
      return toBuilder().setBigQueryServices(testServices).build();
    }

    private static void verifyTableNotExistOrEmpty(
        DatasetService datasetService,
        TableReference tableRef) {
      try {
        if (datasetService.getTable(tableRef) != null) {
          checkState(
              datasetService.isTableEmpty(tableRef),
              "BigQuery table is not empty: %s.",
              BigQueryIO.toTableSpec(tableRef));
        }
      } catch (IOException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new RuntimeException(
            "unable to confirm BigQuery table emptiness for table "
                + BigQueryIO.toTableSpec(tableRef), e);
      }
    }

    @Override
    public void validate(PCollection<T> input) {
      BigQueryOptions options = input.getPipeline().getOptions().as(BigQueryOptions.class);

      // Exactly one of the table and table reference can be configured.
      checkState(
          getJsonTableRef() != null || getTableRefFunction() != null,
          "must set the table reference of a BigQueryIO.Write transform");
      checkState(
          getJsonTableRef() == null || getTableRefFunction() == null,
          "Cannot set both a table reference and a table function for a BigQueryIO.Write"
              + " transform");

      checkArgument(getFormatFunction() != null,
                    "A function must be provided to convert type into a TableRow. "
      + "use BigQueryIO.Write.withFormatFunction to provide a formatting function.");

      // Require a schema if creating one or more tables.
      checkArgument(
          getCreateDisposition() != CreateDisposition.CREATE_IF_NEEDED || getJsonSchema() != null,
          "CreateDisposition is CREATE_IF_NEEDED, however no schema was provided.");

      // The user specified a table.
      if (getJsonTableRef() != null && getValidate()) {
        TableReference table = getTableWithDefaultProject(options).get();

        DatasetService datasetService = getBigQueryServices().getDatasetService(options);
        // Check for destination table presence and emptiness for early failure notification.
        // Note that a presence check can fail when the table or dataset is created by an earlier
        // stage of the pipeline. For these cases the #withoutValidation method can be used to
        // disable the check.
        verifyDatasetPresence(datasetService, table);
        if (getCreateDisposition() == BigQueryIO.Write.CreateDisposition.CREATE_NEVER) {
          verifyTablePresence(datasetService, table);
        }
        if (getWriteDisposition() == BigQueryIO.Write.WriteDisposition.WRITE_EMPTY) {
          verifyTableNotExistOrEmpty(datasetService, table);
        }
      }

      if (input.isBounded() == PCollection.IsBounded.UNBOUNDED || getTableRefFunction() != null) {
        // We will use BigQuery's streaming write API -- validate supported dispositions.
        if (getTableRefFunction() != null) {
          checkArgument(
              getCreateDisposition() != CreateDisposition.CREATE_NEVER,
              "CreateDisposition.CREATE_NEVER is not supported when using a tablespec"
              + " function.");
        }
        if (getJsonSchema() == null) {
          checkArgument(
              getCreateDisposition() == CreateDisposition.CREATE_NEVER,
              "CreateDisposition.CREATE_NEVER must be used if jsonSchema is null.");
        }

        checkArgument(
            getWriteDisposition() != WriteDisposition.WRITE_TRUNCATE,
            "WriteDisposition.WRITE_TRUNCATE is not supported for an unbounded PCollection or"
                + " when using a tablespec function.");
      } else {
        // We will use a BigQuery load job -- validate the temp location.
        String tempLocation = options.getTempLocation();
        checkArgument(
            !Strings.isNullOrEmpty(tempLocation),
            "BigQueryIO.Write needs a GCS temp location to store temp files.");
        if (getBigQueryServices() == null) {
          try {
            GcsPath.fromUri(tempLocation);
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                String.format(
                    "BigQuery temp location expected a valid 'gs://' path, but was given '%s'",
                    tempLocation),
                e);
          }
        }
      }
    }

    @Override
    public PDone expand(PCollection<T> input) {
      Pipeline p = input.getPipeline();
      BigQueryOptions options = p.getOptions().as(BigQueryOptions.class);

      // When writing an Unbounded PCollection, or when a tablespec function is defined, we use
      // StreamWithDeDup and BigQuery's streaming import API.
      if (input.isBounded() == IsBounded.UNBOUNDED || getTableRefFunction() != null) {
        return input.apply(new StreamWithDeDup<T>(this));
      }

      ValueProvider<TableReference> table = getTableWithDefaultProject(options);

      final String stepUuid = randomUUIDString();

      String tempLocation = options.getTempLocation();
      String tempFilePrefix;
      try {
        IOChannelFactory factory = IOChannelUtils.getFactory(tempLocation);
        tempFilePrefix = factory.resolve(
                factory.resolve(tempLocation, "BigQueryWriteTemp"),
                stepUuid);
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Failed to resolve BigQuery temp location in %s", tempLocation),
            e);
      }

      // Create a singleton job ID token at execution time.
      PCollection<String> singleton = p.apply("Create", Create.of(tempFilePrefix));
      PCollectionView<String> jobIdTokenView = p
          .apply("TriggerIdCreation", Create.of("ignored"))
          .apply("CreateJobId", MapElements.via(
              new SimpleFunction<String, String>() {
                @Override
                public String apply(String input) {
                  return stepUuid;
                }
              }))
          .apply(View.<String>asSingleton());

      PCollection<T> typedInputInGlobalWindow =
          input.apply(
              Window.<T>into(new GlobalWindows())
                  .triggering(DefaultTrigger.of())
                  .discardingFiredPanes());
      // Avoid applying the formatFunction if it is the identity formatter.
      PCollection<TableRow> inputInGlobalWindow;
      if (getFormatFunction() == IDENTITY_FORMATTER) {
        inputInGlobalWindow = (PCollection<TableRow>) typedInputInGlobalWindow;
      } else {
        inputInGlobalWindow = typedInputInGlobalWindow
            .apply(MapElements.via(getFormatFunction())
                .withOutputType(new TypeDescriptor<TableRow>() {
                }));
      }

      // PCollection of filename, file byte size.
      PCollection<KV<String, Long>> results = inputInGlobalWindow
          .apply("WriteBundles",
              ParDo.of(new WriteBundles(tempFilePrefix)));

      TupleTag<KV<Long, List<String>>> multiPartitionsTag =
          new TupleTag<KV<Long, List<String>>>("multiPartitionsTag") {};
      TupleTag<KV<Long, List<String>>> singlePartitionTag =
          new TupleTag<KV<Long, List<String>>>("singlePartitionTag") {};

      // Turn the list of files and record counts in a PCollectionView that can be used as a
      // side input.
      PCollectionView<Iterable<KV<String, Long>>> resultsView = results
          .apply("ResultsView", View.<KV<String, Long>>asIterable());
      PCollectionTuple partitions = singleton.apply(ParDo
          .of(new WritePartition(
              resultsView,
              multiPartitionsTag,
              singlePartitionTag))
          .withSideInputs(resultsView)
          .withOutputTags(multiPartitionsTag, TupleTagList.of(singlePartitionTag)));

      // If WriteBundles produced more than MAX_NUM_FILES files or MAX_SIZE_BYTES bytes, then
      // the import needs to be split into multiple partitions, and those partitions will be
      // specified in multiPartitionsTag.
      PCollection<String> tempTables = partitions.get(multiPartitionsTag)
          .apply("MultiPartitionsGroupByKey", GroupByKey.<Long, List<String>>create())
          .apply("MultiPartitionsWriteTables", ParDo.of(new WriteTables(
              false,
              getBigQueryServices(),
              jobIdTokenView,
              tempFilePrefix,
              NestedValueProvider.of(table, new TableRefToJson()),
              getJsonSchema(),
              WriteDisposition.WRITE_EMPTY,
              CreateDisposition.CREATE_IF_NEEDED,
              getTableDescription()))
          .withSideInputs(jobIdTokenView));

      PCollectionView<Iterable<String>> tempTablesView = tempTables
          .apply("TempTablesView", View.<String>asIterable());
      singleton.apply(ParDo
          .of(new WriteRename(
              getBigQueryServices(),
              jobIdTokenView,
              NestedValueProvider.of(table, new TableRefToJson()),
              getWriteDisposition(),
              getCreateDisposition(),
              tempTablesView,
              getTableDescription()))
          .withSideInputs(tempTablesView, jobIdTokenView));

      // Write single partition to final table
      partitions.get(singlePartitionTag)
          .apply("SinglePartitionGroupByKey", GroupByKey.<Long, List<String>>create())
          .apply("SinglePartitionWriteTables", ParDo.of(new WriteTables(
              true,
              getBigQueryServices(),
              jobIdTokenView,
              tempFilePrefix,
              NestedValueProvider.of(table, new TableRefToJson()),
              getJsonSchema(),
              getWriteDisposition(),
              getCreateDisposition(),
              getTableDescription()))
          .withSideInputs(jobIdTokenView));

      return PDone.in(input.getPipeline());
    }

    private static class WriteBundles extends DoFn<TableRow, KV<String, Long>> {
      private transient TableRowWriter writer = null;
      private final String tempFilePrefix;

      WriteBundles(String tempFilePrefix) {
        this.tempFilePrefix = tempFilePrefix;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        if (writer == null) {
          writer = new TableRowWriter(tempFilePrefix);
          writer.open(UUID.randomUUID().toString());
          LOG.debug("Done opening writer {}", writer);
        }
        try {
          writer.write(c.element());
        } catch (Exception e) {
          // Discard write result and close the write.
          try {
            writer.close();
            // The writer does not need to be reset, as this DoFn cannot be reused.
          } catch (Exception closeException) {
            // Do not mask the exception that caused the write to fail.
            e.addSuppressed(closeException);
          }
          throw e;
        }
      }

      @FinishBundle
      public void finishBundle(Context c) throws Exception {
        if (writer != null) {
          c.output(writer.close());
          writer = null;
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);

        builder
            .addIfNotNull(DisplayData.item("tempFilePrefix", tempFilePrefix)
                .withLabel("Temporary File Prefix"));
      }
    }

    @Override
    protected Coder<Void> getDefaultOutputCoder() {
      return VoidCoder.of();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder
          .addIfNotNull(DisplayData.item("table", getJsonTableRef())
            .withLabel("Table Reference"))
          .addIfNotNull(DisplayData.item("schema", getJsonSchema())
            .withLabel("Table Schema"));

      if (getTableRefFunction() != null) {
        builder.add(DisplayData.item("tableFn", getTableRefFunction().getClass())
          .withLabel("Table Reference Function"));
      }

      builder
          .add(DisplayData.item("createDisposition", getCreateDisposition().toString())
            .withLabel("Table CreateDisposition"))
          .add(DisplayData.item("writeDisposition", getWriteDisposition().toString())
            .withLabel("Table WriteDisposition"))
          .addIfNotDefault(DisplayData.item("validation", getValidate())
            .withLabel("Validation Enabled"), true)
          .addIfNotNull(DisplayData.item("tableDescription", getTableDescription())
            .withLabel("Table Description"));
    }

    /** Returns the table schema. */
    public TableSchema getSchema() {
      return fromJsonString(
          getJsonSchema() == null ? null : getJsonSchema().get(), TableSchema.class);
    }

    /**
     * Returns the table to write, or {@code null} if writing with {@code tableRefFunction}.
     *
     * <p>If the table's project is not specified, use the executing project.
     */
    @Nullable private ValueProvider<TableReference> getTableWithDefaultProject(
        BigQueryOptions bqOptions) {
      ValueProvider<TableReference> table = getTable();
      if (table == null) {
        return table;
      }
      if (!table.isAccessible()) {
        LOG.info("Using a dynamic value for table input. This must contain a project"
            + " in the table reference: {}", table);
        return table;
      }
      if (Strings.isNullOrEmpty(table.get().getProjectId())) {
        // If user does not specify a project we assume the table to be located in
        // the default project.
        TableReference tableRef = table.get();
        tableRef.setProjectId(bqOptions.getProject());
        return NestedValueProvider.of(StaticValueProvider.of(
            toJsonString(tableRef)), new JsonTableRefToTableRef());
      }
      return table;
    }

    /** Returns the table reference, or {@code null}. */
    @Nullable
    public ValueProvider<TableReference> getTable() {
      return getJsonTableRef() == null ? null :
          NestedValueProvider.of(getJsonTableRef(), new JsonTableRefToTableRef());
    }


    static class TableRowWriter {
      private static final Coder<TableRow> CODER = TableRowJsonCoder.of();
      private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
      private final String tempFilePrefix;
      private String id;
      private String fileName;
      private WritableByteChannel channel;
      protected String mimeType = MimeTypes.TEXT;
      private CountingOutputStream out;

      TableRowWriter(String basename) {
        this.tempFilePrefix = basename;
      }

      public final void open(String uId) throws Exception {
        id = uId;
        fileName = tempFilePrefix + id;
        LOG.debug("Opening {}.", fileName);
        channel = IOChannelUtils.create(fileName, mimeType);
        try {
          out = new CountingOutputStream(Channels.newOutputStream(channel));
          LOG.debug("Writing header to {}.", fileName);
        } catch (Exception e) {
          try {
            LOG.error("Writing header to {} failed, closing channel.", fileName);
            channel.close();
          } catch (IOException closeException) {
            LOG.error("Closing channel for {} failed", fileName);
          }
          throw e;
        }
        LOG.debug("Starting write of bundle {} to {}.", this.id, fileName);
      }

      public void write(TableRow value) throws Exception {
        CODER.encode(value, out, Context.OUTER);
        out.write(NEWLINE);
      }

      public final KV<String, Long> close() throws IOException {
        channel.close();
        return KV.of(fileName, out.getCount());
      }
    }

    /**
     * Partitions temporary files based on number of files and file sizes.
     */
    static class WritePartition extends DoFn<String, KV<Long, List<String>>> {
      private final PCollectionView<Iterable<KV<String, Long>>> resultsView;
      private TupleTag<KV<Long, List<String>>> multiPartitionsTag;
      private TupleTag<KV<Long, List<String>>> singlePartitionTag;

      public WritePartition(
          PCollectionView<Iterable<KV<String, Long>>> resultsView,
          TupleTag<KV<Long, List<String>>> multiPartitionsTag,
          TupleTag<KV<Long, List<String>>> singlePartitionTag) {
        this.resultsView = resultsView;
        this.multiPartitionsTag = multiPartitionsTag;
        this.singlePartitionTag = singlePartitionTag;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        List<KV<String, Long>> results = Lists.newArrayList(c.sideInput(resultsView));
        if (results.isEmpty()) {
          TableRowWriter writer = new TableRowWriter(c.element());
          writer.open(UUID.randomUUID().toString());
          results.add(writer.close());
        }

        long partitionId = 0;
        int currNumFiles = 0;
        long currSizeBytes = 0;
        List<String> currResults = Lists.newArrayList();
        for (int i = 0; i < results.size(); ++i) {
          KV<String, Long> fileResult = results.get(i);
          if (currNumFiles + 1 > Write.MAX_NUM_FILES
              || currSizeBytes + fileResult.getValue() > Write.MAX_SIZE_BYTES) {
            c.sideOutput(multiPartitionsTag, KV.of(++partitionId, currResults));
            currResults = Lists.newArrayList();
            currNumFiles = 0;
            currSizeBytes = 0;
          }
          ++currNumFiles;
          currSizeBytes += fileResult.getValue();
          currResults.add(fileResult.getKey());
        }
        if (partitionId == 0) {
          c.sideOutput(singlePartitionTag, KV.of(++partitionId, currResults));
        } else {
          c.sideOutput(multiPartitionsTag, KV.of(++partitionId, currResults));
        }
      }
    }

    /**
     * Writes partitions to BigQuery tables.
     */
    static class WriteTables extends DoFn<KV<Long, Iterable<List<String>>>, String> {
      private final boolean singlePartition;
      private final BigQueryServices bqServices;
      private final PCollectionView<String> jobIdToken;
      private final String tempFilePrefix;
      private final ValueProvider<String> jsonTableRef;
      private final ValueProvider<String> jsonSchema;
      private final WriteDisposition writeDisposition;
      private final CreateDisposition createDisposition;
      @Nullable private final String tableDescription;

      public WriteTables(
          boolean singlePartition,
          BigQueryServices bqServices,
          PCollectionView<String> jobIdToken,
          String tempFilePrefix,
          ValueProvider<String> jsonTableRef,
          ValueProvider<String> jsonSchema,
          WriteDisposition writeDisposition,
          CreateDisposition createDisposition,
          @Nullable String tableDescription) {
        this.singlePartition = singlePartition;
        this.bqServices = bqServices;
        this.jobIdToken = jobIdToken;
        this.tempFilePrefix = tempFilePrefix;
        this.jsonTableRef = jsonTableRef;
        this.jsonSchema = jsonSchema;
        this.writeDisposition = writeDisposition;
        this.createDisposition = createDisposition;
        this.tableDescription = tableDescription;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        List<String> partition = Lists.newArrayList(c.element().getValue()).get(0);
        String jobIdPrefix = String.format(
            c.sideInput(jobIdToken) + "_%05d", c.element().getKey());
        TableReference ref = fromJsonString(jsonTableRef.get(), TableReference.class);
        if (!singlePartition) {
          ref.setTableId(jobIdPrefix);
        }

        load(
            bqServices.getJobService(c.getPipelineOptions().as(BigQueryOptions.class)),
            bqServices.getDatasetService(c.getPipelineOptions().as(BigQueryOptions.class)),
            jobIdPrefix,
            ref,
            fromJsonString(
                jsonSchema == null ? null : jsonSchema.get(), TableSchema.class),
            partition,
            writeDisposition,
            createDisposition,
            tableDescription);
        c.output(toJsonString(ref));

        removeTemporaryFiles(c.getPipelineOptions(), tempFilePrefix, partition);
      }

      private void load(
          JobService jobService,
          DatasetService datasetService,
          String jobIdPrefix,
          TableReference ref,
          @Nullable TableSchema schema,
          List<String> gcsUris,
          WriteDisposition writeDisposition,
          CreateDisposition createDisposition,
          @Nullable String tableDescription) throws InterruptedException, IOException {
        JobConfigurationLoad loadConfig = new JobConfigurationLoad()
            .setDestinationTable(ref)
            .setSchema(schema)
            .setSourceUris(gcsUris)
            .setWriteDisposition(writeDisposition.name())
            .setCreateDisposition(createDisposition.name())
            .setSourceFormat("NEWLINE_DELIMITED_JSON");

        String projectId = ref.getProjectId();
        Job lastFailedLoadJob = null;
        for (int i = 0; i < Write.MAX_RETRY_JOBS; ++i) {
          String jobId = jobIdPrefix + "-" + i;
          JobReference jobRef = new JobReference()
              .setProjectId(projectId)
              .setJobId(jobId);
          jobService.startLoadJob(jobRef, loadConfig);
          Job loadJob = jobService.pollJob(jobRef, Write.LOAD_JOB_POLL_MAX_RETRIES);
          Status jobStatus = parseStatus(loadJob);
          switch (jobStatus) {
            case SUCCEEDED:
              if (tableDescription != null) {
                datasetService.patchTableDescription(ref, tableDescription);
              }
              return;
            case UNKNOWN:
              throw new RuntimeException(String.format(
                  "UNKNOWN status of load job [%s]: %s.", jobId, jobToPrettyString(loadJob)));
            case FAILED:
              lastFailedLoadJob = loadJob;
              continue;
            default:
              throw new IllegalStateException(String.format(
                  "Unexpected status [%s] of load job: %s.",
                  jobStatus, jobToPrettyString(loadJob)));
          }
        }
        throw new RuntimeException(String.format(
            "Failed to create load job with id prefix %s, "
                + "reached max retries: %d, last failed load job: %s.",
            jobIdPrefix,
            Write.MAX_RETRY_JOBS,
            jobToPrettyString(lastFailedLoadJob)));
      }

      static void removeTemporaryFiles(
          PipelineOptions options,
          String tempFilePrefix,
          Collection<String> files)
          throws IOException {
        IOChannelFactory factory = IOChannelUtils.getFactory(tempFilePrefix);
        if (factory instanceof GcsIOChannelFactory) {
          GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(options);
          gcsUtil.remove(files);
        } else if (factory instanceof FileIOChannelFactory) {
          for (String filename : files) {
            LOG.debug("Removing file {}", filename);
            boolean exists = Files.deleteIfExists(Paths.get(filename));
            if (!exists) {
              LOG.debug("{} does not exist.", filename);
            }
          }
        } else {
          throw new IOException("Unrecognized file system.");
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);

        builder
            .addIfNotNull(DisplayData.item("tempFilePrefix", tempFilePrefix)
                .withLabel("Temporary File Prefix"))
            .addIfNotNull(DisplayData.item("jsonTableRef", jsonTableRef)
                .withLabel("Table Reference"))
            .addIfNotNull(DisplayData.item("jsonSchema", jsonSchema)
                .withLabel("Table Schema"))
            .addIfNotNull(DisplayData.item("tableDescription", tableDescription)
                .withLabel("Table Description"));
      }
    }

    /**
     * Copies temporary tables to destination table.
     */
    static class WriteRename extends DoFn<String, Void> {
      private final BigQueryServices bqServices;
      private final PCollectionView<String> jobIdToken;
      private final ValueProvider<String> jsonTableRef;
      private final WriteDisposition writeDisposition;
      private final CreateDisposition createDisposition;
      private final PCollectionView<Iterable<String>> tempTablesView;
      @Nullable private final String tableDescription;

      public WriteRename(
          BigQueryServices bqServices,
          PCollectionView<String> jobIdToken,
          ValueProvider<String> jsonTableRef,
          WriteDisposition writeDisposition,
          CreateDisposition createDisposition,
          PCollectionView<Iterable<String>> tempTablesView,
          @Nullable String tableDescription) {
        this.bqServices = bqServices;
        this.jobIdToken = jobIdToken;
        this.jsonTableRef = jsonTableRef;
        this.writeDisposition = writeDisposition;
        this.createDisposition = createDisposition;
        this.tempTablesView = tempTablesView;
        this.tableDescription = tableDescription;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        List<String> tempTablesJson = Lists.newArrayList(c.sideInput(tempTablesView));

        // Do not copy if no temp tables are provided
        if (tempTablesJson.size() == 0) {
          return;
        }

        List<TableReference> tempTables = Lists.newArrayList();
        for (String table : tempTablesJson) {
          tempTables.add(fromJsonString(table, TableReference.class));
        }
        copy(
            bqServices.getJobService(c.getPipelineOptions().as(BigQueryOptions.class)),
            bqServices.getDatasetService(c.getPipelineOptions().as(BigQueryOptions.class)),
            c.sideInput(jobIdToken),
            fromJsonString(jsonTableRef.get(), TableReference.class),
            tempTables,
            writeDisposition,
            createDisposition,
            tableDescription);

        DatasetService tableService =
            bqServices.getDatasetService(c.getPipelineOptions().as(BigQueryOptions.class));
        removeTemporaryTables(tableService, tempTables);
      }

      private void copy(
          JobService jobService,
          DatasetService datasetService,
          String jobIdPrefix,
          TableReference ref,
          List<TableReference> tempTables,
          WriteDisposition writeDisposition,
          CreateDisposition createDisposition,
          @Nullable String tableDescription) throws InterruptedException, IOException {
        JobConfigurationTableCopy copyConfig = new JobConfigurationTableCopy()
            .setSourceTables(tempTables)
            .setDestinationTable(ref)
            .setWriteDisposition(writeDisposition.name())
            .setCreateDisposition(createDisposition.name());

        String projectId = ref.getProjectId();
        Job lastFailedCopyJob = null;
        for (int i = 0; i < Write.MAX_RETRY_JOBS; ++i) {
          String jobId = jobIdPrefix + "-" + i;
          JobReference jobRef = new JobReference()
              .setProjectId(projectId)
              .setJobId(jobId);
          jobService.startCopyJob(jobRef, copyConfig);
          Job copyJob = jobService.pollJob(jobRef, Write.LOAD_JOB_POLL_MAX_RETRIES);
          Status jobStatus = parseStatus(copyJob);
          switch (jobStatus) {
            case SUCCEEDED:
              if (tableDescription != null) {
                datasetService.patchTableDescription(ref, tableDescription);
              }
              return;
            case UNKNOWN:
              throw new RuntimeException(String.format(
                  "UNKNOWN status of copy job [%s]: %s.", jobId, jobToPrettyString(copyJob)));
            case FAILED:
              lastFailedCopyJob = copyJob;
              continue;
            default:
              throw new IllegalStateException(String.format(
                  "Unexpected status [%s] of load job: %s.",
                  jobStatus, jobToPrettyString(copyJob)));
          }
        }
        throw new RuntimeException(String.format(
            "Failed to create copy job with id prefix %s, "
                + "reached max retries: %d, last failed copy job: %s.",
            jobIdPrefix,
            Write.MAX_RETRY_JOBS,
            jobToPrettyString(lastFailedCopyJob)));
      }

      static void removeTemporaryTables(DatasetService tableService,
          List<TableReference> tempTables) {
        for (TableReference tableRef : tempTables) {
          try {
            LOG.debug("Deleting table {}", toJsonString(tableRef));
            tableService.deleteTable(tableRef);
          } catch (Exception e) {
            LOG.warn("Failed to delete the table {}", toJsonString(tableRef), e);
          }
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);

        builder
            .addIfNotNull(DisplayData.item("jsonTableRef", jsonTableRef)
                .withLabel("Table Reference"))
            .add(DisplayData.item("writeDisposition", writeDisposition.toString())
                .withLabel("Write Disposition"))
            .add(DisplayData.item("createDisposition", createDisposition.toString())
                .withLabel("Create Disposition"));
      }
    }
  }

  private static String jobToPrettyString(@Nullable Job job) throws IOException {
    return job == null ? "null" : job.toPrettyString();
  }

  private static String statusToPrettyString(@Nullable JobStatus status) throws IOException {
    return status == null ? "Unknown status: null." : status.toPrettyString();
  }

  private static void verifyDatasetPresence(DatasetService datasetService, TableReference table) {
    try {
      datasetService.getDataset(table.getProjectId(), table.getDatasetId());
    } catch (Exception e) {
      ApiErrorExtractor errorExtractor = new ApiErrorExtractor();
      if ((e instanceof IOException) && errorExtractor.itemNotFound((IOException) e)) {
        throw new IllegalArgumentException(
            String.format(RESOURCE_NOT_FOUND_ERROR, "dataset", BigQueryIO.toTableSpec(table)),
            e);
      } else if (e instanceof  RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(
            String.format(UNABLE_TO_CONFIRM_PRESENCE_OF_RESOURCE_ERROR, "dataset",
                BigQueryIO.toTableSpec(table)),
            e);
      }
    }
  }

  private static void verifyTablePresence(DatasetService datasetService, TableReference table) {
    try {
      datasetService.getTable(table);
    } catch (Exception e) {
      ApiErrorExtractor errorExtractor = new ApiErrorExtractor();
      if ((e instanceof IOException) && errorExtractor.itemNotFound((IOException) e)) {
        throw new IllegalArgumentException(
            String.format(RESOURCE_NOT_FOUND_ERROR, "table", BigQueryIO.toTableSpec(table)), e);
      } else if (e instanceof  RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(
            String.format(UNABLE_TO_CONFIRM_PRESENCE_OF_RESOURCE_ERROR, "table",
                BigQueryIO.toTableSpec(table)),
            e);
      }
    }
  }

  /**
   * Clear the cached map of created tables. Used for testing.
   */
  @VisibleForTesting
  static void clearCreatedTables() {
    StreamingWriteFn.clearCreatedTables();
  }
  /////////////////////////////////////////////////////////////////////////////

  /**
   * Implementation of DoFn to perform streaming BigQuery write.
   */
  @VisibleForTesting
  static class StreamingWriteFn
      extends DoFn<KV<ShardedKey<String>, TableRowInfo>, Void> {
    /** TableSchema in JSON. Use String to make the class Serializable. */
    @Nullable private final ValueProvider<String> jsonTableSchema;

    @Nullable private final String tableDescription;

    private final BigQueryServices bqServices;

    /** JsonTableRows to accumulate BigQuery rows in order to batch writes. */
    private transient Map<String, List<TableRow>> tableRows;

    private final Write.CreateDisposition createDisposition;

    /** The list of unique ids for each BigQuery table row. */
    private transient Map<String, List<String>> uniqueIdsForTableRows;

    /** The list of tables created so far, so we don't try the creation
        each time. */
    private static Set<String> createdTables =
        Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /** Tracks bytes written, exposed as "ByteCount" Metrics Counter. */
    private Counter byteCounter = Metrics.counter(StreamingWriteFn.class, "ByteCount");

    /** Constructor. */
    StreamingWriteFn(@Nullable ValueProvider<TableSchema> schema,
                     Write.CreateDisposition createDisposition,
                     @Nullable String tableDescription, BigQueryServices bqServices) {
      this.jsonTableSchema = schema == null ? null :
          NestedValueProvider.of(schema, new TableSchemaToJsonSchema());
      this.createDisposition = createDisposition;
      this.bqServices = checkNotNull(bqServices, "bqServices");
      this.tableDescription = tableDescription;
    }

    /**
     * Clear the cached map of created tables. Used for testing.
     */
    private static void clearCreatedTables() {
      synchronized (createdTables) {
        createdTables.clear();
      }
    }

    /** Prepares a target BigQuery table. */
    @StartBundle
    public void startBundle(Context context) {
      tableRows = new HashMap<>();
      uniqueIdsForTableRows = new HashMap<>();
    }

    /** Accumulates the input into JsonTableRows and uniqueIdsForTableRows. */
    @ProcessElement
    public void processElement(ProcessContext context) {
      String tableSpec = context.element().getKey().getKey();
      List<TableRow> rows = getOrCreateMapListValue(tableRows, tableSpec);
      List<String> uniqueIds = getOrCreateMapListValue(uniqueIdsForTableRows, tableSpec);

      rows.add(context.element().getValue().tableRow);
      uniqueIds.add(context.element().getValue().uniqueId);
    }

    /** Writes the accumulated rows into BigQuery with streaming API. */
    @FinishBundle
    public void finishBundle(Context context) throws Exception {
      BigQueryOptions options = context.getPipelineOptions().as(BigQueryOptions.class);

      for (Map.Entry<String, List<TableRow>> entry : tableRows.entrySet()) {
        TableReference tableReference = getOrCreateTable(options, entry.getKey());
        flushRows(tableReference, entry.getValue(),
            uniqueIdsForTableRows.get(entry.getKey()), options);
      }
      tableRows.clear();
      uniqueIdsForTableRows.clear();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder
          .addIfNotNull(DisplayData.item("schema", jsonTableSchema)
            .withLabel("Table Schema"))
          .addIfNotNull(DisplayData.item("tableDescription", tableDescription)
            .withLabel("Table Description"));
    }

    public TableReference getOrCreateTable(BigQueryOptions options, String tableSpec)
        throws InterruptedException, IOException {
      TableReference tableReference = parseTableSpec(tableSpec);
      if (createDisposition != createDisposition.CREATE_NEVER
          && !createdTables.contains(tableSpec)) {
        synchronized (createdTables) {
          // Another thread may have succeeded in creating the table in the meanwhile, so
          // check again. This check isn't needed for correctness, but we add it to prevent
          // every thread from attempting a create and overwhelming our BigQuery quota.
          DatasetService datasetService = bqServices.getDatasetService(options);
          if (!createdTables.contains(tableSpec)) {
            if (datasetService.getTable(tableReference) == null) {
              TableSchema tableSchema = JSON_FACTORY.fromString(
                  jsonTableSchema.get(), TableSchema.class);
              datasetService.createTable(
                  new Table()
                      .setTableReference(tableReference)
                      .setSchema(tableSchema)
                      .setDescription(tableDescription));
            }
            createdTables.add(tableSpec);
          }
        }
      }
      return tableReference;
    }

    /**
     * Writes the accumulated rows into BigQuery with streaming API.
     */
    private void flushRows(TableReference tableReference,
        List<TableRow> tableRows, List<String> uniqueIds, BigQueryOptions options)
            throws InterruptedException {
      if (!tableRows.isEmpty()) {
        try {
          DatasetService datasetService = bqServices.getDatasetService(options);
          long totalBytes = datasetService.insertAll(tableReference,
              datasetService.makeInsertBatches(tableRows, uniqueIds));
          byteCounter.inc(totalBytes);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static class ShardedKey<K> {
    private final K key;
    private final int shardNumber;

    public static <K> ShardedKey<K> of(K key, int shardNumber) {
      return new ShardedKey<>(key, shardNumber);
    }

    private ShardedKey(K key, int shardNumber) {
      this.key = key;
      this.shardNumber = shardNumber;
    }

    public K getKey() {
      return key;
    }

    public int getShardNumber() {
      return shardNumber;
    }
  }

  /**
   * A {@link Coder} for {@link ShardedKey}, using a wrapped key {@link Coder}.
   */
  @VisibleForTesting
  static class ShardedKeyCoder<KeyT>
      extends StandardCoder<ShardedKey<KeyT>> {
    public static <KeyT> ShardedKeyCoder<KeyT> of(Coder<KeyT> keyCoder) {
      return new ShardedKeyCoder<>(keyCoder);
    }

    @JsonCreator
    public static <KeyT> ShardedKeyCoder<KeyT> of(
         @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
        List<Coder<KeyT>> components) {
      checkArgument(components.size() == 1, "Expecting 1 component, got %s", components.size());
      return of(components.get(0));
    }

    protected ShardedKeyCoder(Coder<KeyT> keyCoder) {
      this.keyCoder = keyCoder;
      this.shardNumberCoder = VarIntCoder.of();
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(keyCoder);
    }

    @Override
    public void encode(ShardedKey<KeyT> key, OutputStream outStream, Context context)
        throws IOException {
      keyCoder.encode(key.getKey(), outStream, context.nested());
      shardNumberCoder.encode(key.getShardNumber(), outStream, context);
    }

    @Override
    public ShardedKey<KeyT> decode(InputStream inStream, Context context)
        throws IOException {
      return new ShardedKey<>(
          keyCoder.decode(inStream, context.nested()),
          shardNumberCoder.decode(inStream, context));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      keyCoder.verifyDeterministic();
    }

    Coder<KeyT> keyCoder;
    VarIntCoder shardNumberCoder;
  }

  @VisibleForTesting
  static class TableRowInfoCoder extends AtomicCoder<TableRowInfo> {
    private static final TableRowInfoCoder INSTANCE = new TableRowInfoCoder();

    @JsonCreator
    public static TableRowInfoCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(TableRowInfo value, OutputStream outStream, Context context)
      throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null value");
      }
      tableRowCoder.encode(value.tableRow, outStream, context.nested());
      idCoder.encode(value.uniqueId, outStream, context);
    }

    @Override
    public TableRowInfo decode(InputStream inStream, Context context)
      throws IOException {
      return new TableRowInfo(
          tableRowCoder.decode(inStream, context.nested()),
          idCoder.decode(inStream, context));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      throw new NonDeterministicException(this, "TableRows are not deterministic.");
    }

    TableRowJsonCoder tableRowCoder = TableRowJsonCoder.of();
    StringUtf8Coder idCoder = StringUtf8Coder.of();
  }

  private static class TableRowInfo {
    TableRowInfo(TableRow tableRow, String uniqueId) {
      this.tableRow = tableRow;
      this.uniqueId = uniqueId;
    }

    final TableRow tableRow;
    final String uniqueId;
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Fn that tags each table row with a unique id and destination table.
   * To avoid calling UUID.randomUUID() for each element, which can be costly,
   * a randomUUID is generated only once per bucket of data. The actual unique
   * id is created by concatenating this randomUUID with a sequential number.
   */
  @VisibleForTesting
  static class TagWithUniqueIdsAndTable<T>
      extends DoFn<T, KV<ShardedKey<String>, TableRowInfo>> {
    /** TableSpec to write to. */
    private final ValueProvider<String> tableSpec;

    /** User function mapping windowed values to {@link TableReference} in JSON. */
    private final SerializableFunction<ValueInSingleWindow<T>, TableReference> tableRefFunction;

    /** User function mapping user type to a TableRow. */
    private final SerializableFunction<T, TableRow> formatFunction;

    private transient String randomUUID;
    private transient long sequenceNo = 0L;

    TagWithUniqueIdsAndTable(BigQueryOptions options,
        ValueProvider<TableReference> table,
        SerializableFunction<ValueInSingleWindow<T>, TableReference> tableRefFunction,
        SerializableFunction<T, TableRow> formatFunction) {
      checkArgument(table == null ^ tableRefFunction == null,
          "Exactly one of table or tableRefFunction should be set");
      if (table != null) {
        if (table.isAccessible() && Strings.isNullOrEmpty(table.get().getProjectId())) {
          TableReference tableRef = table.get()
              .setProjectId(options.as(BigQueryOptions.class).getProject());
          table = NestedValueProvider.of(
              StaticValueProvider.of(toJsonString(tableRef)),
              new JsonTableRefToTableRef());
        }
        this.tableSpec = NestedValueProvider.of(table, new TableRefToTableSpec());
      } else {
        tableSpec = null;
      }
      this.tableRefFunction = tableRefFunction;
      this.formatFunction = formatFunction;
    }


    @StartBundle
    public void startBundle(Context context) {
      randomUUID = UUID.randomUUID().toString();
    }

    /** Tag the input with a unique id. */
    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window) throws IOException {
      String uniqueId = randomUUID + sequenceNo++;
      ThreadLocalRandom randomGenerator = ThreadLocalRandom.current();
      String tableSpec = tableSpecFromWindowedValue(
          context.getPipelineOptions().as(BigQueryOptions.class),
          ValueInSingleWindow.of(context.element(), context.timestamp(), window, context.pane()));
      // We output on keys 0-50 to ensure that there's enough batching for
      // BigQuery.
      context.output(KV.of(ShardedKey.of(tableSpec, randomGenerator.nextInt(0, 50)),
          new TableRowInfo(formatFunction.apply(context.element()), uniqueId)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.addIfNotNull(DisplayData.item("table", tableSpec));
      if (tableRefFunction != null) {
        builder.add(DisplayData.item("tableFn", tableRefFunction.getClass())
          .withLabel("Table Reference Function"));
      }
    }

    @VisibleForTesting
    ValueProvider<String> getTableSpec() {
      return tableSpec;
    }

    private String tableSpecFromWindowedValue(BigQueryOptions options,
                                              ValueInSingleWindow<T> value) {
      if (tableSpec != null) {
        return tableSpec.get();
      } else {
        TableReference table = tableRefFunction.apply(value);
        if (table.getProjectId() == null) {
          table.setProjectId(options.getProject());
        }
        return toTableSpec(table);
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
  * PTransform that performs streaming BigQuery write. To increase consistency,
  * it leverages BigQuery best effort de-dup mechanism.
   */
  private static class StreamWithDeDup<T> extends PTransform<PCollection<T>, PDone> {
    private final Write<T> write;

    /** Constructor. */
    StreamWithDeDup(Write<T> write) {
      this.write = write;
    }

    @Override
    protected Coder<Void> getDefaultOutputCoder() {
      return VoidCoder.of();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      // A naive implementation would be to simply stream data directly to BigQuery.
      // However, this could occasionally lead to duplicated data, e.g., when
      // a VM that runs this code is restarted and the code is re-run.

      // The above risk is mitigated in this implementation by relying on
      // BigQuery built-in best effort de-dup mechanism.

      // To use this mechanism, each input TableRow is tagged with a generated
      // unique id, which is then passed to BigQuery and used to ignore duplicates.

      PCollection<KV<ShardedKey<String>, TableRowInfo>> tagged =
          input.apply(ParDo.of(new TagWithUniqueIdsAndTable<T>(
              input.getPipeline().getOptions().as(BigQueryOptions.class), write.getTable(),
              write.getTableRefFunction(), write.getFormatFunction())));

      // To prevent having the same TableRow processed more than once with regenerated
      // different unique ids, this implementation relies on "checkpointing", which is
      // achieved as a side effect of having StreamingWriteFn immediately follow a GBK,
      // performed by Reshuffle.
      NestedValueProvider<TableSchema, String> schema =
          write.getJsonSchema() == null
              ? null
              : NestedValueProvider.of(write.getJsonSchema(), new JsonSchemaToTableSchema());
      tagged
          .setCoder(KvCoder.of(ShardedKeyCoder.of(StringUtf8Coder.of()), TableRowInfoCoder.of()))
          .apply(Reshuffle.<ShardedKey<String>, TableRowInfo>of())
          .apply(
              ParDo.of(
                  new StreamingWriteFn(
                      schema,
                      write.getCreateDisposition(),
                      write.getTableDescription(),
                      write.getBigQueryServices())));

      // Note that the implementation to return PDone here breaks the
      // implicit assumption about the job execution order. If a user
      // implements a PTransform that takes PDone returned here as its
      // input, the transform may not necessarily be executed after
      // the BigQueryIO.Write.

      return PDone.in(input.getPipeline());
    }
  }

  /**
   * Status of a BigQuery job or request.
   */
  enum Status {
    SUCCEEDED,
    FAILED,
    UNKNOWN,
  }

  private static Status parseStatus(@Nullable Job job) {
    if (job == null) {
      return Status.UNKNOWN;
    }
    JobStatus status = job.getStatus();
    if (status.getErrorResult() != null) {
      return Status.FAILED;
    } else if (status.getErrors() != null && !status.getErrors().isEmpty()) {
      return Status.FAILED;
    } else {
      return Status.SUCCEEDED;
    }
  }

  @VisibleForTesting
  static String toJsonString(Object item) {
    if (item == null) {
      return null;
    }
    try {
      return JSON_FACTORY.toString(item);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Cannot serialize %s to a JSON string.", item.getClass().getSimpleName()),
          e);
    }
  }

  @VisibleForTesting
  static <T> T fromJsonString(String json, Class<T> clazz) {
    if (json == null) {
      return null;
    }
    try {
      return JSON_FACTORY.fromString(json, clazz);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Cannot deserialize %s from a JSON string: %s.", clazz, json),
          e);
    }
  }

  /**
   * Returns a randomUUID string.
   *
   * <p>{@code '-'} is removed because BigQuery doesn't allow it in dataset id.
   */
  private static String randomUUIDString() {
    return UUID.randomUUID().toString().replaceAll("-", "");
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Disallow construction of utility class. */
  private BigQueryIO() {}

  private static <K, V> List<V> getOrCreateMapListValue(Map<K, List<V>> map, K key) {
    List<V> value = map.get(key);
    if (value == null) {
      value = new ArrayList<>();
      map.put(key, value);
    }
    return value;
  }
}
