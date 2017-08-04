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
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createJobIdToken;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.getExtractJobId;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.resolveTempLocation;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.JsonTableRefToTableRef;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableRefToJson;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableSchemaToJsonSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableSpecToTableRef;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinationsHelpers.ConstantSchemaDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinationsHelpers.SchemaFromViewDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinationsHelpers.TableFunctionDestinations;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
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
 * <p>BigQuery table references are stored as a {@link TableReference}, which comes from the <a
 * href="https://cloud.google.com/bigquery/client-libraries">BigQuery Java Client API</a>. Tables
 * can be referred to as Strings, with or without the {@code projectId}. A helper function is
 * provided ({@link BigQueryHelpers#parseTableSpec(String)}) that parses the following string forms
 * into a {@link TableReference}:
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
 *
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
 *
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
 * <p>See {@link BigQueryIO.Write} for details on how to specify if a write should append to an
 * existing table, replace the table, or verify that the table is empty. Note that the dataset being
 * written to must already exist. Unbounded PCollections can only be written using {@link
 * Write.WriteDisposition#WRITE_EMPTY} or {@link Write.WriteDisposition#WRITE_APPEND}.
 *
 * <h3>Sharding BigQuery output tables</h3>
 *
 * <p>A common use case is to dynamically generate BigQuery table names based on the current window
 * or the current value. To support this, {@link BigQueryIO.Write#to(SerializableFunction)} accepts
 * a function mapping the current element to a tablespec. For example, here's code that outputs
 * daily tables to BigQuery:
 *
 * <pre>{@code
 * PCollection<TableRow> quotes = ...
 * quotes.apply(Window.<TableRow>into(CalendarWindows.days(1)))
 *       .apply(BigQueryIO.writeTableRows()
 *         .withSchema(schema)
 *         .to(new SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination>() {
 *           public TableDestination apply(ValueInSingleWindow<TableRow> value) {
 *             // The cast below is safe because CalendarWindows.days(1) produces IntervalWindows.
 *             String dayString = DateTimeFormat.forPattern("yyyy_MM_dd")
 *                  .withZone(DateTimeZone.UTC)
 *                  .print(((IntervalWindow) value.getWindow()).start());
 *             return new TableDestination(
 *                 "my-project:output.output_table_" + dayString, // Table spec
 *                 "Output for day " + dayString // Table description
 *               );
 *           }
 *         }));
 * }</pre>
 *
 * <p>Note that this also allows the table to be a function of the element as well as the current
 * pane, in the case of triggered windows. In this case it might be convenient to call {@link
 * BigQueryIO#write()} directly instead of using the {@link BigQueryIO#writeTableRows()} helper.
 * This will allow the mapping function to access the element of the user-defined type. In this
 * case, a formatting function must be specified using {@link BigQueryIO.Write#withFormatFunction}
 * to convert each element into a {@link TableRow} object.
 *
 * <p>Per-table schemas can also be provided using {@link BigQueryIO.Write#withSchemaFromView}. This
 * allows you the schemas to be calculated based on a previous pipeline stage or statically via a
 * {@link org.apache.beam.sdk.transforms.Create} transform. This method expects to receive a
 * map-valued {@link PCollectionView}, mapping table specifications (project:dataset.table-id), to
 * JSON formatted {@link TableSchema} objects. All destination tables must be present in this map,
 * or the pipeline will fail to create tables. Care should be taken if the map value is based on a
 * triggered aggregation over and unbounded {@link PCollection}; the side input will contain the
 * entire history of all table schemas ever generated, which might blow up memory usage. This method
 * can also be useful when writing to a single table, as it allows a previous stage to calculate the
 * schema (possibly based on the full collection of records being written to BigQuery).
 *
 * <p>For the most general form of dynamic table destinations and schemas, look at
 * {@link BigQueryIO.Write#to(DynamicDestinations)}.
 *
 * <h3>Permissions</h3>
 *
 * <p>Permission requirements depend on the {@link PipelineRunner} that is used to execute the
 * pipeline. Please refer to the documentation of corresponding {@link PipelineRunner}s for more
 * details.
 *
 * <p>Please see <a href="https://cloud.google.com/bigquery/access-control">BigQuery Access Control
 * </a> for security and permission related information specific to BigQuery.
 */
public class BigQueryIO {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIO.class);

  /**
   * Singleton instance of the JSON factory used to read and write JSON formatted rows.
   */
  static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

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

  static final Pattern TABLE_SPEC = Pattern.compile(DATASET_TABLE_REGEXP);

  /**
   * A formatting function that maps a TableRow to itself. This allows sending a
   * {@code PCollection<TableRow>} directly to BigQueryIO.Write.
   */
  static final SerializableFunction<TableRow, TableRow> IDENTITY_FORMATTER =
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
   *
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
   * }
   * }</pre>
   */
  public static Read read() {
    return new AutoValue_BigQueryIO_Read.Builder()
        .setValidate(true)
        .setWithTemplateCompatibility(false)
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

    abstract Boolean getWithTemplateCompatibility();

    abstract BigQueryServices getBigQueryServices();
    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setJsonTableRef(ValueProvider<String> jsonTableRef);
      abstract Builder setQuery(ValueProvider<String> query);
      abstract Builder setValidate(boolean validate);
      abstract Builder setFlattenResults(Boolean flattenResults);
      abstract Builder setUseLegacySql(Boolean useLegacySql);

      abstract Builder setWithTemplateCompatibility(Boolean useTemplateCompatibility);

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

    /** Same as {@code from(String)}, but with a {@link ValueProvider}. */
    public Read from(ValueProvider<String> tableSpec) {
      ensureFromNotCalledYet();
      return toBuilder()
          .setJsonTableRef(
              NestedValueProvider.of(
                  NestedValueProvider.of(tableSpec, new TableSpecToTableRef()),
                  new TableRefToJson()))
          .build();
    }

    /**
     * Reads results received after executing the given query.
     *
     * <p>By default, the query results will be flattened -- see "flattenResults" in the <a
     * href="https://cloud.google.com/bigquery/docs/reference/v2/jobs">Jobs documentation</a> for
     * more information. To disable flattening, use {@link BigQueryIO.Read#withoutResultFlattening}.
     *
     * <p>By default, the query will use BigQuery's legacy SQL dialect. To use the BigQuery Standard
     * SQL dialect, use {@link BigQueryIO.Read#usingStandardSql}.
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
      return from(StaticValueProvider.of(BigQueryHelpers.toTableSpec(table)));
    }

    private static final String QUERY_VALIDATION_FAILURE_ERROR =
        "Validation of query \"%1$s\" failed. If the query depends on an earlier stage of the"
        + " pipeline, This validation can be disabled using #withoutValidation.";

    /**
     * Disable validation that the table exists or the query succeeds prior to pipeline submission.
     * Basic validation (such as ensuring that a query or table is specified) still occurs.
     */
    public Read withoutValidation() {
      return toBuilder().setValidate(false).build();
    }

    /**
     * Disable <a href="https://cloud.google.com/bigquery/docs/reference/v2/jobs">flattening of
     * query results</a>.
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

    /**
     * Use new template-compatible source implementation.
     *
     * <p>Use new template-compatible source implementation. This implementation is compatible with
     * repeated template invocations. It does not support dynamic work rebalancing.
     */
    @Experimental(Experimental.Kind.SOURCE_SINK)
    public Read withTemplateCompatibility() {
      return toBuilder().setWithTemplateCompatibility(true).build();
    }

    @VisibleForTesting
    Read withTestServices(BigQueryServices testServices) {
      return toBuilder().setBigQueryServices(testServices).build();
    }

    private BigQuerySourceBase createSource(String jobUuid) {
      BigQuerySourceBase source;
      if (getQuery() == null
          || (getQuery().isAccessible() && Strings.isNullOrEmpty(getQuery().get()))) {
        source = BigQueryTableSource.create(jobUuid, getTableProvider(), getBigQueryServices());
      } else {
        source =
            BigQueryQuerySource.create(
                jobUuid, getQuery(), getFlattenResults(), getUseLegacySql(), getBigQueryServices());
      }
      return source;
    }

    @Override
    public void validate(PipelineOptions options) {
      // Even if existence validation is disabled, we need to make sure that the BigQueryIO
      // read is properly specified.
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);

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

      ValueProvider<TableReference> table = getTableProvider();

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
        if (table.isAccessible() && Strings.isNullOrEmpty(table.get().getProjectId())) {
          LOG.info(
              "Project of {} not set. The value of {}.getProject() at execution time will be used.",
              TableReference.class.getSimpleName(),
              BigQueryOptions.class.getSimpleName());
        }
      } else /* query != null */ {
        checkState(
            getFlattenResults() != null, "flattenResults should not be null if query is set");
        checkState(getUseLegacySql() != null, "useLegacySql should not be null if query is set");
      }

      // Note that a table or query check can fail if the table or dataset are created by
      // earlier stages of the pipeline or if a query depends on earlier stages of a pipeline.
      // For these cases the withoutValidation method can be used to disable the check.
      if (getValidate() && table != null && table.isAccessible()
          && table.get().getProjectId() != null) {
        checkState(table.isAccessible(), "Cannot call validate if table is dynamically set.");
        // Check for source table presence for early failure notification.
        DatasetService datasetService = getBigQueryServices().getDatasetService(bqOptions);
        BigQueryHelpers.verifyDatasetPresence(datasetService, table.get());
        BigQueryHelpers.verifyTablePresence(datasetService, table.get());
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
      Pipeline p = input.getPipeline();
      final PCollectionView<String> jobIdTokenView;
      PCollection<String> jobIdTokenCollection = null;
      PCollection<TableRow> rows;
      if (!getWithTemplateCompatibility()) {
        // Create a singleton job ID token at construction time.
        final String staticJobUuid = BigQueryHelpers.randomUUIDString();
        jobIdTokenView =
            p.apply("TriggerIdCreation", Create.of(staticJobUuid))
                .apply("ViewId", View.<String>asSingleton());
        // Apply the traditional Source model.
        rows = p.apply(org.apache.beam.sdk.io.Read.from(createSource(staticJobUuid)));
      } else {
        // Create a singleton job ID token at execution time.
        jobIdTokenCollection =
            p.apply("TriggerIdCreation", Create.of("ignored"))
                .apply(
                    "CreateJobId",
                    MapElements.via(
                        new SimpleFunction<String, String>() {
                          @Override
                          public String apply(String input) {
                            return BigQueryHelpers.randomUUIDString();
                          }
                        }));
        jobIdTokenView = jobIdTokenCollection.apply("ViewId", View.<String>asSingleton());

        final TupleTag<String> filesTag = new TupleTag<>();
        final TupleTag<String> tableSchemaTag = new TupleTag<>();
        PCollectionTuple tuple =
            jobIdTokenCollection.apply(
                "RunCreateJob",
                ParDo.of(
                        new DoFn<String, String>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) throws Exception {
                            String jobUuid = c.element();
                            BigQuerySourceBase source = createSource(jobUuid);
                            String schema =
                                BigQueryHelpers.toJsonString(
                                    source.getSchema(c.getPipelineOptions()));
                            c.output(tableSchemaTag, schema);
                            List<ResourceId> files = source.extractFiles(c.getPipelineOptions());
                            for (ResourceId file : files) {
                              c.output(file.toString());
                            }
                          }
                        })
                    .withOutputTags(filesTag, TupleTagList.of(tableSchemaTag)));
        tuple.get(filesTag).setCoder(StringUtf8Coder.of());
        tuple.get(tableSchemaTag).setCoder(StringUtf8Coder.of());
        final PCollectionView<String> schemaView =
            tuple.get(tableSchemaTag).apply(View.<String>asSingleton());
        rows =
            tuple
                .get(filesTag)
                .apply(
                    WithKeys.of(
                        new SerializableFunction<String, String>() {
                          public String apply(String s) {
                            return s;
                          }
                        }))
                .apply(Reshuffle.<String, String>of())
                .apply(Values.<String>create())
                .apply(
                    "ReadFiles",
                    ParDo.of(
                            new DoFn<String, TableRow>() {
                              @ProcessElement
                              public void processElement(ProcessContext c) throws Exception {
                                TableSchema schema =
                                    BigQueryHelpers.fromJsonString(
                                        c.sideInput(schemaView), TableSchema.class);
                                String jobUuid = c.sideInput(jobIdTokenView);
                                BigQuerySourceBase source = createSource(jobUuid);
                                List<BoundedSource<TableRow>> sources =
                                    source.createSources(
                                        ImmutableList.of(
                                            FileSystems.matchNewResource(
                                                c.element(), false /* is directory */)),
                                        schema);
                                checkArgument(sources.size() == 1, "Expected exactly one source.");
                                BoundedSource<TableRow> avroSource = sources.get(0);
                                BoundedSource.BoundedReader<TableRow> reader =
                                    avroSource.createReader(c.getPipelineOptions());
                                for (boolean more = reader.start(); more; more = reader.advance()) {
                                  c.output(reader.getCurrent());
                                }
                              }
                            })
                        .withSideInputs(schemaView, jobIdTokenView))
                        .setCoder(TableRowJsonCoder.of());
      }
      PassThroughThenCleanup.CleanupOperation cleanupOperation =
          new PassThroughThenCleanup.CleanupOperation() {
            @Override
            void cleanup(PassThroughThenCleanup.ContextContainer c) throws Exception {
              PipelineOptions options = c.getPipelineOptions();
              BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
              String jobUuid = c.getJobId();
              final String extractDestinationDir =
                  resolveTempLocation(bqOptions.getTempLocation(), "BigQueryExtractTemp", jobUuid);
              final String executingProject = bqOptions.getProject();
              JobReference jobRef =
                  new JobReference()
                      .setProjectId(executingProject)
                      .setJobId(getExtractJobId(createJobIdToken(bqOptions.getJobName(), jobUuid)));

              Job extractJob = getBigQueryServices().getJobService(bqOptions).getJob(jobRef);

              if (extractJob != null) {
                List<ResourceId> extractFiles =
                    getExtractFilePaths(extractDestinationDir, extractJob);
                if (extractFiles != null && !extractFiles.isEmpty()) {
                  FileSystems.delete(
                      extractFiles, MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
                }
              }
            }
          };
      return rows.apply(new PassThroughThenCleanup<TableRow>(cleanupOperation, jobIdTokenView));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("table", BigQueryHelpers.displayTable(getTableProvider()))
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
     */
    @Nullable
    public ValueProvider<TableReference> getTableProvider() {
      return getJsonTableRef() == null
          ? null : NestedValueProvider.of(getJsonTableRef(), new JsonTableRefToTableRef());
    }
    /** Returns the table to read, or {@code null} if reading from a query instead. */
    @Nullable
    public TableReference getTable() {
      ValueProvider<TableReference> provider = getTableProvider();
      return provider == null ? null : provider.get();
    }
  }

  static String getExtractDestinationUri(String extractDestinationDir) {
    return String.format("%s/%s", extractDestinationDir, "*.avro");
  }

  static List<ResourceId> getExtractFilePaths(String extractDestinationDir, Job extractJob)
      throws IOException {
    JobStatistics jobStats = extractJob.getStatistics();
    List<Long> counts = jobStats.getExtract().getDestinationUriFileCounts();
    if (counts.size() != 1) {
      String errorMessage = (counts.size() == 0
              ? "No destination uri file count received."
              : String.format(
                  "More than one destination uri file count received. First two are %s, %s",
                  counts.get(0), counts.get(1)));
      throw new RuntimeException(errorMessage);
    }
    long filesCount = counts.get(0);

    ImmutableList.Builder<ResourceId> paths = ImmutableList.builder();
    ResourceId extractDestinationDirResourceId =
        FileSystems.matchNewResource(extractDestinationDir, true /* isDirectory */);
    for (long i = 0; i < filesCount; ++i) {
      ResourceId filePath = extractDestinationDirResourceId.resolve(
          String.format("%012d%s", i, ".avro"),
          ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
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
   * a {@link Write.WriteDisposition#WRITE_EMPTY} disposition that matches the default of
   * BigQuery's Jobs API.
   *
   * <p>Here is a sample transform that produces TableRow values containing "word" and "count"
   * columns:
   *
   * <pre>{@code
   * static class FormatCountsFn extends DoFn<KV<String, Long>, TableRow> {
   *   public void processElement(ProcessContext c) {
   *     TableRow row = new TableRow()
   *         .set("word", c.element().getKey())
   *         .set("count", c.element().getValue().intValue());
   *     c.output(row);
   *   }
   * }
   * }</pre>
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
   * A {@link PTransform} that writes a {@link PCollection} containing {@link TableRow TableRows} to
   * a BigQuery table.
   */
  public static Write<TableRow> writeTableRows() {
    return BigQueryIO.<TableRow>write().withFormatFunction(IDENTITY_FORMATTER);
  }

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, WriteResult> {
    @Nullable abstract ValueProvider<String> getJsonTableRef();
    @Nullable abstract SerializableFunction<ValueInSingleWindow<T>, TableDestination>
      getTableFunction();
    @Nullable abstract SerializableFunction<T, TableRow> getFormatFunction();
    @Nullable abstract DynamicDestinations<T, ?> getDynamicDestinations();
    @Nullable abstract PCollectionView<Map<String, String>> getSchemaFromView();
    @Nullable abstract ValueProvider<String> getJsonSchema();
    abstract CreateDisposition getCreateDisposition();
    abstract WriteDisposition getWriteDisposition();
    /** Table description. Default is empty. */
    @Nullable abstract String getTableDescription();
    /** An option to indicate if table validation is desired. Default is true. */
    abstract boolean getValidate();
    abstract BigQueryServices getBigQueryServices();
    @Nullable abstract Integer getMaxFilesPerBundle();
    @Nullable abstract Long getMaxFileSize();
    @Nullable abstract InsertRetryPolicy getFailedInsertRetryPolicy();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setJsonTableRef(ValueProvider<String> jsonTableRef);
      abstract Builder<T> setTableFunction(
          SerializableFunction<ValueInSingleWindow<T>, TableDestination> tableFunction);
      abstract Builder<T> setFormatFunction(SerializableFunction<T, TableRow> formatFunction);
      abstract Builder<T> setDynamicDestinations(DynamicDestinations<T, ?> dynamicDestinations);
      abstract Builder<T> setSchemaFromView(PCollectionView<Map<String, String>> view);
      abstract Builder<T> setJsonSchema(ValueProvider<String> jsonSchema);
      abstract Builder<T> setCreateDisposition(CreateDisposition createDisposition);
      abstract Builder<T> setWriteDisposition(WriteDisposition writeDisposition);
      abstract Builder<T> setTableDescription(String tableDescription);
      abstract Builder<T> setValidate(boolean validate);
      abstract Builder<T> setBigQueryServices(BigQueryServices bigQueryServices);
      abstract Builder<T> setMaxFilesPerBundle(Integer maxFilesPerBundle);
      abstract Builder<T> setMaxFileSize(Long maxFileSize);
      abstract Builder<T> setFailedInsertRetryPolicy(InsertRetryPolicy retryPolicy);

      abstract Write<T> build();
    }

    /**
     * An enumeration type for the BigQuery create disposition strings.
     *
     * @see
     * <a href="https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.createDisposition">
     *     <code>configuration.query.createDisposition</code> in the BigQuery Jobs API</a>
     */
    public enum CreateDisposition {
      /**
       * Specifics that tables should not be created.
       *
       * <p>If the output table does not exist, the write fails.
       */
      CREATE_NEVER,

      /**
       * Specifies that tables should be created if needed. This is the default behavior.
       *
       * <p>Requires that a table schema is provided via {@link BigQueryIO.Write#withSchema}.
       * This precondition is checked before starting a job. The schema is not required to match an
       * existing table's schema.
       *
       * <p>When this transformation is executed, if the output table does not exist, the table is
       * created from the provided schema. Note that even if the table exists, it may be recreated
       * if necessary when paired with a {@link WriteDisposition#WRITE_TRUNCATE}.
       */
      CREATE_IF_NEEDED
    }

    /**
     * An enumeration type for the BigQuery write disposition strings.
     *
     * @see <a
     *     href="https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.writeDisposition">
     *     <code>configuration.query.writeDisposition</code> in the BigQuery Jobs API</a>
     */
    public enum WriteDisposition {
      /**
       * Specifies that write should replace a table.
       *
       * <p>The replacement may occur in multiple steps - for instance by first removing the
       * existing table, then creating a replacement, then filling it in. This is not an atomic
       * operation, and external programs may see the table in any of these intermediate steps.
       */
      WRITE_TRUNCATE,

      /** Specifies that rows may be appended to an existing table. */
      WRITE_APPEND,

      /**
       * Specifies that the output table must be empty. This is the default behavior.
       *
       * <p>If the output table is not empty, the write fails at runtime.
       *
       * <p>This check may occur long before data is written, and does not guarantee exclusive
       * access to the table. If two programs are run concurrently, each specifying the same output
       * table and a {@link WriteDisposition} of {@link WriteDisposition#WRITE_EMPTY}, it is
       * possible for both to succeed.
       */
      WRITE_EMPTY
    }

    /**
     * Writes to the given table, specified in the format described in {@link
     * BigQueryHelpers#parseTableSpec}.
     */
    public Write<T> to(String tableSpec) {
      return to(StaticValueProvider.of(tableSpec));
    }

    /** Writes to the given table, specified as a {@link TableReference}. */
    public Write<T> to(TableReference table) {
      return to(StaticValueProvider.of(BigQueryHelpers.toTableSpec(table)));
    }

    /** Same as {@link #to(String)}, but with a {@link ValueProvider}. */
    public Write<T> to(ValueProvider<String> tableSpec) {
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
    public Write<T> to(
        SerializableFunction<ValueInSingleWindow<T>, TableDestination> tableFunction) {
      return toBuilder().setTableFunction(tableFunction).build();
    }

    /**
     * Writes to the table and schema specified by the {@link DynamicDestinations} object.
     */
    public Write<T> to(DynamicDestinations<T, ?> dynamicDestinations) {
      return toBuilder().setDynamicDestinations(dynamicDestinations).build();
    }

    /**
     * Formats the user's type into a {@link TableRow} to be written to BigQuery.
     */
    public Write<T> withFormatFunction(SerializableFunction<T, TableRow> formatFunction) {
      return toBuilder().setFormatFunction(formatFunction).build();
    }

    /**
     * Uses the specified schema for rows to be written.
     *
     * <p>The schema is <i>required</i> only if writing to a table that does not already exist, and
     * {@link CreateDisposition} is set to {@link CreateDisposition#CREATE_IF_NEEDED}.
     */
    public Write<T> withSchema(TableSchema schema) {
      return withJsonSchema(StaticValueProvider.of(BigQueryHelpers.toJsonString(schema)));
    }

    /** Same as {@link #withSchema(TableSchema)} but using a deferred {@link ValueProvider}. */
    public Write<T> withSchema(ValueProvider<TableSchema> schema) {
      return withJsonSchema(NestedValueProvider.of(schema, new TableSchemaToJsonSchema()));
    }

    /**
     * Similar to {@link #withSchema(TableSchema)} but takes in a JSON-serialized {@link
     * TableSchema}.
     */
    public Write<T> withJsonSchema(String jsonSchema) {
      return withJsonSchema(StaticValueProvider.of(jsonSchema));
    }

    /**
     * Same as {@link #withJsonSchema(String)} but using a deferred {@link ValueProvider}.
     */
    public Write<T> withJsonSchema(ValueProvider<String> jsonSchema) {
      return toBuilder().setJsonSchema(jsonSchema).build();
    }

    /**
     * Allows the schemas for each table to be computed within the pipeline itself.
     *
     * <p>The input is a map-valued {@link PCollectionView} mapping string tablespecs to
     * JSON-formatted {@link TableSchema}s. Tablespecs must be in the same format as taken by
     * {@link #to(String)}.
     */
    public Write<T> withSchemaFromView(PCollectionView<Map<String, String>> view) {
      return toBuilder().setSchemaFromView(view).build();
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

    /** Specfies a policy for handling failed inserts.
     *
     * <p>Currently this only is allowed when writing an unbounded collection to BigQuery. Bounded
     * collections are written using batch load jobs, so we don't get per-element failures.
     * Unbounded collections are written using streaming inserts, so we have access to per-element
     * insert results.
     */
    public Write<T> withFailedInsertRetryPolicy(InsertRetryPolicy retryPolicy) {
      return toBuilder().setFailedInsertRetryPolicy(retryPolicy).build();
    }

    /** Disables BigQuery table validation. */
    public Write<T> withoutValidation() {
      return toBuilder().setValidate(false).build();
    }

    @VisibleForTesting
    Write<T> withTestServices(BigQueryServices testServices) {
      return toBuilder().setBigQueryServices(testServices).build();
    }

    @VisibleForTesting
    Write<T> withMaxFilesPerBundle(int maxFilesPerBundle) {
      return toBuilder().setMaxFilesPerBundle(maxFilesPerBundle).build();
    }

    @VisibleForTesting
    Write<T> withMaxFileSize(long maxFileSize) {
      return toBuilder().setMaxFileSize(maxFileSize).build();
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      BigQueryOptions options = pipelineOptions.as(BigQueryOptions.class);

      // The user specified a table.
      if (getJsonTableRef() != null && getJsonTableRef().isAccessible() && getValidate()) {
        TableReference table = getTableWithDefaultProject(options).get();
        DatasetService datasetService = getBigQueryServices().getDatasetService(options);
        // Check for destination table presence and emptiness for early failure notification.
        // Note that a presence check can fail when the table or dataset is created by an earlier
        // stage of the pipeline. For these cases the #withoutValidation method can be used to
        // disable the check.
        BigQueryHelpers.verifyDatasetPresence(datasetService, table);
        if (getCreateDisposition() == BigQueryIO.Write.CreateDisposition.CREATE_NEVER) {
          BigQueryHelpers.verifyTablePresence(datasetService, table);
        }
        if (getWriteDisposition() == BigQueryIO.Write.WriteDisposition.WRITE_EMPTY) {
          BigQueryHelpers.verifyTableNotExistOrEmpty(datasetService, table);
        }
      }
    }

    @Override
    public WriteResult expand(PCollection<T> input) {
      // We must have a destination to write to!
      checkState(
          getTableFunction() != null || getJsonTableRef() != null
              || getDynamicDestinations() != null,
          "must set the table reference of a BigQueryIO.Write transform");

      checkArgument(getFormatFunction() != null,
          "A function must be provided to convert type into a TableRow. "
              + "use BigQueryIO.Write.withFormatFunction to provide a formatting function.");

      // Require a schema if creating one or more tables.
      checkArgument(getCreateDisposition() != CreateDisposition.CREATE_IF_NEEDED
              || getJsonSchema() != null
              || getDynamicDestinations() != null
              || getSchemaFromView() != null,
          "CreateDisposition is CREATE_IF_NEEDED, however no schema was provided.");

      List<?> allToArgs = Lists.newArrayList(getJsonTableRef(), getTableFunction(),
          getDynamicDestinations());
      checkArgument(1
              == Iterables.size(Iterables.filter(allToArgs, Predicates.notNull())),
          "Exactly one of jsonTableRef, tableFunction, or " + "dynamicDestinations must be set");

      List<?> allSchemaArgs = Lists.newArrayList(getJsonSchema(), getSchemaFromView(),
          getDynamicDestinations());
      checkArgument(2
              > Iterables.size(Iterables.filter(allSchemaArgs, Predicates.notNull())),
          "No more than one of jsonSchema, schemaFromView, or dynamicDestinations may "
              + "be set");


      DynamicDestinations<T, ?> dynamicDestinations = getDynamicDestinations();
      if (dynamicDestinations == null) {
        if (getJsonTableRef() != null) {
          dynamicDestinations =
              DynamicDestinationsHelpers.ConstantTableDestinations.fromJsonTableRef(
                  getJsonTableRef(), getTableDescription());
        } else if (getTableFunction() != null) {
          dynamicDestinations = new TableFunctionDestinations(getTableFunction());
        }

        // Wrap with a DynamicDestinations class that will provide a schema. There might be no
        // schema provided if the create disposition is CREATE_NEVER.
        if (getJsonSchema() != null) {
          dynamicDestinations =
              new ConstantSchemaDestinations(dynamicDestinations, getJsonSchema());
        } else if (getSchemaFromView() != null) {
          dynamicDestinations =
              new SchemaFromViewDestinations(dynamicDestinations, getSchemaFromView());
        }
      }
      return expandTyped(input, dynamicDestinations);
    }

    private <DestinationT> WriteResult expandTyped(
        PCollection<T> input, DynamicDestinations<T, DestinationT> dynamicDestinations) {
      Coder<DestinationT> destinationCoder = null;
      try {
        destinationCoder = dynamicDestinations.getDestinationCoderWithDefault(
            input.getPipeline().getCoderRegistry());
      } catch (CannotProvideCoderException e) {
          throw new RuntimeException(e);
      }

      PCollection<KV<DestinationT, TableRow>> rowsWithDestination =
          input
              .apply("PrepareWrite", new PrepareWrite<>(dynamicDestinations, getFormatFunction()))
              .setCoder(KvCoder.of(destinationCoder, TableRowJsonCoder.of()));

      // When writing an Unbounded PCollection, we use StreamingInserts and BigQuery's streaming
      // import API.
      if (input.isBounded() == IsBounded.UNBOUNDED) {
        checkArgument(
            getWriteDisposition() != WriteDisposition.WRITE_TRUNCATE,
            "WriteDisposition.WRITE_TRUNCATE is not supported for an unbounded"
                + " PCollection.");
        StreamingInserts<DestinationT> streamingInserts =
            new StreamingInserts<>(getCreateDisposition(), dynamicDestinations)
                .withInsertRetryPolicy(getFailedInsertRetryPolicy())
                .withTestServices((getBigQueryServices()));
        return rowsWithDestination.apply(streamingInserts);
      } else {
        checkArgument(getFailedInsertRetryPolicy() == null,
            "Record-insert retry policies are not supported when using BigQuery load jobs.");

        BatchLoads<DestinationT> batchLoads = new BatchLoads<>(
            getWriteDisposition(),
            getCreateDisposition(),
            getJsonTableRef() != null,
            dynamicDestinations,
            destinationCoder);
        batchLoads.setTestServices(getBigQueryServices());
        if (getMaxFilesPerBundle() != null) {
          batchLoads.setMaxNumWritersPerBundle(getMaxFilesPerBundle());
        }
        if (getMaxFileSize() != null) {
          batchLoads.setMaxFileSize(getMaxFileSize());
        }
        return rowsWithDestination.apply(batchLoads);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.addIfNotNull(DisplayData.item("table", getJsonTableRef())
          .withLabel("Table Reference"));
      if (getJsonSchema() != null) {
        builder.addIfNotNull(DisplayData.item("schema", getJsonSchema()).withLabel("Table Schema"));
      } else {
        builder.add(DisplayData.item("schema", "Custom Schema Function").withLabel("Table Schema"));
      }

      if (getTableFunction() != null) {
        builder.add(DisplayData.item("tableFn", getTableFunction().getClass())
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

    /**
     * Returns the table to write, or {@code null} if writing with {@code tableFunction}.
     *
     * <p>If the table's project is not specified, use the executing project.
     */
    @Nullable
    ValueProvider<TableReference> getTableWithDefaultProject(BigQueryOptions bqOptions) {
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
            BigQueryHelpers.toJsonString(tableRef)), new JsonTableRefToTableRef());
      }
      return table;
    }

    /** Returns the table reference, or {@code null}. */
    @Nullable
    public ValueProvider<TableReference> getTable() {
      return getJsonTableRef() == null ? null :
          NestedValueProvider.of(getJsonTableRef(), new JsonTableRefToTableRef());
    }
  }

  /**
   * Clear the cached map of created tables. Used for testing.
   */
  @VisibleForTesting
  static void clearCreatedTables() {
    CreateTables.clearCreatedTables();
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Disallow construction of utility class. */
  private BigQueryIO() {}
}
