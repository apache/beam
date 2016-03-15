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
package com.google.cloud.dataflow.sdk.io;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.BigQueryServices;
import com.google.cloud.dataflow.sdk.util.BigQueryServices.LoadService;
import com.google.cloud.dataflow.sdk.util.BigQueryServicesImpl;
import com.google.cloud.dataflow.sdk.util.BigQueryTableInserter;
import com.google.cloud.dataflow.sdk.util.BigQueryTableRowIterator;
import com.google.cloud.dataflow.sdk.util.MimeTypes;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.Reshuffle;
import com.google.cloud.dataflow.sdk.util.SystemDoFnInternal;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * {@link PTransform}s for reading and writing
 * <a href="https://developers.google.com/bigquery/">BigQuery</a> tables.
 *
 * <h3>Table References</h3>
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
 * <p>To read from a BigQuery table, apply a {@link BigQueryIO.Read} transformation.
 * This produces a {@link PCollection} of {@link TableRow TableRows} as output:
 * <pre>{@code
 * PCollection<TableRow> shakespeare = pipeline.apply(
 *     BigQueryIO.Read.named("Read")
 *                    .from("clouddataflow-readonly:samples.weather_stations"));
 * }</pre>
 *
 * <p>See {@link TableRow} for more information on the {@link TableRow} object.
 *
 * <p>Users may provide a query to read from rather than reading all of a BigQuery table. If
 * specified, the result obtained by executing the specified query will be used as the data of the
 * input transform.
 *
 * <pre>{@code
 * PCollection<TableRow> shakespeare = pipeline.apply(
 *     BigQueryIO.Read.named("Read")
 *                    .fromQuery("SELECT year, mean_temp FROM samples.weather_stations"));
 * }</pre>
 *
 * <p>When creating a BigQuery input transform, users should provide either a query or a table.
 * Pipeline construction will fail with a validation error if neither or both are specified.
 *
 * <h3>Writing</h3>
 * <p>To write to a BigQuery table, apply a {@link BigQueryIO.Write} transformation.
 * This consumes a {@link PCollection} of {@link TableRow TableRows} as input.
 * <pre>{@code
 * PCollection<TableRow> quotes = ...
 *
 * List<TableFieldSchema> fields = new ArrayList<>();
 * fields.add(new TableFieldSchema().setName("source").setType("STRING"));
 * fields.add(new TableFieldSchema().setName("quote").setType("STRING"));
 * TableSchema schema = new TableSchema().setFields(fields);
 *
 * quotes.apply(BigQueryIO.Write
 *     .named("Write")
 *     .to("my-project:output.output_table")
 *     .withSchema(schema)
 *     .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
 * }</pre>
 *
 * <p>See {@link BigQueryIO.Write} for details on how to specify if a write should
 * append to an existing table, replace the table, or verify that the table is
 * empty. Note that the dataset being written to must already exist. Write
 * dispositions are not supported in streaming mode.
 *
 * <h3>Sharding BigQuery output tables</h3>
 * <p>A common use case is to dynamically generate BigQuery table names based on
 * the current window. To support this,
 * {@link BigQueryIO.Write#to(SerializableFunction)}
 * accepts a function mapping the current window to a tablespec. For example,
 * here's code that outputs daily tables to BigQuery:
 * <pre>{@code
 * PCollection<TableRow> quotes = ...
 * quotes.apply(Window.<TableRow>into(CalendarWindows.days(1)))
 *       .apply(BigQueryIO.Write
 *         .named("Write")
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

  // TODO: make this private and remove improper access from BigQueryIOTranslator.
  public static final String SET_PROJECT_FROM_OPTIONS_WARNING =
      "No project specified for BigQuery table \"%1$s.%2$s\". Assuming it is in \"%3$s\". If the"
      + " table is in a different project please specify it as a part of the BigQuery table"
      + " definition.";

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
  public static class Read {
    /**
     * Returns a {@link Read.Bound} with the given name. The BigQuery table or query to be read
     * from has not yet been configured.
     */
    public static Bound named(String name) {
      return new Bound().named(name);
    }

    /**
     * Reads a BigQuery table specified as {@code "[project_id]:[dataset_id].[table_id]"} or
     * {@code "[dataset_id].[table_id]"} for tables within the current project.
     */
    public static Bound from(String tableSpec) {
      return new Bound().from(tableSpec);
    }

    /**
     * Reads results received after executing the given query.
     */
    public static Bound fromQuery(String query) {
      return new Bound().fromQuery(query);
    }

    /**
     * Reads a BigQuery table specified as a {@link TableReference} object.
     */
    public static Bound from(TableReference table) {
      return new Bound().from(table);
    }

    /**
     * Disables BigQuery table validation, which is enabled by default.
     */
    public static Bound withoutValidation() {
      return new Bound().withoutValidation();
    }

    /**
     * A {@link PTransform} that reads from a BigQuery table and returns a bounded
     * {@link PCollection} of {@link TableRow TableRows}.
     */
    public static class Bound extends PTransform<PInput, PCollection<TableRow>> {
      TableReference table;
      final String query;
      final boolean validate;
      @Nullable
      Boolean flattenResults;

      private static final String QUERY_VALIDATION_FAILURE_ERROR =
          "Validation of query \"%1$s\" failed. If the query depends on an earlier stage of the"
          + " pipeline, This validation can be disabled using #withoutValidation.";

      private Bound() {
        this(null, null, null, true, null);
      }

      private Bound(String name, String query, TableReference reference, boolean validate,
          Boolean flattenResults) {
        super(name);
        this.table = reference;
        this.query = query;
        this.validate = validate;
        this.flattenResults = flattenResults;
      }

      /**
       * Returns a copy of this transform using the name associated with this transformation.
       *
       * <p>Does not modify this object.
       */
      public Bound named(String name) {
        return new Bound(name, query, table, validate, flattenResults);
      }

      /**
       * Returns a copy of this transform that reads from the specified table. Refer to
       * {@link #parseTableSpec(String)} for the specification format.
       *
       * <p>Does not modify this object.
       */
      public Bound from(String tableSpec) {
        return from(parseTableSpec(tableSpec));
      }

      /**
       * Returns a copy of this transform that reads from the specified table.
       *
       * <p>Does not modify this object.
       */
      public Bound from(TableReference table) {
        return new Bound(name, query, table, validate, flattenResults);
      }

      /**
       * Returns a copy of this transform that reads the results of the specified query.
       *
       * <p>Does not modify this object.
       *
       * <p>By default, the query results will be flattened -- see
       * "flattenResults" in the <a href="https://cloud.google.com/bigquery/docs/reference/v2/jobs">
       * Jobs documentation</a> for more information.  To disable flattening, use
       * {@link BigQueryIO.Read.Bound#withoutResultFlattening}.
       */
      public Bound fromQuery(String query) {
        return new Bound(name, query, table, validate,
            MoreObjects.firstNonNull(flattenResults, Boolean.TRUE));
      }

      /**
       * Disable table validation.
       */
      public Bound withoutValidation() {
        return new Bound(name, query, table, false, flattenResults);
      }

      /**
       * Disable <a href="https://cloud.google.com/bigquery/docs/reference/v2/jobs">
       * flattening of query results</a>.
       *
       * <p>Only valid when a query is used ({@link #fromQuery}). Setting this option when reading
       * from a table will cause an error during validation.
       */
      public Bound withoutResultFlattening() {
        return new Bound(name, query, table, validate, false);
      }

      /**
       * Validates the current {@link PTransform}.
       */
      @Override
      public void validate(PInput input) {
        if (table == null && query == null) {
          throw new IllegalStateException(
              "Invalid BigQuery read operation, either table reference or query has to be set");
        } else if (table != null && query != null) {
          throw new IllegalStateException("Invalid BigQuery read operation. Specifies both a"
              + " query and a table, only one of these should be provided");
        } else if (table != null && flattenResults != null) {
          throw new IllegalStateException("Invalid BigQuery read operation. Specifies a"
              + " table with a result flattening preference, which is not configurable");
        } else if (query != null && flattenResults == null) {
          throw new IllegalStateException("Invalid BigQuery read operation. Specifies a"
              + " query without a result flattening preference");
        }

        BigQueryOptions bqOptions = input.getPipeline().getOptions().as(BigQueryOptions.class);
        if (table != null && table.getProjectId() == null) {
          // If user does not specify a project we assume the table to be located in the project
          // that owns the Dataflow job.
          LOG.warn(String.format(SET_PROJECT_FROM_OPTIONS_WARNING, table.getDatasetId(),
              table.getTableId(), bqOptions.getProject()));
          table.setProjectId(bqOptions.getProject());
        }

        if (validate) {
          // Check for source table/query presence for early failure notification.
          // Note that a presence check can fail if the table or dataset are created by earlier
          // stages of the pipeline or if a query depends on earlier stages of a pipeline. For these
          // cases the withoutValidation method can be used to disable the check.
          if (table != null) {
            verifyDatasetPresence(bqOptions, table);
            verifyTablePresence(bqOptions, table);
          }
          if (query != null) {
            dryRunQuery(bqOptions, query);
          }
        }
      }

      private static void dryRunQuery(BigQueryOptions options, String query) {
        Bigquery client = Transport.newBigQueryClient(options).build();
        QueryRequest request = new QueryRequest();
        request.setQuery(query);
        request.setDryRun(true);

        try {
          BigQueryTableRowIterator.executeWithBackOff(
              client.jobs().query(options.getProject(), request), QUERY_VALIDATION_FAILURE_ERROR,
              query);
        } catch (Exception e) {
          throw new IllegalArgumentException(
              String.format(QUERY_VALIDATION_FAILURE_ERROR, query), e);
        }
      }

      @Override
      public PCollection<TableRow> apply(PInput input) {
        return PCollection.<TableRow>createPrimitiveOutputInternal(
            input.getPipeline(),
            WindowingStrategy.globalDefault(),
            IsBounded.BOUNDED)
            // Force the output's Coder to be what the read is using, and
            // unchangeable later, to ensure that we read the input in the
            // format specified by the Read transform.
            .setCoder(TableRowJsonCoder.of());
      }

      @Override
      protected Coder<TableRow> getDefaultOutputCoder() {
        return TableRowJsonCoder.of();
      }

      static {
        DirectPipelineRunner.registerDefaultTransformEvaluator(
            Bound.class, new DirectPipelineRunner.TransformEvaluator<Bound>() {
              @Override
              public void evaluate(
                  Bound transform, DirectPipelineRunner.EvaluationContext context) {
                evaluateReadHelper(transform, context);
              }
            });
      }

      /**
       * Returns the table to write, or {@code null} if reading from a query instead.
       */
      public TableReference getTable() {
        return table;
      }

      /**
       * Returns the query to be read, or {@code null} if reading from a table instead.
       */
      public String getQuery() {
        return query;
      }

      /**
       * Returns true if table validation is enabled.
       */
      public boolean getValidate() {
        return validate;
      }

      /**
       * Returns true/false if result flattening is enabled/disabled, or null if not applicable.
       */
      public Boolean getFlattenResults() {
        return flattenResults;
      }
    }

    /** Disallow construction of utility class. */
    private Read() {}
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link PTransform} that writes a {@link PCollection} containing {@link TableRow TableRows}
   * to a BigQuery table.
   *
   * <p>In BigQuery, each table has an encosing dataset. The dataset being written must already
   * exist.
   *
   * <p>By default, tables will be created if they do not exist, which corresponds to a
   * {@link CreateDisposition#CREATE_IF_NEEDED} disposition that matches the default of BigQuery's
   * Jobs API. A schema must be provided (via {@link BigQueryIO.Write#withSchema(TableSchema)}),
   * or else the transform may fail at runtime with an {@link IllegalArgumentException}.
   *
   * <p>By default, writes require an empty table, which corresponds to
   * a {@link WriteDisposition#WRITE_EMPTY} disposition that matches the
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
  public static class Write {
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

    /**
     * Creates a write transformation with the given transform name. The BigQuery table to be
     * written has not yet been configured.
     */
    public static Bound named(String name) {
      return new Bound().named(name);
    }

    /**
     * Creates a write transformation for the given table specification.
     *
     * <p>Refer to {@link #parseTableSpec(String)} for the specification format.
     */
    public static Bound to(String tableSpec) {
      return new Bound().to(tableSpec);
    }

    /** Creates a write transformation for the given table. */
    public static Bound to(TableReference table) {
      return new Bound().to(table);
    }

    /**
     * Creates a write transformation from a function that maps windows to table specifications.
     * Each time a new window is encountered, this function will be called and the resulting table
     * will be created. Records within that window will be written to the associated table.
     *
     * <p>See {@link #parseTableSpec(String)} for the format that {@code tableSpecFunction} should
     * return.
     *
     * <p>{@code tableSpecFunction} should be deterministic. When given the same window, it should
     * always return the same table specification.
     */
    public static Bound to(SerializableFunction<BoundedWindow, String> tableSpecFunction) {
      return new Bound().to(tableSpecFunction);
    }

    /**
     * Creates a write transformation from a function that maps windows to {@link TableReference}
     * objects.
     *
     * <p>{@code tableRefFunction} should be deterministic. When given the same window, it should
     * always return the same table reference.
     */
    public static Bound toTableReference(
        SerializableFunction<BoundedWindow, TableReference> tableRefFunction) {
      return new Bound().toTableReference(tableRefFunction);
    }

    /**
     * Creates a write transformation with the specified schema to use in table creation.
     *
     * <p>The schema is <i>required</i> only if writing to a table that does not already
     * exist, and {@link CreateDisposition} is set to
     * {@link CreateDisposition#CREATE_IF_NEEDED}.
     */
    public static Bound withSchema(TableSchema schema) {
      return new Bound().withSchema(schema);
    }

    /** Creates a write transformation with the specified options for creating the table. */
    public static Bound withCreateDisposition(CreateDisposition disposition) {
      return new Bound().withCreateDisposition(disposition);
    }

    /** Creates a write transformation with the specified options for writing to the table. */
    public static Bound withWriteDisposition(WriteDisposition disposition) {
      return new Bound().withWriteDisposition(disposition);
    }

    /**
     * Creates a write transformation with BigQuery table validation disabled.
     */
    public static Bound withoutValidation() {
      return new Bound().withoutValidation();
    }

    /**
     * A {@link PTransform} that can write either a bounded or unbounded
     * {@link PCollection} of {@link TableRow TableRows} to a BigQuery table.
     */
    public static class Bound extends PTransform<PCollection<TableRow>, PDone> {
      @Nullable final String jsonTableRef;

      @Nullable final SerializableFunction<BoundedWindow, TableReference> tableRefFunction;

      // Table schema. The schema is required only if the table does not exist.
      @Nullable final String jsonSchema;

      // Options for creating the table. Valid values are CREATE_IF_NEEDED and
      // CREATE_NEVER.
      final CreateDisposition createDisposition;

      // Options for writing to the table. Valid values are WRITE_TRUNCATE,
      // WRITE_APPEND and WRITE_EMPTY.
      final WriteDisposition writeDisposition;

      // An option to indicate if table validation is desired. Default is true.
      final boolean validate;

      // A fake or mock BigQueryServices for tests.
      @Nullable private BigQueryServices testBigQueryServices;

      private static class TranslateTableSpecFunction implements
          SerializableFunction<BoundedWindow, TableReference> {
        private SerializableFunction<BoundedWindow, String> tableSpecFunction;

        TranslateTableSpecFunction(SerializableFunction<BoundedWindow, String> tableSpecFunction) {
          this.tableSpecFunction = tableSpecFunction;
        }

        @Override
        public TableReference apply(BoundedWindow value) {
          return parseTableSpec(tableSpecFunction.apply(value));
        }
      }

      /**
       * @deprecated Should be private. Instead, use one of the factory methods in
       * {@link BigQueryIO.Write}, such as {@link BigQueryIO.Write#to(String)}, to create an
       * instance of this class.
       */
      @Deprecated
      public Bound() {
        this(null, null, null, null, CreateDisposition.CREATE_IF_NEEDED,
            WriteDisposition.WRITE_EMPTY, true, null);
      }

      private Bound(String name, @Nullable String jsonTableRef,
          @Nullable SerializableFunction<BoundedWindow, TableReference> tableRefFunction,
          @Nullable String jsonSchema,
          CreateDisposition createDisposition, WriteDisposition writeDisposition, boolean validate,
          @Nullable BigQueryServices testBigQueryServices) {
        super(name);
        this.jsonTableRef = jsonTableRef;
        this.tableRefFunction = tableRefFunction;
        this.jsonSchema = jsonSchema;
        this.createDisposition = checkNotNull(createDisposition, "createDisposition");
        this.writeDisposition = checkNotNull(writeDisposition, "writeDisposition");
        this.validate = validate;
        this.testBigQueryServices = testBigQueryServices;
      }

      /**
       * Returns a copy of this write transformation, but with the specified transform name.
       *
       * <p>Does not modify this object.
       */
      public Bound named(String name) {
        return new Bound(name, jsonTableRef, tableRefFunction, jsonSchema, createDisposition,
            writeDisposition, validate, testBigQueryServices);
      }

      /**
       * Returns a copy of this write transformation, but writing to the specified table. Refer to
       * {@link #parseTableSpec(String)} for the specification format.
       *
       * <p>Does not modify this object.
       */
      public Bound to(String tableSpec) {
        return to(parseTableSpec(tableSpec));
      }

      /**
       * Returns a copy of this write transformation, but writing to the specified table.
       *
       * <p>Does not modify this object.
       */
      public Bound to(TableReference table) {
        return new Bound(name, toJsonString(table), tableRefFunction, jsonSchema, createDisposition,
            writeDisposition, validate, testBigQueryServices);
      }

      /**
       * Returns a copy of this write transformation, but using the specified function to determine
       * which table to write to for each window.
       *
       * <p>Does not modify this object.
       *
       * <p>{@code tableSpecFunction} should be deterministic. When given the same window, it
       * should always return the same table specification.
       */
      public Bound to(
          SerializableFunction<BoundedWindow, String> tableSpecFunction) {
        return toTableReference(new TranslateTableSpecFunction(tableSpecFunction));
      }

      /**
       * Returns a copy of this write transformation, but using the specified function to determine
       * which table to write to for each window.
       *
       * <p>Does not modify this object.
       *
       * <p>{@code tableRefFunction} should be deterministic. When given the same window, it should
       * always return the same table reference.
       */
      public Bound toTableReference(
          SerializableFunction<BoundedWindow, TableReference> tableRefFunction) {
        return new Bound(name, jsonTableRef, tableRefFunction, jsonSchema, createDisposition,
            writeDisposition, validate, testBigQueryServices);
      }

      /**
       * Returns a copy of this write transformation, but using the specified schema for rows
       * to be written.
       *
       * <p>Does not modify this object.
       */
      public Bound withSchema(TableSchema schema) {
        return new Bound(name, jsonTableRef, tableRefFunction, toJsonString(schema),
            createDisposition, writeDisposition, validate, testBigQueryServices);
      }

      /**
       * Returns a copy of this write transformation, but using the specified create disposition.
       *
       * <p>Does not modify this object.
       */
      public Bound withCreateDisposition(CreateDisposition createDisposition) {
        return new Bound(name, jsonTableRef, tableRefFunction, jsonSchema, createDisposition,
            writeDisposition, validate, testBigQueryServices);
      }

      /**
       * Returns a copy of this write transformation, but using the specified write disposition.
       *
       * <p>Does not modify this object.
       */
      public Bound withWriteDisposition(WriteDisposition writeDisposition) {
        return new Bound(name, jsonTableRef, tableRefFunction, jsonSchema, createDisposition,
            writeDisposition, validate, testBigQueryServices);
      }

      /**
       * Returns a copy of this write transformation, but without BigQuery table validation.
       *
       * <p>Does not modify this object.
       */
      public Bound withoutValidation() {
        return new Bound(name, jsonTableRef, tableRefFunction, jsonSchema, createDisposition,
            writeDisposition, false, testBigQueryServices);
      }

      @VisibleForTesting
      Bound withTestServices(BigQueryServices testServices) {
        return new Bound(name, jsonTableRef, tableRefFunction, jsonSchema, createDisposition,
            writeDisposition, validate, testServices);
      }

      private static void verifyTableEmpty(
          BigQueryOptions options,
          TableReference table) {
        try {
          Bigquery client = Transport.newBigQueryClient(options).build();
          BigQueryTableInserter inserter = new BigQueryTableInserter(client);
          if (!inserter.isEmpty(table)) {
            throw new IllegalArgumentException(
                "BigQuery table is not empty: " + BigQueryIO.toTableSpec(table));
          }
        } catch (IOException e) {
          ApiErrorExtractor errorExtractor = new ApiErrorExtractor();
          if (errorExtractor.itemNotFound(e)) {
            // Nothing to do. If the table does not exist, it is considered empty.
          } else {
            throw new RuntimeException(
                "unable to confirm BigQuery table emptiness for table "
                    + BigQueryIO.toTableSpec(table), e);
          }
        }
      }

      @Override
      public PDone apply(PCollection<TableRow> input) {
        BigQueryOptions options = input.getPipeline().getOptions().as(BigQueryOptions.class);

        TableReference table = getTable();
        if (table == null && tableRefFunction == null) {
          throw new IllegalStateException(
              "must set the table reference of a BigQueryIO.Write transform");
        }
        if (table != null && tableRefFunction != null) {
          throw new IllegalStateException(
              "Cannot set both a table reference and a table function for a BigQueryIO.Write "
                + "transform");
        }

        if (createDisposition == CreateDisposition.CREATE_IF_NEEDED && jsonSchema == null) {
          throw new IllegalArgumentException("CreateDisposition is CREATE_IF_NEEDED, "
              + "however no schema was provided.");
        }

        if (table != null && table.getProjectId() == null) {
          // If user does not specify a project we assume the table to be located in the project
          // that owns the Dataflow job.
          String projectIdFromOptions = options.getProject();
          LOG.warn(String.format(BigQueryIO.SET_PROJECT_FROM_OPTIONS_WARNING, table.getDatasetId(),
              table.getTableId(), projectIdFromOptions));
          table.setProjectId(projectIdFromOptions);
        }

        // Check for destination table presence and emptiness for early failure notification.
        // Note that a presence check can fail if the table or dataset are created by earlier stages
        // of the pipeline. For these cases the withoutValidation method can be used to disable
        // the check.
        // Unfortunately we can't validate anything early in case tableRefFunction is specified.
        if (table != null && validate) {
          verifyDatasetPresence(options, table);
          if (getCreateDisposition() == BigQueryIO.Write.CreateDisposition.CREATE_NEVER) {
            verifyTablePresence(options, table);
          }
          if (getWriteDisposition() == BigQueryIO.Write.WriteDisposition.WRITE_EMPTY) {
            verifyTableEmpty(options, table);
          }
        }

        // In streaming, BigQuery write is taken care of by StreamWithDeDup transform.
        // We also currently do this if a tablespec function is specified.
        if (options.isStreaming() || tableRefFunction != null) {
          if (createDisposition == CreateDisposition.CREATE_NEVER) {
            throw new IllegalArgumentException("CreateDispostion.CREATE_NEVER is not "
                + "supported for unbounded PCollections or when using tablespec functions.");
          }

          if (writeDisposition == WriteDisposition.WRITE_TRUNCATE) {
            throw new IllegalArgumentException("WriteDisposition.WRITE_TRUNCATE is not "
                + "supported for unbounded PCollections or when using tablespec functions.");
          }

          return input.apply(new StreamWithDeDup(table, tableRefFunction, getSchema()));
        }

        String tempLocation = options.getTempLocation();
        checkArgument(!Strings.isNullOrEmpty(tempLocation),
            "BigQueryIO.Write needs a GCS temp location to store temp files.");
        if (testBigQueryServices == null) {
          try {
            GcsPath.fromUri(tempLocation);
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                "BigQuery temp location expected a valid 'gs://' path, but was given '%s'",
                tempLocation), e);
          }
        }
        String jobIdToken = UUID.randomUUID().toString();
        String tempFilePrefix = tempLocation + "/BigQuerySinkTemp/" + jobIdToken;
        BigQueryServices bqServices = getBigQueryServices();
        return input.apply("Write", com.google.cloud.dataflow.sdk.io.Write.to(
            new BigQuerySink(
                jobIdToken,
                jsonTableRef,
                jsonSchema,
                getWriteDisposition(),
                getCreateDisposition(),
                tempFilePrefix,
                input.getCoder(),
                bqServices)));
      }

      @Override
      protected Coder<Void> getDefaultOutputCoder() {
        return VoidCoder.of();
      }

      /** Returns the create disposition. */
      public CreateDisposition getCreateDisposition() {
        return createDisposition;
      }

      /** Returns the write disposition. */
      public WriteDisposition getWriteDisposition() {
        return writeDisposition;
      }

      /** Returns the table schema. */
      public TableSchema getSchema() {
        return fromJsonString(jsonSchema, TableSchema.class);
      }

      /** Returns the table reference, or {@code null} if a . */
      public TableReference getTable() {
        return fromJsonString(jsonTableRef, TableReference.class);
      }

      /** Returns {@code true} if table validation is enabled. */
      public boolean getValidate() {
        return validate;
      }

      private BigQueryServices getBigQueryServices() {
        if (testBigQueryServices != null) {
          return testBigQueryServices;
        } else {
          return new BigQueryServicesImpl();
        }
      }
    }

    /** Disallow construction of utility class. */
    private Write() {}
  }

  /**
   * {@link BigQuerySink} is implemented as a {@link FileBasedSink}.
   *
   * <p>It uses BigQuery load job to import files into BigQuery.
   */
  static class BigQuerySink extends FileBasedSink<TableRow> {
    private final String jobIdToken;
    @Nullable private final String jsonTable;
    @Nullable private final String jsonSchema;
    private final WriteDisposition writeDisposition;
    private final CreateDisposition createDisposition;
    private final Coder<TableRow> coder;
    private final BigQueryServices bqServices;

    public BigQuerySink(
        String jobIdToken,
        @Nullable String jsonTable,
        @Nullable String jsonSchema,
        WriteDisposition writeDisposition,
        CreateDisposition createDisposition,
        String tempFile,
        Coder<TableRow> coder,
        BigQueryServices bqServices) {
      super(tempFile, ".json");
      this.jobIdToken = checkNotNull(jobIdToken, "jobIdToken");
      this.jsonTable = jsonTable;
      this.jsonSchema = jsonSchema;
      this.writeDisposition = checkNotNull(writeDisposition, "writeDisposition");
      this.createDisposition = checkNotNull(createDisposition, "createDisposition");
      this.coder = checkNotNull(coder, "coder");
      this.bqServices = checkNotNull(bqServices, "bqServices");
     }

    @Override
    public FileBasedSink.FileBasedWriteOperation<TableRow> createWriteOperation(
        PipelineOptions options) {
      return new BigQueryWriteOperation(this);
    }

    private static class BigQueryWriteOperation extends FileBasedWriteOperation<TableRow> {
      // The maximum number of retry load jobs.
      private static final int MAX_RETRY_LOAD_JOBS = 3;

      private final BigQuerySink bigQuerySink;

      private BigQueryWriteOperation(BigQuerySink sink) {
        super(checkNotNull(sink, "sink"));
        this.bigQuerySink = sink;
      }

      @Override
      public FileBasedWriter<TableRow> createWriter(PipelineOptions options) throws Exception {
        return new TableRowWriter(this, bigQuerySink.coder);
      }

      @Override
      public void finalize(Iterable<FileResult> writerResults, PipelineOptions options)
          throws IOException, InterruptedException {
        try {
          BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
          List<String> tempFiles = Lists.newArrayList();
          for (FileResult result : writerResults) {
            tempFiles.add(result.getFilename());
          }
          if (!tempFiles.isEmpty()) {
              load(
                  bigQuerySink.bqServices.getLoadService(bqOptions),
                  bigQuerySink.jobIdToken,
                  fromJsonString(bigQuerySink.jsonTable, TableReference.class),
                  tempFiles,
                  fromJsonString(bigQuerySink.jsonSchema, TableSchema.class),
                  bigQuerySink.writeDisposition,
                  bigQuerySink.createDisposition);
          }
        } finally {
          removeTemporaryFiles(options);
        }
      }

      /**
       * Import files into BigQuery with load jobs.
       *
       * <p>Returns if files are successfully loaded into BigQuery.
       * Throws a RuntimeException if:
       *     1. The status of one load job is UNKNOWN. This is to avoid duplicating data.
       *     2. It exceeds {@code MAX_RETRY_LOAD_JOBS}.
       *
       * <p>If a load job failed, it will try another load job with a different job id.
       */
      private void load(
          LoadService loadService,
          String jobIdPrefix,
          TableReference ref,
          List<String> gcsUris,
          @Nullable TableSchema schema,
          WriteDisposition writeDisposition,
          CreateDisposition createDisposition) throws InterruptedException, IOException {
        JobConfigurationLoad loadConfig = new JobConfigurationLoad();
        loadConfig.setSourceUris(gcsUris);
        loadConfig.setDestinationTable(ref);
        loadConfig.setSchema(schema);
        loadConfig.setWriteDisposition(writeDisposition.name());
        loadConfig.setCreateDisposition(createDisposition.name());
        loadConfig.setSourceFormat("NEWLINE_DELIMITED_JSON");

        boolean retrying = false;
        String projectId = ref.getProjectId();
        for (int i = 0; i < MAX_RETRY_LOAD_JOBS; ++i) {
          String jobId = jobIdPrefix + "-" + i;
          if (retrying) {
            LOG.info("Previous load jobs failed, retrying.");
          }
          LOG.info("Starting BigQuery load job: {}", jobId);
          loadService.startLoadJob(jobId, loadConfig);
          BigQueryServices.Status jobStatus = loadService.pollJobStatus(projectId, jobId);
          switch (jobStatus) {
            case SUCCEEDED:
              return;
            case UNKNOWN:
              throw new RuntimeException("Failed to poll the load job status.");
            case FAILED:
              LOG.info("BigQuery load job failed: {}", jobId);
              retrying = true;
              continue;
            default:
              throw new IllegalStateException("Unexpected job status: " + jobStatus);
          }
        }
        throw new RuntimeException(
            "Failed to create the load job, reached max retries: " + MAX_RETRY_LOAD_JOBS);
      }
    }

    private static class TableRowWriter extends FileBasedWriter<TableRow> {
      private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
      private final Coder<TableRow> coder;
      private OutputStream out;

      public TableRowWriter(
          FileBasedWriteOperation<TableRow> writeOperation, Coder<TableRow> coder) {
        super(writeOperation);
        this.mimeType = MimeTypes.TEXT;
        this.coder = coder;
      }

      @Override
      protected void prepareWrite(WritableByteChannel channel) throws Exception {
        out = Channels.newOutputStream(channel);
      }

      @Override
      public void write(TableRow value) throws Exception {
        // Use Context.OUTER to encode and NEWLINE as the delimeter.
        coder.encode(value, out, Context.OUTER);
        out.write(NEWLINE);
      }
    }
  }

  private static void verifyDatasetPresence(BigQueryOptions options, TableReference table) {
    try {
      Bigquery client = Transport.newBigQueryClient(options).build();
      BigQueryTableRowIterator.executeWithBackOff(
          client.datasets().get(table.getProjectId(), table.getDatasetId()),
          RESOURCE_NOT_FOUND_ERROR, "dataset", BigQueryIO.toTableSpec(table));
    } catch (Exception e) {
      ApiErrorExtractor errorExtractor = new ApiErrorExtractor();
      if ((e instanceof IOException) && errorExtractor.itemNotFound((IOException) e)) {
        throw new IllegalArgumentException(
            String.format(RESOURCE_NOT_FOUND_ERROR, "dataset", BigQueryIO.toTableSpec(table)),
            e);
      } else {
        throw new RuntimeException(
            String.format(UNABLE_TO_CONFIRM_PRESENCE_OF_RESOURCE_ERROR, "dataset",
                BigQueryIO.toTableSpec(table)),
            e);
      }
    }
  }

  private static void verifyTablePresence(BigQueryOptions options, TableReference table) {
    try {
      Bigquery client = Transport.newBigQueryClient(options).build();
      BigQueryTableRowIterator.executeWithBackOff(
          client.tables().get(table.getProjectId(), table.getDatasetId(), table.getTableId()),
          RESOURCE_NOT_FOUND_ERROR, "table", BigQueryIO.toTableSpec(table));
    } catch (Exception e) {
      ApiErrorExtractor errorExtractor = new ApiErrorExtractor();
      if ((e instanceof IOException) && errorExtractor.itemNotFound((IOException) e)) {
        throw new IllegalArgumentException(
            String.format(RESOURCE_NOT_FOUND_ERROR, "table", BigQueryIO.toTableSpec(table)), e);
      } else {
        throw new RuntimeException(
            String.format(UNABLE_TO_CONFIRM_PRESENCE_OF_RESOURCE_ERROR, "table",
                BigQueryIO.toTableSpec(table)),
            e);
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Implementation of DoFn to perform streaming BigQuery write.
   */
  @SystemDoFnInternal
  private static class StreamingWriteFn
      extends DoFn<KV<ShardedKey<String>, TableRowInfo>, Void> {
    /** TableSchema in JSON. Use String to make the class Serializable. */
    private final String jsonTableSchema;

    /** JsonTableRows to accumulate BigQuery rows in order to batch writes. */
    private transient Map<String, List<TableRow>> tableRows;

    /** The list of unique ids for each BigQuery table row. */
    private transient Map<String, List<String>> uniqueIdsForTableRows;

    /** The list of tables created so far, so we don't try the creation
        each time. */
    private static Set<String> createdTables =
        Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /** Tracks bytes written, exposed as "ByteCount" Counter. */
    private Aggregator<Long, Long> byteCountAggregator =
        createAggregator("ByteCount", new Sum.SumLongFn());

    /** Constructor. */
    StreamingWriteFn(TableSchema schema) {
      jsonTableSchema = toJsonString(schema);
    }

    /** Prepares a target BigQuery table. */
    @Override
    public void startBundle(Context context) {
      tableRows = new HashMap<>();
      uniqueIdsForTableRows = new HashMap<>();
    }

    /** Accumulates the input into JsonTableRows and uniqueIdsForTableRows. */
    @Override
    public void processElement(ProcessContext context) {
      String tableSpec = context.element().getKey().getKey();
      List<TableRow> rows = getOrCreateMapListValue(tableRows, tableSpec);
      List<String> uniqueIds = getOrCreateMapListValue(uniqueIdsForTableRows, tableSpec);

      rows.add(context.element().getValue().tableRow);
      uniqueIds.add(context.element().getValue().uniqueId);
    }

    /** Writes the accumulated rows into BigQuery with streaming API. */
    @Override
    public void finishBundle(Context context) throws Exception {
      BigQueryOptions options = context.getPipelineOptions().as(BigQueryOptions.class);
      Bigquery client = Transport.newBigQueryClient(options).build();

      for (String tableSpec : tableRows.keySet()) {
        TableReference tableReference = getOrCreateTable(options, tableSpec);
        flushRows(client, tableReference, tableRows.get(tableSpec),
            uniqueIdsForTableRows.get(tableSpec));
      }
      tableRows.clear();
      uniqueIdsForTableRows.clear();
    }

    public TableReference getOrCreateTable(BigQueryOptions options, String tableSpec)
        throws IOException {
      TableReference tableReference = parseTableSpec(tableSpec);
      if (!createdTables.contains(tableSpec)) {
        synchronized (createdTables) {
          // Another thread may have succeeded in creating the table in the meanwhile, so
          // check again. This check isn't needed for correctness, but we add it to prevent
          // every thread from attempting a create and overwhelming our BigQuery quota.
          if (!createdTables.contains(tableSpec)) {
            TableSchema tableSchema = JSON_FACTORY.fromString(jsonTableSchema, TableSchema.class);
            Bigquery client = Transport.newBigQueryClient(options).build();
            BigQueryTableInserter inserter = new BigQueryTableInserter(client);
            inserter.getOrCreateTable(tableReference, WriteDisposition.WRITE_APPEND,
                CreateDisposition.CREATE_IF_NEEDED, tableSchema);
            createdTables.add(tableSpec);
          }
        }
      }
      return tableReference;
    }

    /** Writes the accumulated rows into BigQuery with streaming API. */
    private void flushRows(Bigquery client, TableReference tableReference,
        List<TableRow> tableRows, List<String> uniqueIds) {
      if (!tableRows.isEmpty()) {
        try {
          BigQueryTableInserter inserter = new BigQueryTableInserter(client);
          inserter.insertAll(tableReference, tableRows, uniqueIds, byteCountAggregator);
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
      return new ShardedKey<K>(key, shardNumber);
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
  private static class ShardedKeyCoder<KeyT>
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
      return new ShardedKey<KeyT>(
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

  private static class TableRowInfoCoder extends AtomicCoder<TableRowInfo> {
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
      idCoder.encode(value.uniqueId, outStream, context.nested());
    }

    @Override
    public TableRowInfo decode(InputStream inStream, Context context)
      throws IOException {
      return new TableRowInfo(
          tableRowCoder.decode(inStream, context.nested()),
          idCoder.decode(inStream, context.nested()));
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
  private static class TagWithUniqueIdsAndTable
      extends DoFn<TableRow, KV<ShardedKey<String>, TableRowInfo>>
      implements DoFn.RequiresWindowAccess {
    /** TableSpec to write to. */
    private final String tableSpec;

    /** User function mapping windows to {@link TableReference} in JSON. */
    private final SerializableFunction<BoundedWindow, TableReference> tableRefFunction;

    private transient String randomUUID;
    private transient long sequenceNo = 0L;

    TagWithUniqueIdsAndTable(BigQueryOptions options, TableReference table,
        SerializableFunction<BoundedWindow, TableReference> tableRefFunction) {
      checkArgument(table == null ^ tableRefFunction == null,
          "Exactly one of table or tableRefFunction should be set");
      if (table != null) {
        if (table.getProjectId() == null) {
          table.setProjectId(options.as(BigQueryOptions.class).getProject());
        }
        this.tableSpec = toTableSpec(table);
      } else {
        tableSpec = null;
      }
      this.tableRefFunction = tableRefFunction;
    }


    @Override
    public void startBundle(Context context) {
      randomUUID = UUID.randomUUID().toString();
    }

    /** Tag the input with a unique id. */
    @Override
    public void processElement(ProcessContext context) throws IOException {
      String uniqueId = randomUUID + sequenceNo++;
      ThreadLocalRandom randomGenerator = ThreadLocalRandom.current();
      String tableSpec = tableSpecFromWindow(
          context.getPipelineOptions().as(BigQueryOptions.class), context.window());
      // We output on keys 0-50 to ensure that there's enough batching for
      // BigQuery.
      context.output(KV.of(ShardedKey.of(tableSpec, randomGenerator.nextInt(0, 50)),
          new TableRowInfo(context.element(), uniqueId)));
    }

    private String tableSpecFromWindow(BigQueryOptions options, BoundedWindow window) {
      if (tableSpec != null) {
        return tableSpec;
      } else {
        TableReference table = tableRefFunction.apply(window);
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
  private static class StreamWithDeDup extends PTransform<PCollection<TableRow>, PDone> {
    private final transient TableReference tableReference;
    private final SerializableFunction<BoundedWindow, TableReference> tableRefFunction;
    private final transient TableSchema tableSchema;

    /** Constructor. */
    StreamWithDeDup(TableReference tableReference,
        SerializableFunction<BoundedWindow, TableReference> tableRefFunction,
        TableSchema tableSchema) {
      this.tableReference = tableReference;
      this.tableRefFunction = tableRefFunction;
      this.tableSchema = tableSchema;
    }

    @Override
    protected Coder<Void> getDefaultOutputCoder() {
      return VoidCoder.of();
    }

    @Override
    public PDone apply(PCollection<TableRow> input) {
      // A naive implementation would be to simply stream data directly to BigQuery.
      // However, this could occasionally lead to duplicated data, e.g., when
      // a VM that runs this code is restarted and the code is re-run.

      // The above risk is mitigated in this implementation by relying on
      // BigQuery built-in best effort de-dup mechanism.

      // To use this mechanism, each input TableRow is tagged with a generated
      // unique id, which is then passed to BigQuery and used to ignore duplicates.

      PCollection<KV<ShardedKey<String>, TableRowInfo>> tagged = input.apply(ParDo.of(
          new TagWithUniqueIdsAndTable(input.getPipeline().getOptions().as(BigQueryOptions.class),
              tableReference, tableRefFunction)));

      // To prevent having the same TableRow processed more than once with regenerated
      // different unique ids, this implementation relies on "checkpointing", which is
      // achieved as a side effect of having StreamingWriteFn immediately follow a GBK,
      // performed by Reshuffle.
      tagged
          .setCoder(KvCoder.of(ShardedKeyCoder.of(StringUtf8Coder.of()), TableRowInfoCoder.of()))
          .apply(Reshuffle.<ShardedKey<String>, TableRowInfo>of())
          .apply(ParDo.of(new StreamingWriteFn(tableSchema)));

      // Note that the implementation to return PDone here breaks the
      // implicit assumption about the job execution order. If a user
      // implements a PTransform that takes PDone returned here as its
      // input, the transform may not necessarily be executed after
      // the BigQueryIO.Write.

      return PDone.in(input.getPipeline());
    }
  }

  private static String toJsonString(Object item) {
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

  private static <T> T fromJsonString(String json, Class<T> clazz) {
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

  /////////////////////////////////////////////////////////////////////////////

  /** Disallow construction of utility class. */
  private BigQueryIO() {}

  /**
   * Direct mode read evaluator.
   *
   * <p>This loads the entire table into an in-memory PCollection.
   */
  private static void evaluateReadHelper(
      Read.Bound transform, DirectPipelineRunner.EvaluationContext context) {
    BigQueryOptions options = context.getPipelineOptions();
    Bigquery client = Transport.newBigQueryClient(options).build();
    if (transform.table != null && transform.table.getProjectId() == null) {
      transform.table.setProjectId(options.getProject());
    }

    BigQueryTableRowIterator iterator;
    if (transform.query != null) {
      LOG.info("Reading from BigQuery query {}", transform.query);
      iterator =
          BigQueryTableRowIterator.fromQuery(
              transform.query, options.getProject(), client, transform.getFlattenResults());
    } else {
      LOG.info("Reading from BigQuery table {}", toTableSpec(transform.table));
      iterator = BigQueryTableRowIterator.fromTable(transform.table, client);
    }

    try (BigQueryTableRowIterator ignored = iterator) {
      List<TableRow> elems = new ArrayList<>();
      iterator.open();
      while (iterator.advance()) {
        elems.add(iterator.getCurrent());
      }
      LOG.info("Number of records read from BigQuery: {}", elems.size());
      context.setPCollection(context.getOutput(transform), elems);
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException(e);
    }
  }

  private static <K, V> List<V> getOrCreateMapListValue(Map<K, List<V>> map, K key) {
    List<V> value = map.get(key);
    if (value == null) {
      value = new ArrayList<>();
      map.put(key, value);
    }
    return value;
  }
}
