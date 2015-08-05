/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.worker.BigQueryReader;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.BigQueryTableInserter;
import com.google.cloud.dataflow.sdk.util.BigQueryTableRowIterator;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.ReaderUtils;
import com.google.cloud.dataflow.sdk.util.Reshuffle;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

/**
 * {@link PTransform}s for reading and writing
 * <a href="https://developers.google.com/bigquery/">BigQuery</a> tables.
 * <p><h3>Table References</h3>
 * A fully-qualified BigQuery table name consists of three components:
 * <ul>
 *   <li>{@code projectId}: the Cloud project id (defaults to
 *       {@link GcpOptions#getProject()}).
 *   <li>{@code datasetId}: the BigQuery dataset id, unique within a project.
 *   <li>{@code tableId}: a table id, unique within a dataset.
 * </ul>
 * <p>
 * BigQuery table references are stored as a {@link TableReference}, which comes
 * from the <a href="https://cloud.google.com/bigquery/client-libraries">
 * BigQuery Java Client API</a>.
 * Tables can be referred to as Strings, with or without the {@code projectId}.
 * A helper function is provided ({@link BigQueryIO#parseTableSpec(String)})
 * that parses the following string forms into a {@link TableReference}:
 * <ul>
 *   <li>[{@code project_id}]:[{@code dataset_id}].[{@code table_id}]
 *   <li>[{@code dataset_id}].[{@code table_id}]
 * </ul>
 * <p><h3>Reading</h3>
 * To read from a BigQuery table, apply a {@link BigQueryIO.Read} transformation.
 * This produces a {@code PCollection<TableRow>} as output:
 * <pre>{@code
 * PCollection<TableRow> shakespeare = pipeline.apply(
 *     BigQueryIO.Read
 *         .named("Read")
 *         .from("clouddataflow-readonly:samples.weather_stations");
 * }</pre>
 *
 * <p> Users may provide a query to read from rather than reading all of a BigQuery table. If
 * specified, the result obtained by executing the specified query will be used as the data of the
 * input transform.
 *
 * <pre>{@code
 * PCollection<TableRow> shakespeare = pipeline.apply(
 *     BigQueryIO.Read
 *         .named("Read")
 *         .fromQuery("SELECT year, mean_temp FROM samples.weather_stations");
 * }</pre>
 *
 * <p> When creating a BigQuery input transform, users should provide either a query or a table.
 * Pipeline construction will fail with a validation error if neither or both are specified.
 *
 * <p><h3>Writing</h3>
 * To write to a BigQuery table, apply a {@link BigQueryIO.Write} transformation.
 * This consumes a {@code PCollection<TableRow>} as input.
 * <p>
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
 * <p>
 * See {@link BigQueryIO.Write} for details on how to specify if a write should
 * append to an existing table, replace the table, or verify that the table is
 * empty. Note that the dataset being written to must already exist. Write
 * dispositions are not supported in streaming mode.
 *
 * <p><h3>Sharding BigQuery output tables</h3>
 * A common use case is to dynamically generate BigQuery table names based on
 * the current window. To support this,
 * {@link BigQueryIO.Write#to(SerializableFunction)}
 * accepts a function mapping the current window to a tablespec. For example,
 * here's code that outputs daily tables to BigQuery:
 * <pre>{@code
 * PCollection<TableRow> quotes = ...
 * quotes.apply(Window.<TableRow>info(CalendarWindows.days(1)))
 *       .apply(BigQueryIO.Write
 *         .named("Write")
 *         .withSchema(schema)
 *         .to(new SerializableFunction<BoundedWindow, String>() {
 *               public String apply(BoundedWindow window) {
 *                 String dayString = DateTimeFormat.forPattern("yyyy_MM_dd").parseDateTime(
 *                   ((DaysWindow) window).getStartDate());
 *                 return "my-project:output.output_table_" + dayString;
 *               }
 *             }));
 *
 * }</pre>
 *
 * <p> Per-window tables are not yet supported in batch mode.
 *
 * @see <a href="https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableRow.html">TableRow</a>
 *
 * <p><h3>Permissions</h3>
 * Permission requirements depend on the
 * {@link com.google.cloud.dataflow.sdk.runners.PipelineRunner PipelineRunner} that is
 * used to execute the Dataflow job. Please refer to the documentation of corresponding
 * {@code PipelineRunner}s for more details.
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
   * Matches table specifications in the form
   * "[project_id]:[dataset_id].[table_id]" or "[dataset_id].[table_id]".
   */
  private static final String DATASET_TABLE_REGEXP =
      String.format("((?<PROJECT>%s):)?(?<DATASET>%s)\\.(?<TABLE>%s)", PROJECT_ID_REGEXP,
          DATASET_REGEXP, TABLE_REGEXP);

  private static final Pattern TABLE_SPEC = Pattern.compile(DATASET_TABLE_REGEXP);

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
   * "[project_id]:[dataset_id].[table_id]" or "[dataset_id].[table_id]".
   * <p>
   * If the project id is omitted, the default project id is used.
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
   * Returns a canonical string representation of the TableReference.
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
   * <p>
   * Each TableRow record contains values indexed by column name.  Here is a
   * sample processing function that processes a "line" column from rows:
   * <pre><code>
   * static class ExtractWordsFn extends DoFn{@literal <TableRow, String>} {
   *   {@literal @}Override
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
   * </code></pre>
   */
  public static class Read {
    public static Bound named(String name) {
      return new Bound().named(name);
    }

    /**
     * Reads a BigQuery table specified as
     * "[project_id]:[dataset_id].[table_id]" or "[dataset_id].[table_id]" for
     * tables within the current project.
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
     * Reads a BigQuery table specified as a TableReference object.
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
      private static final long serialVersionUID = 0;

      TableReference table;
      final String query;
      final boolean validate;

      private static final String QUERY_VALIDATION_FAILURE_ERROR =
          "Validation of query \"%1$s\" failed. If the query depends on an earlier stage of the"
          + " pipeline, This validation can be disabled using #withoutValidation.";

      Bound() {
        query = null;
        table = null;
        this.validate = true;
      }

      Bound(String name, String query, TableReference reference, boolean validate) {
        super(name);
        this.table = reference;
        this.query = query;
        this.validate = validate;
      }

      /**
       * Sets the name associated with this transformation.
       */
      public Bound named(String name) {
        return new Bound(name, query, table, validate);
      }

      /**
       * Sets the table specification.
       * <p>
       * Refer to {@link #parseTableSpec(String)} for the specification format.
       */
      public Bound from(String tableSpec) {
        return from(parseTableSpec(tableSpec));
      }

      /**
       * Sets the BigQuery query to be used.
       */
      public Bound fromQuery(String query) {
        return new Bound(name, query, table, validate);
      }

      /**
       * Sets the table specification.
       */
      public Bound from(TableReference table) {
        return new Bound(name, query, table, validate);
      }

      /**
       * Disable table validation.
       */
      public Bound withoutValidation() {
        return new Bound(name, query, table, false);
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
       * Returns the table to write.
       */
      public TableReference getTable() {
        return table;
      }

      public String getQuery() {
        return query;
      }

      /**
       * Returns true if table validation is enabled.
       */
      public boolean getValidate() {
        return validate;
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link PTransform} that writes a {@link PCollection} containing {@link TableRow TableRows}
   * to a BigQuery table.
   * <p>
   * By default, tables will be created if they do not exist, which
   * corresponds to a {@code CreateDisposition.CREATE_IF_NEEDED} disposition
   * that matches the default of BigQuery's Jobs API.  A schema must be
   * provided (via {@link Write#withSchema}), or else the transform may fail
   * at runtime with an {@link java.lang.IllegalArgumentException}.
   * <p>
   * The dataset being written must already exist.
   * <p>
   * By default, writes require an empty table, which corresponds to
   * a {@code WriteDisposition.WRITE_EMPTY} disposition that matches the
   * default of BigQuery's Jobs API.
   * <p>
   * Here is a sample transform that produces TableRow values containing
   * "word" and "count" columns:
   * <pre><code>
   * static class FormatCountsFn extends DoFnP{@literal <KV<String, Long>, TableRow>} {
   *   {@literal @}Override
   *   public void processElement(ProcessContext c) {
   *     TableRow row = new TableRow()
   *         .set("word", c.element().getKey())
   *         .set("count", c.element().getValue().intValue());
   *     c.output(row);
   *   }
   * }
   * </code></pre>
   */
  public static class Write {
    /**
     * An enumeration type for the BigQuery create disposition strings publicly
     * documented as {@code CREATE_NEVER}, and {@code CREATE_IF_NEEDED}.
     */
    public enum CreateDisposition {
      /**
       * Specifics that tables should not be created.
       * <p>
       * If the output table does not exist, the write fails.
       */
      CREATE_NEVER,

      /**
       * Specifies that tables should be created if needed. This is the default
       * behavior.
       * <p>
       * Requires that a table schema is provided via {@link Write#withSchema}.
       * This precondition is checked before starting a job. The schema is
       * not required to match an existing table's schema.
       * <p>
       * When this transformation is executed, if the output table does not
       * exist, the table is created from the provided schema. Note that even if
       * the table exists, it may be recreated if necessary when paired with a
       * {@link WriteDisposition#WRITE_TRUNCATE}.
       */
      CREATE_IF_NEEDED
    }

    /**
     * An enumeration type for the BigQuery write disposition strings publicly
     * documented as {@code WRITE_TRUNCATE}, {@code WRITE_APPEND}, and
     * {@code WRITE_EMPTY}.
     */
    public enum WriteDisposition {
      /**
       * Specifies that write should replace a table.
       * <p>
       * The replacement may occur in multiple steps - for instance by first
       * removing the existing table, then creating a replacement, then filling
       * it in.  This is not an atomic operation, and external programs may
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
       * <p>
       * If the output table is not empty, the write fails at runtime.
       * <p>
       * This check may occur long before data is written, and does not
       * guarantee exclusive access to the table.  If two programs are run
       * concurrently, each specifying the same output table and
       * a {@link WriteDisposition} of {@code WRITE_EMPTY}, it is possible
       * for both to succeed.
       */
      WRITE_EMPTY
    }

    /**
     * Sets the name associated with this transformation.
     */
    public static Bound named(String name) {
      return new Bound().named(name);
    }

    /**
     * Creates a write transformation for the given table specification.
     * <p>
     * Refer to {@link #parseTableSpec(String)} for the specification format.
     */
    public static Bound to(String tableSpec) {
      return new Bound().to(tableSpec);
    }

    /** Creates a write transformation for the given table. */
    public static Bound to(TableReference table) {
      return new Bound().to(table);
    }

    /** Creates a write transformation from a function that maps windows to table specifications.
     * Each time a new window is encountered, this function will be called and the resulting table
     * will be created. Records within that window will be written to the associated table.
     * <p>
     * See {@link #parseTableSpec(String)} for the format that tableSpecFunction should return.
     * <p>
     * tableSpecFunction should be determinstic. When given the same window, it should always return
     * the same table specification.
     */
    public static Bound to(SerializableFunction<BoundedWindow, String> tableSpecFunction) {
      return new Bound().to(tableSpecFunction);
    }

    /** Creates a write transformation from a function that maps windows to TableReference objects.
     */
    public static Bound toTableReference(
        SerializableFunction<BoundedWindow, TableReference> tableRefFunction) {
      return new Bound().toTableReference(tableRefFunction);
    }

    /**
     * Specifies a table schema to use in table creation.
     * <p>
     * The schema is required only if writing to a table that does not already
     * exist, and {@link BigQueryIO.Write.CreateDisposition} is set to
     * {@code CREATE_IF_NEEDED}.
     */
    public static Bound withSchema(TableSchema schema) {
      return new Bound().withSchema(schema);
    }

    /** Specifies options for creating the table. */
    public static Bound withCreateDisposition(CreateDisposition disposition) {
      return new Bound().withCreateDisposition(disposition);
    }

    /** Specifies options for writing to the table. */
    public static Bound withWriteDisposition(WriteDisposition disposition) {
      return new Bound().withWriteDisposition(disposition);
    }

    /**
     * Disables BigQuery table validation, which is enabled by default.
     */
    public static Bound withoutValidation() {
      return new Bound().withoutValidation();
    }

    /**
     * A {@link PTransform} that can write either a bounded or unbounded
     * {@link PCollection} of {@link TableRow TableRows} to a BigQuery table.
     */
    public static class Bound extends PTransform<PCollection<TableRow>, PDone> {
      private static final long serialVersionUID = 0;

      final TableReference table;

      final SerializableFunction<BoundedWindow, TableReference> tableRefFunction;

      // Table schema. The schema is required only if the table does not exist.
      final TableSchema schema;

      // Options for creating the table. Valid values are CREATE_IF_NEEDED and
      // CREATE_NEVER.
      final CreateDisposition createDisposition;

      // Options for writing to the table. Valid values are WRITE_TRUNCATE,
      // WRITE_APPEND and WRITE_EMPTY.
      final WriteDisposition writeDisposition;

      // An option to indicate if table validation is desired. Default is true.
      final boolean validate;

      private static class TranslateTableSpecFunction implements
          SerializableFunction<BoundedWindow, TableReference> {
        private static final long serialVersionUID = 0;

        private SerializableFunction<BoundedWindow, String> tableSpecFunction;

        TranslateTableSpecFunction(SerializableFunction<BoundedWindow, String> tableSpecFunction) {
          this.tableSpecFunction = tableSpecFunction;
        }

        @Override
        public TableReference apply(BoundedWindow value) {
          return parseTableSpec(tableSpecFunction.apply(value));
        }
      }

      public Bound() {
        this.table = null;
        this.tableRefFunction = null;
        this.schema = null;
        this.createDisposition = CreateDisposition.CREATE_IF_NEEDED;
        this.writeDisposition = WriteDisposition.WRITE_EMPTY;
        this.validate = true;
      }

      Bound(String name, TableReference ref,
          SerializableFunction<BoundedWindow, TableReference> tableRefFunction, TableSchema schema,
          CreateDisposition createDisposition, WriteDisposition writeDisposition,
          boolean validate) {
        super(name);
        this.table = ref;
        this.tableRefFunction = tableRefFunction;
        this.schema = schema;
        this.createDisposition = createDisposition;
        this.writeDisposition = writeDisposition;
        this.validate = validate;
      }

      /**
       * Sets the name associated with this transformation.
       */
      public Bound named(String name) {
        return new Bound(name, table, tableRefFunction, schema, createDisposition,
            writeDisposition, validate);
      }

      /**
       * Specifies the table specification.
       * <p>
       * Refer to {@link #parseTableSpec(String)} for the specification format.
       */
      public Bound to(String tableSpec) {
        return to(parseTableSpec(tableSpec));
      }

      /**
       * Specifies the table to be written to.
       */
      public Bound to(TableReference table) {
        return new Bound(name, table, tableRefFunction, schema, createDisposition,
            writeDisposition, validate);
      }

      public Bound to(
          SerializableFunction<BoundedWindow, String> tableSpecFunction) {
        return toTableReference(new TranslateTableSpecFunction(tableSpecFunction));
      }

      public Bound toTableReference(
          SerializableFunction<BoundedWindow, TableReference> tableRefFunction) {
        return new Bound(name, table, tableRefFunction, schema, createDisposition,
            writeDisposition, validate);
      }

      /**
       * Specifies the table schema, used if the table is created.
       */
      public Bound withSchema(TableSchema schema) {
        return new Bound(name, table, tableRefFunction, schema, createDisposition,
            writeDisposition, validate);
      }

      /** Specifies options for creating the table. */
      public Bound withCreateDisposition(CreateDisposition createDisposition) {
        return new Bound(name, table, tableRefFunction, schema, createDisposition,
            writeDisposition, validate);
      }

      /** Specifies options for writing the table. */
      public Bound withWriteDisposition(WriteDisposition writeDisposition) {
        return new Bound(name, table, tableRefFunction, schema, createDisposition,
            writeDisposition, validate);
      }

      /**
       * Disable table validation.
       */
      public Bound withoutValidation() {
        return new Bound(name, table, tableRefFunction, schema, createDisposition,
            writeDisposition, false);
      }

      private static void verifyTableEmpty(
          BigQueryOptions options,
          TableReference table) {
        try {
          Bigquery client = Transport.newBigQueryClient(options).build();
          BigQueryTableInserter inserter = new BigQueryTableInserter(client, table);
          if (!inserter.isEmpty()) {
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

        if (table == null && tableRefFunction == null) {
          throw new IllegalStateException(
              "must set the table reference of a BigQueryIO.Write transform");
        }
        if (table != null && tableRefFunction != null) {
          throw new IllegalStateException(
              "Cannot set both a table reference and a table function for a BigQueryIO.Write "
                + "transform");
        }

        if (createDisposition == CreateDisposition.CREATE_IF_NEEDED && schema == null) {
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

          return input.apply(new StreamWithDeDup(table, tableRefFunction, schema));
        }

        return PDone.in(input.getPipeline());
      }

      @Override
      protected Coder<Void> getDefaultOutputCoder() {
        return VoidCoder.of();
      }

      static {
        DirectPipelineRunner.registerDefaultTransformEvaluator(
            Bound.class, new DirectPipelineRunner.TransformEvaluator<Bound>() {
              @Override
              public void evaluate(
                  Bound transform, DirectPipelineRunner.EvaluationContext context) {
                evaluateWriteHelper(transform, context);
              }
            });
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
        return schema;
      }

      /** Returns the table reference. */
      public TableReference getTable() {
        return table;
      }

      /** Returns true if table validation is enabled. */
      public boolean getValidate() {
        return validate;
      }
    }
  }

  public static void verifyDatasetPresence(BigQueryOptions options, TableReference table) {
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

  public static void verifyTablePresence(BigQueryOptions options, TableReference table) {
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
  private static class StreamingWriteFn
      extends DoFn<KV<ShardedKey<String>, TableRowInfo>, Void> {
    private static final long serialVersionUID = 0;

    /** TableSchema in JSON.  Use String to make the class Serializable. */
    private final String jsonTableSchema;

    /** JsonTableRows to accumulate BigQuery rows in order to batch writes. */
    private transient Map<String, List<TableRow>> tableRows;

    /** The list of unique ids for each BigQuery table row. */
    private transient Map<String, List<String>> uniqueIdsForTableRows;

    /** The list of tables created so far, so we don't try the creation
        each time. */
    private static Set<String> createdTables =
        Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /** Constructor. */
    StreamingWriteFn(TableSchema schema) {
      try {
        jsonTableSchema = JSON_FACTORY.toString(schema);
      } catch (IOException e) {
        throw new RuntimeException("Cannot initialize BigQuery streaming writer.", e);
      }
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
            BigQueryTableInserter inserter = new BigQueryTableInserter(client, tableReference);
            inserter.tryCreateTable(tableSchema);
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
          BigQueryTableInserter inserter = new BigQueryTableInserter(client, tableReference);
          inserter.insertAll(tableRows, uniqueIds);
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
   * A {@link Coder} for {@code ShardedKey}, using a wrapped key {@code Coder}.
   */
  public static class ShardedKeyCoder<KeyT>
      extends StandardCoder<ShardedKey<KeyT>> {
    private static final long serialVersionUID = 0;

    public static <KeyT> ShardedKeyCoder<KeyT> of(Coder<KeyT> keyCoder) {
      return new ShardedKeyCoder<>(keyCoder);
    }

    @JsonCreator
    public static <KeyT> ShardedKeyCoder<KeyT> of(
         @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
        List<Coder<KeyT>> components) {
      Preconditions.checkArgument(components.size() == 1,
          "Expecting 1 component, got " + components.size());
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
    private static final long serialVersionUID = 0;
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
    private static final long serialVersionUID = 0;

    /** TableSpec to write to. */
    private final String tableSpec;

    /** User function mapping windows to TableReference in JSON. */
    private final SerializableFunction<BoundedWindow, TableReference> tableRefFunction;

    private transient String randomUUID;
    private transient long sequenceNo = 0L;

    TagWithUniqueIdsAndTable(BigQueryOptions options, TableReference table,
        SerializableFunction<BoundedWindow, TableReference> tableRefFunction) {
      Preconditions.checkArgument(table == null ^ tableRefFunction == null,
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
    private static final long serialVersionUID = 0;

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
      // implicit assumption about the job execution order.  If a user
      // implements a PTransform that takes PDone returned here as its
      // input, the transform may not necessarily be executed after
      // the BigQueryIO.Write.

      return PDone.in(input.getPipeline());
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Direct mode read evaluator.
   * <p>
   * This loads the entire table into an in-memory PCollection.
   */
  private static void evaluateReadHelper(
      Read.Bound transform, DirectPipelineRunner.EvaluationContext context) {
    BigQueryOptions options = context.getPipelineOptions();
    Bigquery client = Transport.newBigQueryClient(options).build();
    if (transform.table != null && transform.table.getProjectId() == null) {
      transform.table.setProjectId(options.getProject());
    }

    BigQueryReader reader = null;
    if (transform.query != null) {
      LOG.info("Reading from BigQuery query {}", transform.query);
      reader = new BigQueryReader(client, transform.query, options.getProject());
    } else {
      reader = new BigQueryReader(client, transform.table);
      LOG.info("Reading from BigQuery table {}", toTableSpec(transform.table));
    }

    List<WindowedValue<TableRow>> elems = ReaderUtils.readElemsFromReader(reader);
    LOG.info("Number of records read from BigQuery: {}", elems.size());
    context.setPCollectionWindowedValue(context.getOutput(transform), elems);
  }

  private static <K, V> List<V> getOrCreateMapListValue(Map<K, List<V>> map, K key) {
    List<V> value = map.get(key);
    if (value == null) {
      value = new ArrayList<>();
      map.put(key, value);
    }
    return value;
  }

  /**
   * Direct mode write evaluator.
   * <p>
   * This writes the entire table in a single BigQuery request.
   * The table will be created if necessary.
   */
  private static void evaluateWriteHelper(
      Write.Bound transform, DirectPipelineRunner.EvaluationContext context) {
    BigQueryOptions options = context.getPipelineOptions();
    Bigquery client = Transport.newBigQueryClient(options).build();

    try {
      Map<BoundedWindow, List<TableRow>> tableRows = new HashMap<>();
      for (WindowedValue<TableRow> windowedValue : context.getPCollectionWindowedValues(
          context.getInput(transform))) {
        for (BoundedWindow window : windowedValue.getWindows()) {
          List<TableRow> rows = getOrCreateMapListValue(tableRows, window);
          rows.add(windowedValue.getValue());
        }
      }

      for (BoundedWindow window : tableRows.keySet()) {
        TableReference ref;
        if (transform.tableRefFunction != null) {
          ref = transform.tableRefFunction.apply(window);
        } else {
          ref = transform.table;
        }
        if (ref.getProjectId() == null) {
          ref.setProjectId(options.getProject());
        }
        LOG.info("Writing to BigQuery table {}", toTableSpec(ref));

        BigQueryTableInserter inserter = new BigQueryTableInserter(client, ref);
        inserter.getOrCreateTable(
            transform.writeDisposition, transform.createDisposition, transform.schema);
        inserter.insertAll(tableRows.get(window));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
