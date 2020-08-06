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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.resolveTempLocation;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.createTempTableReference;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.Storage.Stream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.JsonTableRefToTableRef;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableRefToJson;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableSchemaToJsonSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableSpecToTableRef;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TimePartitioningToJson;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.JobType;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySourceBase.ExtractResult;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinationsHelpers.ConstantSchemaDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinationsHelpers.ConstantTimePartitioningDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinationsHelpers.SchemaFromViewDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinationsHelpers.TableFunctionDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.PassThroughThenCleanup.CleanupOperation;
import org.apache.beam.sdk.io.gcp.bigquery.PassThroughThenCleanup.ContextContainer;
import org.apache.beam.sdk.io.gcp.bigquery.RowWriterFactory.OutputType;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s for reading and writing <a
 * href="https://developers.google.com/bigquery/">BigQuery</a> tables.
 *
 * <h3>Table References</h3>
 *
 * <p>A fully-qualified BigQuery table name consists of three components:
 *
 * <ul>
 *   <li>{@code projectId}: the Cloud project id (defaults to {@link GcpOptions#getProject()}).
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
 * <h3>BigQuery Concepts</h3>
 *
 * <p>Tables have rows ({@link TableRow}) and each row has cells ({@link TableCell}). A table has a
 * schema ({@link TableSchema}), which in turn describes the schema of each cell ({@link
 * TableFieldSchema}). The terms field and cell are used interchangeably.
 *
 * <p>{@link TableSchema}: describes the schema (types and order) for values in each row. It has one
 * attribute, 'fields', which is list of {@link TableFieldSchema} objects.
 *
 * <p>{@link TableFieldSchema}: describes the schema (type, name) for one field. It has several
 * attributes, including 'name' and 'type'. Common values for the type attribute are: 'STRING',
 * 'INTEGER', 'FLOAT', 'BOOLEAN', 'NUMERIC', 'GEOGRAPHY'. All possible values are described at: <a
 * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types">
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types</a>
 *
 * <p>{@link TableRow}: Holds all values in a table row. Has one attribute, 'f', which is a list of
 * {@link TableCell} instances.
 *
 * <p>{@link TableCell}: Holds the value for one cell (or field). Has one attribute, 'v', which is
 * the value of the table cell.
 *
 * <p>As of Beam 2.7.0, the NUMERIC data type is supported. This data type supports high-precision
 * decimal numbers (precision of 38 digits, scale of 9 digits). The GEOGRAPHY data type works with
 * Well-Known Text (See <a href="https://en.wikipedia.org/wiki/Well-known_text">
 * https://en.wikipedia.org/wiki/Well-known_text</a>) format for reading and writing to BigQuery.
 * BigQuery IO requires values of BYTES datatype to be encoded using base64 encoding when writing to
 * BigQuery. When bytes are read from BigQuery they are returned as base64-encoded strings.
 *
 * <h3>Reading</h3>
 *
 * <p>Reading from BigQuery is supported by {@link #read(SerializableFunction)}, which parses
 * records in <a href="https://cloud.google.com/bigquery/data-formats#avro_format">AVRO format</a>
 * into a custom type (see the table below for type conversion) using a specified parse function,
 * and by {@link #readTableRows} which parses them into {@link TableRow}, which may be more
 * convenient but has lower performance.
 *
 * <p>Both functions support reading either from a table or from the result of a query, via {@link
 * TypedRead#from(String)} and {@link TypedRead#fromQuery} respectively. Exactly one of these must
 * be specified.
 *
 * <p>If you are reading from an authorized view wih {@link TypedRead#fromQuery}, you need to use
 * {@link TypedRead#withQueryLocation(String)} to set the location of the BigQuery job. Otherwise,
 * Beam will ty to determine that location by reading the metadata of the dataset that contains the
 * underlying tables. With authorized views, that will result in a 403 error and the query will not
 * be resolved.
 *
 * <p><b>Type Conversion Table</b>
 *
 * <table border="1" cellspacing="1">
 *   <tr>
 *     <td> <b>BigQuery standard SQL type</b> </td> <td> <b>Avro type</b> </td> <td> <b>Java type</b> </td>
 *   </tr>
 *   <tr>
 *     <td> BOOLEAN </td> <td> boolean </td> <td> Boolean </td>
 *   </tr>
 *   <tr>
 *     <td> INT64 </td> <td> long </td> <td> Long </td>
 *   </tr>
 *   <tr>
 *     <td> FLOAT64 </td> <td> double </td> <td> Double </td>
 *   </tr>
 *   <tr>
 *     <td> BYTES </td> <td> bytes </td> <td> java.nio.ByteBuffer </td>
 *   </tr>
 *   <tr>
 *     <td> STRING </td> <td> string </td> <td> CharSequence </td>
 *   </tr>
 *   <tr>
 *     <td> DATE </td> <td> int </td> <td> Integer </td>
 *   </tr>
 *   <tr>
 *     <td> DATETIME </td> <td> string </td> <td> CharSequence </td>
 *   </tr>
 *   <tr>
 *     <td> TIMESTAMP </td> <td> long </td> <td> Long </td>
 *   </tr>
 *   <tr>
 *     <td> TIME </td> <td> long </td> <td> Long </td>
 *   </tr>
 *   <tr>
 *     <td> NUMERIC </td> <td> bytes </td> <td> java.nio.ByteBuffer </td>
 *   </tr>
 *   <tr>
 *     <td> GEOGRAPHY </td> <td> string </td> <td> CharSequence </td>
 *   </tr>
 *   <tr>
 *     <td> ARRAY </td> <td> array </td> <td> java.util.Collection </td>
 *   </tr>
 *   <tr>
 *     <td> STRUCT </td> <td> record </td> <td> org.apache.avro.generic.GenericRecord </td>
 *   </tr>
 * </table>
 *
 * <p><b>Example: Reading rows of a table as {@link TableRow}.</b>
 *
 * <pre>{@code
 * PCollection<TableRow> weatherData = pipeline.apply(
 *     BigQueryIO.readTableRows().from("clouddataflow-readonly:samples.weather_stations"));
 * }</pre>
 *
 * <b>Example: Reading rows of a table and parsing them into a custom type.</b>
 *
 * <pre>{@code
 * PCollection<WeatherRecord> weatherData = pipeline.apply(
 *    BigQueryIO
 *      .read(new SerializableFunction<SchemaAndRecord, WeatherRecord>() {
 *        public WeatherRecord apply(SchemaAndRecord schemaAndRecord) {
 *          return new WeatherRecord(...);
 *        }
 *      })
 *      .from("clouddataflow-readonly:samples.weather_stations"))
 *      .withCoder(SerializableCoder.of(WeatherRecord.class));
 * }</pre>
 *
 * <p>Note: When using {@link #read(SerializableFunction)}, you may sometimes need to use {@link
 * TypedRead#withCoder(Coder)} to specify a {@link Coder} for the result type, if Beam fails to
 * infer it automatically.
 *
 * <p><b>Example: Reading results of a query as {@link TableRow}.</b>
 *
 * <pre>{@code
 * PCollection<TableRow> meanTemperatureData = pipeline.apply(BigQueryIO.readTableRows()
 *     .fromQuery("SELECT year, mean_temp FROM [samples.weather_stations]"));
 * }</pre>
 *
 * <p>Users can optionally specify a query priority using {@link
 * TypedRead#withQueryPriority(TypedRead.QueryPriority)} and a geographic location where the query
 * will be executed using {@link TypedRead#withQueryLocation(String)}. Query location must be
 * specified for jobs that are not executed in US or EU, or if you are reading from an authorized
 * view. See <a href="https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query">BigQuery
 * Jobs: query</a>.
 *
 * <h3>Writing</h3>
 *
 * <p>To write to a BigQuery table, apply a {@link BigQueryIO.Write} transformation. This consumes a
 * {@link PCollection} of a user-defined type when using {@link BigQueryIO#write()} (recommended),
 * or a {@link PCollection} of {@link TableRow TableRows} as input when using {@link
 * BigQueryIO#writeTableRows()} (not recommended). When using a user-defined type, one of the
 * following must be provided.
 *
 * <ul>
 *   <li>{@link BigQueryIO.Write#withAvroFormatFunction(SerializableFunction)} (recommended) to
 *       write data using avro records.
 *   <li>{@link BigQueryIO.Write#withAvroWriter} to write avro data using a user-specified {@link
 *       DatumWriter} (and format function).
 *   <li>{@link BigQueryIO.Write#withFormatFunction(SerializableFunction)} to write data as json
 *       encoded {@link TableRow TableRows}.
 * </ul>
 *
 * If {@link BigQueryIO.Write#withAvroFormatFunction(SerializableFunction)} or {@link
 * BigQueryIO.Write#withAvroWriter} is used, the table schema MUST be specified using one of the
 * {@link Write#withJsonSchema(String)}, {@link Write#withJsonSchema(ValueProvider)}, {@link
 * Write#withSchemaFromView(PCollectionView)} methods, or {@link Write#to(DynamicDestinations)}.
 *
 * <pre>{@code
 * class Quote {
 *   final Instant timestamp;
 *   final String exchange;
 *   final String symbol;
 *   final double price;
 *
 *   Quote(Instant timestamp, String exchange, String symbol, double price) {
 *     // initialize all member variables.
 *   }
 * }
 *
 * PCollection<Quote> quotes = ...
 *
 * quotes.apply(BigQueryIO
 *     .<Quote>write()
 *     .to("my-project:my_dataset.my_table")
 *     .withSchema(new TableSchema().setFields(
 *         ImmutableList.of(
 *           new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"),
 *           new TableFieldSchema().setName("exchange").setType("STRING"),
 *           new TableFieldSchema().setName("symbol").setType("STRING"),
 *           new TableFieldSchema().setName("price").setType("FLOAT"))))
 *     .withFormatFunction(quote -> new TableRow().set(..set the columns..))
 *     .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
 * }</pre>
 *
 * <p>See {@link BigQueryIO.Write} for details on how to specify if a write should append to an
 * existing table, replace the table, or verify that the table is empty. Note that the dataset being
 * written to must already exist. Unbounded PCollections can only be written using {@link
 * Write.WriteDisposition#WRITE_EMPTY} or {@link Write.WriteDisposition#WRITE_APPEND}.
 *
 * <p>BigQueryIO supports automatically inferring the BigQuery table schema from the Beam schema on
 * the input PCollection. Beam can also automatically format the input into a TableRow in this case,
 * if no format function is provide. In the above example, the quotes PCollection has a schema that
 * Beam infers from the Quote POJO. So the write could be done more simply as follows:
 *
 * <pre>{@code
 * {@literal @}DefaultSchema(JavaFieldSchema.class)
 * class Quote {
 *   final Instant timestamp;
 *   final String exchange;
 *   final String symbol;
 *   final double price;
 *
 *   {@literal @}SchemaCreate
 *   Quote(Instant timestamp, String exchange, String symbol, double price) {
 *     // initialize all member variables.
 *   }
 * }
 *
 * PCollection<Quote> quotes = ...
 *
 * quotes.apply(BigQueryIO
 *     .<Quote>write()
 *     .to("my-project:my_dataset.my_table")
 *     .useBeamSchema()
 *     .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
 * }</pre>
 *
 * <h3>Loading historical data into time-partitioned BigQuery tables</h3>
 *
 * <p>To load historical data into a time-partitioned BigQuery table, specify {@link
 * BigQueryIO.Write#withTimePartitioning} with a {@link TimePartitioning#setField(String) field}
 * used for <a
 * href="https://cloud.google.com/bigquery/docs/partitioned-tables#partitioned_tables">column-based
 * partitioning</a>. For example:
 *
 * <pre>{@code
 * PCollection<Quote> quotes = ...;
 *
 * quotes.apply(BigQueryIO.write()
 *         .withSchema(schema)
 *         .withFormatFunction(quote -> new TableRow()
 *            .set("timestamp", quote.getTimestamp())
 *            .set(..other columns..))
 *         .to("my-project:my_dataset.my_table")
 *         .withTimePartitioning(new TimePartitioning().setField("time")));
 * }</pre>
 *
 * <h3>Writing different values to different tables</h3>
 *
 * <p>A common use case is to dynamically generate BigQuery table names based on the current value.
 * To support this, {@link BigQueryIO.Write#to(SerializableFunction)} accepts a function mapping the
 * current element to a tablespec. For example, here's code that outputs quotes of different stocks
 * to different tables:
 *
 * <pre>{@code
 * PCollection<Quote> quotes = ...;
 *
 * quotes.apply(BigQueryIO.write()
 *         .withSchema(schema)
 *         .withFormatFunction(quote -> new TableRow()...)
 *         .to((ValueInSingleWindow<Quote> quote) -> {
 *             String symbol = quote.getSymbol();
 *             return new TableDestination(
 *                 "my-project:my_dataset.quotes_" + symbol, // Table spec
 *                 "Quotes of stock " + symbol // Table description
 *               );
 *           });
 * }</pre>
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
 * <p>For the most general form of dynamic table destinations and schemas, look at {@link
 * BigQueryIO.Write#to(DynamicDestinations)}.
 *
 * <h3>Insertion Method</h3>
 *
 * {@link BigQueryIO.Write} supports two methods of inserting data into BigQuery specified using
 * {@link BigQueryIO.Write#withMethod}. If no method is supplied, then a default method will be
 * chosen based on the input PCollection. See {@link BigQueryIO.Write.Method} for more information
 * about the methods. The different insertion methods provide different tradeoffs of cost, quota,
 * and data consistency; please see BigQuery documentation for more information about these
 * tradeoffs.
 *
 * <h3>Usage with templates</h3>
 *
 * <p>When using {@link #read} or {@link #readTableRows()} in a template, it's required to specify
 * {@link Read#withTemplateCompatibility()}. Specifying this in a non-template pipeline is not
 * recommended because it has somewhat lower performance.
 *
 * <p>When using {@link #write()} or {@link #writeTableRows()} with batch loads in a template, it is
 * recommended to specify {@link Write#withCustomGcsTempLocation}. Writing to BigQuery via batch
 * loads involves writing temporary files to this location, so the location must be accessible at
 * pipeline execution time. By default, this location is captured at pipeline <i>construction</i>
 * time, may be inaccessible if the template may be reused from a different project or at a moment
 * when the original location no longer exists. {@link
 * Write#withCustomGcsTempLocation(ValueProvider)} allows specifying the location as an argument to
 * the template invocation.
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

  /**
   * Template for BigQuery jobs created by BigQueryIO. This template is: {@code
   * "beam_bq_job_{TYPE}_{JOB_ID}_{STEP}_{RANDOM}"}, where:
   *
   * <ul>
   *   <li>{@code TYPE} represents the BigQuery job type (e.g. extract / copy / load / query)
   *   <li>{@code JOB_ID} is the Beam job name.
   *   <li>{@code STEP} is a UUID representing the the Dataflow step that created the BQ job.
   *   <li>{@code RANDOM} is a random string.
   * </ul>
   *
   * <p><b>NOTE:</b> This job name template does not have backwards compatibility guarantees.
   */
  public static final String BIGQUERY_JOB_TEMPLATE = "beam_bq_job_{TYPE}_{JOB_ID}_{STEP}_{RANDOM}";

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIO.class);

  /** Singleton instance of the JSON factory used to read and write JSON formatted rows. */
  static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

  /**
   * Project IDs must contain 6-63 lowercase letters, digits, or dashes. IDs must start with a
   * letter and may not end with a dash. This regex isn't exact - this allows for patterns that
   * would be rejected by the service, but this is sufficient for basic parsing of table references.
   */
  private static final String PROJECT_ID_REGEXP = "[a-z][-a-z0-9:.]{4,61}[a-z0-9]";

  /** Regular expression that matches Dataset IDs. */
  private static final String DATASET_REGEXP = "[-\\w.]{1,1024}";

  /** Regular expression that matches Table IDs. */
  private static final String TABLE_REGEXP = "[-\\w$@]{1,1024}";

  /**
   * Matches table specifications in the form {@code "[project_id]:[dataset_id].[table_id]"} or
   * {@code "[dataset_id].[table_id]"}.
   */
  private static final String DATASET_TABLE_REGEXP =
      String.format(
          "((?<PROJECT>%s):)?(?<DATASET>%s)\\.(?<TABLE>%s)",
          PROJECT_ID_REGEXP, DATASET_REGEXP, TABLE_REGEXP);

  static final Pattern TABLE_SPEC = Pattern.compile(DATASET_TABLE_REGEXP);

  /**
   * A formatting function that maps a TableRow to itself. This allows sending a {@code
   * PCollection<TableRow>} directly to BigQueryIO.Write.
   */
  static final SerializableFunction<TableRow, TableRow> IDENTITY_FORMATTER = input -> input;

  static final SerializableFunction<org.apache.avro.Schema, DatumWriter<GenericRecord>>
      GENERIC_DATUM_WRITER_FACTORY = schema -> new GenericDatumWriter<>();

  private static final SerializableFunction<TableSchema, org.apache.avro.Schema>
      DEFAULT_AVRO_SCHEMA_FACTORY =
          new SerializableFunction<TableSchema, org.apache.avro.Schema>() {
            @Override
            public org.apache.avro.Schema apply(TableSchema input) {
              return BigQueryAvroUtils.toGenericAvroSchema("root", input.getFields());
            }
          };

  /**
   * @deprecated Use {@link #read(SerializableFunction)} or {@link #readTableRows} instead. {@link
   *     #readTableRows()} does exactly the same as {@link #read}, however {@link
   *     #read(SerializableFunction)} performs better.
   */
  @Deprecated
  public static Read read() {
    return new Read();
  }

  /**
   * Like {@link #read(SerializableFunction)} but represents each row as a {@link TableRow}.
   *
   * <p>This method is more convenient to use in some cases, but usually has significantly lower
   * performance than using {@link #read(SerializableFunction)} directly to parse data into a
   * domain-specific type, due to the overhead of converting the rows to {@link TableRow}.
   */
  public static TypedRead<TableRow> readTableRows() {
    return read(new TableRowParser()).withCoder(TableRowJsonCoder.of());
  }

  /** Like {@link #readTableRows()} but with {@link Schema} support. */
  public static TypedRead<TableRow> readTableRowsWithSchema() {
    return read(new TableRowParser())
        .withCoder(TableRowJsonCoder.of())
        .withBeamRowConverters(
            TypeDescriptor.of(TableRow.class),
            BigQueryUtils.tableRowToBeamRow(),
            BigQueryUtils.tableRowFromBeamRow());
  }

  /**
   * Reads from a BigQuery table or query and returns a {@link PCollection} with one element per
   * each row of the table or query result, parsed from the BigQuery AVRO format using the specified
   * function.
   *
   * <p>Each {@link SchemaAndRecord} contains a BigQuery {@link TableSchema} and a {@link
   * GenericRecord} representing the row, indexed by column name. Here is a sample parse function
   * that parses click events from a table.
   *
   * <pre>{@code
   * class ClickEvent { long userId; String url; ... }
   *
   * p.apply(BigQueryIO.read(new SerializableFunction<SchemaAndRecord, ClickEvent>() {
   *   public ClickEvent apply(SchemaAndRecord record) {
   *     GenericRecord r = record.getRecord();
   *     return new ClickEvent((Long) r.get("userId"), (String) r.get("url"));
   *   }
   * }).from("...");
   * }</pre>
   */
  public static <T> TypedRead<T> read(SerializableFunction<SchemaAndRecord, T> parseFn) {
    return new AutoValue_BigQueryIO_TypedRead.Builder<T>()
        .setValidate(true)
        .setWithTemplateCompatibility(false)
        .setBigQueryServices(new BigQueryServicesImpl())
        .setParseFn(parseFn)
        .setMethod(Method.DEFAULT)
        .setUseAvroLogicalTypes(false)
        .build();
  }

  @VisibleForTesting
  static class TableRowParser implements SerializableFunction<SchemaAndRecord, TableRow> {

    public static final TableRowParser INSTANCE = new TableRowParser();

    @Override
    public TableRow apply(SchemaAndRecord schemaAndRecord) {
      return BigQueryAvroUtils.convertGenericRecordToTableRow(
          schemaAndRecord.getRecord(), schemaAndRecord.getTableSchema());
    }
  }

  /** Implementation of {@link BigQueryIO#read()}. */
  public static class Read extends PTransform<PBegin, PCollection<TableRow>> {
    private final TypedRead<TableRow> inner;

    Read() {
      this(BigQueryIO.read(TableRowParser.INSTANCE).withCoder(TableRowJsonCoder.of()));
    }

    Read(TypedRead<TableRow> inner) {
      this.inner = inner;
    }

    @Override
    public PCollection<TableRow> expand(PBegin input) {
      return input.apply(inner);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      this.inner.populateDisplayData(builder);
    }

    boolean getValidate() {
      return this.inner.getValidate();
    }

    ValueProvider<String> getQuery() {
      return this.inner.getQuery();
    }

    Read withTestServices(BigQueryServices testServices) {
      return new Read(this.inner.withTestServices(testServices));
    }

    ///////////////////////////////////////////////////////////////////

    /** Returns the table to read, or {@code null} if reading from a query instead. */
    public @Nullable ValueProvider<TableReference> getTableProvider() {
      return this.inner.getTableProvider();
    }

    /** Returns the table to read, or {@code null} if reading from a query instead. */
    public @Nullable TableReference getTable() {
      return this.inner.getTable();
    }

    /**
     * Reads a BigQuery table specified as {@code "[project_id]:[dataset_id].[table_id]"} or {@code
     * "[dataset_id].[table_id]"} for tables within the current project.
     */
    public Read from(String tableSpec) {
      return new Read(this.inner.from(tableSpec));
    }

    /** Same as {@code from(String)}, but with a {@link ValueProvider}. */
    public Read from(ValueProvider<String> tableSpec) {
      return new Read(this.inner.from(tableSpec));
    }

    /** Read from table specified by a {@link TableReference}. */
    public Read from(TableReference table) {
      return new Read(this.inner.from(table));
    }

    /**
     * Reads results received after executing the given query.
     *
     * <p>By default, the query results will be flattened -- see "flattenResults" in the <a
     * href="https://cloud.google.com/bigquery/docs/reference/v2/jobs">Jobs documentation</a> for
     * more information. To disable flattening, use {@link Read#withoutResultFlattening}.
     *
     * <p>By default, the query will use BigQuery's legacy SQL dialect. To use the BigQuery Standard
     * SQL dialect, use {@link Read#usingStandardSql}.
     */
    public Read fromQuery(String query) {
      return new Read(this.inner.fromQuery(query));
    }

    /** Same as {@code fromQuery(String)}, but with a {@link ValueProvider}. */
    public Read fromQuery(ValueProvider<String> query) {
      return new Read(this.inner.fromQuery(query));
    }

    /**
     * Disable validation that the table exists or the query succeeds prior to pipeline submission.
     * Basic validation (such as ensuring that a query or table is specified) still occurs.
     */
    public Read withoutValidation() {
      return new Read(this.inner.withoutValidation());
    }

    /**
     * Disable <a href="https://cloud.google.com/bigquery/docs/reference/v2/jobs">flattening of
     * query results</a>.
     *
     * <p>Only valid when a query is used ({@link #fromQuery}). Setting this option when reading
     * from a table will cause an error during validation.
     */
    public Read withoutResultFlattening() {
      return new Read(this.inner.withoutResultFlattening());
    }

    /**
     * Enables BigQuery's Standard SQL dialect when reading from a query.
     *
     * <p>Only valid when a query is used ({@link #fromQuery}). Setting this option when reading
     * from a table will cause an error during validation.
     */
    public Read usingStandardSql() {
      return new Read(this.inner.usingStandardSql());
    }

    /**
     * Use new template-compatible source implementation.
     *
     * <p>Use new template-compatible source implementation. This implementation is compatible with
     * repeated template invocations. It does not support dynamic work rebalancing.
     */
    public Read withTemplateCompatibility() {
      return new Read(this.inner.withTemplateCompatibility());
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link BigQueryIO#read(SerializableFunction)}. */
  @AutoValue
  public abstract static class TypedRead<T> extends PTransform<PBegin, PCollection<T>> {
    /** Determines the method used to read data from BigQuery. */
    public enum Method {
      /** The default behavior if no method is explicitly set. Currently {@link #EXPORT}. */
      DEFAULT,

      /**
       * Export data to Google Cloud Storage in Avro format and read data files from that location.
       */
      EXPORT,

      /** Read the contents of a table directly using the BigQuery storage API. */
      DIRECT_READ,
    }

    interface ToBeamRowFunction<T>
        extends SerializableFunction<Schema, SerializableFunction<T, Row>> {}

    interface FromBeamRowFunction<T>
        extends SerializableFunction<Schema, SerializableFunction<Row, T>> {}

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setJsonTableRef(ValueProvider<String> jsonTableRef);

      abstract Builder<T> setQuery(ValueProvider<String> query);

      abstract Builder<T> setValidate(boolean validate);

      abstract Builder<T> setFlattenResults(Boolean flattenResults);

      abstract Builder<T> setUseLegacySql(Boolean useLegacySql);

      abstract Builder<T> setWithTemplateCompatibility(Boolean useTemplateCompatibility);

      abstract Builder<T> setBigQueryServices(BigQueryServices bigQueryServices);

      abstract Builder<T> setQueryPriority(QueryPriority priority);

      abstract Builder<T> setQueryLocation(String location);

      abstract Builder<T> setQueryTempDataset(String queryTempDataset);

      abstract Builder<T> setMethod(Method method);

      /**
       * @deprecated Use {@link #setSelectedFields(ValueProvider)} and {@link
       *     #setRowRestriction(ValueProvider)} instead.
       */
      @Deprecated
      @Experimental(Kind.SOURCE_SINK)
      abstract Builder<T> setReadOptions(TableReadOptions readOptions);

      abstract Builder<T> setSelectedFields(ValueProvider<List<String>> selectedFields);

      abstract Builder<T> setRowRestriction(ValueProvider<String> rowRestriction);

      abstract TypedRead<T> build();

      abstract Builder<T> setParseFn(SerializableFunction<SchemaAndRecord, T> parseFn);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setKmsKey(String kmsKey);

      @Experimental(Kind.SCHEMAS)
      abstract Builder<T> setTypeDescriptor(TypeDescriptor<T> typeDescriptor);

      @Experimental(Kind.SCHEMAS)
      abstract Builder<T> setToBeamRowFn(ToBeamRowFunction<T> toRowFn);

      @Experimental(Kind.SCHEMAS)
      abstract Builder<T> setFromBeamRowFn(FromBeamRowFunction<T> fromRowFn);

      abstract Builder<T> setUseAvroLogicalTypes(Boolean useAvroLogicalTypes);
    }

    abstract @Nullable ValueProvider<String> getJsonTableRef();

    abstract @Nullable ValueProvider<String> getQuery();

    abstract boolean getValidate();

    abstract @Nullable Boolean getFlattenResults();

    abstract @Nullable Boolean getUseLegacySql();

    abstract Boolean getWithTemplateCompatibility();

    abstract BigQueryServices getBigQueryServices();

    abstract SerializableFunction<SchemaAndRecord, T> getParseFn();

    abstract @Nullable QueryPriority getQueryPriority();

    abstract @Nullable String getQueryLocation();

    abstract @Nullable String getQueryTempDataset();

    abstract Method getMethod();

    /** @deprecated Use {@link #getSelectedFields()} and {@link #getRowRestriction()} instead. */
    @Deprecated
    @Experimental(Kind.SOURCE_SINK)
    abstract @Nullable TableReadOptions getReadOptions();

    abstract @Nullable ValueProvider<List<String>> getSelectedFields();

    abstract @Nullable ValueProvider<String> getRowRestriction();

    abstract @Nullable Coder<T> getCoder();

    abstract @Nullable String getKmsKey();

    @Nullable
    @Experimental(Kind.SCHEMAS)
    abstract TypeDescriptor<T> getTypeDescriptor();

    @Nullable
    @Experimental(Kind.SCHEMAS)
    abstract ToBeamRowFunction<T> getToBeamRowFn();

    @Nullable
    @Experimental(Kind.SCHEMAS)
    abstract FromBeamRowFunction<T> getFromBeamRowFn();

    abstract Boolean getUseAvroLogicalTypes();

    /**
     * An enumeration type for the priority of a query.
     *
     * @see <a href="https://cloud.google.com/bigquery/docs/running-queries">Running Interactive and
     *     Batch Queries in the BigQuery documentation</a>
     */
    public enum QueryPriority {
      /**
       * Specifies that a query should be run with an INTERACTIVE priority.
       *
       * <p>Interactive mode allows for BigQuery to execute the query as soon as possible. These
       * queries count towards your concurrent rate limit and your daily limit.
       */
      INTERACTIVE,

      /**
       * Specifies that a query should be run with a BATCH priority.
       *
       * <p>Batch mode queries are queued by BigQuery. These are started as soon as idle resources
       * are available, usually within a few minutes. Batch queries don’t count towards your
       * concurrent rate limit.
       */
      BATCH
    }

    @VisibleForTesting
    Coder<T> inferCoder(CoderRegistry coderRegistry) {
      if (getCoder() != null) {
        return getCoder();
      }

      try {
        return coderRegistry.getCoder(TypeDescriptors.outputOf(getParseFn()));
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException(
            "Unable to infer coder for output of parseFn. Specify it explicitly using withCoder().",
            e);
      }
    }

    private BigQuerySourceDef createSourceDef() {
      BigQuerySourceDef sourceDef;
      if (getQuery() == null) {
        sourceDef = BigQueryTableSourceDef.create(getBigQueryServices(), getTableProvider());
      } else {
        sourceDef =
            BigQueryQuerySourceDef.create(
                getBigQueryServices(),
                getQuery(),
                getFlattenResults(),
                getUseLegacySql(),
                MoreObjects.firstNonNull(getQueryPriority(), QueryPriority.BATCH),
                getQueryLocation(),
                getQueryTempDataset(),
                getKmsKey());
      }
      return sourceDef;
    }

    private BigQueryStorageQuerySource<T> createStorageQuerySource(
        String stepUuid, Coder<T> outputCoder) {
      return BigQueryStorageQuerySource.create(
          stepUuid,
          getQuery(),
          getFlattenResults(),
          getUseLegacySql(),
          MoreObjects.firstNonNull(getQueryPriority(), QueryPriority.BATCH),
          getQueryLocation(),
          getQueryTempDataset(),
          getKmsKey(),
          getParseFn(),
          outputCoder,
          getBigQueryServices());
    }

    private static final String QUERY_VALIDATION_FAILURE_ERROR =
        "Validation of query \"%1$s\" failed. If the query depends on an earlier stage of the"
            + " pipeline, This validation can be disabled using #withoutValidation.";

    @Override
    public void validate(PipelineOptions options) {
      // Even if existence validation is disabled, we need to make sure that the BigQueryIO
      // read is properly specified.
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);

      if (getMethod() != Method.DIRECT_READ) {
        String tempLocation = bqOptions.getTempLocation();
        checkArgument(
            !Strings.isNullOrEmpty(tempLocation),
            "BigQueryIO.Read needs a GCS temp location to store temp files."
                + "This can be set with option --tempLocation.");
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

      ValueProvider<TableReference> table = getTableProvider();

      // Note that a table or query check can fail if the table or dataset are created by
      // earlier stages of the pipeline or if a query depends on earlier stages of a pipeline.
      // For these cases the withoutValidation method can be used to disable the check.
      if (getValidate()) {
        if (table != null) {
          checkArgument(table.isAccessible(), "Cannot call validate if table is dynamically set.");
        }
        if (table != null && table.get().getProjectId() != null) {
          // Check for source table presence for early failure notification.
          DatasetService datasetService = getBigQueryServices().getDatasetService(bqOptions);
          BigQueryHelpers.verifyDatasetPresence(datasetService, table.get());
          BigQueryHelpers.verifyTablePresence(datasetService, table.get());
        } else if (getQuery() != null) {
          checkArgument(
              getQuery().isAccessible(), "Cannot call validate if query is dynamically set.");
          JobService jobService = getBigQueryServices().getJobService(bqOptions);
          try {
            jobService.dryRunQuery(
                bqOptions.getProject(),
                new JobConfigurationQuery()
                    .setQuery(getQuery().get())
                    .setFlattenResults(getFlattenResults())
                    .setUseLegacySql(getUseLegacySql()),
                getQueryLocation());
          } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format(QUERY_VALIDATION_FAILURE_ERROR, getQuery().get()), e);
          }

          DatasetService datasetService = getBigQueryServices().getDatasetService(bqOptions);
          // If the user provided a temp dataset, check if the dataset exists before launching the
          // query
          if (getQueryTempDataset() != null) {
            Optional<String> queryTempDataset = Optional.ofNullable(getQueryTempDataset());

            // The temp table is only used for dataset and project id validation, not for table name
            // validation
            TableReference tempTable =
                new TableReference()
                    .setProjectId(bqOptions.getProject())
                    .setDatasetId(getQueryTempDataset())
                    .setTableId("dummy table");
            BigQueryHelpers.verifyDatasetPresence(datasetService, tempTable);
          }
        }
      }
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      ValueProvider<TableReference> table = getTableProvider();

      if (table != null) {
        checkArgument(getQuery() == null, "from() and fromQuery() are exclusive");
        checkArgument(
            getQueryPriority() == null,
            "withQueryPriority() can only be specified when using fromQuery()");
        checkArgument(
            getFlattenResults() == null,
            "Invalid BigQueryIO.Read: Specifies a table with a result flattening"
                + " preference, which only applies to queries");
        checkArgument(
            getUseLegacySql() == null,
            "Invalid BigQueryIO.Read: Specifies a table with a SQL dialect"
                + " preference, which only applies to queries");
        checkArgument(
            getQueryTempDataset() == null,
            "Invalid BigQueryIO.Read: Specifies a temp dataset, which can"
                + " only be specified when using fromQuery()");
        if (table.isAccessible() && Strings.isNullOrEmpty(table.get().getProjectId())) {
          LOG.info(
              "Project of {} not set. The value of {}.getProject() at execution time will be used.",
              TableReference.class.getSimpleName(),
              BigQueryOptions.class.getSimpleName());
        }
      } else {
        checkArgument(getQuery() != null, "Either from() or fromQuery() is required");
        checkArgument(
            getFlattenResults() != null, "flattenResults should not be null if query is set");
        checkArgument(getUseLegacySql() != null, "useLegacySql should not be null if query is set");
      }
      checkArgument(getParseFn() != null, "A parseFn is required");

      // if both toRowFn and fromRowFn values are set, enable Beam schema support
      boolean beamSchemaEnabled = false;
      if (getTypeDescriptor() != null && getToBeamRowFn() != null && getFromBeamRowFn() != null) {
        beamSchemaEnabled = true;
      }

      Pipeline p = input.getPipeline();
      final Coder<T> coder = inferCoder(p.getCoderRegistry());

      if (getMethod() == Method.DIRECT_READ) {
        return expandForDirectRead(input, coder);
      }

      checkArgument(
          getReadOptions() == null,
          "Invalid BigQueryIO.Read: Specifies table read options, "
              + "which only applies when using Method.DIRECT_READ");

      checkArgument(
          getSelectedFields() == null,
          "Invalid BigQueryIO.Read: Specifies selected fields, "
              + "which only applies when using Method.DIRECT_READ");

      checkArgument(
          getRowRestriction() == null,
          "Invalid BigQueryIO.Read: Specifies row restriction, "
              + "which only applies when using Method.DIRECT_READ");

      final BigQuerySourceDef sourceDef = createSourceDef();
      final PCollectionView<String> jobIdTokenView;
      PCollection<String> jobIdTokenCollection;
      PCollection<T> rows;
      if (!getWithTemplateCompatibility()) {
        // Create a singleton job ID token at construction time.
        final String staticJobUuid = BigQueryHelpers.randomUUIDString();
        jobIdTokenView =
            p.apply("TriggerIdCreation", Create.of(staticJobUuid))
                .apply("ViewId", View.asSingleton());
        // Apply the traditional Source model.
        rows =
            p.apply(
                org.apache.beam.sdk.io.Read.from(
                    sourceDef.toSource(
                        staticJobUuid, coder, getParseFn(), getUseAvroLogicalTypes())));
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
        jobIdTokenView = jobIdTokenCollection.apply("ViewId", View.asSingleton());

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
                            BigQuerySourceBase<T> source =
                                sourceDef.toSource(
                                    jobUuid, coder, getParseFn(), getUseAvroLogicalTypes());
                            BigQueryOptions options =
                                c.getPipelineOptions().as(BigQueryOptions.class);
                            ExtractResult res = source.extractFiles(options);
                            LOG.info("Extract job produced {} files", res.extractedFiles.size());
                            source.cleanupTempResource(options);
                            for (ResourceId file : res.extractedFiles) {
                              c.output(file.toString());
                            }
                            c.output(tableSchemaTag, BigQueryHelpers.toJsonString(res.schema));
                          }
                        })
                    .withOutputTags(filesTag, TupleTagList.of(tableSchemaTag)));
        tuple.get(filesTag).setCoder(StringUtf8Coder.of());
        tuple.get(tableSchemaTag).setCoder(StringUtf8Coder.of());
        final PCollectionView<String> schemaView =
            tuple.get(tableSchemaTag).apply(View.asSingleton());
        rows =
            tuple
                .get(filesTag)
                .apply(Reshuffle.viaRandomKey())
                .apply(
                    "ReadFiles",
                    ParDo.of(
                            new DoFn<String, T>() {
                              @ProcessElement
                              public void processElement(ProcessContext c) throws Exception {
                                TableSchema schema =
                                    BigQueryHelpers.fromJsonString(
                                        c.sideInput(schemaView), TableSchema.class);
                                String jobUuid = c.sideInput(jobIdTokenView);
                                BigQuerySourceBase<T> source =
                                    sourceDef.toSource(
                                        jobUuid, coder, getParseFn(), getUseAvroLogicalTypes());
                                List<BoundedSource<T>> sources =
                                    source.createSources(
                                        ImmutableList.of(
                                            FileSystems.matchNewResource(
                                                c.element(), false /* is directory */)),
                                        schema,
                                        null);
                                checkArgument(sources.size() == 1, "Expected exactly one source.");
                                BoundedSource<T> avroSource = sources.get(0);
                                BoundedSource.BoundedReader<T> reader =
                                    avroSource.createReader(c.getPipelineOptions());
                                for (boolean more = reader.start(); more; more = reader.advance()) {
                                  c.output(reader.getCurrent());
                                }
                              }
                            })
                        .withSideInputs(schemaView, jobIdTokenView))
                .setCoder(coder);
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
                      .setJobId(
                          BigQueryResourceNaming.createJobIdPrefix(
                              bqOptions.getJobName(), jobUuid, JobType.EXPORT));

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

      rows = rows.apply(new PassThroughThenCleanup<>(cleanupOperation, jobIdTokenView));

      if (beamSchemaEnabled) {
        BigQueryOptions bqOptions = p.getOptions().as(BigQueryOptions.class);
        Schema beamSchema = sourceDef.getBeamSchema(bqOptions);
        SerializableFunction<T, Row> toBeamRow = getToBeamRowFn().apply(beamSchema);
        SerializableFunction<Row, T> fromBeamRow = getFromBeamRowFn().apply(beamSchema);

        rows.setSchema(beamSchema, getTypeDescriptor(), toBeamRow, fromBeamRow);
      }
      return rows;
    }

    private PCollection<T> expandForDirectRead(PBegin input, Coder<T> outputCoder) {
      ValueProvider<TableReference> tableProvider = getTableProvider();
      Pipeline p = input.getPipeline();
      if (tableProvider != null) {
        // No job ID is required. Read directly from BigQuery storage.
        return p.apply(
            org.apache.beam.sdk.io.Read.from(
                BigQueryStorageTableSource.create(
                    tableProvider,
                    getReadOptions(),
                    getSelectedFields(),
                    getRowRestriction(),
                    getParseFn(),
                    outputCoder,
                    getBigQueryServices())));
      }

      checkArgument(
          getReadOptions() == null,
          "Invalid BigQueryIO.Read: Specifies table read options, "
              + "which only applies when reading from a table");

      checkArgument(
          getSelectedFields() == null,
          "Invalid BigQueryIO.Read: Specifies selected fields, "
              + "which only applies when reading from a table");

      checkArgument(
          getRowRestriction() == null,
          "Invalid BigQueryIO.Read: Specifies row restriction, "
              + "which only applies when reading from a table");

      //
      // N.B. All of the code below exists because the BigQuery storage API can't (yet) read from
      // all anonymous tables, so we need the job ID to reason about the name of the destination
      // table for the query to read the data and subsequently delete the table and dataset. Once
      // the storage API can handle anonymous tables, the storage source should be modified to use
      // anonymous tables and all of the code related to job ID generation and table and dataset
      // cleanup can be removed. [BEAM-6931]
      //

      PCollectionView<String> jobIdTokenView;
      PCollection<T> rows;

      if (!getWithTemplateCompatibility()) {
        // Create a singleton job ID token at pipeline construction time.
        String staticJobUuid = BigQueryHelpers.randomUUIDString();
        jobIdTokenView =
            p.apply("TriggerIdCreation", Create.of(staticJobUuid))
                .apply("ViewId", View.asSingleton());
        // Apply the traditional Source model.
        rows =
            p.apply(
                org.apache.beam.sdk.io.Read.from(
                    createStorageQuerySource(staticJobUuid, outputCoder)));
      } else {
        // Create a singleton job ID token at pipeline execution time.
        PCollection<String> jobIdTokenCollection =
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

        jobIdTokenView = jobIdTokenCollection.apply("ViewId", View.asSingleton());

        TupleTag<Stream> streamsTag = new TupleTag<>();
        TupleTag<ReadSession> readSessionTag = new TupleTag<>();
        TupleTag<String> tableSchemaTag = new TupleTag<>();

        PCollectionTuple tuple =
            jobIdTokenCollection.apply(
                "RunQueryJob",
                ParDo.of(
                        new DoFn<String, Stream>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) throws Exception {
                            BigQueryOptions options =
                                c.getPipelineOptions().as(BigQueryOptions.class);
                            String jobUuid = c.element();
                            // Execute the query and get the destination table holding the results.
                            // The getTargetTable call runs a new instance of the query and returns
                            // the destination table created to hold the results.
                            BigQueryStorageQuerySource<T> querySource =
                                createStorageQuerySource(jobUuid, outputCoder);
                            Table queryResultTable = querySource.getTargetTable(options);

                            // Create a read session without specifying a desired stream count and
                            // let the BigQuery storage server pick the number of streams.
                            CreateReadSessionRequest request =
                                CreateReadSessionRequest.newBuilder()
                                    .setParent("projects/" + options.getProject())
                                    .setTableReference(
                                        BigQueryHelpers.toTableRefProto(
                                            queryResultTable.getTableReference()))
                                    .setRequestedStreams(0)
                                    .build();

                            ReadSession readSession;
                            try (StorageClient storageClient =
                                getBigQueryServices().getStorageClient(options)) {
                              readSession = storageClient.createReadSession(request);
                            }

                            for (Stream stream : readSession.getStreamsList()) {
                              c.output(stream);
                            }

                            c.output(readSessionTag, readSession);
                            c.output(
                                tableSchemaTag,
                                BigQueryHelpers.toJsonString(queryResultTable.getSchema()));
                          }
                        })
                    .withOutputTags(
                        streamsTag, TupleTagList.of(readSessionTag).and(tableSchemaTag)));

        tuple.get(streamsTag).setCoder(ProtoCoder.of(Stream.class));
        tuple.get(readSessionTag).setCoder(ProtoCoder.of(ReadSession.class));
        tuple.get(tableSchemaTag).setCoder(StringUtf8Coder.of());

        PCollectionView<ReadSession> readSessionView =
            tuple.get(readSessionTag).apply("ReadSessionView", View.asSingleton());
        PCollectionView<String> tableSchemaView =
            tuple.get(tableSchemaTag).apply("TableSchemaView", View.asSingleton());

        rows =
            tuple
                .get(streamsTag)
                .apply(Reshuffle.viaRandomKey())
                .apply(
                    ParDo.of(
                            new DoFn<Stream, T>() {
                              @ProcessElement
                              public void processElement(ProcessContext c) throws Exception {
                                ReadSession readSession = c.sideInput(readSessionView);
                                TableSchema tableSchema =
                                    BigQueryHelpers.fromJsonString(
                                        c.sideInput(tableSchemaView), TableSchema.class);
                                Stream stream = c.element();

                                BigQueryStorageStreamSource<T> streamSource =
                                    BigQueryStorageStreamSource.create(
                                        readSession,
                                        stream,
                                        tableSchema,
                                        getParseFn(),
                                        outputCoder,
                                        getBigQueryServices());

                                // Read all of the data from the stream. In the event that this work
                                // item fails and is rescheduled, the same rows will be returned in
                                // the same order.
                                BoundedSource.BoundedReader<T> reader =
                                    streamSource.createReader(c.getPipelineOptions());
                                for (boolean more = reader.start(); more; more = reader.advance()) {
                                  c.output(reader.getCurrent());
                                }
                              }
                            })
                        .withSideInputs(readSessionView, tableSchemaView))
                .setCoder(outputCoder);
      }

      PassThroughThenCleanup.CleanupOperation cleanupOperation =
          new CleanupOperation() {
            @Override
            void cleanup(ContextContainer c) throws Exception {
              BigQueryOptions options = c.getPipelineOptions().as(BigQueryOptions.class);
              String jobUuid = c.getJobId();

              Optional<String> queryTempDataset = Optional.ofNullable(getQueryTempDataset());

              TableReference tempTable =
                  createTempTableReference(
                      options.getProject(),
                      BigQueryResourceNaming.createJobIdPrefix(
                          options.getJobName(), jobUuid, JobType.QUERY),
                      queryTempDataset);

              DatasetService datasetService = getBigQueryServices().getDatasetService(options);
              LOG.info("Deleting temporary table with query results {}", tempTable);
              datasetService.deleteTable(tempTable);
              // Delete dataset only if it was created by Beam
              boolean datasetCreatedByBeam = !queryTempDataset.isPresent();
              if (datasetCreatedByBeam) {
                LOG.info(
                    "Deleting temporary dataset with query results {}", tempTable.getDatasetId());
                datasetService.deleteDataset(tempTable.getProjectId(), tempTable.getDatasetId());
              }
            }
          };

      return rows.apply(new PassThroughThenCleanup<>(cleanupOperation, jobIdTokenView));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("table", BigQueryHelpers.displayTable(getTableProvider()))
                  .withLabel("Table"))
          .addIfNotNull(DisplayData.item("query", getQuery()).withLabel("Query"))
          .addIfNotNull(
              DisplayData.item("flattenResults", getFlattenResults())
                  .withLabel("Flatten Query Results"))
          .addIfNotNull(
              DisplayData.item("useLegacySql", getUseLegacySql())
                  .withLabel("Use Legacy SQL Dialect"))
          .addIfNotDefault(
              DisplayData.item("validation", getValidate()).withLabel("Validation Enabled"), true);
    }

    /** Ensures that methods of the from() / fromQuery() family are called at most once. */
    private void ensureFromNotCalledYet() {
      checkState(
          getJsonTableRef() == null && getQuery() == null, "from() or fromQuery() already called");
    }

    private void ensureReadOptionsNotSet() {
      checkState(getReadOptions() == null, "withReadOptions() already called");
    }

    private void ensureReadOptionsFieldsNotSet() {
      checkState(
          getSelectedFields() == null && getRowRestriction() == null,
          "setSelectedFields() or setRowRestriction already called");
    }

    /** See {@link Read#getTableProvider()}. */
    public @Nullable ValueProvider<TableReference> getTableProvider() {
      return getJsonTableRef() == null
          ? null
          : NestedValueProvider.of(getJsonTableRef(), new JsonTableRefToTableRef());
    }

    /** See {@link Read#getTable()}. */
    public @Nullable TableReference getTable() {
      ValueProvider<TableReference> provider = getTableProvider();
      return provider == null ? null : provider.get();
    }

    /**
     * Sets a {@link Coder} for the result of the parse function. This may be required if a coder
     * can not be inferred automatically.
     */
    public TypedRead<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    /** For query sources, use this Cloud KMS key to encrypt any temporary tables created. */
    public TypedRead<T> withKmsKey(String kmsKey) {
      return toBuilder().setKmsKey(kmsKey).build();
    }

    /**
     * Sets the functions to convert elements to/from {@link Row} objects.
     *
     * <p>Setting these conversion functions is necessary to enable {@link Schema} support.
     */
    @Experimental(Kind.SCHEMAS)
    public TypedRead<T> withBeamRowConverters(
        TypeDescriptor<T> typeDescriptor,
        ToBeamRowFunction<T> toRowFn,
        FromBeamRowFunction<T> fromRowFn) {
      return toBuilder()
          .setTypeDescriptor(typeDescriptor)
          .setToBeamRowFn(toRowFn)
          .setFromBeamRowFn(fromRowFn)
          .build();
    }

    /** See {@link Read#from(String)}. */
    public TypedRead<T> from(String tableSpec) {
      return from(StaticValueProvider.of(tableSpec));
    }

    /** See {@link Read#from(ValueProvider)}. */
    public TypedRead<T> from(ValueProvider<String> tableSpec) {
      ensureFromNotCalledYet();
      return toBuilder()
          .setJsonTableRef(
              NestedValueProvider.of(
                  NestedValueProvider.of(tableSpec, new TableSpecToTableRef()),
                  new TableRefToJson()))
          .build();
    }

    /** See {@link Read#fromQuery(String)}. */
    public TypedRead<T> fromQuery(String query) {
      return fromQuery(StaticValueProvider.of(query));
    }

    /** See {@link Read#fromQuery(ValueProvider)}. */
    public TypedRead<T> fromQuery(ValueProvider<String> query) {
      ensureFromNotCalledYet();
      return toBuilder().setQuery(query).setFlattenResults(true).setUseLegacySql(true).build();
    }

    /** See {@link Read#from(TableReference)}. */
    public TypedRead<T> from(TableReference table) {
      return from(StaticValueProvider.of(BigQueryHelpers.toTableSpec(table)));
    }

    /** See {@link Read#withoutValidation()}. */
    public TypedRead<T> withoutValidation() {
      return toBuilder().setValidate(false).build();
    }

    /** See {@link Read#withoutResultFlattening()}. */
    public TypedRead<T> withoutResultFlattening() {
      return toBuilder().setFlattenResults(false).build();
    }

    /** See {@link Read#usingStandardSql()}. */
    public TypedRead<T> usingStandardSql() {
      return toBuilder().setUseLegacySql(false).build();
    }

    /** See {@link QueryPriority}. */
    public TypedRead<T> withQueryPriority(QueryPriority priority) {
      return toBuilder().setQueryPriority(priority).build();
    }

    /**
     * BigQuery geographic location where the query <a
     * href="https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs">job</a> will be
     * executed. If not specified, Beam tries to determine the location by examining the tables
     * referenced by the query. Location must be specified for queries not executed in US or EU, or
     * when you are reading from an authorized view. See <a
     * href="https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query">BigQuery Jobs:
     * query</a>.
     */
    public TypedRead<T> withQueryLocation(String location) {
      return toBuilder().setQueryLocation(location).build();
    }

    /**
     * Temporary dataset reference when using {@link #fromQuery(String)}. When reading from a query,
     * BigQuery will create a temporary dataset and a temporary table to store the results of the
     * query. With this option, you can set an existing dataset to create the temporary table.
     * BigQueryIO will create a temporary table in that dataset, and will remove it once it is not
     * needed. No other tables in the dataset will be modified. If your job does not have
     * permissions to create a new dataset, and you want to use {@link #fromQuery(String)} (for
     * instance, to read from a view), you should use this option. Remember that the dataset must
     * exist and your job needs permissions to create and remove tables inside that dataset.
     */
    public TypedRead<T> withQueryTempDataset(String queryTempDatasetRef) {
      return toBuilder().setQueryTempDataset(queryTempDatasetRef).build();
    }

    /** See {@link Method}. */
    public TypedRead<T> withMethod(Method method) {
      return toBuilder().setMethod(method).build();
    }

    /**
     * @deprecated Use {@link #withSelectedFields(List)} and {@link #withRowRestriction(String)}
     *     instead.
     */
    @Deprecated
    @Experimental(Kind.SOURCE_SINK)
    public TypedRead<T> withReadOptions(TableReadOptions readOptions) {
      ensureReadOptionsFieldsNotSet();
      return toBuilder().setReadOptions(readOptions).build();
    }

    /** See {@link #withSelectedFields(ValueProvider)}. */
    public TypedRead<T> withSelectedFields(List<String> selectedFields) {
      return withSelectedFields(StaticValueProvider.of(selectedFields));
    }

    /**
     * Read only the specified fields (columns) from a BigQuery table. Fields may not be returned in
     * the order specified. If no value is specified, then all fields are returned.
     *
     * <p>Requires {@link Method#DIRECT_READ}. Not compatible with {@link #fromQuery(String)}.
     */
    public TypedRead<T> withSelectedFields(ValueProvider<List<String>> selectedFields) {
      ensureReadOptionsNotSet();
      return toBuilder().setSelectedFields(selectedFields).build();
    }

    /** See {@link #withRowRestriction(ValueProvider)}. */
    public TypedRead<T> withRowRestriction(String rowRestriction) {
      return withRowRestriction(StaticValueProvider.of(rowRestriction));
    }

    /**
     * Read only rows which match the specified filter, which must be a SQL expression compatible
     * with <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/">Google standard
     * SQL</a>. If no value is specified, then all rows are returned.
     *
     * <p>Requires {@link Method#DIRECT_READ}. Not compatible with {@link #fromQuery(String)}.
     */
    public TypedRead<T> withRowRestriction(ValueProvider<String> rowRestriction) {
      ensureReadOptionsNotSet();
      return toBuilder().setRowRestriction(rowRestriction).build();
    }

    public TypedRead<T> withTemplateCompatibility() {
      return toBuilder().setWithTemplateCompatibility(true).build();
    }

    @VisibleForTesting
    TypedRead<T> withTestServices(BigQueryServices testServices) {
      return toBuilder().setBigQueryServices(testServices).build();
    }

    public TypedRead<T> useAvroLogicalTypes() {
      return toBuilder().setUseAvroLogicalTypes(true).build();
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
      String errorMessage =
          counts.isEmpty()
              ? "No destination uri file count received."
              : String.format(
                  "More than one destination uri file count received. First two are %s, %s",
                  counts.get(0), counts.get(1));
      throw new RuntimeException(errorMessage);
    }
    long filesCount = counts.get(0);

    ImmutableList.Builder<ResourceId> paths = ImmutableList.builder();
    ResourceId extractDestinationDirResourceId =
        FileSystems.matchNewResource(extractDestinationDir, true /* isDirectory */);
    for (long i = 0; i < filesCount; ++i) {
      ResourceId filePath =
          extractDestinationDirResourceId.resolve(
              String.format("%012d%s", i, ".avro"),
              ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
      paths.add(filePath);
    }
    return paths.build();
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link PTransform} that writes a {@link PCollection} to a BigQuery table. A formatting
   * function must be provided to convert each input element into a {@link TableRow} using {@link
   * Write#withFormatFunction(SerializableFunction)}.
   *
   * <p>In BigQuery, each table has an enclosing dataset. The dataset being written must already
   * exist.
   *
   * <p>By default, tables will be created if they do not exist, which corresponds to a {@link
   * Write.CreateDisposition#CREATE_IF_NEEDED} disposition that matches the default of BigQuery's
   * Jobs API. A schema must be provided (via {@link Write#withSchema(TableSchema)}), or else the
   * transform may fail at runtime with an {@link IllegalArgumentException}.
   *
   * <p>By default, writes require an empty table, which corresponds to a {@link
   * Write.WriteDisposition#WRITE_EMPTY} disposition that matches the default of BigQuery's Jobs
   * API.
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
        .setSchemaUpdateOptions(Collections.emptySet())
        .setNumFileShards(0)
        .setMethod(Write.Method.DEFAULT)
        .setExtendedErrorInfo(false)
        .setSkipInvalidRows(false)
        .setIgnoreUnknownValues(false)
        .setIgnoreInsertIds(false)
        .setUseAvroLogicalTypes(false)
        .setMaxFilesPerPartition(BatchLoads.DEFAULT_MAX_FILES_PER_PARTITION)
        .setMaxBytesPerPartition(BatchLoads.DEFAULT_MAX_BYTES_PER_PARTITION)
        .setOptimizeWrites(false)
        .setUseBeamSchema(false)
        .build();
  }

  /**
   * A {@link PTransform} that writes a {@link PCollection} containing {@link TableRow TableRows} to
   * a BigQuery table.
   *
   * <p>It is recommended to instead use {@link #write} with {@link
   * Write#withFormatFunction(SerializableFunction)}.
   */
  public static Write<TableRow> writeTableRows() {
    return BigQueryIO.<TableRow>write().withFormatFunction(IDENTITY_FORMATTER);
  }

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, WriteResult> {
    /** Determines the method used to insert data in BigQuery. */
    public enum Method {
      /**
       * The default behavior if no method is explicitly set. If the input is bounded, then file
       * loads will be used. If the input is unbounded, then streaming inserts will be used.
       */
      DEFAULT,

      /**
       * Use BigQuery load jobs to insert data. Records will first be written to files, and these
       * files will be loaded into BigQuery. This is the default method when the input is bounded.
       * This method can be chosen for unbounded inputs as well, as long as a triggering frequency
       * is also set using {@link #withTriggeringFrequency}. BigQuery has daily quotas on the number
       * of load jobs allowed per day, so be careful not to set the triggering frequency too
       * frequent. For more information, see <a
       * href="https://cloud.google.com/bigquery/docs/loading-data-cloud-storage">Loading Data from
       * Cloud Storage</a>.
       */
      FILE_LOADS,

      /**
       * Use the BigQuery streaming insert API to insert data. This provides the lowest-latency
       * insert path into BigQuery, and therefore is the default method when the input is unbounded.
       * BigQuery will make a strong effort to ensure no duplicates when using this path, however
       * there are some scenarios in which BigQuery is unable to make this guarantee (see
       * https://cloud.google.com/bigquery/streaming-data-into-bigquery). A query can be run over
       * the output table to periodically clean these rare duplicates. Alternatively, using the
       * {@link #FILE_LOADS} insert method does guarantee no duplicates, though the latency for the
       * insert into BigQuery will be much higher. For more information, see <a
       * href="https://cloud.google.com/bigquery/streaming-data-into-bigquery">Streaming Data into
       * BigQuery</a>.
       */
      STREAMING_INSERTS
    }

    abstract @Nullable ValueProvider<String> getJsonTableRef();

    abstract @Nullable SerializableFunction<ValueInSingleWindow<T>, TableDestination>
        getTableFunction();

    abstract @Nullable SerializableFunction<T, TableRow> getFormatFunction();

    abstract RowWriterFactory.@Nullable AvroRowWriterFactory<T, ?, ?> getAvroRowWriterFactory();

    abstract @Nullable SerializableFunction<TableSchema, org.apache.avro.Schema>
        getAvroSchemaFactory();

    abstract boolean getUseAvroLogicalTypes();

    abstract @Nullable DynamicDestinations<T, ?> getDynamicDestinations();

    abstract @Nullable PCollectionView<Map<String, String>> getSchemaFromView();

    abstract @Nullable ValueProvider<String> getJsonSchema();

    abstract @Nullable ValueProvider<String> getJsonTimePartitioning();

    abstract @Nullable Clustering getClustering();

    abstract CreateDisposition getCreateDisposition();

    abstract WriteDisposition getWriteDisposition();

    abstract Set<SchemaUpdateOption> getSchemaUpdateOptions();
    /** Table description. Default is empty. */
    abstract @Nullable String getTableDescription();
    /** An option to indicate if table validation is desired. Default is true. */
    abstract boolean getValidate();

    abstract BigQueryServices getBigQueryServices();

    abstract @Nullable Integer getMaxFilesPerBundle();

    abstract @Nullable Long getMaxFileSize();

    abstract int getNumFileShards();

    abstract int getMaxFilesPerPartition();

    abstract long getMaxBytesPerPartition();

    abstract @Nullable Duration getTriggeringFrequency();

    abstract Method getMethod();

    abstract @Nullable ValueProvider<String> getLoadJobProjectId();

    abstract @Nullable InsertRetryPolicy getFailedInsertRetryPolicy();

    abstract @Nullable ValueProvider<String> getCustomGcsTempLocation();

    abstract boolean getExtendedErrorInfo();

    abstract Boolean getSkipInvalidRows();

    abstract Boolean getIgnoreUnknownValues();

    abstract Boolean getIgnoreInsertIds();

    abstract @Nullable String getKmsKey();

    abstract Boolean getOptimizeWrites();

    @Experimental(Kind.SCHEMAS)
    abstract Boolean getUseBeamSchema();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setJsonTableRef(ValueProvider<String> jsonTableRef);

      abstract Builder<T> setTableFunction(
          SerializableFunction<ValueInSingleWindow<T>, TableDestination> tableFunction);

      abstract Builder<T> setFormatFunction(SerializableFunction<T, TableRow> formatFunction);

      abstract Builder<T> setAvroRowWriterFactory(
          RowWriterFactory.AvroRowWriterFactory<T, ?, ?> avroRowWriterFactory);

      abstract Builder<T> setAvroSchemaFactory(
          SerializableFunction<TableSchema, org.apache.avro.Schema> avroSchemaFactory);

      abstract Builder<T> setUseAvroLogicalTypes(boolean useAvroLogicalTypes);

      abstract Builder<T> setDynamicDestinations(DynamicDestinations<T, ?> dynamicDestinations);

      abstract Builder<T> setSchemaFromView(PCollectionView<Map<String, String>> view);

      abstract Builder<T> setJsonSchema(ValueProvider<String> jsonSchema);

      abstract Builder<T> setJsonTimePartitioning(ValueProvider<String> jsonTimePartitioning);

      abstract Builder<T> setClustering(Clustering clustering);

      abstract Builder<T> setCreateDisposition(CreateDisposition createDisposition);

      abstract Builder<T> setWriteDisposition(WriteDisposition writeDisposition);

      abstract Builder<T> setSchemaUpdateOptions(Set<SchemaUpdateOption> schemaUpdateOptions);

      abstract Builder<T> setTableDescription(String tableDescription);

      abstract Builder<T> setValidate(boolean validate);

      abstract Builder<T> setBigQueryServices(BigQueryServices bigQueryServices);

      abstract Builder<T> setMaxFilesPerBundle(Integer maxFilesPerBundle);

      abstract Builder<T> setMaxFileSize(Long maxFileSize);

      abstract Builder<T> setNumFileShards(int numFileShards);

      abstract Builder<T> setMaxFilesPerPartition(int maxFilesPerPartition);

      abstract Builder<T> setMaxBytesPerPartition(long maxBytesPerPartition);

      abstract Builder<T> setTriggeringFrequency(Duration triggeringFrequency);

      abstract Builder<T> setMethod(Method method);

      abstract Builder<T> setLoadJobProjectId(ValueProvider<String> loadJobProjectId);

      abstract Builder<T> setFailedInsertRetryPolicy(InsertRetryPolicy retryPolicy);

      abstract Builder<T> setCustomGcsTempLocation(ValueProvider<String> customGcsTempLocation);

      abstract Builder<T> setExtendedErrorInfo(boolean extendedErrorInfo);

      abstract Builder<T> setSkipInvalidRows(Boolean skipInvalidRows);

      abstract Builder<T> setIgnoreUnknownValues(Boolean ignoreUnknownValues);

      abstract Builder<T> setIgnoreInsertIds(Boolean ignoreInsertIds);

      abstract Builder<T> setKmsKey(String kmsKey);

      abstract Builder<T> setOptimizeWrites(Boolean optimizeWrites);

      @Experimental(Kind.SCHEMAS)
      abstract Builder<T> setUseBeamSchema(Boolean useBeamSchema);

      abstract Write<T> build();
    }

    /**
     * An enumeration type for the BigQuery create disposition strings.
     *
     * @see <a
     *     href="https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.createDisposition">
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
       * <p>Requires that a table schema is provided via {@link BigQueryIO.Write#withSchema}. This
       * precondition is checked before starting a job. The schema is not required to match an
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
     * An enumeration type for the BigQuery schema update options strings.
     *
     * <p>Note from the BigQuery API doc -- Schema update options are supported in two cases: when
     * writeDisposition is WRITE_APPEND; when writeDisposition is WRITE_TRUNCATE and the destination
     * table is a partition of a table, specified by partition decorators.
     *
     * @see <a
     *     href="https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationquery">
     *     <code>configuration.query.schemaUpdateOptions</code> in the BigQuery Jobs API</a>
     */
    public enum SchemaUpdateOption {
      /** Allow adding a nullable field to the schema. */
      ALLOW_FIELD_ADDITION,

      /** Allow relaxing a required field in the original schema to nullable. */
      ALLOW_FIELD_RELAXATION
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
      checkArgument(tableSpec != null, "tableSpec can not be null");
      return toBuilder()
          .setJsonTableRef(
              NestedValueProvider.of(
                  NestedValueProvider.of(tableSpec, new TableSpecToTableRef()),
                  new TableRefToJson()))
          .build();
    }

    /**
     * Writes to table specified by the specified table function. The table is a function of {@link
     * ValueInSingleWindow}, so can be determined by the value or by the window.
     *
     * <p>If the function produces destinations configured with clustering fields, ensure that
     * {@link #withClustering()} is also set so that the clustering configurations get properly
     * encoded and decoded.
     */
    public Write<T> to(
        SerializableFunction<ValueInSingleWindow<T>, TableDestination> tableFunction) {
      checkArgument(tableFunction != null, "tableFunction can not be null");
      return toBuilder().setTableFunction(tableFunction).build();
    }

    /**
     * Writes to the table and schema specified by the {@link DynamicDestinations} object.
     *
     * <p>If any of the returned destinations are configured with clustering fields, ensure that the
     * passed {@link DynamicDestinations} object returns {@link TableDestinationCoderV3} when {@link
     * DynamicDestinations#getDestinationCoder()} is called.
     */
    public Write<T> to(DynamicDestinations<T, ?> dynamicDestinations) {
      checkArgument(dynamicDestinations != null, "dynamicDestinations can not be null");
      return toBuilder().setDynamicDestinations(dynamicDestinations).build();
    }

    /** Formats the user's type into a {@link TableRow} to be written to BigQuery. */
    public Write<T> withFormatFunction(SerializableFunction<T, TableRow> formatFunction) {
      return toBuilder().setFormatFunction(formatFunction).build();
    }

    /**
     * Formats the user's type into a {@link GenericRecord} to be written to BigQuery. The
     * GenericRecords are written as avro using the standard {@link GenericDatumWriter}.
     *
     * <p>This is mutually exclusive with {@link #withFormatFunction}, only one may be set.
     */
    public Write<T> withAvroFormatFunction(
        SerializableFunction<AvroWriteRequest<T>, GenericRecord> avroFormatFunction) {
      return withAvroWriter(avroFormatFunction, GENERIC_DATUM_WRITER_FACTORY);
    }

    /**
     * Writes the user's type as avro using the supplied {@link DatumWriter}.
     *
     * <p>This is mutually exclusive with {@link #withFormatFunction}, only one may be set.
     *
     * <p>Overwrites {@link #withAvroFormatFunction} if it has been set.
     */
    public Write<T> withAvroWriter(
        SerializableFunction<org.apache.avro.Schema, DatumWriter<T>> writerFactory) {
      return withAvroWriter(AvroWriteRequest::getElement, writerFactory);
    }

    /**
     * Convert's the user's type to an avro record using the supplied avroFormatFunction. Records
     * are then written using the supplied writer instances returned from writerFactory.
     *
     * <p>This is mutually exclusive with {@link #withFormatFunction}, only one may be set.
     *
     * <p>Overwrites {@link #withAvroFormatFunction} if it has been set.
     */
    public <AvroT> Write<T> withAvroWriter(
        SerializableFunction<AvroWriteRequest<T>, AvroT> avroFormatFunction,
        SerializableFunction<org.apache.avro.Schema, DatumWriter<AvroT>> writerFactory) {
      return toBuilder()
          .setOptimizeWrites(true)
          .setAvroRowWriterFactory(RowWriterFactory.avroRecords(avroFormatFunction, writerFactory))
          .build();
    }

    /**
     * Uses the specified function to convert a {@link TableSchema} to a {@link
     * org.apache.avro.Schema}.
     *
     * <p>If not specified, the TableSchema will automatically be converted to an avro schema.
     */
    public Write<T> withAvroSchemaFactory(
        SerializableFunction<TableSchema, org.apache.avro.Schema> avroSchemaFactory) {
      return toBuilder().setAvroSchemaFactory(avroSchemaFactory).build();
    }

    /**
     * Uses the specified schema for rows to be written.
     *
     * <p>The schema is <i>required</i> only if writing to a table that does not already exist, and
     * {@link CreateDisposition} is set to {@link CreateDisposition#CREATE_IF_NEEDED}.
     */
    public Write<T> withSchema(TableSchema schema) {
      checkArgument(schema != null, "schema can not be null");
      return withJsonSchema(StaticValueProvider.of(BigQueryHelpers.toJsonString(schema)));
    }

    /** Same as {@link #withSchema(TableSchema)} but using a deferred {@link ValueProvider}. */
    public Write<T> withSchema(ValueProvider<TableSchema> schema) {
      checkArgument(schema != null, "schema can not be null");
      return withJsonSchema(NestedValueProvider.of(schema, new TableSchemaToJsonSchema()));
    }

    /**
     * Similar to {@link #withSchema(TableSchema)} but takes in a JSON-serialized {@link
     * TableSchema}.
     */
    public Write<T> withJsonSchema(String jsonSchema) {
      checkArgument(jsonSchema != null, "jsonSchema can not be null");
      return withJsonSchema(StaticValueProvider.of(jsonSchema));
    }

    /** Same as {@link #withJsonSchema(String)} but using a deferred {@link ValueProvider}. */
    public Write<T> withJsonSchema(ValueProvider<String> jsonSchema) {
      checkArgument(jsonSchema != null, "jsonSchema can not be null");
      return toBuilder().setJsonSchema(jsonSchema).build();
    }

    /**
     * Allows the schemas for each table to be computed within the pipeline itself.
     *
     * <p>The input is a map-valued {@link PCollectionView} mapping string tablespecs to
     * JSON-formatted {@link TableSchema}s. Tablespecs must be in the same format as taken by {@link
     * #to(String)}.
     */
    public Write<T> withSchemaFromView(PCollectionView<Map<String, String>> view) {
      checkArgument(view != null, "view can not be null");
      return toBuilder().setSchemaFromView(view).build();
    }

    /**
     * Allows newly created tables to include a {@link TimePartitioning} class. Can only be used
     * when writing to a single table. If {@link #to(SerializableFunction)} or {@link
     * #to(DynamicDestinations)} is used to write dynamic tables, time partitioning can be directly
     * set in the returned {@link TableDestination}.
     */
    public Write<T> withTimePartitioning(TimePartitioning partitioning) {
      checkArgument(partitioning != null, "partitioning can not be null");
      return withJsonTimePartitioning(
          StaticValueProvider.of(BigQueryHelpers.toJsonString(partitioning)));
    }

    /**
     * Like {@link #withTimePartitioning(TimePartitioning)} but using a deferred {@link
     * ValueProvider}.
     */
    public Write<T> withTimePartitioning(ValueProvider<TimePartitioning> partitioning) {
      checkArgument(partitioning != null, "partitioning can not be null");
      return withJsonTimePartitioning(
          NestedValueProvider.of(partitioning, new TimePartitioningToJson()));
    }

    /** The same as {@link #withTimePartitioning}, but takes a JSON-serialized object. */
    public Write<T> withJsonTimePartitioning(ValueProvider<String> partitioning) {
      checkArgument(partitioning != null, "partitioning can not be null");
      return toBuilder().setJsonTimePartitioning(partitioning).build();
    }

    /**
     * Specifies the clustering fields to use when writing to a single output table. Can only be
     * used when {@link#withTimePartitioning(TimePartitioning)} is set. If {@link
     * #to(SerializableFunction)} or {@link #to(DynamicDestinations)} is used to write to dynamic
     * tables, the fields here will be ignored; call {@link #withClustering()} instead.
     */
    public Write<T> withClustering(Clustering clustering) {
      checkArgument(clustering != null, "clustering can not be null");
      return toBuilder().setClustering(clustering).build();
    }

    /**
     * Allows writing to clustered tables when {@link #to(SerializableFunction)} or {@link
     * #to(DynamicDestinations)} is used. The returned {@link TableDestination} objects should
     * specify the time partitioning and clustering fields per table. If writing to a single table,
     * use {@link #withClustering(Clustering)} instead to pass a {@link Clustering} instance that
     * specifies the static clustering fields to use.
     *
     * <p>Setting this option enables use of {@link TableDestinationCoderV3} which encodes
     * clustering information. The updated coder is compatible with non-clustered tables, so can be
     * freely set for newly deployed pipelines, but note that pipelines using an older coder must be
     * drained before setting this option, since {@link TableDestinationCoderV3} will not be able to
     * read state written with a previous version.
     */
    public Write<T> withClustering() {
      return toBuilder().setClustering(new Clustering()).build();
    }

    /** Specifies whether the table should be created if it does not exist. */
    public Write<T> withCreateDisposition(CreateDisposition createDisposition) {
      checkArgument(createDisposition != null, "createDisposition can not be null");
      return toBuilder().setCreateDisposition(createDisposition).build();
    }

    /** Specifies what to do with existing data in the table, in case the table already exists. */
    public Write<T> withWriteDisposition(WriteDisposition writeDisposition) {
      checkArgument(writeDisposition != null, "writeDisposition can not be null");
      return toBuilder().setWriteDisposition(writeDisposition).build();
    }

    /** Allows the schema of the destination table to be updated as a side effect of the write. */
    public Write<T> withSchemaUpdateOptions(Set<SchemaUpdateOption> schemaUpdateOptions) {
      checkArgument(schemaUpdateOptions != null, "schemaUpdateOptions can not be null");
      return toBuilder().setSchemaUpdateOptions(schemaUpdateOptions).build();
    }

    /** Specifies the table description. */
    public Write<T> withTableDescription(String tableDescription) {
      checkArgument(tableDescription != null, "tableDescription can not be null");
      return toBuilder().setTableDescription(tableDescription).build();
    }

    /**
     * Specfies a policy for handling failed inserts.
     *
     * <p>Currently this only is allowed when writing an unbounded collection to BigQuery. Bounded
     * collections are written using batch load jobs, so we don't get per-element failures.
     * Unbounded collections are written using streaming inserts, so we have access to per-element
     * insert results.
     */
    public Write<T> withFailedInsertRetryPolicy(InsertRetryPolicy retryPolicy) {
      checkArgument(retryPolicy != null, "retryPolicy can not be null");
      return toBuilder().setFailedInsertRetryPolicy(retryPolicy).build();
    }

    /** Disables BigQuery table validation. */
    public Write<T> withoutValidation() {
      return toBuilder().setValidate(false).build();
    }

    /**
     * Choose the method used to write data to BigQuery. See the Javadoc on {@link Method} for
     * information and restrictions of the different methods.
     */
    public Write<T> withMethod(Method method) {
      checkArgument(method != null, "method can not be null");
      return toBuilder().setMethod(method).build();
    }

    /**
     * Set the project the BigQuery load job will be initiated from. This is only applicable when
     * the write method is set to {@link Method#FILE_LOADS}. If omitted, the project of the
     * destination table is used.
     */
    public Write<T> withLoadJobProjectId(String loadJobProjectId) {
      return withLoadJobProjectId(StaticValueProvider.of(loadJobProjectId));
    }

    public Write<T> withLoadJobProjectId(ValueProvider<String> loadJobProjectId) {
      checkArgument(loadJobProjectId != null, "loadJobProjectId can not be null");
      return toBuilder().setLoadJobProjectId(loadJobProjectId).build();
    }

    /**
     * Choose the frequency at which file writes are triggered.
     *
     * <p>This is only applicable when the write method is set to {@link Method#FILE_LOADS}, and
     * only when writing an unbounded {@link PCollection}.
     *
     * <p>Every triggeringFrequency duration, a BigQuery load job will be generated for all the data
     * written since the last load job. BigQuery has limits on how many load jobs can be triggered
     * per day, so be careful not to set this duration too low, or you may exceed daily quota. Often
     * this is set to 5 or 10 minutes to ensure that the project stays well under the BigQuery
     * quota. See <a href="https://cloud.google.com/bigquery/quota-policy">Quota Policy</a> for more
     * information about BigQuery quotas.
     */
    public Write<T> withTriggeringFrequency(Duration triggeringFrequency) {
      checkArgument(triggeringFrequency != null, "triggeringFrequency can not be null");
      return toBuilder().setTriggeringFrequency(triggeringFrequency).build();
    }

    /**
     * Control how many file shards are written when using BigQuery load jobs. Applicable only when
     * also setting {@link #withTriggeringFrequency}.
     */
    public Write<T> withNumFileShards(int numFileShards) {
      checkArgument(numFileShards > 0, "numFileShards must be > 0, but was: %s", numFileShards);
      return toBuilder().setNumFileShards(numFileShards).build();
    }

    /**
     * Provides a custom location on GCS for storing temporary files to be loaded via BigQuery batch
     * load jobs. See "Usage with templates" in {@link BigQueryIO} documentation for discussion.
     */
    public Write<T> withCustomGcsTempLocation(ValueProvider<String> customGcsTempLocation) {
      checkArgument(customGcsTempLocation != null, "customGcsTempLocation can not be null");
      return toBuilder().setCustomGcsTempLocation(customGcsTempLocation).build();
    }

    /**
     * Enables extended error information by enabling {@link WriteResult#getFailedInsertsWithErr()}
     *
     * <p>ATM this only works if using {@link Method#STREAMING_INSERTS}. See {@link
     * Write#withMethod(Method)}.
     */
    public Write<T> withExtendedErrorInfo() {
      return toBuilder().setExtendedErrorInfo(true).build();
    }

    /**
     * Insert all valid rows of a request, even if invalid rows exist. This is only applicable when
     * the write method is set to {@link Method#STREAMING_INSERTS}. The default value is false,
     * which causes the entire request to fail if any invalid rows exist.
     */
    public Write<T> skipInvalidRows() {
      return toBuilder().setSkipInvalidRows(true).build();
    }

    /**
     * Accept rows that contain values that do not match the schema. The unknown values are ignored.
     * Default is false, which treats unknown values as errors.
     */
    public Write<T> ignoreUnknownValues() {
      return toBuilder().setIgnoreUnknownValues(true).build();
    }

    /**
     * Enables interpreting logical types into their corresponding types (ie. TIMESTAMP), instead of
     * only using their raw types (ie. LONG).
     */
    public Write<T> useAvroLogicalTypes() {
      return toBuilder().setUseAvroLogicalTypes(true).build();
    }

    /**
     * Setting this option to true disables insertId based data deduplication offered by BigQuery.
     * For more information, please see
     * https://cloud.google.com/bigquery/streaming-data-into-bigquery#disabling_best_effort_de-duplication.
     */
    public Write<T> ignoreInsertIds() {
      return toBuilder().setIgnoreInsertIds(true).build();
    }

    public Write<T> withKmsKey(String kmsKey) {
      return toBuilder().setKmsKey(kmsKey).build();
    }

    /**
     * If true, enables new codepaths that are expected to use less resources while writing to
     * BigQuery. Not enabled by default in order to maintain backwards compatibility.
     */
    public Write<T> optimizedWrites() {
      return toBuilder().setOptimizeWrites(true).build();
    }

    /**
     * If true, then the BigQuery schema will be inferred from the input schema. If no
     * formatFunction is set, then BigQueryIO will automatically turn the input records into
     * TableRows that match the schema.
     */
    @Experimental(Kind.SCHEMAS)
    public Write<T> useBeamSchema() {
      return toBuilder().setUseBeamSchema(true).build();
    }

    @VisibleForTesting
    /** This method is for test usage only */
    public Write<T> withTestServices(BigQueryServices testServices) {
      checkArgument(testServices != null, "testServices can not be null");
      return toBuilder().setBigQueryServices(testServices).build();
    }

    /**
     * Control how many files will be written concurrently by a single worker when using BigQuery
     * load jobs before spilling to a shuffle. When data comes into this transform, it is written to
     * one file per destination per worker. When there are more files than maxFilesPerBundle
     * (DEFAULT: 20), the data is shuffled (i.e. Grouped By Destination), and written to files
     * one-by-one-per-worker. This flag sets the maximum number of files that a single worker can
     * write concurrently before shuffling the data. This flag should be used with caution. Setting
     * a high number can increase the memory pressure on workers, and setting a low number can make
     * a pipeline slower (due to the need to shuffle data).
     */
    public Write<T> withMaxFilesPerBundle(int maxFilesPerBundle) {
      checkArgument(
          maxFilesPerBundle > 0, "maxFilesPerBundle must be > 0, but was: %s", maxFilesPerBundle);
      return toBuilder().setMaxFilesPerBundle(maxFilesPerBundle).build();
    }

    @VisibleForTesting
    Write<T> withMaxFileSize(long maxFileSize) {
      checkArgument(maxFileSize > 0, "maxFileSize must be > 0, but was: %s", maxFileSize);
      return toBuilder().setMaxFileSize(maxFileSize).build();
    }

    @VisibleForTesting
    Write<T> withMaxFilesPerPartition(int maxFilesPerPartition) {
      checkArgument(
          maxFilesPerPartition > 0,
          "maxFilesPerPartition must be > 0, but was: %s",
          maxFilesPerPartition);
      return toBuilder().setMaxFilesPerPartition(maxFilesPerPartition).build();
    }

    /**
     * Control how much data will be assigned to a single BigQuery load job. If the amount of data
     * flowing into one {@code BatchLoads} partition exceeds this value, that partition will be
     * handled via multiple load jobs.
     *
     * <p>The default value (11 TiB) respects BigQuery's maximum size per load job limit and is
     * appropriate for most use cases. Reducing the value of this parameter can improve stability
     * when loading to tables with complex schemas containing thousands of fields.
     *
     * @see <a href="https://cloud.google.com/bigquery/quotas#load_jobs">BigQuery Load Job
     *     Limits</a>
     */
    public Write<T> withMaxBytesPerPartition(long maxBytesPerPartition) {
      checkArgument(
          maxBytesPerPartition > 0,
          "maxBytesPerPartition must be > 0, but was: %s",
          maxBytesPerPartition);
      return toBuilder().setMaxBytesPerPartition(maxBytesPerPartition).build();
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

    private Method resolveMethod(PCollection<T> input) {
      if (getMethod() != Method.DEFAULT) {
        return getMethod();
      }
      // By default, when writing an Unbounded PCollection, we use StreamingInserts and
      // BigQuery's streaming import API.
      return (input.isBounded() == IsBounded.UNBOUNDED)
          ? Method.STREAMING_INSERTS
          : Method.FILE_LOADS;
    }

    @Override
    public WriteResult expand(PCollection<T> input) {
      // We must have a destination to write to!
      checkArgument(
          getTableFunction() != null
              || getJsonTableRef() != null
              || getDynamicDestinations() != null,
          "must set the table reference of a BigQueryIO.Write transform");

      List<?> allToArgs =
          Lists.newArrayList(getJsonTableRef(), getTableFunction(), getDynamicDestinations());
      checkArgument(
          1
              == Iterables.size(
                  allToArgs.stream()
                      .filter(Predicates.notNull()::apply)
                      .collect(Collectors.toList())),
          "Exactly one of jsonTableRef, tableFunction, or " + "dynamicDestinations must be set");

      List<?> allSchemaArgs =
          Lists.newArrayList(getJsonSchema(), getSchemaFromView(), getDynamicDestinations());
      checkArgument(
          2
              > Iterables.size(
                  allSchemaArgs.stream()
                      .filter(Predicates.notNull()::apply)
                      .collect(Collectors.toList())),
          "No more than one of jsonSchema, schemaFromView, or dynamicDestinations may " + "be set");

      Method method = resolveMethod(input);
      if (input.isBounded() == IsBounded.UNBOUNDED && method == Method.FILE_LOADS) {
        checkArgument(
            getTriggeringFrequency() != null,
            "When writing an unbounded PCollection via FILE_LOADS, "
                + "triggering frequency must be specified");
      } else {
        checkArgument(
            getTriggeringFrequency() == null && getNumFileShards() == 0,
            "Triggering frequency or number of file shards can be specified only when writing "
                + "an unbounded PCollection via FILE_LOADS, but: the collection was %s "
                + "and the method was %s",
            input.isBounded(),
            method);
      }

      if (method != Method.FILE_LOADS) {
        // we only support writing avro for FILE_LOADS
        checkArgument(
            getAvroRowWriterFactory() == null,
            "Writing avro formatted data is only supported for FILE_LOADS, however "
                + "the method was %s",
            method);
      }

      if (getJsonTimePartitioning() != null) {
        checkArgument(
            getDynamicDestinations() == null,
            "The supplied DynamicDestinations object can directly set TimePartitioning."
                + " There is no need to call BigQueryIO.Write.withTimePartitioning.");
        checkArgument(
            getTableFunction() == null,
            "The supplied getTableFunction object can directly set TimePartitioning."
                + " There is no need to call BigQueryIO.Write.withTimePartitioning.");
      }
      if (getClustering() != null && getClustering().getFields() != null) {
        checkArgument(
            getJsonTimePartitioning() != null,
            "Clustering fields can only be set when TimePartitioning is set.");
      }

      DynamicDestinations<T, ?> dynamicDestinations = getDynamicDestinations();
      if (dynamicDestinations == null) {
        if (getJsonTableRef() != null) {
          dynamicDestinations =
              DynamicDestinationsHelpers.ConstantTableDestinations.fromJsonTableRef(
                  getJsonTableRef(), getTableDescription());
        } else if (getTableFunction() != null) {
          dynamicDestinations =
              new TableFunctionDestinations<>(getTableFunction(), getClustering() != null);
        }

        // Wrap with a DynamicDestinations class that will provide a schema. There might be no
        // schema provided if the create disposition is CREATE_NEVER.
        if (getJsonSchema() != null) {
          dynamicDestinations =
              new ConstantSchemaDestinations<>(
                  (DynamicDestinations<T, TableDestination>) dynamicDestinations, getJsonSchema());
        } else if (getSchemaFromView() != null) {
          dynamicDestinations =
              new SchemaFromViewDestinations<>(
                  (DynamicDestinations<T, TableDestination>) dynamicDestinations,
                  getSchemaFromView());
        }

        // Wrap with a DynamicDestinations class that will provide the proper TimePartitioning.
        if (getJsonTimePartitioning() != null) {
          dynamicDestinations =
              new ConstantTimePartitioningDestinations<>(
                  (DynamicDestinations<T, TableDestination>) dynamicDestinations,
                  getJsonTimePartitioning(),
                  StaticValueProvider.of(BigQueryHelpers.toJsonString(getClustering())));
        }
      }
      return expandTyped(input, dynamicDestinations);
    }

    private <DestinationT> WriteResult expandTyped(
        PCollection<T> input, DynamicDestinations<T, DestinationT> dynamicDestinations) {
      boolean optimizeWrites = getOptimizeWrites();
      SerializableFunction<T, TableRow> formatFunction = getFormatFunction();
      RowWriterFactory.AvroRowWriterFactory<T, ?, DestinationT> avroRowWriterFactory =
          (RowWriterFactory.AvroRowWriterFactory<T, ?, DestinationT>) getAvroRowWriterFactory();

      boolean hasSchema =
          getJsonSchema() != null
              || getDynamicDestinations() != null
              || getSchemaFromView() != null;

      if (getUseBeamSchema()) {
        checkArgument(input.hasSchema());
        optimizeWrites = true;

        checkArgument(
            avroRowWriterFactory == null,
            "avro avroFormatFunction is unsupported when using Beam schemas.");

        if (formatFunction == null) {
          // If no format function set, then we will automatically convert the input type to a
          // TableRow.
          // TODO: it would be trivial to convert to avro records here instead.
          formatFunction = BigQueryUtils.toTableRow(input.getToRowFunction());
        }
        // Infer the TableSchema from the input Beam schema.
        TableSchema tableSchema = BigQueryUtils.toTableSchema(input.getSchema());
        dynamicDestinations =
            new ConstantSchemaDestinations<>(
                dynamicDestinations,
                StaticValueProvider.of(BigQueryHelpers.toJsonString(tableSchema)));
      } else {
        // Require a schema if creating one or more tables.
        checkArgument(
            getCreateDisposition() != CreateDisposition.CREATE_IF_NEEDED || hasSchema,
            "CreateDisposition is CREATE_IF_NEEDED, however no schema was provided.");
      }

      Coder<DestinationT> destinationCoder = null;
      try {
        destinationCoder =
            dynamicDestinations.getDestinationCoderWithDefault(
                input.getPipeline().getCoderRegistry());
      } catch (CannotProvideCoderException e) {
        throw new RuntimeException(e);
      }

      Method method = resolveMethod(input);
      if (optimizeWrites) {
        RowWriterFactory<T, DestinationT> rowWriterFactory;
        if (avroRowWriterFactory != null) {
          checkArgument(
              formatFunction == null,
              "Only one of withFormatFunction or withAvroFormatFunction/withAvroWriter maybe set, not both.");

          SerializableFunction<TableSchema, org.apache.avro.Schema> avroSchemaFactory =
              getAvroSchemaFactory();
          if (avroSchemaFactory == null) {
            checkArgument(
                hasSchema,
                "A schema must be provided if an avroFormatFunction "
                    + "is set but no avroSchemaFactory is defined.");
            avroSchemaFactory = DEFAULT_AVRO_SCHEMA_FACTORY;
          }
          rowWriterFactory = avroRowWriterFactory.prepare(dynamicDestinations, avroSchemaFactory);
        } else if (formatFunction != null) {
          rowWriterFactory = RowWriterFactory.tableRows(formatFunction);
        } else {
          throw new IllegalArgumentException(
              "A function must be provided to convert the input type into a TableRow or "
                  + "GenericRecord. Use BigQueryIO.Write.withFormatFunction or "
                  + "BigQueryIO.Write.withAvroFormatFunction to provide a formatting function. "
                  + "A format function is not required if Beam schemas are used.");
        }

        PCollection<KV<DestinationT, T>> rowsWithDestination =
            input
                .apply(
                    "PrepareWrite",
                    new PrepareWrite<>(dynamicDestinations, SerializableFunctions.identity()))
                .setCoder(KvCoder.of(destinationCoder, input.getCoder()));
        return continueExpandTyped(
            rowsWithDestination,
            input.getCoder(),
            destinationCoder,
            dynamicDestinations,
            rowWriterFactory,
            method);
      } else {
        checkArgument(avroRowWriterFactory == null);
        checkArgument(
            formatFunction != null,
            "A function must be provided to convert the input type into a TableRow or "
                + "GenericRecord. Use BigQueryIO.Write.withFormatFunction or "
                + "BigQueryIO.Write.withAvroFormatFunction to provide a formatting function. "
                + "A format function is not required if Beam schemas are used.");

        PCollection<KV<DestinationT, TableRow>> rowsWithDestination =
            input
                .apply("PrepareWrite", new PrepareWrite<>(dynamicDestinations, formatFunction))
                .setCoder(KvCoder.of(destinationCoder, TableRowJsonCoder.of()));

        RowWriterFactory<TableRow, DestinationT> rowWriterFactory =
            RowWriterFactory.tableRows(SerializableFunctions.identity());

        return continueExpandTyped(
            rowsWithDestination,
            TableRowJsonCoder.of(),
            destinationCoder,
            dynamicDestinations,
            rowWriterFactory,
            method);
      }
    }

    private <DestinationT, ElementT> WriteResult continueExpandTyped(
        PCollection<KV<DestinationT, ElementT>> input,
        Coder<ElementT> elementCoder,
        Coder<DestinationT> destinationCoder,
        DynamicDestinations<T, DestinationT> dynamicDestinations,
        RowWriterFactory<ElementT, DestinationT> rowWriterFactory,
        Method method) {
      if (method == Method.STREAMING_INSERTS) {
        checkArgument(
            getWriteDisposition() != WriteDisposition.WRITE_TRUNCATE,
            "WriteDisposition.WRITE_TRUNCATE is not supported for an unbounded" + " PCollection.");
        InsertRetryPolicy retryPolicy =
            MoreObjects.firstNonNull(getFailedInsertRetryPolicy(), InsertRetryPolicy.alwaysRetry());

        checkArgument(
            rowWriterFactory.getOutputType() == RowWriterFactory.OutputType.JsonTableRow,
            "Avro output is not supported when method == STREAMING_INSERTS");

        RowWriterFactory.TableRowWriterFactory<ElementT, DestinationT> tableRowWriterFactory =
            (RowWriterFactory.TableRowWriterFactory<ElementT, DestinationT>) rowWriterFactory;

        StreamingInserts<DestinationT, ElementT> streamingInserts =
            new StreamingInserts<>(
                    getCreateDisposition(),
                    dynamicDestinations,
                    elementCoder,
                    tableRowWriterFactory.getToRowFn())
                .withInsertRetryPolicy(retryPolicy)
                .withTestServices(getBigQueryServices())
                .withExtendedErrorInfo(getExtendedErrorInfo())
                .withSkipInvalidRows(getSkipInvalidRows())
                .withIgnoreUnknownValues(getIgnoreUnknownValues())
                .withIgnoreInsertIds(getIgnoreInsertIds())
                .withKmsKey(getKmsKey());
        return input.apply(streamingInserts);
      } else {
        checkArgument(
            getFailedInsertRetryPolicy() == null,
            "Record-insert retry policies are not supported when using BigQuery load jobs.");

        if (getUseAvroLogicalTypes()) {
          checkArgument(
              rowWriterFactory.getOutputType() == OutputType.AvroGenericRecord,
              "useAvroLogicalTypes can only be set with Avro output.");
        }

        BatchLoads<DestinationT, ElementT> batchLoads =
            new BatchLoads<>(
                getWriteDisposition(),
                getCreateDisposition(),
                getJsonTableRef() != null,
                dynamicDestinations,
                destinationCoder,
                getCustomGcsTempLocation(),
                getLoadJobProjectId(),
                getIgnoreUnknownValues(),
                elementCoder,
                rowWriterFactory,
                getKmsKey(),
                getClustering() != null,
                getUseAvroLogicalTypes());
        batchLoads.setTestServices(getBigQueryServices());
        if (getSchemaUpdateOptions() != null) {
          batchLoads.setSchemaUpdateOptions(getSchemaUpdateOptions());
        }
        if (getMaxFilesPerBundle() != null) {
          batchLoads.setMaxNumWritersPerBundle(getMaxFilesPerBundle());
        }
        if (getMaxFileSize() != null) {
          batchLoads.setMaxFileSize(getMaxFileSize());
        }
        batchLoads.setMaxFilesPerPartition(getMaxFilesPerPartition());
        batchLoads.setMaxBytesPerPartition(getMaxBytesPerPartition());

        // When running in streaming (unbounded mode) we want to retry failed load jobs
        // indefinitely. Failing the bundle is expensive, so we set a fairly high limit on retries.
        if (IsBounded.UNBOUNDED.equals(input.isBounded())) {
          batchLoads.setMaxRetryJobs(1000);
        }
        batchLoads.setTriggeringFrequency(getTriggeringFrequency());
        batchLoads.setNumFileShards(getNumFileShards());
        return input.apply(batchLoads);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.addIfNotNull(
          DisplayData.item("table", getJsonTableRef()).withLabel("Table Reference"));
      if (getJsonSchema() != null) {
        builder.addIfNotNull(DisplayData.item("schema", getJsonSchema()).withLabel("Table Schema"));
      } else {
        builder.add(DisplayData.item("schema", "Custom Schema Function").withLabel("Table Schema"));
      }

      if (getTableFunction() != null) {
        builder.add(
            DisplayData.item("tableFn", getTableFunction().getClass())
                .withLabel("Table Reference Function"));
      }

      builder
          .add(
              DisplayData.item("createDisposition", getCreateDisposition().toString())
                  .withLabel("Table CreateDisposition"))
          .add(
              DisplayData.item("writeDisposition", getWriteDisposition().toString())
                  .withLabel("Table WriteDisposition"))
          .add(
              DisplayData.item("schemaUpdateOptions", getSchemaUpdateOptions().toString())
                  .withLabel("Table SchemaUpdateOptions"))
          .addIfNotDefault(
              DisplayData.item("validation", getValidate()).withLabel("Validation Enabled"), true)
          .addIfNotNull(
              DisplayData.item("tableDescription", getTableDescription())
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
        LOG.info(
            "Using a dynamic value for table input. This must contain a project"
                + " in the table reference: {}",
            table);
        return table;
      }
      if (Strings.isNullOrEmpty(table.get().getProjectId())) {
        // If user does not specify a project we assume the table to be located in
        // the default project.
        TableReference tableRef = table.get();
        tableRef.setProjectId(bqOptions.getProject());
        return NestedValueProvider.of(
            StaticValueProvider.of(BigQueryHelpers.toJsonString(tableRef)),
            new JsonTableRefToTableRef());
      }
      return table;
    }

    /** Returns the table reference, or {@code null}. */
    public @Nullable ValueProvider<TableReference> getTable() {
      return getJsonTableRef() == null
          ? null
          : NestedValueProvider.of(getJsonTableRef(), new JsonTableRefToTableRef());
    }
  }

  /** Clear the cached map of created tables. Used for testing. */
  @VisibleForTesting
  static void clearCreatedTables() {
    CreateTables.clearCreatedTables();
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Disallow construction of utility class. */
  private BigQueryIO() {}
}
