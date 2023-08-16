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
package org.apache.beam.examples.complete.datatokenization;

import static org.apache.beam.examples.complete.datatokenization.utils.DurationUtils.parseDuration;
import static org.apache.beam.examples.complete.datatokenization.utils.SchemasUtils.DEADLETTER_SCHEMA;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.examples.complete.datatokenization.options.DataTokenizationOptions;
import org.apache.beam.examples.complete.datatokenization.transforms.DataProtectors.RowToTokenizedRow;
import org.apache.beam.examples.complete.datatokenization.transforms.JsonToBeamRow;
import org.apache.beam.examples.complete.datatokenization.transforms.SerializableFunctions;
import org.apache.beam.examples.complete.datatokenization.transforms.io.TokenizationBigQueryIO;
import org.apache.beam.examples.complete.datatokenization.transforms.io.TokenizationBigTableIO;
import org.apache.beam.examples.complete.datatokenization.transforms.io.TokenizationFileSystemIO;
import org.apache.beam.examples.complete.datatokenization.utils.ErrorConverters;
import org.apache.beam.examples.complete.datatokenization.utils.FailsafeElement;
import org.apache.beam.examples.complete.datatokenization.utils.FailsafeElementCoder;
import org.apache.beam.examples.complete.datatokenization.utils.RowToCsv;
import org.apache.beam.examples.complete.datatokenization.utils.SchemasUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DataTokenization} pipeline reads data from one of the supported sources, tokenizes
 * data with external API calls to some tokenization server, and writes data into one of the
 * supported sinks. <br>
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>Java 8
 *   <li>Data schema (JSON with an array of fields described in BigQuery format)
 *   <li>1 of supported sources to read data from
 *       <ul>
 *         <li>File system (Only JSON or CSV)
 *         <li><a href=https://cloud.google.com/pubsub>Google Pub/Sub</a>
 *       </ul>
 *   <li>1 of supported destination sinks to write data into
 *       <ul>
 *         <li>File system (Only JSON or CSV)
 *         <li><a href=https://cloud.google.com/bigquery>Google Cloud BigQuery</a>
 *         <li><a href=https://cloud.google.com/bigtable>Cloud BigTable</a>
 *       </ul>
 *   <li>A configured tokenization server
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * <b>Gradle Preparation</b>
 * To run this example your  build.gradle file should contain the following task
 * to execute the pipeline:
 *   {@code
 *   task execute (type:JavaExec) {
 *      mainClass = System.getProperty("mainClass")
 *      classpath = sourceSets.main.runtimeClasspath
 *      systemProperties System.getProperties()
 *      args System.getProperty("exec.args", "").split()
 *   }
 *   }
 * This task allows to run the pipeline via the following command:
 *   {@code
 *   gradle clean execute -DmainClass=org.apache.beam.examples.complete.datatokenization.DataTokenization \
 *        -Dexec.args="--<argument>=<value> --<argument>=<value>"
 *   }
 * <b>Running the pipeline</b>
 * To execute this pipeline, specify the parameters:
 *
 * - Data schema
 *     - <b><i>dataSchemaPath</i></b>: Path to data schema (JSON format) compatible with BigQuery.
 * - 1 specified input source out of these:
 *     - File System
 *         - <b><i>inputFilePattern</i></b>: Filepattern for files to read data from
 *         - <b><i>inputFileFormat</i></b>: File format of input files. Supported formats: JSON, CSV
 *         - In case if input data is in CSV format:
 *             - <b><i>csvContainsHeaders</i></b>: `true` if file(s) in bucket to read data from contain headers,
 *               and `false` otherwise
 *             - <b><i>csvDelimiter</i></b>: Delimiting character in CSV. Default: use delimiter provided in
 *               csvFormat
 *             - <b><i>csvFormat</i></b>: Csv format according to Apache Commons CSV format. Default is:
 *               [Apache Commons CSV default](https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.html#DEFAULT)
 *               . Must match format names exactly found
 *               at: https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.Predefined.html
 *     - Google Pub/Sub
 *         - <b><i>pubsubTopic</i></b>: The Cloud Pub/Sub topic to read from, in the format of '
 *           projects/yourproject/topics/yourtopic'
 * - 1 specified output sink out of these:
 *     - File System
 *         - <b><i>outputDirectory</i></b>: Directory to write data to
 *         - <b><i>outputFileFormat</i></b>: File format of output files. Supported formats: JSON, CSV
 *         - <b><i>windowDuration</i></b>: The window duration in which data will be written. Should be specified
 *           only for 'Pub/Sub -> FileSystem' case. Defaults to 30s.
 *
 *           Allowed formats are:
 *             - Ns (for seconds, example: 5s),
 *             - Nm (for minutes, example: 12m),
 *             - Nh (for hours, example: 2h).
 *     - Google Cloud BigQuery
 *         - <b><i>bigQueryTableName</i></b>: Cloud BigQuery table name to write into
 *         - <b><i>tempLocation</i></b>: Folder in a Google Cloud Storage bucket, which is needed for
 *           BigQuery to handle data writing
 *     - Cloud BigTable
 *         - <b><i>bigTableProjectId</i></b>: Id of the project where the Cloud BigTable instance to write into
 *           is located
 *         - <b><i>bigTableInstanceId</i></b>: Id of the Cloud BigTable instance to write into
 *         - <b><i>bigTableTableId</i></b>: Id of the Cloud BigTable table to write into
 *         - <b><i>bigTableKeyColumnName</i></b>: Column name to use as a key in Cloud BigTable
 *         - <b><i>bigTableColumnFamilyName</i></b>: Column family name to use in Cloud BigTable
 * - RPC server parameters
 *     - <b><i>rpcUri</i></b>: URI for the API calls to RPC server
 *     - <b><i>batchSize</i></b>: Size of the batch to send to RPC server per request
 *
 * The template allows for the user to supply the following optional parameter:
 *
 * - <b><i>nonTokenizedDeadLetterPath</i></b>: Folder where failed to tokenize data will be stored
 *
 *
 * Specify the parameters in the following format:
 *
 * {@code
 * --dataSchemaPath="path-to-data-schema-in-json-format"
 * --inputFilePattern="path-pattern-to-input-data"
 * --outputDirectory="path-to-output-directory"
 * # example for CSV case
 * --inputFileFormat="CSV"
 * --outputFileFormat="CSV"
 * --csvContainsHeaders="true"
 * --nonTokenizedDeadLetterPath="path-to-errors-rows-writing"
 * --batchSize=batch-size-number
 * --rpcUri=http://host:port/tokenize
 * }
 *
 * By default, this will run the pipeline locally with the DirectRunner. To change the runner, specify:
 *
 * {@code
 * --runner=YOUR_SELECTED_RUNNER
 * }
 * </pre>
 */
public class DataTokenization {

  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(DataTokenization.class);

  /** String/String Coder for FailsafeElement. */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

  /** The default suffix for error tables if dead letter table is not specified. */
  private static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

  /** The tag for the main output for the UDF. */
  private static final TupleTag<Row> TOKENIZATION_OUT = new TupleTag<Row>() {};

  /** The tag for the dead-letter output of the udf. */
  static final TupleTag<FailsafeElement<Row, Row>> TOKENIZATION_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<Row, Row>>() {};

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    DataTokenizationOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DataTokenizationOptions.class);
    FileSystems.setDefaultPipelineOptions(options);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  @SuppressWarnings({"dereference.of.nullable", "argument"})
  public static PipelineResult run(DataTokenizationOptions options) {
    SchemasUtils schema = null;
    try {
      schema = new SchemasUtils(options.getDataSchemaPath(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.error("Failed to retrieve schema for data.", e);
    }
    checkArgument(schema != null, "Data schema is mandatory.");

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    // Register the coder for pipeline
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);
    coderRegistry.registerCoderForType(
        RowCoder.of(schema.getBeamSchema()).getEncodedTypeDescriptor(),
        RowCoder.of(schema.getBeamSchema()));

    /*
     * Row/Row Coder for FailsafeElement.
     */
    FailsafeElementCoder<Row, Row> coder =
        FailsafeElementCoder.of(
            RowCoder.of(schema.getBeamSchema()), RowCoder.of(schema.getBeamSchema()));

    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    PCollection<Row> rows;
    if (options.getInputFilePattern() != null) {
      rows = new TokenizationFileSystemIO(options).read(pipeline, schema);
    } else if (options.getPubsubTopic() != null) {
      rows =
          pipeline
              .apply(
                  "ReadMessagesFromPubsub",
                  PubsubIO.readStrings().fromTopic(options.getPubsubTopic()))
              .apply(
                  "TransformToBeamRow",
                  new JsonToBeamRow(options.getNonTokenizedDeadLetterPath(), schema));
      if (options.getOutputDirectory() != null) {
        rows = rows.apply(Window.into(FixedWindows.of(parseDuration(options.getWindowDuration()))));
      }
    } else {
      throw new IllegalStateException(
          "No source is provided, please configure File System or Pub/Sub");
    }

    /*
    Tokenize data using remote API call
     */
    PCollectionTuple tokenizedRows =
        rows.setRowSchema(schema.getBeamSchema())
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.rows()))
                    .via((Row row) -> KV.of(0, row)))
            .setCoder(KvCoder.of(VarIntCoder.of(), RowCoder.of(schema.getBeamSchema())))
            .apply(
                "DsgTokenization",
                RowToTokenizedRow.newBuilder()
                    .setBatchSize(options.getBatchSize())
                    .setRpcURI(options.getRpcUri())
                    .setSchema(schema.getBeamSchema())
                    .setSuccessTag(TOKENIZATION_OUT)
                    .setFailureTag(TOKENIZATION_DEADLETTER_OUT)
                    .build());

    String csvDelimiter = options.getCsvDelimiter();
    if (options.getNonTokenizedDeadLetterPath() != null) {
      /*
      Write tokenization errors to dead-letter sink
       */
      tokenizedRows
          .get(TOKENIZATION_DEADLETTER_OUT)
          .apply(
              "ConvertToCSV",
              MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                  .via(
                      (FailsafeElement<Row, Row> fse) ->
                          FailsafeElement.of(
                              new RowToCsv(csvDelimiter).getCsvFromRow(fse.getOriginalPayload()),
                              new RowToCsv(csvDelimiter).getCsvFromRow(fse.getPayload()))))
          .apply(
              "WriteTokenizationErrorsToFS",
              ErrorConverters.WriteErrorsToTextIO.<String, String>newBuilder()
                  .setErrorWritePath(options.getNonTokenizedDeadLetterPath())
                  .setTranslateFunction(SerializableFunctions.getCsvErrorConverter())
                  .build());
    }

    if (options.getOutputDirectory() != null) {
      new TokenizationFileSystemIO(options)
          .write(tokenizedRows.get(TOKENIZATION_OUT), schema.getBeamSchema());
    } else if (options.getBigQueryTableName() != null) {
      WriteResult writeResult =
          TokenizationBigQueryIO.write(
              tokenizedRows.get(TOKENIZATION_OUT),
              options.getBigQueryTableName(),
              schema.getBigQuerySchema());
      writeResult
          .getFailedInsertsWithErr()
          .apply(
              "WrapInsertionErrors",
              MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                  .via(TokenizationBigQueryIO::wrapBigQueryInsertError))
          .setCoder(FAILSAFE_ELEMENT_CODER)
          .apply(
              "WriteInsertionFailedRecords",
              ErrorConverters.WriteStringMessageErrors.newBuilder()
                  .setErrorRecordsTable(
                      options.getBigQueryTableName() + DEFAULT_DEADLETTER_TABLE_SUFFIX)
                  .setErrorRecordsTableSchema(DEADLETTER_SCHEMA)
                  .build());
    } else if (options.getBigTableInstanceId() != null) {
      new TokenizationBigTableIO(options)
          .write(tokenizedRows.get(TOKENIZATION_OUT), schema.getBeamSchema());
    } else {
      throw new IllegalStateException(
          "No sink is provided, please configure BigQuery or BigTable.");
    }

    return pipeline.run();
  }
}
