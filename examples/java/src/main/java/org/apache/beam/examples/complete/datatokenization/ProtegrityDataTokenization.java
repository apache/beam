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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.examples.complete.datatokenization.options.ProtegrityDataTokenizationOptions;
import org.apache.beam.examples.complete.datatokenization.transforms.ProtegrityDataProtectors.RowToTokenizedRow;
import org.apache.beam.examples.complete.datatokenization.transforms.io.BigQueryIO;
import org.apache.beam.examples.complete.datatokenization.transforms.io.BigTableIO;
import org.apache.beam.examples.complete.datatokenization.transforms.io.GcsIO;
import org.apache.beam.examples.complete.datatokenization.utils.SchemasUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ProtegrityDataTokenization} pipeline.
 */
public class ProtegrityDataTokenization {

  /**
   * Logger for class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(ProtegrityDataTokenization.class);

  /**
   * The default suffix for error tables if dead letter table is not specified.
   */
  private static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

  /**
   * The tag for the main output for the UDF.
   */
  private static final TupleTag<Row> TOKENIZATION_OUT = new TupleTag<Row>() {
  };

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    ProtegrityDataTokenizationOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(ProtegrityDataTokenizationOptions.class);
    FileSystems.setDefaultPipelineOptions(options);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(
      ProtegrityDataTokenizationOptions options) {
    SchemasUtils schema = null;
    try {
      schema = new SchemasUtils(options.getDataSchemaGcsPath(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.error("Failed to retrieve schema for data.", e);
    }
    checkArgument(schema != null, "Data schema is mandatory.");

    Map<String, String> dataElements =
        schema.getDataElementsToTokenize(options.getPayloadConfigGcsPath());

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    // Register the coder for pipeline
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        RowCoder.of(schema.getBeamSchema()).getEncodedTypeDescriptor(),
        RowCoder.of(schema.getBeamSchema()));

    PCollection<String> jsons;
    if (options.getInputGcsFilePattern() != null) {
      jsons = new GcsIO(options).read(pipeline, schema.getJsonBeamSchema());
    } else if (options.getPubsubTopic() != null) {
      jsons =
          pipeline.apply(
              "ReadMessagesFromPubsub", PubsubIO.readStrings().fromTopic(options.getPubsubTopic()));
    } else {
      throw new IllegalStateException("No source is provided, please configure GCS or Pub/Sub");
    }

    JsonToRow.ParseResult rows =
        jsons.apply(
            "JsonToRow",
            JsonToRow.withExceptionReporting(schema.getBeamSchema()).withExtendedErrorInfo());

//    if (options.getNonTokenizedDeadLetterGcsPath() != null) {
//      /*
//       * Write Row conversion errors to filesystem specified path
//       */
//      rows.getFailedToParseLines()
//          .apply(
//              "ToFailsafeElement",
//              MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
//                  .via(
//                      (Row errRow) ->
//                          FailsafeElement.of(errRow.getString("line"), errRow.getString("line"))
//                              .setErrorMessage(errRow.getString("err"))))
//          .apply(
//              "WriteCsvConversionErrorsToGcs",
//              ErrorConverters.WriteStringMessageErrorsAsCsv.newBuilder()
//                  .setCsvDelimiter(options.getCsvDelimiter())
//                  .setErrorWritePath(options.getNonTokenizedDeadLetterGcsPath())
//                  .build());
//    }
    /*
    Tokenize data using remote API call
     */
    PCollectionTuple tokenizedRows =
        rows.getResults()
            .setRowSchema(schema.getBeamSchema())
            .apply(
                MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.rows()))
                    .via((Row row) -> KV.of(0, row)))
            .setCoder(KvCoder.of(VarIntCoder.of(), RowCoder.of(schema.getBeamSchema())))
            .apply(
                "DsgTokenization",
                RowToTokenizedRow.newBuilder()
                    .setBatchSize(options.getBatchSize())
                    .setDsgURI(options.getDsgUri())
                    .setSchema(schema.getBeamSchema())
                    .setDataElements(dataElements)
                    .setSuccessTag(TOKENIZATION_OUT)
                    .setFailureTag(TOKENIZATION_DEADLETTER_OUT)
                    .build());

//    String csvDelimiter = options.getCsvDelimiter();
//    if (options.getNonTokenizedDeadLetterGcsPath() != null) {
//      /*
//      Write tokenization errors to dead-letter sink
//       */
//      tokenizedRows
//          .get(TOKENIZATION_DEADLETTER_OUT)
//          .apply(
//              "ConvertToCSV",
//              MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
//                  .via(
//                      (FailsafeElement<Row, Row> fse) ->
//                          FailsafeElement.of(
//                              new RowToCsv(csvDelimiter).getCsvFromRow(fse.getOriginalPayload()),
//                              new RowToCsv(csvDelimiter).getCsvFromRow(fse.getPayload()))))
//          .apply(
//              "WriteTokenizationErrorsToGcs",
//              ErrorConverters.WriteStringMessageErrorsAsCsv.newBuilder()
//                  .setCsvDelimiter(options.getCsvDelimiter())
//                  .setErrorWritePath(options.getNonTokenizedDeadLetterGcsPath())
//                  .build());
//    }

    if (options.getOutputGcsDirectory() != null) {
      new GcsIO(options).write(tokenizedRows.get(TOKENIZATION_OUT), schema.getBeamSchema());
    } else if (options.getBigQueryTableName() != null) {
      WriteResult writeResult =
          BigQueryIO.write(
              tokenizedRows.get(TOKENIZATION_OUT),
              options.getBigQueryTableName(),
              schema.getBigQuerySchema());
//      writeResult
//          .getFailedInsertsWithErr()
//          .apply(
//              "WrapInsertionErrors",
//              MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
//                  .via(BigQueryIO::wrapBigQueryInsertError))
//          .setCoder(FAILSAFE_ELEMENT_CODER)
//          .apply(
//              "WriteInsertionFailedRecords",
//              ErrorConverters.WriteStringMessageErrors.newBuilder()
//                  .setErrorRecordsTable(
//                      options.getBigQueryTableName() + DEFAULT_DEADLETTER_TABLE_SUFFIX)
//                  .setErrorRecordsTableSchema(SchemaUtils.DEADLETTER_SCHEMA)
//                  .build());
    } else if (options.getBigTableInstanceId() != null) {
      new BigTableIO(options).write(tokenizedRows.get(TOKENIZATION_OUT), schema.getBeamSchema());
    } else {
      throw new IllegalStateException(
          "No sink is provided, please configure BigQuery or BigTable.");
    }

    return pipeline.run();
  }
}
