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
package org.apache.beam.examples.complete.datatokenization.transforms.io;

import org.apache.beam.examples.complete.datatokenization.options.DataTokenizationOptions;
import org.apache.beam.examples.complete.datatokenization.transforms.JsonToBeamRow;
import org.apache.beam.examples.complete.datatokenization.transforms.SerializableFunctions;
import org.apache.beam.examples.complete.datatokenization.utils.CsvConverters;
import org.apache.beam.examples.complete.datatokenization.utils.ErrorConverters;
import org.apache.beam.examples.complete.datatokenization.utils.FailsafeElement;
import org.apache.beam.examples.complete.datatokenization.utils.RowToCsv;
import org.apache.beam.examples.complete.datatokenization.utils.SchemasUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;

/** The {@link TokenizationFileSystemIO} class to read/write data from/into File Systems. */
public class TokenizationFileSystemIO {

  /** The tag for the headers of the CSV if required. */
  static final TupleTag<String> CSV_HEADERS = new TupleTag<String>() {};

  /** The tag for the lines of the CSV. */
  static final TupleTag<String> CSV_LINES = new TupleTag<String>() {};

  /** The tag for the dead-letter output. */
  static final TupleTag<FailsafeElement<String, String>> PROCESSING_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for the main output. */
  static final TupleTag<FailsafeElement<String, String>> PROCESSING_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** Supported format to read from GCS. */
  public enum FORMAT {
    JSON,
    CSV
  }

  /**
   * Necessary {@link PipelineOptions} options for Pipelines that operate with JSON/CSV data in FS.
   */
  public interface FileSystemPipelineOptions extends PipelineOptions {

    @Description("Filepattern for files to read data from")
    String getInputFilePattern();

    void setInputFilePattern(String inputFilePattern);

    @Description("File format of input files. Supported formats: JSON, CSV")
    @Default.Enum("JSON")
    TokenizationFileSystemIO.FORMAT getInputFileFormat();

    void setInputFileFormat(FORMAT inputFileFormat);

    @Description("Directory to write data to")
    String getOutputDirectory();

    void setOutputDirectory(String outputDirectory);

    @Description("File format of output files. Supported formats: JSON, CSV")
    @Default.Enum("JSON")
    TokenizationFileSystemIO.FORMAT getOutputFileFormat();

    void setOutputFileFormat(FORMAT outputFileFormat);

    @Description(
        "The window duration in which data will be written. "
            + "Should be specified only for 'Pub/Sub -> FS' case. Defaults to 30s. "
            + "Allowed formats are: "
            + "Ns (for seconds, example: 5s), "
            + "Nm (for minutes, example: 12m), "
            + "Nh (for hours, example: 2h).")
    @Default.String("30s")
    String getWindowDuration();

    void setWindowDuration(String windowDuration);

    // CSV parameters
    @Description("If file(s) contain headers")
    Boolean getCsvContainsHeaders();

    void setCsvContainsHeaders(Boolean csvContainsHeaders);

    @Description("Delimiting character in CSV. Default: use delimiter provided in csvFormat")
    @Default.InstanceFactory(CsvConverters.DelimiterFactory.class)
    String getCsvDelimiter();

    void setCsvDelimiter(String csvDelimiter);

    @Description(
        "Csv format according to Apache Commons CSV format. Default is: Apache Commons CSV"
            + " default\n"
            + "https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.html#DEFAULT\n"
            + "Must match format names exactly found at: "
            + "https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.Predefined.html")
    @Default.String("Default")
    String getCsvFormat();

    void setCsvFormat(String csvFormat);
  }

  private final DataTokenizationOptions options;

  public TokenizationFileSystemIO(DataTokenizationOptions options) {
    this.options = options;
  }

  public PCollection<Row> read(Pipeline pipeline, SchemasUtils schema) {
    switch (options.getInputFileFormat()) {
      case JSON:
        return readJson(pipeline)
            .apply(new JsonToBeamRow(options.getNonTokenizedDeadLetterPath(), schema));
      case CSV:
        return readCsv(pipeline, schema)
            .apply(new JsonToBeamRow(options.getNonTokenizedDeadLetterPath(), schema));
      default:
        throw new IllegalStateException(
            "No valid format for input data is provided. Please, choose JSON or CSV.");
    }
  }

  private PCollection<String> readJson(Pipeline pipeline) {
    return pipeline.apply("ReadJsonFromFiles", TextIO.read().from(options.getInputFilePattern()));
  }

  private PCollection<String> readCsv(Pipeline pipeline, SchemasUtils schema) {
    /*
     * Step 1: Read CSV file(s) from File System using {@link CsvConverters.ReadCsv}.
     */
    PCollectionTuple csvLines = readCsv(pipeline);
    /*
     * Step 2: Convert lines to Json.
     */
    PCollectionTuple jsons = csvLineToJson(csvLines, schema.getJsonBeamSchema());

    if (options.getNonTokenizedDeadLetterPath() != null) {
      /*
       * Step 3: Write jsons to dead-letter that weren't successfully processed.
       */
      jsons
          .get(PROCESSING_DEADLETTER_OUT)
          .apply(
              "WriteCsvConversionErrorsToFS",
              ErrorConverters.WriteErrorsToTextIO.<String, String>newBuilder()
                  .setErrorWritePath(options.getNonTokenizedDeadLetterPath())
                  .setTranslateFunction(SerializableFunctions.getCsvErrorConverter())
                  .build());
    }

    /*
     * Step 4: Get jsons that were successfully processed.
     */
    return jsons
        .get(PROCESSING_OUT)
        .apply(
            "GetJson",
            MapElements.into(TypeDescriptors.strings()).via(FailsafeElement::getPayload));
  }

  private PCollectionTuple readCsv(Pipeline pipeline) {
    return pipeline.apply(
        "ReadCsvFromFiles",
        CsvConverters.ReadCsv.newBuilder()
            .setCsvFormat(options.getCsvFormat())
            .setDelimiter(options.getCsvDelimiter())
            .setHasHeaders(options.getCsvContainsHeaders())
            .setInputFileSpec(options.getInputFilePattern())
            .setHeaderTag(CSV_HEADERS)
            .setLineTag(CSV_LINES)
            .build());
  }

  private PCollectionTuple csvLineToJson(PCollectionTuple csvLines, String jsonSchema) {
    return csvLines.apply(
        "LineToJson",
        CsvConverters.LineToFailsafeJson.newBuilder()
            .setDelimiter(options.getCsvDelimiter())
            .setJsonSchema(jsonSchema)
            .setHeaderTag(CSV_HEADERS)
            .setLineTag(CSV_LINES)
            .setUdfOutputTag(PROCESSING_OUT)
            .setUdfDeadletterTag(PROCESSING_DEADLETTER_OUT)
            .build());
  }

  public PDone write(PCollection<Row> input, Schema schema) {
    switch (options.getOutputFileFormat()) {
      case JSON:
        return writeJson(input);
      case CSV:
        return writeCsv(input, schema);
      default:
        throw new IllegalStateException(
            "No valid format for output data is provided. Please, choose JSON or CSV.");
    }
  }

  private PDone writeJson(PCollection<Row> input) {
    PCollection<String> jsons = input.apply("RowsToJSON", ToJson.of());

    if (jsons.isBounded() == IsBounded.BOUNDED) {
      return jsons.apply("WriteToFS", TextIO.write().to(options.getOutputDirectory()));
    } else {
      return jsons.apply(
          "WriteToFS",
          TextIO.write().withWindowedWrites().withNumShards(1).to(options.getOutputDirectory()));
    }
  }

  private PDone writeCsv(PCollection<Row> input, Schema schema) {
    String header = String.join(options.getCsvDelimiter(), schema.getFieldNames());
    String csvDelimiter = options.getCsvDelimiter();

    PCollection<String> csvs =
        input.apply(
            "ConvertToCSV",
            MapElements.into(TypeDescriptors.strings())
                .via((Row inputRow) -> new RowToCsv(csvDelimiter).getCsvFromRow(inputRow)));

    if (csvs.isBounded() == IsBounded.BOUNDED) {
      return csvs.apply(
          "WriteToFS", TextIO.write().to(options.getOutputDirectory()).withHeader(header));
    } else {
      return csvs.apply(
          "WriteToFS",
          TextIO.write()
              .withWindowedWrites()
              .withNumShards(1)
              .to(options.getOutputDirectory())
              .withHeader(header));
    }
  }
}
