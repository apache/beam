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
package org.apache.beam.examples.complete.datatokenization.utils;

import static org.apache.beam.examples.complete.datatokenization.utils.SchemasUtils.getGcsFileAsString;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common transforms for Csv files. */
@SuppressWarnings({"argument"})
public class CsvConverters {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(CsvConverters.class);

  private static final String SUCCESSFUL_TO_JSON_COUNTER = "SuccessfulToJsonCounter";

  private static final String FAILED_TO_JSON_COUNTER = "FailedToJsonCounter";

  private static JsonParser jsonParser = new JsonParser();

  /**
   * Builds Json string from list of values and headers or values and schema if schema is provided.
   *
   * @param headers optional list of strings which is the header of the Csv file.
   * @param values list of strings which are combined with header or json schema to create Json
   *     string.
   * @param jsonSchemaString
   * @return Json string containing object.
   * @throws IOException thrown if Json object is not able to be written.
   * @throws NumberFormatException thrown if value cannot be parsed into type successfully.
   */
  static String buildJsonString(
      @Nullable List<String> headers, List<String> values, @Nullable String jsonSchemaString)
      throws Exception {

    StringWriter stringWriter = new StringWriter();
    JsonWriter writer = new JsonWriter(stringWriter);

    if (jsonSchemaString != null) {
      JsonArray jsonSchema = jsonParser.parse(jsonSchemaString).getAsJsonArray();
      writer.beginObject();

      for (int i = 0; i < jsonSchema.size(); i++) {
        JsonObject jsonObject = jsonSchema.get(i).getAsJsonObject();
        String type = jsonObject.get("type").getAsString().toUpperCase();
        writer.name(jsonObject.get("name").getAsString());

        switch (type) {
          case "LONG":
            writer.value(Long.parseLong(values.get(i)));
            break;

          case "DOUBLE":
            writer.value(Double.parseDouble(values.get(i)));
            break;

          case "INTEGER":
            writer.value(Integer.parseInt(values.get(i)));
            break;

          case "SHORT":
            writer.value(Short.parseShort(values.get(i)));
            break;

          case "BYTE":
            writer.value(Byte.parseByte(values.get(i)));
            break;

          case "FLOAT":
            writer.value(Float.parseFloat(values.get(i)));
            break;

          case "TEXT":
          case "KEYWORD":
          case "STRING":
            writer.value(values.get(i));
            break;

          default:
            LOG.error("Invalid data type, got: " + type);
            throw new RuntimeException("Invalid data type, got: " + type);
        }
      }
      writer.endObject();
      writer.close();
      return stringWriter.toString();

    } else if (headers != null) {

      writer.beginObject();

      for (int i = 0; i < headers.size(); i++) {
        writer.name(headers.get(i));
        writer.value(values.get(i));
      }

      writer.endObject();
      writer.close();
      return stringWriter.toString();

    } else {
      LOG.error("No headers or schema specified");
      throw new RuntimeException("No headers or schema specified");
    }
  }

  /**
   * Gets Csv format accoring to <a
   * href="https://javadoc.io/doc/org.apache.commons/commons-csv">Apache Commons CSV</a>. If user
   * passed invalid format error is thrown.
   */
  public static CSVFormat getCsvFormat(String formatString, @Nullable String delimiter) {

    CSVFormat format = CSVFormat.Predefined.valueOf(formatString).getFormat();

    // If a delimiter has been passed set it here.
    if (delimiter != null) {
      return format.withDelimiter(delimiter.charAt(0));
    }
    return format;
  }

  /** Necessary {@link PipelineOptions} options for Csv Pipelines. */
  public interface CsvPipelineOptions extends PipelineOptions {
    @Description("Pattern to where data lives, ex: gs://mybucket/somepath/*.csv")
    String getInputFileSpec();

    void setInputFileSpec(String inputFileSpec);

    @Description("If file(s) contain headers")
    Boolean getContainsHeaders();

    void setContainsHeaders(Boolean containsHeaders);

    @Description("Deadletter table for failed inserts in form: <project-id>:<dataset>.<table>")
    String getDeadletterTable();

    void setDeadletterTable(String deadletterTable);

    @Description("Delimiting character. Default: use delimiter provided in csvFormat")
    @Default.InstanceFactory(DelimiterFactory.class)
    String getDelimiter();

    void setDelimiter(String delimiter);

    @Description(
        "Csv format according to Apache Commons CSV format. Default is: Apache Commons CSV"
            + " default\n"
            + "https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.html#DEFAULT\n"
            + "Must match format names exactly found at: "
            + "https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.Predefined.html")
    @Default.String("Default")
    String getCsvFormat();

    void setCsvFormat(String csvFormat);

    @Description("Optional: Path to JSON schema, ex gs://path/to/schema. ")
    String getJsonSchemaPath();

    void setJsonSchemaPath(String jsonSchemaPath);

    @Description("Set to true if number of files is in the tens of thousands. Default: false")
    @Default.Boolean(false)
    Boolean getLargeNumFiles();

    void setLargeNumFiles(Boolean largeNumFiles);
  }

  /**
   * Default value factory to get delimiter from Csv format so that if the user does not pass one
   * in, it matches the supplied {@link CsvPipelineOptions#getCsvFormat()}.
   */
  public static class DelimiterFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      CSVFormat csvFormat = getCsvFormat(options.as(CsvPipelineOptions.class).getCsvFormat(), null);
      return String.valueOf(csvFormat.getDelimiter());
    }
  }

  /**
   * The {@link LineToFailsafeJson} interface converts a line from a Csv file into a Json string.
   * Uses either: Javascript Udf, Json schema or the headers of the file to create the Json object
   * which is then added to the {@link FailsafeElement} as the new payload.
   */
  @AutoValue
  public abstract static class LineToFailsafeJson
      extends PTransform<PCollectionTuple, PCollectionTuple> {

    public static Builder newBuilder() {
      return new AutoValue_CsvConverters_LineToFailsafeJson.Builder();
    }

    public abstract String delimiter();

    @Nullable
    public abstract String jsonSchemaPath();

    @Nullable
    public abstract String jsonSchema();

    public abstract TupleTag<String> headerTag();

    public abstract TupleTag<String> lineTag();

    public abstract TupleTag<FailsafeElement<String, String>> udfOutputTag();

    public abstract TupleTag<FailsafeElement<String, String>> udfDeadletterTag();

    @Override
    public PCollectionTuple expand(PCollectionTuple lines) {

      PCollectionView<String> headersView = null;

      // Convert csv lines into Failsafe elements so that we can recover over multiple transforms.
      PCollection<FailsafeElement<String, String>> lineFailsafeElements =
          lines
              .get(lineTag())
              .apply("LineToFailsafeElement", ParDo.of(new LineToFailsafeElementFn()));

      // If no udf then use json schema
      String schemaPath = jsonSchemaPath();
      if (schemaPath != null || jsonSchema() != null) {

        String schema;
        if (schemaPath != null) {
          schema = getGcsFileAsString(schemaPath);
        } else {
          schema = jsonSchema();
        }

        return lineFailsafeElements.apply(
            "LineToDocumentUsingSchema",
            ParDo.of(
                    new FailsafeElementToJsonFn(
                        headersView, schema, delimiter(), udfDeadletterTag()))
                .withOutputTags(udfOutputTag(), TupleTagList.of(udfDeadletterTag())));
      }

      // Run if using headers
      headersView = lines.get(headerTag()).apply(Sample.any(1)).apply(View.asSingleton());

      PCollectionView<String> finalHeadersView = headersView;
      lines
          .get(headerTag())
          .apply(
              "CheckHeaderConsistency",
              ParDo.of(
                      new DoFn<String, String>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          String headers = c.sideInput(finalHeadersView);
                          if (!c.element().equals(headers)) {
                            LOG.error("Headers do not match, consistency cannot be guaranteed");
                            throw new RuntimeException(
                                "Headers do not match, consistency cannot be guaranteed");
                          }
                        }
                      })
                  .withSideInputs(finalHeadersView));

      return lineFailsafeElements.apply(
          "LineToDocumentWithHeaders",
          ParDo.of(
                  new FailsafeElementToJsonFn(
                      headersView, jsonSchemaPath(), delimiter(), udfDeadletterTag()))
              .withSideInputs(headersView)
              .withOutputTags(udfOutputTag(), TupleTagList.of(udfDeadletterTag())));
    }

    /** Builder for {@link LineToFailsafeJson}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDelimiter(String delimiter);

      public abstract Builder setJsonSchemaPath(String jsonSchemaPath);

      public abstract Builder setJsonSchema(String jsonSchema);

      public abstract Builder setHeaderTag(TupleTag<String> headerTag);

      public abstract Builder setLineTag(TupleTag<String> lineTag);

      public abstract Builder setUdfOutputTag(
          TupleTag<FailsafeElement<String, String>> udfOutputTag);

      public abstract Builder setUdfDeadletterTag(
          TupleTag<FailsafeElement<String, String>> udfDeadletterTag);

      public abstract LineToFailsafeJson build();
    }
  }

  /**
   * The {@link FailsafeElementToJsonFn} class creates a Json string from a failsafe element.
   *
   * <p>{@link FailsafeElementToJsonFn#FailsafeElementToJsonFn(PCollectionView, String, String,
   * TupleTag)}
   */
  public static class FailsafeElementToJsonFn
      extends DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>> {

    @Nullable public final String jsonSchema;
    public final String delimiter;
    public final TupleTag<FailsafeElement<String, String>> udfDeadletterTag;
    @Nullable private final PCollectionView<String> headersView;
    private Counter successCounter =
        Metrics.counter(FailsafeElementToJsonFn.class, SUCCESSFUL_TO_JSON_COUNTER);
    private Counter failedCounter =
        Metrics.counter(FailsafeElementToJsonFn.class, FAILED_TO_JSON_COUNTER);

    FailsafeElementToJsonFn(
        PCollectionView<String> headersView,
        String jsonSchema,
        String delimiter,
        TupleTag<FailsafeElement<String, String>> udfDeadletterTag) {
      this.headersView = headersView;
      this.jsonSchema = jsonSchema;
      this.delimiter = delimiter;
      this.udfDeadletterTag = udfDeadletterTag;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<String, String> element = context.element();
      List<String> header = null;

      if (this.headersView != null) {
        header = Arrays.asList(context.sideInput(this.headersView).split(this.delimiter));
      }

      List<String> record = Arrays.asList(element.getOriginalPayload().split(this.delimiter));

      try {
        String json = buildJsonString(header, record, this.jsonSchema);
        context.output(FailsafeElement.of(element.getOriginalPayload(), json));
        successCounter.inc();
      } catch (Exception e) {
        failedCounter.inc();
        context.output(
            this.udfDeadletterTag,
            FailsafeElement.of(element)
                .setErrorMessage(e.getMessage())
                .setStacktrace(Throwables.getStackTraceAsString(e)));
      }
    }
  }

  /**
   * The {@link LineToFailsafeElementFn} wraps an csv line with the {@link FailsafeElement} class so
   * errors can be recovered from and the original message can be output to a error records table.
   */
  static class LineToFailsafeElementFn extends DoFn<String, FailsafeElement<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      String message = context.element();
      context.output(FailsafeElement.of(message, message));
    }
  }

  /**
   * The {@link ReadCsv} class is a {@link PTransform} that reads from one for more Csv files. The
   * transform returns a {@link PCollectionTuple} consisting of the following {@link PCollection}:
   *
   * <ul>
   *   <li>{@link ReadCsv#headerTag()} - Contains headers found in files if read with headers,
   *       contains empty {@link PCollection} if no headers.
   *   <li>{@link ReadCsv#lineTag()} - Contains Csv lines as a {@link PCollection} of strings.
   * </ul>
   */
  @AutoValue
  public abstract static class ReadCsv extends PTransform<PBegin, PCollectionTuple> {

    public static Builder newBuilder() {
      return new AutoValue_CsvConverters_ReadCsv.Builder();
    }

    public abstract String csvFormat();

    @Nullable
    public abstract String delimiter();

    public abstract Boolean hasHeaders();

    public abstract String inputFileSpec();

    public abstract TupleTag<String> headerTag();

    public abstract TupleTag<String> lineTag();

    @Override
    public PCollectionTuple expand(PBegin input) {

      if (hasHeaders()) {
        return input
            .apply("MatchFilePattern", FileIO.match().filepattern(inputFileSpec()))
            .apply("ReadMatches", FileIO.readMatches())
            .apply(
                "ReadCsvWithHeaders",
                ParDo.of(new GetCsvHeadersFn(headerTag(), lineTag(), csvFormat(), delimiter()))
                    .withOutputTags(headerTag(), TupleTagList.of(lineTag())));
      }

      return PCollectionTuple.of(
          lineTag(), input.apply("ReadCsvWithoutHeaders", TextIO.read().from(inputFileSpec())));
    }

    /** Builder for {@link ReadCsv}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setCsvFormat(String csvFormat);

      public abstract Builder setDelimiter(@Nullable String delimiter);

      public abstract Builder setHasHeaders(Boolean hasHeaders);

      public abstract Builder setInputFileSpec(String inputFileSpec);

      public abstract Builder setHeaderTag(TupleTag<String> headerTag);

      public abstract Builder setLineTag(TupleTag<String> lineTag);

      abstract ReadCsv autoBuild();

      public ReadCsv build() {

        ReadCsv readCsv = autoBuild();

        checkArgument(readCsv.inputFileSpec() != null, "Input file spec must be provided.");

        checkArgument(readCsv.csvFormat() != null, "Csv format must not be null.");

        checkArgument(readCsv.hasHeaders() != null, "Header information must be provided.");

        return readCsv;
      }
    }
  }

  /**
   * The {@link GetCsvHeadersFn} class gets the header of a Csv file and outputs it as a string. The
   * csv format provided in {@link CsvConverters#getCsvFormat(String, String)} is used to get the
   * header.
   */
  static class GetCsvHeadersFn extends DoFn<ReadableFile, String> {

    private final TupleTag<String> headerTag;
    private final TupleTag<String> linesTag;
    private CSVFormat csvFormat;

    GetCsvHeadersFn(
        TupleTag<String> headerTag, TupleTag<String> linesTag, String csvFormat, String delimiter) {
      this.headerTag = headerTag;
      this.linesTag = linesTag;
      this.csvFormat = getCsvFormat(csvFormat, delimiter);
    }

    @ProcessElement
    public void processElement(ProcessContext context, MultiOutputReceiver outputReceiver) {
      ReadableFile f = context.element();
      String headers;
      List<String> records = null;
      String delimiter = String.valueOf(this.csvFormat.getDelimiter());
      try {
        String csvFileString = f.readFullyAsUTF8String();
        StringReader reader = new StringReader(csvFileString);
        CSVParser parser = CSVParser.parse(reader, this.csvFormat.withFirstRecordAsHeader());
        records =
            parser.getRecords().stream()
                .map(i -> String.join(delimiter, i))
                .collect(Collectors.toList());
        headers = String.join(delimiter, parser.getHeaderNames());
      } catch (IOException ioe) {
        LOG.error("Headers do not match, consistency cannot be guaranteed");
        throw new RuntimeException("Could not read Csv headers: " + ioe.getMessage());
      }
      outputReceiver.get(this.headerTag).output(headers);
      records.forEach(r -> outputReceiver.get(this.linesTag).output(r));
    }
  }
}
