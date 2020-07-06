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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.sdk.util.RowJsonUtils.jsonToRow;
import static org.apache.beam.sdk.util.RowJsonUtils.newObjectMapperWith;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * <i>Experimental</i>
 *
 * <p>Creates a {@link PTransform} to convert input JSON objects to {@link Row Rows} with given
 * {@link Schema}.
 *
 * <p>Currently supported {@link Schema} field types are:
 *
 * <ul>
 *   <li>{@link Schema.TypeName#BYTE}
 *   <li>{@link Schema.TypeName#INT16}
 *   <li>{@link Schema.TypeName#INT32}
 *   <li>{@link Schema.TypeName#INT64}
 *   <li>{@link Schema.TypeName#FLOAT}
 *   <li>{@link Schema.TypeName#DOUBLE}
 *   <li>{@link Schema.TypeName#BOOLEAN}
 *   <li>{@link Schema.TypeName#STRING}
 * </ul>
 *
 * <p>For specifics of JSON deserialization see {@link RowJsonDeserializer}.
 *
 * <p>Conversion is strict, with minimal type coercion:
 *
 * <p>Booleans are only parsed from {@code true} or {@code false} literals, not from {@code "true"}
 * or {@code "false"} strings or any other values (exception is thrown in these cases).
 *
 * <p>If a JSON number doesn't fit into the corresponding schema field type, an exception is be
 * thrown. Strings are not auto-converted to numbers. Floating point numbers are not auto-converted
 * to integral numbers. Precision loss also causes exceptions.
 *
 * <p>Only JSON string values can be parsed into {@link TypeName#STRING}. Numbers, booleans are not
 * automatically converted, exceptions are thrown in these cases.
 *
 * <p>If a schema field is missing from the JSON value, an exception will be thrown.
 *
 * <p>Explicit {@code null} literals are allowed in JSON objects. No other values are parsed into
 * {@code null}.
 */
@Experimental(Kind.SCHEMAS)
public class JsonToRow {

  public static PTransform<PCollection<String>, PCollection<Row>> withSchema(Schema rowSchema) {
    return JsonToRowFn.forSchema(rowSchema);
  }

  static class JsonToRowFn extends PTransform<PCollection<String>, PCollection<Row>> {
    private transient volatile @Nullable ObjectMapper objectMapper;
    private Schema schema;

    static JsonToRowFn forSchema(Schema rowSchema) {
      // Throw exception if this schema is not supported by RowJson
      RowJson.verifySchemaSupported(rowSchema);
      return new JsonToRowFn(rowSchema);
    }

    private JsonToRowFn(Schema schema) {
      this.schema = schema;
    }

    @Override
    public PCollection<Row> expand(PCollection<String> jsonStrings) {
      return jsonStrings
          .apply(
              ParDo.of(
                  new DoFn<String, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      context.output(jsonToRow(objectMapper(), context.element()));
                    }
                  }))
          .setRowSchema(schema);
    }

    private ObjectMapper objectMapper() {
      if (this.objectMapper == null) {
        synchronized (this) {
          if (this.objectMapper == null) {
            this.objectMapper = newObjectMapperWith(RowJsonDeserializer.forSchema(this.schema));
          }
        }
      }

      return this.objectMapper;
    }
  }

  /**
   * Enable Exception Reporting support. If this value is set errors in the parsing layer are
   * returned as Row objects within a {@link ParseResult}
   *
   * <p>You can access the results by using {@link JsonToRow#withExceptionReporting(Schema)}:
   *
   * <p>ParseResult results = jsonPersons.apply(JsonToRow.withExceptionReporting(PERSON_SCHEMA));
   *
   * <p>Then access the parsed results via, {@link ParseResult#getResults()}
   *
   * <p>{@Code PCollection<Row> personRows = results.getResults()}
   *
   * <p>And access the failed to parse results via, {@link ParseResult#getFailedToParseLines()}
   *
   * <p>{@Code PCollection<Row> errorsLines = results.getFailedToParseLines()}
   *
   * <p>This will produce a Row with Schema {@link JsonToRowWithErrFn#ERROR_ROW_SCHEMA}
   *
   * <p>To access the reason for the failure you will need to first enable extended error reporting.
   * {@link JsonToRowWithErrFn#withExtendedErrorInfo()} {@Code ParseResult results =
   * jsonPersons.apply(JsonToRow.withExceptionReporting(PERSON_SCHEMA).withExtendedErrorInfo()); }
   *
   * <p>This will provide access to the reason for the Parse failure. The call to {@link
   * ParseResult#getFailedToParseLines()} will produce a Row with Schema {@link
   * JsonToRowWithErrFn#ERROR_ROW_WITH_ERR_MSG_SCHEMA}
   *
   * @return {@link JsonToRowWithErrFn}
   */
  @Experimental(Kind.SCHEMAS)
  public static JsonToRowWithErrFn withExceptionReporting(Schema rowSchema) {
    return JsonToRowWithErrFn.forSchema(rowSchema);
  }

  @AutoValue
  public abstract static class JsonToRowWithErrFn
      extends PTransform<PCollection<String>, ParseResult> {

    private static final String LINE_FIELD_NAME = "line";
    private static final String ERROR_FIELD_NAME = "err";

    public static final Schema ERROR_ROW_SCHEMA =
        Schema.of(Field.of(LINE_FIELD_NAME, FieldType.STRING));

    public static final Schema ERROR_ROW_WITH_ERR_MSG_SCHEMA =
        Schema.of(
            Field.of(LINE_FIELD_NAME, FieldType.STRING),
            Field.of(ERROR_FIELD_NAME, FieldType.STRING));

    static final TupleTag<Row> PARSED_LINE = new TupleTag<Row>() {};
    static final TupleTag<Row> PARSE_ERROR = new TupleTag<Row>() {};

    abstract Schema getSchema();

    abstract String getLineFieldName();

    abstract String getErrorFieldName();

    abstract boolean getExtendedErrorInfo();

    abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {
      abstract Builder setSchema(Schema value);

      abstract Builder setLineFieldName(String value);

      abstract Builder setErrorFieldName(String value);

      abstract Builder setExtendedErrorInfo(boolean value);

      abstract JsonToRowWithErrFn build();
    }

    static JsonToRowWithErrFn forSchema(Schema rowSchema) {
      // Throw exception if this schema is not supported by RowJson
      RowJson.verifySchemaSupported(rowSchema);
      return new AutoValue_JsonToRow_JsonToRowWithErrFn.Builder()
          .setSchema(rowSchema)
          .setExtendedErrorInfo(false)
          .setLineFieldName(LINE_FIELD_NAME)
          .setErrorFieldName(ERROR_FIELD_NAME)
          .build();
    }

    /**
     * Adds the error message to the returned error Row.
     *
     * @return {@link JsonToRow}
     */
    public JsonToRowWithErrFn withExtendedErrorInfo() {
      return this.toBuilder().setExtendedErrorInfo(true).build();
    }

    /**
     * Sets the field name for the line field in the returned Row.
     *
     * @return {@link JsonToRow}
     */
    public JsonToRowWithErrFn setLineField(String lineField) {
      return this.toBuilder().setLineFieldName(lineField).build();
    }

    /**
     * Adds the error message to the returned error Row.
     *
     * @return {@link JsonToRow}
     */
    public JsonToRowWithErrFn setErrorField(String errorField) {
      if (!this.getExtendedErrorInfo()) {
        throw new IllegalArgumentException(
            "This option is only available with Extended Error Info.");
      }
      return this.toBuilder().setErrorFieldName(errorField).build();
    }

    @Override
    public ParseResult expand(PCollection<String> jsonStrings) {

      PCollectionTuple result =
          jsonStrings.apply(
              ParDo.of(ParseWithError.create(this))
                  .withOutputTags(PARSED_LINE, TupleTagList.of(PARSE_ERROR)));

      PCollection<Row> failures;

      if (getExtendedErrorInfo()) {
        failures =
            result.get(PARSE_ERROR).setRowSchema(JsonToRowWithErrFn.ERROR_ROW_WITH_ERR_MSG_SCHEMA);
      } else {
        failures = result.get(PARSE_ERROR).setRowSchema(JsonToRowWithErrFn.ERROR_ROW_SCHEMA);
      }

      return ParseResult.resultBuilder()
          .setCallingPipeline(jsonStrings.getPipeline())
          .setJsonToRowWithErrFn(this)
          .setParsedLine(result.get(PARSED_LINE).setRowSchema(this.getSchema()))
          .setFailedParse(failures)
          .build();
    }

    @AutoValue
    protected abstract static class ParseWithError extends DoFn<String, Row> {
      private transient volatile @Nullable ObjectMapper objectMapper;

      public abstract JsonToRowWithErrFn getJsonToRowWithErrFn();

      public abstract Builder toBuilder();

      @AutoValue.Builder
      public abstract static class Builder {
        public abstract Builder setJsonToRowWithErrFn(JsonToRowWithErrFn value);

        public abstract ParseWithError build();
      }

      public static ParseWithError create(JsonToRowWithErrFn jsonToRowWithErrFn) {
        return new AutoValue_JsonToRow_JsonToRowWithErrFn_ParseWithError.Builder()
            .setJsonToRowWithErrFn(jsonToRowWithErrFn)
            .build();
      }

      @ProcessElement
      public void processElement(@Element String element, MultiOutputReceiver output) {
        try {

          output.get(PARSED_LINE).output(jsonToRow(objectMapper(), element));

        } catch (Exception ex) {

          if (getJsonToRowWithErrFn().getExtendedErrorInfo()) {
            output
                .get(PARSE_ERROR)
                .output(
                    Row.withSchema(ERROR_ROW_WITH_ERR_MSG_SCHEMA)
                        .addValue(element)
                        .addValue(ex.getMessage())
                        .build());
          } else {
            output
                .get(PARSE_ERROR)
                .output(Row.withSchema(ERROR_ROW_SCHEMA).addValue(element).build());
          }
        }
      }

      private ObjectMapper objectMapper() {
        if (this.objectMapper == null) {
          synchronized (this) {
            if (this.objectMapper == null) {
              this.objectMapper =
                  newObjectMapperWith(
                      RowJsonDeserializer.forSchema(getJsonToRowWithErrFn().getSchema()));
            }
          }
        }

        return this.objectMapper;
      }
    }
  }

  /** The result of a {@link JsonToRow#withExceptionReporting(Schema)} transform. */
  @AutoValue
  public abstract static class ParseResult implements POutput {

    abstract JsonToRowWithErrFn getJsonToRowWithErrFn();

    abstract PCollection<Row> getParsedLine();

    abstract PCollection<Row> getFailedParse();

    abstract ParseResult.Builder toBuilder();

    abstract Pipeline getCallingPipeline();

    @AutoValue.Builder
    public abstract static class Builder {
      abstract Builder setJsonToRowWithErrFn(JsonToRowWithErrFn value);

      abstract Builder setParsedLine(PCollection<Row> value);

      abstract Builder setFailedParse(PCollection<Row> value);

      abstract Builder setCallingPipeline(Pipeline value);

      abstract ParseResult build();
    }

    public static ParseResult.Builder resultBuilder() {
      return new AutoValue_JsonToRow_ParseResult.Builder();
    }

    @Override
    public Pipeline getPipeline() {
      return getCallingPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          JsonToRowWithErrFn.PARSED_LINE,
          getParsedLine(),
          JsonToRowWithErrFn.PARSE_ERROR,
          getFailedParse());
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}

    /** Returns a {@link PCollection} containing the {@link Row}s that have been parsed. */
    public PCollection<Row> getResults() {
      return getParsedLine();
    }

    /**
     * Returns a {@link PCollection} containing the {@link Row}s that didn't parse.
     *
     * <p>If {@link JsonToRowWithErrFn#withExtendedErrorInfo()} was set then the schema will also
     * include the error message.
     */
    public PCollection<Row> getFailedToParseLines() {
      return getFailedParse();
    }
  }
}
