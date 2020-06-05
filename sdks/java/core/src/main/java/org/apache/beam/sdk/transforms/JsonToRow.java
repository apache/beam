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
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

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

  private static final String LINE_FIELD_NAME = "line";
  private static final String ERROR_FIELD_NAME = "err";

  public static final Schema ERROR_ROW_SCHEMA =
      Schema.of(
          Field.of(LINE_FIELD_NAME, FieldType.STRING),
          Field.of(ERROR_FIELD_NAME, FieldType.STRING));

  public static final TupleTag<Row> MAIN_TUPLE_TAG = new TupleTag<Row>() {};
  public static final TupleTag<Row> DEAD_LETTER_TUPLE_TAG = new TupleTag<Row>() {};

  public static PTransform<PCollection<String>, PCollection<Row>> withSchema(Schema rowSchema) {
    return JsonToRowFn.forSchema(rowSchema);
  }

  /**
   * Enable Dead letter support. If this value is set errors in the parsing layer are returned as
   * Row objects of form: {@link JsonToRow#ERROR_ROW_SCHEMA} line : The original json string err :
   * The error message from the parsing function.
   *
   * <p>You can access the results by using:
   *
   * <p>{@link JsonToRow#MAIN_TUPLE_TAG}
   *
   * <p>{@Code PCollection<Row> personRows =
   * results.get(JsonToRow.MAIN_TUPLE_TAG).setRowSchema(personSchema)}
   *
   * <p>{@link JsonToRow#DEAD_LETTER_TUPLE_TAG}
   *
   * <p>{@Code PCollection<Row> errors =
   * results.get(JsonToRow.DEAD_LETTER_TUPLE_TAG).setRowSchema(JsonToRow.ERROR_ROW_SCHEMA);}
   *
   * @return {@link JsonToRowWithFailureCaptureFn}
   */
  public static JsonToRowWithFailureCaptureFn withDeadLetter(Schema rowSchema) {
    return JsonToRowWithFailureCaptureFn.forSchema(rowSchema);
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

  static class JsonToRowWithFailureCaptureFn
      extends PTransform<PCollection<String>, PCollectionTuple> {
    private transient volatile @Nullable ObjectMapper objectMapper;
    private Schema schema;
    private static final String METRIC_NAMESPACE = "JsonToRowFn";
    private static final String DEAD_LETTER_METRIC_NAME = "JsonToRowFn_ParseFailure";

    private Distribution jsonConversionErrors =
        Metrics.distribution(METRIC_NAMESPACE, DEAD_LETTER_METRIC_NAME);

    public static final TupleTag<Row> main = MAIN_TUPLE_TAG;
    public static final TupleTag<Row> deadLetter = DEAD_LETTER_TUPLE_TAG;

    PCollection<Row> deadLetterCollection;

    static JsonToRowWithFailureCaptureFn forSchema(Schema rowSchema) {
      // Throw exception if this schema is not supported by RowJson
      RowJson.verifySchemaSupported(rowSchema);
      return new JsonToRowWithFailureCaptureFn(rowSchema);
    }

    private JsonToRowWithFailureCaptureFn(Schema schema) {
      this.schema = schema;
    }

    @Override
    public PCollectionTuple expand(PCollection<String> jsonStrings) {

      return jsonStrings.apply(
          ParDo.of(
                  new DoFn<String, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      try {
                        context.output(jsonToRow(objectMapper(), context.element()));
                      } catch (Exception ex) {
                        context.output(
                            deadLetter,
                            Row.withSchema(ERROR_ROW_SCHEMA)
                                .addValue(context.element())
                                .addValue(ex.getMessage())
                                .build());
                      }
                    }
                  })
              .withOutputTags(main, TupleTagList.of(deadLetter)));
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
}
