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
package org.apache.beam.sdk.io.csv;

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.nullableAllPrimitiveDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.nullableAllPrimitiveDataTypesFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.nullableAllPrimitiveDataTypesToRowFn;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NullableAllPrimitiveDataTypes;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CsvIOParseTest {

  private static final String[] HEADER =
      new String[] {"aBoolean", "aDouble", "aFloat", "anInteger", "aLong", "aString"};
  private static final Coder<NullableAllPrimitiveDataTypes>
      NULLABLE_ALL_PRIMITIVE_DATA_TYPES_CODER =
          SchemaCoder.of(
              NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
              NULLABLE_ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR,
              nullableAllPrimitiveDataTypesToRowFn(),
              nullableAllPrimitiveDataTypesFromRowFn());
  private static final SerializableFunction<Row, Row> ROW_ROW_SERIALIZABLE_FUNCTION = row -> row;
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void isSerializable() throws Exception {
    SerializableUtils.ensureSerializable(CsvIOParse.class);
  }

  @Test
  public void parseRows() {
    PCollection<String> records =
        csvRecords(
            pipeline,
            "# This is a comment",
            "aBoolean,aDouble,aFloat,anInteger,aLong,aString",
            "true,1.0,2.0,3,4,foo",
            "🏵,6.0,7.0,8,9,bar",
            "false,12.0,14.0,8,24,\"foo\nbar\"",
            "true,1.0,2.0,3,4,foo$,bar");
    List<Row> want =
        Arrays.asList(
            Row.withSchema(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withFieldValue("aBoolean", true)
                .withFieldValue("aDouble", 1.0)
                .withFieldValue("aFloat", 2.0f)
                .withFieldValue("anInteger", 3)
                .withFieldValue("aLong", 4L)
                .withFieldValue("aString", "foo")
                .build(),
            Row.withSchema(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withFieldValue("aBoolean", null)
                .withFieldValue("aDouble", 6.0)
                .withFieldValue("aFloat", 7.0f)
                .withFieldValue("anInteger", 8)
                .withFieldValue("aLong", 9L)
                .withFieldValue("aString", "bar")
                .build(),
            Row.withSchema(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withFieldValue("aBoolean", false)
                .withFieldValue("aDouble", 12.0)
                .withFieldValue("aFloat", 14.0f)
                .withFieldValue("anInteger", 8)
                .withFieldValue("aLong", 24L)
                .withFieldValue("aString", "foo\nbar")
                .build(),
            Row.withSchema(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
                .withFieldValue("aBoolean", true)
                .withFieldValue("aDouble", 1.0)
                .withFieldValue("aFloat", 2.0f)
                .withFieldValue("anInteger", 3)
                .withFieldValue("aLong", 4L)
                .withFieldValue("aString", "foo,bar")
                .build());

    CsvIOParseResult<Row> result =
        records.apply(
            underTest(
                NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
                csvFormat(),
                emptyCustomProcessingMap(),
                ROW_ROW_SERIALIZABLE_FUNCTION,
                RowCoder.of(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)));
    PAssert.that(result.getOutput()).containsInAnyOrder(want);
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void parsePOJOs() {
    PCollection<String> records =
        csvRecords(
            pipeline,
            "# This is a comment",
            "aBoolean,aDouble,aFloat,anInteger,aLong,aString",
            "true,1.0,2.0,3,4,foo",
            "🏵,6.0,7.0,8,9,bar",
            "false,12.0,14.0,8,24,\"foo\nbar\"",
            "true,1.0,2.0,3,4,foo$,bar");
    List<SchemaAwareJavaBeans.NullableAllPrimitiveDataTypes> want =
        Arrays.asList(
            nullableAllPrimitiveDataTypes(true, 1.0d, 2.0f, 3, 4L, "foo"),
            nullableAllPrimitiveDataTypes(null, 6.0d, 7.0f, 8, 9L, "bar"),
            nullableAllPrimitiveDataTypes(false, 12.0d, 14.0f, 8, 24L, "foo\nbar"),
            nullableAllPrimitiveDataTypes(true, 1.0d, 2.0f, 3, 4L, "foo,bar"));

    CsvIOParseResult<NullableAllPrimitiveDataTypes> result =
        records.apply(
            underTest(
                NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
                csvFormat(),
                emptyCustomProcessingMap(),
                nullableAllPrimitiveDataTypesFromRowFn(),
                NULLABLE_ALL_PRIMITIVE_DATA_TYPES_CODER));
    PAssert.that(result.getOutput()).containsInAnyOrder(want);
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenCustomRecordParsingLambdas_parsesRows() {
    PCollection<String> records =
        csvRecords(
            pipeline, "aBoolean,aDouble,aFloat,anInteger,aLong,aString", "true,1.0,2.0,3,4,foo");
    Row want =
        Row.withSchema(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .withFieldValue("aBoolean", false)
            .withFieldValue("aDouble", 2.0)
            .withFieldValue("aFloat", 4.0f)
            .withFieldValue("anInteger", 6)
            .withFieldValue("aLong", 8L)
            .withFieldValue("aString", "foofoo")
            .build();
    CsvIOParse<Row> underTest =
        underTest(
                NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
                csvFormat(),
                emptyCustomProcessingMap(),
                ROW_ROW_SERIALIZABLE_FUNCTION,
                RowCoder.of(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA))
            .withCustomRecordParsing(
                "aBoolean",
                (SerializableFunction<String, Object>) input -> !Boolean.parseBoolean(input),
                Boolean.class)
            .withCustomRecordParsing(
                "aDouble",
                (SerializableFunction<String, Object>) input -> Double.parseDouble(input) * 2,
                Double.class)
            .withCustomRecordParsing(
                "aFloat",
                (SerializableFunction<String, Object>) input -> Float.parseFloat(input) * 2,
                Float.class)
            .withCustomRecordParsing(
                "anInteger",
                (SerializableFunction<String, Object>) input -> Integer.parseInt(input) * 2,
                Integer.class)
            .withCustomRecordParsing(
                "aLong",
                (SerializableFunction<String, Object>) input -> Long.parseLong(input) * 2,
                Long.class)
            .withCustomRecordParsing(
                "aString",
                (SerializableFunction<String, Object>) input -> input + input,
                String.class);
    CsvIOParseResult<Row> result = records.apply(underTest);
    PAssert.that(result.getOutput()).containsInAnyOrder(want);
    PAssert.that(result.getErrors()).empty();
    pipeline.run();
  }

  @Test
  public void givenCustomRecordParsingLambdas_parsesToPOJO() {}

  @Test
  public void givenCustomRecordParsingFieldNameNotPresentInSchema_throws() {}

  @Test
  public void givenCustomRecordParsingLambdaOutputTypeNotCompatibleWithSchemaFieldType_throws() {}

  private static CSVFormat csvFormat() {
    return CSVFormat.DEFAULT
        .withAllowDuplicateHeaderNames(false)
        .withHeader(HEADER)
        .withCommentMarker('#')
        .withNullString("🏵")
        .withEscape('$');
  }

  private static PCollection<String> csvRecords(Pipeline pipeline, String... lines) {
    return pipeline.apply(
        Create.of(Arrays.asList(lines)).withCoder(NullableCoder.of(StringUtf8Coder.of())));
  }

  private static <T> CsvIOParse<T> underTest(
      Schema schema,
      CSVFormat csvFormat,
      Map<String, SerializableFunction<String, Object>> customProcessingMap,
      SerializableFunction<Row, T> fromRowFn,
      Coder<T> coder) {
    CsvIOParseConfiguration.Builder<T> configBuilder =
        CsvIOParseConfiguration.<T>builder()
            .setSchema(schema)
            .setCsvFormat(csvFormat)
            .setCustomProcessingMap(customProcessingMap)
            .setFromRowFn(fromRowFn)
            .setCoder(coder);
    return CsvIOParse.<T>builder().setConfigBuilder(configBuilder).build();
  }

  private static Map<String, SerializableFunction<String, Object>> emptyCustomProcessingMap() {
    return new HashMap<>();
  }
}
