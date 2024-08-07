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
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.nullableAllPrimitiveDataTypes;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.junit.Assert.assertThrows;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NullableAllPrimitiveDataTypes;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CsvIOTest {
  private static final String[] HEADER =
      new String[] {"aBoolean", "aDouble", "aFloat", "anInteger", "aLong", "aString"};

  @Test
  public void parseRows() {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> input =
        csvRecords(
            pipeline,
            "# This is a comment",
            "aBoolean,aDouble,aFloat,anInteger,aLong,aString",
            "true,1.0,2.0,3,4,foo",
            "N/A,6.0,7.0,8,9,bar",
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

    CsvIOParse<Row> underTest =
        CsvIO.parseRows(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA, csvFormat());
    CsvIOParseResult<Row> result = input.apply(underTest);
    PAssert.that(result.getOutput()).containsInAnyOrder(want);
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void parsesPOJOs() {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> input =
        csvRecords(
            pipeline,
            "# This is a comment",
            "aBoolean,aDouble,aFloat,anInteger,aLong,aString",
            "true,1.0,2.0,3,4,foo",
            "N/A,6.0,7.0,8,9,bar",
            "false,12.0,14.0,8,24,\"foo\nbar\"",
            "true,1.0,2.0,3,4,foo$,bar");
    List<SchemaAwareJavaBeans.NullableAllPrimitiveDataTypes> want =
        Arrays.asList(
            nullableAllPrimitiveDataTypes(true, 1.0d, 2.0f, 3, 4L, "foo"),
            nullableAllPrimitiveDataTypes(null, 6.0d, 7.0f, 8, 9L, "bar"),
            nullableAllPrimitiveDataTypes(false, 12.0d, 14.0f, 8, 24L, "foo\nbar"),
            nullableAllPrimitiveDataTypes(true, 1.0d, 2.0f, 3, 4L, "foo,bar"));

    CsvIOParse<NullableAllPrimitiveDataTypes> underTest =
        CsvIO.parse(NullableAllPrimitiveDataTypes.class, csvFormat());
    CsvIOParseResult<NullableAllPrimitiveDataTypes> result = input.apply(underTest);
    PAssert.that(result.getOutput()).containsInAnyOrder(want);
    PAssert.that(result.getErrors()).empty();

    pipeline.run();
  }

  @Test
  public void givenInvalidCsvFormat_throws() {
    Pipeline pipeline = Pipeline.create();
    CSVFormat csvFormat =
        CSVFormat.DEFAULT
            .withHeader("a_string", "an_integer", "a_double")
            .withAllowDuplicateHeaderNames(true);
    Schema schema =
        Schema.builder()
            .addStringField("a_string")
            .addInt32Field("an_integer")
            .addDoubleField("a_double")
            .build();
    assertThrows(IllegalArgumentException.class, () -> CsvIO.parseRows(schema, csvFormat));
    pipeline.run();
  }

  @Test
  public void givenMismatchedCsvFormatAndSchema_throws() {
    Pipeline pipeline = Pipeline.create();
    CSVFormat csvFormat =
        CSVFormat.DEFAULT
            .withHeader("a_string", "an_integer", "a_double")
            .withAllowDuplicateHeaderNames(true);
    Schema schema = Schema.builder().addStringField("a_string").addDoubleField("a_double").build();
    assertThrows(IllegalArgumentException.class, () -> CsvIO.parseRows(schema, csvFormat));
    pipeline.run();
  }

  @Test
  public void givenNullSchema_throws() {
    Pipeline pipeline = Pipeline.create();
    assertThrows(NullPointerException.class, () -> CsvIO.parseRows(null, csvFormat()));
    pipeline.run();
  }

  @Test
  public void givenNonSchemaMappedClass_throws() {
    Pipeline pipeline = Pipeline.create();
    CSVFormat csvFormat =
        CSVFormat.DEFAULT
            .withHeader("a_string", "an_integer", "a_double")
            .withAllowDuplicateHeaderNames(false);
    assertThrows(
        IllegalStateException.class, () -> CsvIO.parse(NonSchemaMappedPojo.class, csvFormat));
    pipeline.run();
  }

  @Test
  public void givenStringToRecordError_emits() {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> input = pipeline.apply(Create.of("true,\"1.1,3.141592,1,5,foo"));
    Schema schema =
        Schema.builder()
            .addBooleanField("aBoolean")
            .addDoubleField("aDouble")
            .addFloatField("aFloat")
            .addInt32Field("anInteger")
            .addInt64Field("aLong")
            .addStringField("aString")
            .build();
    CsvIOParse<Row> underTest = CsvIO.parseRows(schema, csvFormat().withQuote('"'));
    CsvIOParseResult<Row> result = input.apply(underTest);
    PAssert.thatSingleton(result.getErrors().apply("Total Errors", Count.globally())).isEqualTo(1L);
    PAssert.thatSingleton(
            stackTraceContains(result.getErrors(), CsvIOStringToCsvRecord.class.getName()))
        .isEqualTo(1L);

    pipeline.run();
  }

  @Test
  public void givenRecordToObjectError_emits() {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> input =
        pipeline.apply(Create.of("true,1.1,3.141592,this_is_an_error,5,foo"));
    Schema schema =
        Schema.builder()
            .addBooleanField("aBoolean")
            .addDoubleField("aDouble")
            .addFloatField("aFloat")
            .addInt32Field("anInteger")
            .addInt64Field("aLong")
            .addStringField("aString")
            .build();
    CsvIOParse<Row> underTest = CsvIO.parseRows(schema, csvFormat().withQuote('"'));
    CsvIOParseResult<Row> result = input.apply(underTest);
    PAssert.thatSingleton(result.getErrors().apply(Count.globally())).isEqualTo(1L);
    PAssert.thatSingleton(
            stackTraceContains(result.getErrors(), CsvIORecordToObjects.class.getName()))
        .isEqualTo(1L);
    pipeline.run();
  }

  @Test
  public void givenStringToRecordError_RecordToObjectError_emits() {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> input =
        pipeline.apply(
            Create.of("true,\"1.1,3.141592,1,5,foo", "true,1.1,3.141592,this_is_an_error,5,foo"));
    Schema schema =
        Schema.builder()
            .addBooleanField("aBoolean")
            .addDoubleField("aDouble")
            .addFloatField("aFloat")
            .addInt32Field("anInteger")
            .addInt64Field("aLong")
            .addStringField("aString")
            .build();
    CsvIOParse<Row> underTest = CsvIO.parseRows(schema, csvFormat().withQuote('"'));
    CsvIOParseResult<Row> result = input.apply(underTest);
    PAssert.thatSingleton(result.getErrors().apply(Count.globally())).isEqualTo(2L);
    PAssert.thatSingleton(
            stackTraceContains(result.getErrors(), CsvIOStringToCsvRecord.class.getName()))
        .isEqualTo(1L);
    PAssert.thatSingleton(
            stackTraceContains(result.getErrors(), CsvIORecordToObjects.class.getName()))
        .isEqualTo(1L);

    pipeline.run();
  }

  private static PCollection<Long> stackTraceContains(
      PCollection<CsvIOParseError> errors, String match) {
    return errors
        .apply(match, Filter.by(input -> checkStateNotNull(input).getStackTrace().contains(match)))
        .apply(match, Count.globally());
  }

  private static CSVFormat csvFormat() {
    return CSVFormat.DEFAULT
        .withAllowDuplicateHeaderNames(false)
        .withHeader(HEADER)
        .withCommentMarker('#')
        .withNullString("N/A")
        .withEscape('$');
  }

  private static PCollection<String> csvRecords(Pipeline pipeline, String... lines) {
    return pipeline.apply(
        Create.of(Arrays.asList(lines)).withCoder(NullableCoder.of(StringUtf8Coder.of())));
  }

  private static class NonSchemaMappedPojo implements Serializable {
    private final String aString;
    private final Integer anInteger;
    private final Double aDouble;

    private NonSchemaMappedPojo(String aString, Integer anInteger, Double aDouble) {
      this.aString = aString;
      this.anInteger = anInteger;
      this.aDouble = aDouble;
    }

    public String getAString() {
      return aString;
    }

    public Integer getAnInteger() {
      return anInteger;
    }

    public Double getADouble() {
      return aDouble;
    }
  }
}
