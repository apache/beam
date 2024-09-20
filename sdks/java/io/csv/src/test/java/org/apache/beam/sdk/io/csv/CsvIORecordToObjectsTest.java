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

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.AllPrimitiveDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NullableAllPrimitiveDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_TYPE_DESCRIPTOR;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TimeContaining;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypesFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypesToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.nullableAllPrimitiveDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.nullableAllPrimitiveDataTypesFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.nullableAllPrimitiveDataTypesToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.timeContaining;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.timeContainingFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.timeContainingToRowFn;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.csv.CSVFormat;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CsvIORecordToObjects}. */
@RunWith(JUnit4.class)
public class CsvIORecordToObjectsTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  private static final SerializableFunction<Row, Row> ROW_ROW_SERIALIZABLE_FUNCTION = row -> row;
  private static final RowCoder ALL_PRIMITIVE_DATA_TYPES_ROW_CODER =
      RowCoder.of(ALL_PRIMITIVE_DATA_TYPES_SCHEMA);
  private static final Coder<Row> NULLABLE_ALL_PRIMITIVE_DATA_TYPES_ROW_CODER =
      NullableCoder.of(RowCoder.of(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA));
  private static final Coder<SchemaAwareJavaBeans.AllPrimitiveDataTypes>
      ALL_PRIMITIVE_DATA_TYPES_CODER =
          SchemaCoder.of(
              ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
              ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR,
              allPrimitiveDataTypesToRowFn(),
              allPrimitiveDataTypesFromRowFn());
  private static final Coder<NullableAllPrimitiveDataTypes>
      NULLABLE_ALL_PRIMITIVE_DATA_TYPES_CODER =
          SchemaCoder.of(
              NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
              NULLABLE_ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR,
              nullableAllPrimitiveDataTypesToRowFn(),
              nullableAllPrimitiveDataTypesFromRowFn());
  private static final Coder<Row> TIME_CONTAINING_ROW_CODER = RowCoder.of(TIME_CONTAINING_SCHEMA);
  private static final Coder<TimeContaining> TIME_CONTAINING_POJO_CODER =
      SchemaCoder.of(
          TIME_CONTAINING_SCHEMA,
          TIME_CONTAINING_TYPE_DESCRIPTOR,
          timeContainingToRowFn(),
          timeContainingFromRowFn());

  @Test
  public void isSerializable() {
    SerializableUtils.ensureSerializable(CsvIORecordToObjects.class);
  }

  @Test
  public void parsesToRows() {
    PCollection<List<String>> input =
        csvRecords(pipeline, "true", "1.0", "2.0", "3.0", "4", "5", "foo");
    Row want =
        Row.withSchema(ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .withFieldValues(
                ImmutableMap.of(
                    "aBoolean",
                    true,
                    "aDecimal",
                    BigDecimal.valueOf(1.0),
                    "aDouble",
                    2.0,
                    "aFloat",
                    3.0f,
                    "anInteger",
                    4,
                    "aLong",
                    5L,
                    "aString",
                    "foo"))
            .build();
    CsvIORecordToObjects<Row> underTest =
        underTest(
            ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
            allPrimitiveDataTypesCsvFormat(),
            emptyCustomProcessingMap(),
            ROW_ROW_SERIALIZABLE_FUNCTION,
            ALL_PRIMITIVE_DATA_TYPES_ROW_CODER);
    CsvIOParseResult<Row> result = input.apply(underTest);
    PAssert.that(result.getOutput()).containsInAnyOrder(want);
    PAssert.that(result.getErrors()).empty();
    pipeline.run();
  }

  @Test
  public void parsesToPojos() {
    PCollection<List<String>> input =
        csvRecords(pipeline, "true", "1.0", "2.0", "3.0", "4", "5", "foo");
    SchemaAwareJavaBeans.AllPrimitiveDataTypes want =
        allPrimitiveDataTypes(true, BigDecimal.valueOf(1.0), 2.0d, 3.0f, 4, 5L, "foo");
    CsvIORecordToObjects<AllPrimitiveDataTypes> underTest =
        underTest(
            ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
            allPrimitiveDataTypesCsvFormat(),
            emptyCustomProcessingMap(),
            allPrimitiveDataTypesFromRowFn(),
            ALL_PRIMITIVE_DATA_TYPES_CODER);
    CsvIOParseResult<AllPrimitiveDataTypes> result = input.apply(underTest);
    PAssert.that(result.getOutput()).containsInAnyOrder(want);
    PAssert.that(result.getErrors()).empty();
    pipeline.run();
  }

  @Test
  public void givenNullableField_containsNullCell_parsesToRows() {
    PCollection<List<String>> input = csvRecords(pipeline, "true", "1.0", "2.0", "3", "4", null);
    Row want =
        Row.withSchema(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA)
            .withFieldValue("aBoolean", true)
            .withFieldValue("aDouble", 1.0)
            .withFieldValue("aFloat", 2.0f)
            .withFieldValue("anInteger", 3)
            .withFieldValue("aLong", 4L)
            .withFieldValue("aString", null)
            .build();

    CsvIORecordToObjects<Row> underTest =
        underTest(
            NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
            nullableAllPrimitiveDataTypesCsvFormat(),
            emptyCustomProcessingMap(),
            ROW_ROW_SERIALIZABLE_FUNCTION,
            NULLABLE_ALL_PRIMITIVE_DATA_TYPES_ROW_CODER);
    CsvIOParseResult<Row> result = input.apply(underTest);
    PAssert.that(result.getOutput()).containsInAnyOrder(want);
    PAssert.that(result.getErrors()).empty();
    pipeline.run();
  }

  @Test
  public void givenNullableField_containsNullCell_parsesToPojos() {
    PCollection<List<String>> input = csvRecords(pipeline, "true", "1.0", "2.0", "3", "4", null);
    SchemaAwareJavaBeans.NullableAllPrimitiveDataTypes want =
        nullableAllPrimitiveDataTypes(true, 1.0, 2.0f, 3, 4L, null);

    CsvIORecordToObjects<NullableAllPrimitiveDataTypes> underTest =
        underTest(
            NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
            nullableAllPrimitiveDataTypesCsvFormat(),
            emptyCustomProcessingMap(),
            nullableAllPrimitiveDataTypesFromRowFn(),
            NULLABLE_ALL_PRIMITIVE_DATA_TYPES_CODER);
    CsvIOParseResult<NullableAllPrimitiveDataTypes> result = input.apply(underTest);
    PAssert.that(result.getOutput()).containsInAnyOrder(want);
    PAssert.that(result.getErrors()).empty();
    pipeline.run();
  }

  @Test
  public void givenNoNullableField_containsNullCell_throws() {
    PCollection<List<String>> input =
        csvRecords(pipeline, "true", "1.0", "2.0", "3.0", "4", "5", null);
    pipeline.apply(
        "Null Cell with No Nullable Fields",
        Create.of(
                Collections.singletonList(
                    Arrays.asList("true", "1.0", "2.0", "3.0", "4", "5", null)))
            .withCoder(ListCoder.of(NullableCoder.of(StringUtf8Coder.of()))));
    CsvIORecordToObjects<AllPrimitiveDataTypes> underTest =
        underTest(
            ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
            allPrimitiveDataTypesCsvFormat(),
            emptyCustomProcessingMap(),
            allPrimitiveDataTypesFromRowFn(),
            ALL_PRIMITIVE_DATA_TYPES_CODER);
    CsvIOParseResult<AllPrimitiveDataTypes> result = input.apply(underTest);
    PAssert.that(result.getOutput()).empty();
    PAssert.thatSingleton(result.getErrors().apply(Count.globally())).isEqualTo(1L);
    pipeline.run();
  }

  @Test
  public void givenAllNullableFields_emptyRecord_parsesToRows() {
    PCollection<List<String>> input = emptyCsvRecords(pipeline);
    CsvIORecordToObjects<Row> underTest =
        underTest(
            NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
            nullableAllPrimitiveDataTypesCsvFormat(),
            emptyCustomProcessingMap(),
            ROW_ROW_SERIALIZABLE_FUNCTION,
            NULLABLE_ALL_PRIMITIVE_DATA_TYPES_ROW_CODER);
    CsvIOParseResult<Row> result = input.apply(underTest);
    PAssert.that(result.getOutput()).empty();
    PAssert.that(result.getErrors()).empty();
    pipeline.run();
  }

  @Test
  public void givenAllNullableFields_emptyRecord_parsesToPojos() {
    PCollection<List<String>> input = emptyCsvRecords(pipeline);
    CsvIORecordToObjects<AllPrimitiveDataTypes> underTest =
        underTest(
            ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
            allPrimitiveDataTypesCsvFormat(),
            emptyCustomProcessingMap(),
            allPrimitiveDataTypesFromRowFn(),
            ALL_PRIMITIVE_DATA_TYPES_CODER);
    CsvIOParseResult<AllPrimitiveDataTypes> result = input.apply(underTest);
    PAssert.that(result.getOutput()).empty();
    PAssert.that(result.getErrors()).empty();
    pipeline.run();
  }

  @Test
  public void givenFieldHasCustomProcessing_parsesToRows() {
    PCollection<List<String>> input =
        csvRecords(
            pipeline,
            "2024-07-25T11:25:14.000Z",
            "2024-07-25T11:26:01.000Z,2024-07-25T11:26:22.000Z,2024-07-25T11:26:38.000Z");
    Row want =
        Row.withSchema(TIME_CONTAINING_SCHEMA)
            .withFieldValue("instant", Instant.parse("2024-07-25T11:25:14.000Z"))
            .withFieldValue(
                "instantList",
                Arrays.asList(
                    Instant.parse("2024-07-25T11:26:01.000Z"),
                    Instant.parse("2024-07-25T11:26:22.000Z"),
                    Instant.parse("2024-07-25T11:26:38.000Z")))
            .build();
    CsvIORecordToObjects<Row> underTest =
        underTest(
            TIME_CONTAINING_SCHEMA,
            timeContainingCsvFormat(),
            timeContainingCustomProcessingMap(),
            ROW_ROW_SERIALIZABLE_FUNCTION,
            TIME_CONTAINING_ROW_CODER);
    CsvIOParseResult<Row> result = input.apply(underTest);
    PAssert.that(result.getOutput()).containsInAnyOrder(want);
    PAssert.that(result.getErrors()).empty();
    pipeline.run();
  }

  @Test
  public void givenFieldHasCustomProcessing_parsesToPojos() {
    PCollection<List<String>> input =
        csvRecords(
            pipeline,
            "2024-07-25T11:25:14.000Z",
            "2024-07-25T11:26:01.000Z,2024-07-25T11:26:22.000Z,2024-07-25T11:26:38.000Z");
    TimeContaining want =
        timeContaining(
            Instant.parse("2024-07-25T11:25:14.000Z"),
            Arrays.asList(
                Instant.parse("2024-07-25T11:26:01.000Z"),
                Instant.parse("2024-07-25T11:26:22.000Z"),
                Instant.parse("2024-07-25T11:26:38.000Z")));
    CsvIORecordToObjects<TimeContaining> underTest =
        underTest(
            TIME_CONTAINING_SCHEMA,
            timeContainingCsvFormat(),
            timeContainingCustomProcessingMap(),
            timeContainingFromRowFn(),
            TIME_CONTAINING_POJO_CODER);
    CsvIOParseResult<TimeContaining> result = input.apply(underTest);
    PAssert.that(result.getOutput()).containsInAnyOrder(want);
    PAssert.that(result.getErrors()).empty();
    pipeline.run();
  }

  @Test
  public void givenInvalidCell_throws() {
    PCollection<List<String>> input =
        csvRecords(pipeline, "true", "invalid cell for Decimal", "2.0", "3.0", "4", "5", "foo");
    CsvIORecordToObjects<AllPrimitiveDataTypes> underTest =
        underTest(
            ALL_PRIMITIVE_DATA_TYPES_SCHEMA,
            allPrimitiveDataTypesCsvFormat(),
            emptyCustomProcessingMap(),
            allPrimitiveDataTypesFromRowFn(),
            ALL_PRIMITIVE_DATA_TYPES_CODER);
    CsvIOParseResult<AllPrimitiveDataTypes> result = input.apply(underTest);
    PAssert.that(result.getOutput()).empty();
    PAssert.thatSingleton(result.getErrors().apply(Count.globally())).isEqualTo(1L);

    pipeline.run();
  }

  @Test
  public void givenInvalidCustomProcessing_throws() {
    PCollection<List<String>> input =
        csvRecords(
            pipeline,
            "2024-07-25T11:25:14.000Z",
            "2024-15-25T11:26:01.000Z,2024-24-25T11:26:22.000Z,2024-96-25T11:26:38.000Z");
    CsvIORecordToObjects<TimeContaining> underTest =
        underTest(
            TIME_CONTAINING_SCHEMA,
            timeContainingCsvFormat(),
            timeContainingCustomProcessingMap(),
            timeContainingFromRowFn(),
            TIME_CONTAINING_POJO_CODER);
    CsvIOParseResult<TimeContaining> result = input.apply(underTest);
    PAssert.that(result.getOutput()).empty();
    PAssert.thatSingleton(result.getErrors().apply(Count.globally())).isEqualTo(1L);

    pipeline.run();
  }

  private static PCollection<List<String>> csvRecords(Pipeline pipeline, String... cells) {
    return pipeline.apply(
        Create.of(Collections.singletonList(Arrays.asList(cells)))
            .withCoder(ListCoder.of(NullableCoder.of(StringUtf8Coder.of()))));
  }

  private static PCollection<List<String>> emptyCsvRecords(Pipeline pipeline) {
    return pipeline.apply(Create.empty(ListCoder.of(StringUtf8Coder.of())));
  }

  private static <T> CsvIORecordToObjects<T> underTest(
      Schema schema,
      CSVFormat csvFormat,
      Map<String, SerializableFunction<String, Object>> customProcessingMap,
      SerializableFunction<Row, T> fromRowFn,
      Coder<T> coder) {
    CsvIOParseConfiguration<T> configuration =
        CsvIOParseConfiguration.<T>builder()
            .setSchema(schema)
            .setCsvFormat(csvFormat)
            .setCustomProcessingMap(customProcessingMap)
            .setFromRowFn(fromRowFn)
            .setCoder(coder)
            .build();
    return new CsvIORecordToObjects<>(configuration);
  }

  private static Map<String, SerializableFunction<String, Object>> emptyCustomProcessingMap() {
    return new HashMap<>();
  }

  private static Map<String, SerializableFunction<String, Object>>
      timeContainingCustomProcessingMap() {
    Map<String, SerializableFunction<String, Object>> customProcessingMap = new HashMap<>();
    customProcessingMap.put(
        "instantList",
        input -> {
          List<String> cells = Arrays.asList(input.split(","));
          List<Instant> output = new ArrayList<>();
          cells.forEach(cell -> output.add(Instant.parse(cell)));
          return output;
        });
    return customProcessingMap;
  }

  private static CSVFormat allPrimitiveDataTypesCsvFormat() {
    return CSVFormat.DEFAULT
        .withAllowDuplicateHeaderNames(false)
        .withHeader("aBoolean", "aDecimal", "aDouble", "aFloat", "anInteger", "aLong", "aString");
  }

  private static CSVFormat nullableAllPrimitiveDataTypesCsvFormat() {
    return CSVFormat.DEFAULT
        .withAllowDuplicateHeaderNames(false)
        .withHeader("aBoolean", "aDouble", "aFloat", "anInteger", "aLong", "aString")
        .withNullString("null");
  }

  private static CSVFormat timeContainingCsvFormat() {
    return CSVFormat.DEFAULT
        .withAllowDuplicateHeaderNames(false)
        .withHeader("instant", "instantList");
  }
}
