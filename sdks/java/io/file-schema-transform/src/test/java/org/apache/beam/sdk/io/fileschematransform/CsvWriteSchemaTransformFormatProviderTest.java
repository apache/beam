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
package org.apache.beam.sdk.io.fileschematransform;

import static java.util.Objects.requireNonNull;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.BYTE_TYPE_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.DOUBLY_NESTED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.SINGLY_NESTED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration.csvConfigurationBuilder;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestData.DATA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.CSV;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProvider.RESULT_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ByteType;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TimeContaining;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CsvWriteSchemaTransformFormatProvider}. */
@RunWith(JUnit4.class)
public class CsvWriteSchemaTransformFormatProviderTest
    extends FileWriteSchemaTransformFormatProviderTest {

  @Rule
  public TestPipeline errorPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Override
  protected String getFormat() {
    return CSV;
  }

  @Override
  protected String getFilenamePrefix() {
    return "/out";
  }

  @Override
  protected void assertFolderContainsInAnyOrder(String folder, List<Row> rows, Schema beamSchema) {
    PCollection<String> actual = readPipeline.apply(TextIO.read().from(folder + "*"));
    CSVFormat csvFormat =
        CSVFormat.Predefined.valueOf(
                requireNonNull(buildConfiguration(folder).getCsvConfiguration())
                    .getPredefinedCsvFormat())
            .getFormat();
    List<String> expected = toCsv(rows, beamSchema, csvFormat);
    PAssert.that(actual).containsInAnyOrder(expected);
  }

  private static List<String> toCsv(List<Row> rows, Schema beamSchema, CSVFormat csvFormat) {
    beamSchema = beamSchema.sorted();
    List<String> result = new ArrayList<>();
    csvFormat = csvFormat.withSkipHeaderRecord().withHeader();
    String header = csvFormat.format(beamSchema.getFieldNames().toArray());
    result.add(header);
    for (Row row : rows) {
      List<Object> values = new ArrayList<>();
      for (String column : beamSchema.getFieldNames()) {
        values.add(row.getValue(column));
      }
      String record = csvFormat.format(values.toArray());
      result.add(record);
    }
    return result;
  }

  @Override
  protected FileWriteSchemaTransformConfiguration buildConfiguration(String folder) {
    return defaultConfiguration(folder)
        .toBuilder()
        .setNumShards(1)
        .setCsvConfiguration(
            csvConfigurationBuilder()
                .setPredefinedCsvFormat(CSVFormat.Predefined.Default.name())
                .build())
        .build();
  }

  @Override
  protected Optional<String> expectedErrorWhenCompressionSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenParquetConfigurationSet() {
    return Optional.of(
        "configuration with org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration$ParquetConfiguration is not compatible with a csv format");
  }

  @Override
  protected Optional<String> expectedErrorWhenXmlConfigurationSet() {
    return Optional.of(
        "configuration with org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration$XmlConfiguration is not compatible with a csv format");
  }

  @Override
  protected Optional<String> expectedErrorWhenNumShardsSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenShardNameTemplateSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenCsvConfigurationSet() {
    return Optional.empty();
  }

  @Override
  public void arrayPrimitiveDataTypes() {
    assertThrowsWith(
        "columns in header match fields in Schema with invalid types: integerList,stringList,doubleList,floatList,booleanList,longList. See CsvIO#VALID_FIELD_TYPE_SET for a list of valid field types.",
        "arrayPrimitiveDataTypes",
        DATA.arrayPrimitiveDataTypesRows,
        ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA);
  }

  @Override
  public void doublyNestedDataTypesRepeat() {
    assertThrowsWith(
        "columns in header match fields in Schema with invalid types: singlyNestedDataTypes,singlyNestedDataTypesList. See CsvIO#VALID_FIELD_TYPE_SET for a list of valid field types.",
        "doublyNestedDataTypesRepeat",
        DATA.doublyNestedDataTypesRepeatRows,
        DOUBLY_NESTED_DATA_TYPES_SCHEMA);
  }

  @Override
  public void doublyNestedDataTypesNoRepeat() {
    assertThrowsWith(
        "columns in header match fields in Schema with invalid types: singlyNestedDataTypes,singlyNestedDataTypesList. See CsvIO#VALID_FIELD_TYPE_SET for a list of valid field types.",
        "doublyNestedDataTypesNoRepeat",
        DATA.doublyNestedDataTypesNoRepeatRows,
        DOUBLY_NESTED_DATA_TYPES_SCHEMA);
  }

  @Override
  public void singlyNestedDataTypesRepeated() {
    assertThrowsWith(
        "columns in header match fields in Schema with invalid types: allPrimitiveDataTypes,allPrimitiveDataTypesList. See CsvIO#VALID_FIELD_TYPE_SET for a list of valid field types.",
        "singlyNestedDataTypesRepeated",
        DATA.singlyNestedDataTypesRepeatedRows,
        SINGLY_NESTED_DATA_TYPES_SCHEMA);
  }

  @Override
  public void singlyNestedDataTypesNoRepeat() {
    assertThrowsWith(
        "columns in header match fields in Schema with invalid types: allPrimitiveDataTypes,allPrimitiveDataTypesList. See CsvIO#VALID_FIELD_TYPE_SET for a list of valid field types.",
        "singlyNestedDataTypesNoRepeat",
        DATA.singlyNestedDataTypesNoRepeatRows,
        SINGLY_NESTED_DATA_TYPES_SCHEMA);
  }

  private void assertThrowsWith(
      String expectedMessage, String name, List<Row> rows, Schema schema) {
    PCollection<Row> input = errorPipeline.apply(Create.of(rows).withRowSchema(schema));
    FileWriteSchemaTransformConfiguration configuration = buildConfiguration(name);
    IllegalArgumentException exception =
        assertThrows(
            name,
            IllegalArgumentException.class,
            () -> input.apply(getProvider().buildTransform(configuration, schema)));

    assertEquals(name, expectedMessage, exception.getMessage());
  }

  @Override
  public void timeContaining() {
    PCollection<Row> invalidInput =
        errorPipeline.apply(
            Create.of(DATA.timeContainingRows).withRowSchema(TIME_CONTAINING_SCHEMA));
    FileWriteSchemaTransformConfiguration configuration = buildConfiguration("timeContaining");
    IllegalArgumentException exception =
        assertThrows(
            "Schema should throw Exception for containing a field with a repeated type",
            IllegalArgumentException.class,
            () ->
                invalidInput.apply(
                    getProvider().buildTransform(configuration, TIME_CONTAINING_SCHEMA)));

    assertEquals(
        "columns in header match fields in Schema with invalid types: instantList. See CsvIO#VALID_FIELD_TYPE_SET for a list of valid field types.",
        exception.getMessage());
  }

  @Test
  public void timeContainingSchemaWithListRemovedShouldWriteCSV() {
    String prefix =
        folder(TimeContaining.class, "timeContainingSchemaWithListRemovedShouldWriteCSV");
    String validField = "instant";
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.timeContainingRows).withRowSchema(TIME_CONTAINING_SCHEMA));
    PCollection<Row> modifiedInput = input.apply(Select.fieldNames(validField));
    Schema modifiedSchema = modifiedInput.getSchema();
    FileWriteSchemaTransformConfiguration configuration = buildConfiguration(prefix);
    PCollection<String> result =
        modifiedInput
            .apply(getProvider().buildTransform(configuration, modifiedSchema))
            .get(RESULT_TAG);
    PCollection<Long> numFiles = result.apply(Count.globally());
    PAssert.thatSingleton(numFiles).isEqualTo(1L);
    writePipeline.run().waitUntilFinish();

    PCollection<String> csv =
        readPipeline.apply(TextIO.read().from(configuration.getFilenamePrefix() + "*"));
    List<String> expected = new ArrayList<>();
    expected.add(validField);
    DateTimeFormatter formatter = ISODateTimeFormat.dateTime();
    for (Row row : DATA.timeContainingRows) {
      expected.add(formatter.print(row.getDateTime(validField)));
    }
    PAssert.that(csv).containsInAnyOrder(expected);
    readPipeline.run();
  }

  @Test
  public void byteTypeNonRepeated() {
    System.out.println("BYTE TYPE NON REPEATED");
    String prefix = folder(ByteType.class, "byteTypeNonRepeated");
    String validField = "byte";
    PCollection<Row> input =
        writePipeline.apply(Create.of(DATA.byteTypeRows).withRowSchema(BYTE_TYPE_SCHEMA));
    PCollection<Row> modifiedInput = input.apply(Select.fieldNames(validField));
    Schema modifiedSchema = modifiedInput.getSchema();
    FileWriteSchemaTransformConfiguration configuration = buildConfiguration(prefix);
    PCollection<String> result =
        modifiedInput
            .apply(getProvider().buildTransform(configuration, modifiedSchema))
            .get(RESULT_TAG);
    PCollection<Long> numFiles = result.apply(Count.globally());
    PAssert.thatSingleton(numFiles).isEqualTo(1L);
    writePipeline.run().waitUntilFinish();

    PCollection<String> csv =
        readPipeline.apply(TextIO.read().from(configuration.getFilenamePrefix() + "*"));
    List<String> expected = new ArrayList<>();
    expected.add(validField);
    for (Row row : DATA.byteTypeRows) {
      expected.add(row.getByte(validField).toString());
    }
    PAssert.that(csv).containsInAnyOrder(expected);
    readPipeline.run();
  }
}
