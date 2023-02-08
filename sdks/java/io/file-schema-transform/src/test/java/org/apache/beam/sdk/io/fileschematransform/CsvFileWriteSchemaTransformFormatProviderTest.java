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

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.DOUBLY_NESTED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.SINGLY_NESTED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration.csvConfigurationBuilder;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestData.DATA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.CSV;
import static org.junit.Assert.assertThrows;

import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CsvWriteSchemaTransformFormatProvider}. */
@RunWith(JUnit4.class)
public class CsvFileWriteSchemaTransformFormatProviderTest
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
    PCollection<Long> actual =
        readPipeline.apply(TextIO.read().from(folder + "*")).apply(Count.globally());
    // TODO(https://github.com/apache/beam/issues/24552) refactor test to compare string CSV to Row
    // converted values.
    PAssert.thatSingleton(actual).notEqualTo(0L);
  }

  @Override
  protected FileWriteSchemaTransformConfiguration buildConfiguration(String folder) {
    return defaultConfiguration(folder)
        .toBuilder()
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
        "arrayPrimitiveDataTypes",
        DATA.arrayPrimitiveDataTypesRows,
        ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA);
  }

  @Override
  public void doublyNestedDataTypesRepeat() {
    assertThrowsWith(
        "doublyNestedDataTypesRepeat",
        DATA.doublyNestedDataTypesRepeatRows,
        DOUBLY_NESTED_DATA_TYPES_SCHEMA);
  }

  @Override
  public void doublyNestedDataTypesNoRepeat() {
    assertThrowsWith(
        "doublyNestedDataTypesNoRepeat",
        DATA.doublyNestedDataTypesNoRepeatRows,
        DOUBLY_NESTED_DATA_TYPES_SCHEMA);
  }

  @Override
  public void singlyNestedDataTypesRepeated() {
    assertThrowsWith(
        "singlyNestedDataTypesRepeated",
        DATA.singlyNestedDataTypesRepeatedRows,
        SINGLY_NESTED_DATA_TYPES_SCHEMA);
  }

  @Override
  public void singlyNestedDataTypesNoRepeat() {
    assertThrowsWith(
        "singlyNestedDataTypesNoRepeat",
        DATA.singlyNestedDataTypesNoRepeatRows,
        SINGLY_NESTED_DATA_TYPES_SCHEMA);
  }

  private void assertThrowsWith(String name, List<Row> rows, Schema schema) {
    PCollection<Row> input = errorPipeline.apply(Create.of(rows).withRowSchema(schema));
    FileWriteSchemaTransformConfiguration configuration = buildConfiguration(name);
    assertThrows(
        IllegalArgumentException.class,
        () -> input.apply(getProvider().buildTransform(configuration, schema)));
  }

  @Override
  public void timeContaining() {
    PCollection<Row> input =
        errorPipeline.apply(
            Create.of(DATA.timeContainingRows).withRowSchema(TIME_CONTAINING_SCHEMA));
    FileWriteSchemaTransformConfiguration configuration = buildConfiguration("timeContaining");
    assertThrows(
        IllegalArgumentException.class,
        () -> input.apply(getProvider().buildTransform(configuration, TIME_CONTAINING_SCHEMA)));
  }
}
