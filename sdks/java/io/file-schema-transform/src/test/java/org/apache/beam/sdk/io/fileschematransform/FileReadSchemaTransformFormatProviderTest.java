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

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.BYTE_SEQUENCE_TYPE_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.BYTE_TYPE_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.DOUBLY_NESTED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.SINGLY_NESTED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.apache.beam.sdk.io.fileschematransform.FileReadSchemaTransformProvider.FILEPATTERN_ROW_FIELD_NAME;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration.parquetConfigurationBuilder;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestData.DATA;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public abstract class FileReadSchemaTransformFormatProviderTest {

  /** Returns the format of the {@linke FileReadSchemaTransformFormatProviderTest} subclass. */
  protected abstract String getFormat();

  /** Given a Beam Schema, returns the relevant source's String schema representation. */
  protected abstract String getStringSchemaFromBeamSchema(Schema beamSchema);

  /**
   * Writes {@link Row}s to files then reads from those files. Performs a {@link
   * org.apache.beam.sdk.testing.PAssert} check to validate the written and read {@link Row}s are
   * equal.
   */
  protected abstract void runWriteAndReadTest(
      Schema schema, List<Row> rows, String filePath, String schemaFilePath);

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Rule public TestName testName = new TestName();

  protected Schema getFilepatternSchema() {
    return Schema.of(Field.of("filepattern", FieldType.STRING));
  }

  protected String getFilePath() {
    return getFolder() + "/test";
  }

  protected String getFolder() {
    try {
      return tmpFolder.newFolder(getFormat(), testName.getMethodName()).getAbsolutePath();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  public void testInvalidConfigsFailToBuild() {
    List<FileReadSchemaTransformConfiguration.Builder> invalidConfigs =
        Arrays.asList(
            // No schema set
            FileReadSchemaTransformConfiguration.builder().setFormat(getFormat()),
            // Invalid format
            FileReadSchemaTransformConfiguration.builder().setFormat("invalid format"),
            // Setting terminate seconds without a poll interval
            FileReadSchemaTransformConfiguration.builder()
                .setFormat(getFormat())
                .setSchema("schema")
                .setTerminateAfterSecondsSinceNewOutput(10L));

    for (FileReadSchemaTransformConfiguration.Builder config : invalidConfigs) {
      assertThrows(
          IllegalArgumentException.class,
          () -> {
            config.build();
          });
    }
  }

  @Test
  public void testAllPrimitiveDataTypes() {
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.allPrimitiveDataTypesRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath, null);
  }

  @Test
  public void testNullableAllPrimitiveDataTypes() {
    Schema schema = NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.nullableAllPrimitiveDataTypesRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath, null);
  }

  @Test
  public void testTimeContaining() {
    // JSON schemas don't support DATETIME or other logical types
    assumeTrue(!getFormat().equals("json"));

    Schema schema = TIME_CONTAINING_SCHEMA;
    List<Row> rows = DATA.timeContainingRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath, null);
  }

  @Test
  public void testByteType() {
    List<String> formatsThatSupportSingleByteType = Arrays.asList("csv", "json", "xml");
    assumeTrue(formatsThatSupportSingleByteType.contains(getFormat()));

    Schema schema = BYTE_TYPE_SCHEMA;
    List<Row> rows = DATA.byteTypeRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath, null);
  }

  @Test
  public void testByteSequenceType() {
    List<String> formatsThatSupportByteSequenceType = Arrays.asList("avro", "parquet");
    assumeTrue(formatsThatSupportByteSequenceType.contains(getFormat()));

    Schema schema = BYTE_SEQUENCE_TYPE_SCHEMA;
    List<Row> rows = DATA.byteSequenceTypeRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath, null);
  }

  @Test
  public void testArrayPrimitiveDataTypes() {
    Schema schema = ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.arrayPrimitiveDataTypesRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath, null);
  }

  @Test
  public void testNestedRepeatedDataTypes() {
    Schema schema = SINGLY_NESTED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.singlyNestedDataTypesRepeatedRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath, null);
  }

  @Test
  public void testDoublyNestedRepeatedDataTypes() {
    Schema schema = DOUBLY_NESTED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.doublyNestedDataTypesRepeatRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath, null);
  }

  @Test
  public void testReadWithSchemaFilePath() throws Exception {
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.allPrimitiveDataTypesRows;
    String folder = getFolder();
    String filePath = folder + "/test";
    String schemaFilePath = folder + "all_primitive_data_types_schema";

    String schemaString = getStringSchemaFromBeamSchema(schema);
    PrintWriter writer = new PrintWriter(schemaFilePath, StandardCharsets.UTF_8.name());
    writer.println(schemaString);
    writer.close();
    runWriteAndReadTest(schema, rows, filePath, null);
  }

  @Test
  public void testWriteAndReadWithSchemaTransforms() {
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.allPrimitiveDataTypesRows;
    String folder = getFolder();

    FileWriteSchemaTransformConfiguration writeConfig =
        FileWriteSchemaTransformConfiguration.builder()
            .setFormat(getFormat())
            .setFilenamePrefix(folder)
            .build();
    if (getFormat().equals("parquet")) {
      writeConfig =
          writeConfig
              .toBuilder()
              .setParquetConfiguration(
                  parquetConfigurationBuilder()
                      .setCompressionCodecName(CompressionCodecName.GZIP.name())
                      .build())
              .build();
    }
    FileReadSchemaTransformConfiguration readConfig =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat(getFormat())
            .setSchema(getStringSchemaFromBeamSchema(schema))
            .build();

    SchemaTransform writeTransform = new FileWriteSchemaTransformProvider().from(writeConfig);
    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(readConfig);

    Schema filePatternSchema = getFilepatternSchema();
    PCollection<Row> inputRows = writePipeline.apply(Create.of(rows).withRowSchema(schema));
    PCollection<Row> filePatterns =
        PCollectionRowTuple.of("input", inputRows)
            .apply(writeTransform)
            .get("output")
            .setRowSchema(FileWriteSchemaTransformProvider.OUTPUT_SCHEMA)
            .apply(
                MapElements.into(TypeDescriptors.rows())
                    .via(
                        (Row row) -> {
                          String file = row.getString("fileName");
                          return Row.withSchema(filePatternSchema)
                              .withFieldValue(FILEPATTERN_ROW_FIELD_NAME, file)
                              .build();
                        }))
            .setRowSchema(filePatternSchema);

    PCollection<Row> outputRows =
        PCollectionRowTuple.of("input", filePatterns).apply(readTransform).get("output");

    if (getFormat().equals("json")) {
      rows =
          rows.stream()
              .map(row -> JsonReadSchemaTransformFormatProviderTest.getExpectedRow(row))
              .collect(Collectors.toList());
    }
    PAssert.that(outputRows).containsInAnyOrder(rows);
    writePipeline.run().waitUntilFinish();
  }
}
