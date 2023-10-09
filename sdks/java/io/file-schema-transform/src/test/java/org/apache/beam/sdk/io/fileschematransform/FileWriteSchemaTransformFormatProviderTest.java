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
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration.csvConfigurationBuilder;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration.parquetConfigurationBuilder;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration.xmlConfigurationBuilder;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestData.DATA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.loadProviders;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProvider.RESULT_TAG;
import static org.apache.beam.sdk.values.TypeDescriptors.booleans;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.AllPrimitiveDataTypes;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Base class for tests of {@link FileWriteSchemaTransformFormatProvider} implementations. */
abstract class FileWriteSchemaTransformFormatProviderTest {

  /**
   * The {@link FileWriteSchemaTransformConfiguration#getFormat()} mapped to this {@link
   * FileWriteSchemaTransformFormatProvider}.
   */
  protected abstract String getFormat();

  /**
   * The filename prefix of sharded files, required by {@link org.apache.beam.sdk.io.TextIO.Write}
   * based {@link FileWriteSchemaTransformFormatProvider}s.
   */
  protected abstract String getFilenamePrefix();

  /**
   * Asserts whether the {@link FileWriteSchemaTransformFormatProvider} wrote expected contents to
   * {@link FileWriteSchemaTransformConfiguration#getFilenamePrefix()}.
   */
  protected abstract void assertFolderContainsInAnyOrder(
      String folder, List<Row> rows, Schema beamSchema);

  /**
   * Builds {@link FileWriteSchemaTransformConfiguration} specifying the folder {@link
   * FileWriteSchemaTransformConfiguration#getFilenamePrefix()}.
   */
  protected abstract FileWriteSchemaTransformConfiguration buildConfiguration(String folder);

  /**
   * The expected error message when {@link FileWriteSchemaTransformConfiguration#getCompression()}
   * is not null.
   */
  protected abstract Optional<String> expectedErrorWhenCompressionSet();

  @Test
  public void withCompression() {
    String to = folder(AllPrimitiveDataTypes.class, "with_compression");
    Compression compression = Compression.GZIP;
    FileWriteSchemaTransformConfiguration configuration =
        buildConfiguration(to).toBuilder().setCompression(compression.name()).build();

    FileWriteSchemaTransformProvider provider = new FileWriteSchemaTransformProvider();

    if (expectedErrorWhenCompressionSet().isPresent()) {
      IllegalArgumentException invalidConfiguration =
          assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));
      assertEquals(expectedErrorWhenCompressionSet().get(), invalidConfiguration.getMessage());
      return;
    }

    PCollection<String> files =
        applyProviderAndAssertFilesWritten(
            DATA.allPrimitiveDataTypesRows, ALL_PRIMITIVE_DATA_TYPES_SCHEMA, configuration);

    PCollection<String> extension =
        files
            .apply(
                "extract extension",
                MapElements.into(strings())
                    .via(fullName -> fullName != null ? Files.getFileExtension(fullName) : null))
            .apply("distinct extensions", Distinct.create());

    PCollection<Boolean> isCompressed =
        files
            .apply(
                "isCompressed",
                MapElements.into(booleans())
                    .via(filename -> filename != null && compression.isCompressed(filename)))
            .apply("distinct isCompressed", Distinct.create());

    PAssert.thatSingleton("Filenames end with compression name", extension).isEqualTo("gz");

    PAssert.thatSingleton("Files should be compressed", isCompressed).isEqualTo(true);

    writePipeline.run();
  }

  /**
   * The expected error message when {@link
   * FileWriteSchemaTransformConfiguration#getParquetConfiguration()} ()} is not null.
   */
  protected abstract Optional<String> expectedErrorWhenParquetConfigurationSet();

  @Test
  public void invalidConfigurationWithParquet() {
    String to = folder(getFormat(), "configuration_with_parquet");
    FileWriteSchemaTransformConfiguration configuration =
        buildConfiguration(to)
            .toBuilder()
            .setParquetConfiguration(
                parquetConfigurationBuilder()
                    .setCompressionCodecName(CompressionCodecName.GZIP.name())
                    .build())
            .build();

    FileWriteSchemaTransformProvider provider = new FileWriteSchemaTransformProvider();

    if (!expectedErrorWhenParquetConfigurationSet().isPresent()) {
      // we do not expect an error
      provider.from(configuration);
      return;
    }

    IllegalArgumentException invalidConfigurationError =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertEquals(
        expectedErrorWhenParquetConfigurationSet().get(), invalidConfigurationError.getMessage());
  }

  /**
   * The expected error message when {@link
   * FileWriteSchemaTransformConfiguration#getXmlConfiguration()} ()} is not null.
   */
  protected abstract Optional<String> expectedErrorWhenXmlConfigurationSet();

  @Test
  public void invalidConfigurationWithXml() {
    String to = folder(getFormat(), "configuration_with_xml");
    FileWriteSchemaTransformConfiguration configuration =
        buildConfiguration(to)
            .toBuilder()
            .setXmlConfiguration(
                xmlConfigurationBuilder()
                    .setRootElement("rootElement")
                    .setCharset(Charset.defaultCharset().name())
                    .build())
            .build();

    FileWriteSchemaTransformProvider provider = new FileWriteSchemaTransformProvider();
    if (!expectedErrorWhenXmlConfigurationSet().isPresent()) {
      // No error expected
      provider.from(configuration);
      return;
    }

    IllegalArgumentException configurationError =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));

    assertEquals(expectedErrorWhenXmlConfigurationSet().get(), configurationError.getMessage());
  }

  /**
   * The expected error message when {@link FileWriteSchemaTransformConfiguration#getNumShards()}
   * ()} is not null.
   */
  protected abstract Optional<String> expectedErrorWhenNumShardsSet();

  @Test
  public void numShardsSetConfiguration() {
    String to = folder(AllPrimitiveDataTypes.class, "num_shards_configuration");
    int expectedNumShards = 10;
    FileWriteSchemaTransformConfiguration configuration =
        buildConfiguration(to).toBuilder().setNumShards(expectedNumShards).build();

    if (expectedErrorWhenNumShardsSet().isPresent()) {
      FileWriteSchemaTransformProvider provider = new FileWriteSchemaTransformProvider();
      IllegalArgumentException configurationError =
          assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));
      assertEquals(expectedErrorWhenNumShardsSet().get(), configurationError.getMessage());
      return;
    }

    List<Row> rows = new ArrayList<>(DATA.allPrimitiveDataTypesRows);
    for (int i = 0; i < 100; i++) {
      rows.addAll(DATA.allPrimitiveDataTypesRows);
    }

    PCollection<String> files =
        applyProviderAndAssertFilesWritten(rows, ALL_PRIMITIVE_DATA_TYPES_SCHEMA, configuration);
    PCollection<Long> count = files.apply(Count.globally());
    PAssert.thatSingleton("Amount of files created should match numShards", count)
        .isEqualTo(Integer.valueOf(expectedNumShards).longValue());

    writePipeline.run();
  }

  /**
   * The expected error message when {@link
   * FileWriteSchemaTransformConfiguration#getShardNameTemplate()} ()} is not null.
   */
  protected abstract Optional<String> expectedErrorWhenShardNameTemplateSet();

  @Test
  public void shardNameTemplateSetConfiguration() {
    String to = folder(AllPrimitiveDataTypes.class, "shard_name_template");
    String shardNameTemplate = "-SS-of-NN";
    FileWriteSchemaTransformConfiguration configuration =
        buildConfiguration(to).toBuilder().setShardNameTemplate(shardNameTemplate).build();

    if (expectedErrorWhenShardNameTemplateSet().isPresent()) {
      FileWriteSchemaTransformProvider provider = new FileWriteSchemaTransformProvider();
      IllegalArgumentException configurationError =
          assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));
      assertEquals(expectedErrorWhenShardNameTemplateSet().get(), configurationError.getMessage());
      return;
    }

    PCollection<String> files =
        applyProviderAndAssertFilesWritten(
            DATA.allPrimitiveDataTypesRows, ALL_PRIMITIVE_DATA_TYPES_SCHEMA, configuration);
    PAssert.that("All file names match shard name template", files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              for (String name : names) {
                assertTrue(name.matches("^.*\\d\\d-of-\\d\\d.*$"));
              }
              return null;
            });

    writePipeline.run();
  }

  /**
   * The expected error message when {@link
   * FileWriteSchemaTransformConfiguration#getCsvConfiguration()} ()} is not null.
   */
  protected abstract Optional<String> expectedErrorWhenCsvConfigurationSet();

  @Test
  public void csvConfigurationSet() {
    String to = folder(getFormat(), "csv_configuration");
    FileWriteSchemaTransformProvider provider = new FileWriteSchemaTransformProvider();
    FileWriteSchemaTransformConfiguration configuration =
        buildConfiguration(to)
            .toBuilder()
            .setCsvConfiguration(
                csvConfigurationBuilder()
                    .setPredefinedCsvFormat(CSVFormat.Predefined.Default.name())
                    .build())
            .build();
    if (!expectedErrorWhenCsvConfigurationSet().isPresent()) {
      // No error expected
      provider.from(configuration);
      return;
    }
    IllegalArgumentException configurationError =
        assertThrows(IllegalArgumentException.class, () -> provider.from(configuration));
    assertEquals(expectedErrorWhenCsvConfigurationSet().get(), configurationError.getMessage());
  }

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void allPrimitiveDataTypes() {
    String to = folder(SchemaAwareJavaBeans.AllPrimitiveDataTypes.class);
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.allPrimitiveDataTypesRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    writePipeline.run().waitUntilFinish();
    assertFolderContainsInAnyOrder(to, rows, schema);
    readPipeline.run();
  }

  @Test
  public void nullableAllPrimitiveDataTypes() {
    String to = folder(SchemaAwareJavaBeans.NullableAllPrimitiveDataTypes.class);
    Schema schema = NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.nullableAllPrimitiveDataTypesRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    writePipeline.run().waitUntilFinish();
    assertFolderContainsInAnyOrder(to, rows, schema);
    readPipeline.run();
  }

  @Test
  public void timeContaining() {
    String to = folder(SchemaAwareJavaBeans.TimeContaining.class);
    Schema schema = TIME_CONTAINING_SCHEMA;
    List<Row> rows = DATA.timeContainingRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    writePipeline.run().waitUntilFinish();
    assertFolderContainsInAnyOrder(to, rows, schema);
    readPipeline.run();
  }

  @Test
  public void byteTypes() {
    List<String> formatsThatSupportSingleByteType = Arrays.asList("json", "xml");
    assumeTrue(formatsThatSupportSingleByteType.contains(getFormat()));

    String to = folder(SchemaAwareJavaBeans.ByteType.class);
    Schema schema = BYTE_TYPE_SCHEMA;
    List<Row> rows = DATA.byteTypeRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    writePipeline.run().waitUntilFinish();
    assertFolderContainsInAnyOrder(to, rows, schema);
    readPipeline.run();
  }

  @Test
  public void byteSequenceTypes() {
    List<String> formatsThatSupportByteSequenceType = Arrays.asList("avro", "parquet");
    assumeTrue(formatsThatSupportByteSequenceType.contains(getFormat()));

    String to = folder(SchemaAwareJavaBeans.ByteSequenceType.class);
    Schema schema = BYTE_SEQUENCE_TYPE_SCHEMA;
    List<Row> rows = DATA.byteSequenceTypeRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    writePipeline.run().waitUntilFinish();
    assertFolderContainsInAnyOrder(to, rows, schema);
    readPipeline.run();
  }

  @Test
  public void arrayPrimitiveDataTypes() {
    String to = folder(SchemaAwareJavaBeans.ArrayPrimitiveDataTypes.class);
    Schema schema = ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.arrayPrimitiveDataTypesRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    writePipeline.run().waitUntilFinish();
    assertFolderContainsInAnyOrder(to, rows, schema);
    readPipeline.run();
  }

  @Test
  public void singlyNestedDataTypesNoRepeat() {
    String to = folder(SchemaAwareJavaBeans.SinglyNestedDataTypes.class, "no_repeat");
    Schema schema = SINGLY_NESTED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.singlyNestedDataTypesNoRepeatRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    writePipeline.run().waitUntilFinish();
    assertFolderContainsInAnyOrder(to, rows, schema);
    readPipeline.run();
  }

  @Test
  public void singlyNestedDataTypesRepeated() {
    String to = folder(SchemaAwareJavaBeans.SinglyNestedDataTypes.class, "repeated");
    Schema schema = SINGLY_NESTED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.singlyNestedDataTypesNoRepeatRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    writePipeline.run().waitUntilFinish();
    assertFolderContainsInAnyOrder(to, rows, schema);
    readPipeline.run();
  }

  @Test
  public void doublyNestedDataTypesNoRepeat() {
    String to = folder(SchemaAwareJavaBeans.DoublyNestedDataTypes.class, "no_repeat");
    Schema schema = DOUBLY_NESTED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.doublyNestedDataTypesNoRepeatRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    writePipeline.run().waitUntilFinish();
    assertFolderContainsInAnyOrder(to, rows, schema);
    readPipeline.run();
  }

  @Test
  public void doublyNestedDataTypesRepeat() {
    String to = folder(SchemaAwareJavaBeans.DoublyNestedDataTypes.class, "repeated");
    Schema schema = DOUBLY_NESTED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.doublyNestedDataTypesRepeatRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    writePipeline.run().waitUntilFinish();
    assertFolderContainsInAnyOrder(to, rows, schema);
    readPipeline.run();
  }

  protected FileWriteSchemaTransformFormatProvider getProvider() {
    return loadProviders().get(getFormat());
  }

  protected <T> String folder(Class<T> clazz, String additionalPath) {
    return folder(getFormat(), clazz.getSimpleName(), additionalPath);
  }

  private <T> String folder(Class<T> clazz) {
    return folder(getFormat(), clazz.getSimpleName());
  }

  private String folder(String... paths) {
    try {
      return tmpFolder.newFolder(paths).getAbsolutePath() + getFilenamePrefix();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private PCollection<String> applyProviderAndAssertFilesWritten(
      String folder, List<Row> rows, Schema schema) {
    return applyProviderAndAssertFilesWritten(rows, schema, buildConfiguration(folder));
  }

  private PCollection<String> applyProviderAndAssertFilesWritten(
      List<Row> rows, Schema schema, FileWriteSchemaTransformConfiguration configuration) {
    PCollection<Row> input = writePipeline.apply(Create.of(rows).withRowSchema(schema));
    PCollection<String> files =
        input.apply(getProvider().buildTransform(configuration, schema)).get(RESULT_TAG);
    PCollection<Long> count = files.apply("count number of files", Count.globally());
    PAssert.thatSingleton("At least one file should be written", count).notEqualTo(0L);
    return files;
  }

  protected FileWriteSchemaTransformConfiguration defaultConfiguration(String folder) {
    return FileWriteSchemaTransformConfiguration.builder()
        .setFormat(getFormat())
        .setFilenamePrefix(folder)
        .build();
  }
}
