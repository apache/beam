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
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.io.Compression.AUTO;
import static org.apache.beam.sdk.io.Compression.DEFLATE;
import static org.apache.beam.sdk.io.Compression.GZIP;
import static org.apache.beam.sdk.io.Compression.UNCOMPRESSED;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.TFRecordReadSchemaTransformProvider.TFRecordReadSchemaTransform;
import org.apache.beam.sdk.io.TFRecordWriteSchemaTransformProvider.TFRecordWriteSchemaTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for TFRecordIO Read and Write transforms. */
@RunWith(JUnit4.class)
public class TFRecordSchemaTransformProviderTest {

  /*
  From https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/tfrecordio_test.py
  Created by running following code in python:
  >>> import tensorflow as tf
  >>> import base64
  >>> writer = tf.python_io.TFRecordWriter('/tmp/python_foo.tfrecord')
  >>> writer.write('foo')
  >>> writer.close()
  >>> with open('/tmp/python_foo.tfrecord', 'rb') as f:
  ...   data = base64.b64encode(f.read())
  ...   print data
  */
  private static final String FOO_RECORD_BASE64 = "AwAAAAAAAACwmUkOZm9vYYq+/g==";

  // Same as above but containing two records ['foo', 'bar']
  private static final String FOO_BAR_RECORD_BASE64 =
      "AwAAAAAAAACwmUkOZm9vYYq+/gMAAAAAAAAAsJlJDmJhckYA5cg=";
  private static final String BAR_FOO_RECORD_BASE64 =
      "AwAAAAAAAACwmUkOYmFyRgDlyAMAAAAAAAAAsJlJDmZvb2GKvv4=";

  private static final String[] FOO_RECORDS = {"foo"};
  private static final String[] FOO_BAR_RECORDS = {"foo", "bar"};

  private static final Iterable<String> EMPTY = Collections.emptyList();
  private static final Iterable<String> SMALL = makeLines(10, 4);
  private static final Iterable<String> LARGE = makeLines(1000, 4);
  private static final Iterable<String> LARGE_RECORDS = makeLines(100, 100000);

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testReadInvalidConfigurations() {
    String filePattern = "foo.*";
    String compression = "AUTO";

    // Invalid filepattern
    assertThrows(
        IllegalStateException.class,
        () -> {
          TFRecordReadSchemaTransformConfiguration.builder()
              .setValidate(true)
              .setCompression(compression)
              .setFilePattern(filePattern)
              .build()
              .validate();
        });

    // Filepattern unset
    assertThrows(
        IllegalStateException.class,
        () -> {
          TFRecordReadSchemaTransformConfiguration.builder()
              .setValidate(true)
              .setCompression(compression)
              // .setFilePattern(StaticValueProvider.of("vegetable")) File pattern is mandatory
              .build()
              .validate();
        });

    // Validate unset
    assertThrows(
        IllegalStateException.class,
        () -> {
          TFRecordReadSchemaTransformConfiguration.builder()
              // .setValidate(true) // Validate is mandatory
              .setCompression(compression)
              .setFilePattern(filePattern)
              .build()
              .validate();
        });

    // Compression unset
    assertThrows(
        IllegalStateException.class,
        () -> {
          TFRecordReadSchemaTransformConfiguration.builder()
              .setValidate(false)
              // .setCompression(Compression.AUTO) // Compression is mandatory
              .setFilePattern(filePattern)
              .build()
              .validate();
        });
  }

  @Test
  public void testWriteInvalidConfigurations() throws Exception {
    String fileName = "foo";
    String filenameSuffix = "bar";
    String shardTemplate = "xyz";
    String compression = "AUTO";
    Integer numShards = 10;

    // NumShards unset
    assertThrows(
        IllegalStateException.class,
        () -> {
          TFRecordWriteSchemaTransformConfiguration.builder()
              .setOutputPrefix(tempFolder.getRoot().toPath().toString() + fileName)
              .setFilenameSuffix(filenameSuffix)
              .setShardTemplate(shardTemplate)
              // .setNumShards(numShards) // NumShards is mandatory
              .setCompression(compression)
              .setNoSpilling(true)
              .build();
        });

    // Compression unset
    assertThrows(
        IllegalStateException.class,
        () -> {
          TFRecordWriteSchemaTransformConfiguration.builder()
              .setOutputPrefix(tempFolder.getRoot().toPath().toString() + fileName)
              .setFilenameSuffix(filenameSuffix)
              .setShardTemplate(shardTemplate)
              .setNumShards(numShards)
              // .setCompression(compression) // Compression is mandatory
              .setNoSpilling(true)
              .build();
        });

    // NoSpilling unset
    assertThrows(
        IllegalStateException.class,
        () -> {
          TFRecordWriteSchemaTransformConfiguration.builder()
              // .setOutputPrefix(tempFolder.getRoot().toPath().toString() + fileName) //
              // outputPrefix is mandatory
              .setFilenameSuffix(filenameSuffix)
              .setShardTemplate(shardTemplate)
              .setNumShards(numShards)
              .setCompression(compression)
              .build();
        });
  }

  @Test
  public void testReadBuildTransform() {
    TFRecordReadSchemaTransformProvider provider = new TFRecordReadSchemaTransformProvider();
    provider.from(
        TFRecordReadSchemaTransformConfiguration.builder()
            .setValidate(false)
            .setCompression("AUTO")
            .setFilePattern("foo.*")
            .build());
  }

  @Test
  public void testWriteBuildTransform() {
    TFRecordWriteSchemaTransformProvider provider = new TFRecordWriteSchemaTransformProvider();
    provider.from(
        TFRecordWriteSchemaTransformConfiguration.builder()
            .setOutputPrefix(tempFolder.getRoot().toPath().toString())
            .setFilenameSuffix("bar")
            .setShardTemplate("xyz")
            .setNumShards(10)
            .setCompression("UNCOMPRESSED")
            .setNoSpilling(true)
            .build());
  }

  @Test
  public void testReadFindTransformAndMakeItWork() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == TFRecordReadSchemaTransformProvider.class)
            .collect(Collectors.toList());
    SchemaTransformProvider tfrecordProvider = providers.get(0);
    assertEquals(tfrecordProvider.outputCollectionNames(), Lists.newArrayList("output", "errors"));
    assertEquals(tfrecordProvider.inputCollectionNames(), Lists.newArrayList());

    assertEquals(
        Sets.newHashSet("file_pattern", "compression", "validate", "error_handling"),
        tfrecordProvider.configurationSchema().getFields().stream()
            .map(field -> field.getName())
            .collect(Collectors.toSet()));
  }

  @Test
  public void testWriteFindTransformAndMakeItWork() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == TFRecordWriteSchemaTransformProvider.class)
            .collect(Collectors.toList());
    SchemaTransformProvider tfrecordProvider = providers.get(0);
    assertEquals(tfrecordProvider.outputCollectionNames(), Lists.newArrayList("output", "errors"));

    assertEquals(
        Sets.newHashSet(
            "output_prefix",
            "filename_suffix",
            "shard_template",
            "num_shards",
            "compression",
            "no_spilling",
            "max_num_writers_per_bundle",
            "error_handling"),
        tfrecordProvider.configurationSchema().getFields().stream()
            .map(field -> field.getName())
            .collect(Collectors.toSet()));
  }

  /** Tests that TFRecordReadSchemaTransformProvider is presented. */
  @Test
  public void testReadNamed() {
    readPipeline.enableAbandonedNodeEnforcement(false);
    PCollectionRowTuple begin = PCollectionRowTuple.empty(readPipeline);
    SchemaTransform transform =
        new TFRecordReadSchemaTransformProvider()
            .from(
                TFRecordReadSchemaTransformConfiguration.builder()
                    .setValidate(false)
                    .setCompression("AUTO")
                    .setFilePattern("foo.*")
                    .build());

    PCollectionRowTuple reads = begin.apply(transform);
    String name = reads.get("output").getName();
    assertThat(name, startsWith("TFRecordReadSchemaTransformProvider.TFRecordReadSchemaTransform"));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadOne() throws Exception {
    runTestRead(FOO_RECORD_BASE64, FOO_RECORDS);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadTwo() throws Exception {
    runTestRead(FOO_BAR_RECORD_BASE64, FOO_BAR_RECORDS);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteOne() throws Exception {
    runTestWrite(FOO_RECORDS, FOO_RECORD_BASE64);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteTwo() throws Exception {
    runTestWrite(FOO_BAR_RECORDS, FOO_BAR_RECORD_BASE64, BAR_FOO_RECORD_BASE64);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadInvalidRecord() throws Exception {
    expectedException.expectMessage("Not a valid TFRecord. Fewer than 12 bytes.");
    runTestRead("bar".getBytes(StandardCharsets.UTF_8), new String[0]);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadInvalidLengthMask() throws Exception {
    expectedException.expectCause(hasMessage(containsString("Mismatch of length mask")));
    byte[] data = BaseEncoding.base64().decode(FOO_RECORD_BASE64);
    data[9] += (byte) 1;
    runTestRead(data, FOO_RECORDS);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadInvalidDataMask() throws Exception {
    expectedException.expectCause(hasMessage(containsString("Mismatch of data mask")));
    byte[] data = BaseEncoding.base64().decode(FOO_RECORD_BASE64);
    data[16] += (byte) 1;
    runTestRead(data, FOO_RECORDS);
  }

  private void runTestRead(String base64, String[] expected) throws IOException {
    runTestRead(BaseEncoding.base64().decode(base64), expected);
  }

  /** Tests {@link TFRecordReadSchemaTransformProvider}. */
  private void runTestRead(byte[] data, String[] expected) throws IOException {
    // Create temp filename
    File tmpFile =
        Files.createTempFile(tempFolder.getRoot().toPath(), "file", ".tfrecords").toFile();
    String filename = tmpFile.getPath();
    try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
      fos.write(data);
    }

    // Create transform provider with configuration data
    TFRecordReadSchemaTransformProvider provider = new TFRecordReadSchemaTransformProvider();
    String compression = "AUTO";
    TFRecordReadSchemaTransformConfiguration configuration =
        TFRecordReadSchemaTransformConfiguration.builder()
            .setValidate(true)
            .setCompression(compression)
            .setFilePattern(filename)
            .build();
    TFRecordReadSchemaTransform transform =
        (TFRecordReadSchemaTransform) provider.from(configuration);

    // Create PCollectionRowTuples input data and apply transform to read
    PCollectionRowTuple input = PCollectionRowTuple.empty(readPipeline);
    PCollectionRowTuple reads = input.apply(transform);

    // Create expected row data
    Schema schema = Schema.of(Schema.Field.of("record", Schema.FieldType.BYTES));
    List<Row> row =
        Arrays.stream(expected)
            .map(str -> str.getBytes(StandardCharsets.UTF_8))
            .map(bytes -> Row.withSchema(schema).addValue(bytes).build())
            .collect(Collectors.toList());
    PAssert.that(reads.get("output")).containsInAnyOrder(row);

    readPipeline.run().waitUntilFinish();
  }

  /** Tests {@link TFRecordWriteSchemaTransformProvider}. */
  private void runTestWrite(String[] elems, String... base64) throws IOException {
    // Create temp filename
    File tmpFile =
        Files.createTempFile(tempFolder.getRoot().toPath(), "file", ".tfrecords").toFile();
    String filename = tmpFile.getPath();

    // Create beam row schema
    Schema schema = Schema.of(Schema.Field.of("record", Schema.FieldType.BYTES));

    // Create transform provider with configuration data
    TFRecordWriteSchemaTransformProvider provider = new TFRecordWriteSchemaTransformProvider();
    String compression = "UNCOMPRESSED";
    TFRecordWriteSchemaTransformConfiguration configuration =
        TFRecordWriteSchemaTransformConfiguration.builder()
            .setOutputPrefix(filename)
            .setCompression(compression)
            .setNumShards(0)
            .setNoSpilling(true)
            .build();
    TFRecordWriteSchemaTransform transform =
        (TFRecordWriteSchemaTransform) provider.from(configuration);

    // Create Beam row byte data
    List<Row> rows =
        Arrays.stream(elems)
            .map(str -> str.getBytes(StandardCharsets.UTF_8))
            .map(bytes -> Row.withSchema(schema).addValue(bytes).build())
            .collect(Collectors.toList());

    // Create PColleciton input beam row data on pipeline and apply transform
    PCollection<Row> input = writePipeline.apply(Create.of(rows).withRowSchema(schema));
    PCollectionRowTuple rowTuple = PCollectionRowTuple.of("input", input);
    rowTuple.apply(transform);

    // Run pipeline
    writePipeline.run().waitUntilFinish();

    assertTrue("File should exist", tmpFile.exists());
    assertTrue("File should have content", tmpFile.length() > 0);

    FileInputStream fis = new FileInputStream(tmpFile);
    String written = BaseEncoding.base64().encode(ByteStreams.toByteArray(fis));
    assertThat(written, is(in(base64)));
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTrip() throws IOException {
    runTestRoundTrip(LARGE, 10, ".tfrecords", UNCOMPRESSED, UNCOMPRESSED);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripWithEmptyData() throws IOException {
    runTestRoundTrip(EMPTY, 10, ".tfrecords", UNCOMPRESSED, UNCOMPRESSED);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripWithOneShards() throws IOException {
    runTestRoundTrip(LARGE, 1, ".tfrecords", UNCOMPRESSED, UNCOMPRESSED);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripWithSuffix() throws IOException {
    runTestRoundTrip(LARGE, 10, ".suffix", UNCOMPRESSED, UNCOMPRESSED);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripGzip() throws IOException {
    runTestRoundTrip(LARGE, 10, ".tfrecords", GZIP, GZIP);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripZlib() throws IOException {
    runTestRoundTrip(SMALL, 10, ".tfrecords", DEFLATE, DEFLATE);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripUncompressedFilesWithAuto() throws IOException {
    runTestRoundTrip(LARGE, 10, ".tfrecords", UNCOMPRESSED, AUTO);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripGzipFilesWithAuto() throws IOException {
    runTestRoundTrip(LARGE, 10, ".tfrecords", GZIP, AUTO);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripZlibFilesWithAuto() throws IOException {
    runTestRoundTrip(LARGE, 10, ".tfrecords", DEFLATE, AUTO);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripLargeRecords() throws IOException {
    runTestRoundTrip(LARGE_RECORDS, 10, ".tfrecords", UNCOMPRESSED, UNCOMPRESSED);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripLargeRecordsGzip() throws IOException {
    runTestRoundTrip(LARGE_RECORDS, 10, ".tfrecords", GZIP, GZIP);
  }

  private void runTestRoundTrip(
      Iterable<String> elems,
      int numShards,
      String suffix,
      Compression writeCompression,
      Compression readCompression)
      throws IOException {

    // Create directory for files to be written to
    Path baseDir = Files.createTempDirectory(tempFolder.getRoot().toPath(), "test-rt");
    String outputNameViaWrite = "via-write";
    String baseFilenameViaWrite = baseDir.resolve(outputNameViaWrite).toString();

    // Create beam row schema
    Schema schema = Schema.of(Schema.Field.of("record", Schema.FieldType.BYTES));

    // Create write transform provider with write configuration data
    TFRecordWriteSchemaTransformProvider writeProvider = new TFRecordWriteSchemaTransformProvider();
    TFRecordWriteSchemaTransformConfiguration writeConfiguration =
        TFRecordWriteSchemaTransformConfiguration.builder()
            .setNumShards(numShards)
            .setFilenameSuffix(suffix)
            .setCompression(writeCompression.toString())
            .setOutputPrefix(baseFilenameViaWrite)
            .setNoSpilling(true)
            .build();
    TFRecordWriteSchemaTransform transform =
        (TFRecordWriteSchemaTransform) writeProvider.from(writeConfiguration);

    // Create input Beam row byte data
    List<Row> rows =
        StreamSupport.stream(elems.spliterator(), false)
            .map(str -> str.getBytes(StandardCharsets.UTF_8))
            .map(bytes -> Row.withSchema(schema).addValue(bytes).build())
            .collect(Collectors.toList());

    // Create PColleciton input beam row data on pipeline and apply transform
    PCollection<Row> writeInput = writePipeline.apply(Create.of(rows).withRowSchema(schema));
    PCollectionRowTuple rowTuple = PCollectionRowTuple.of("input", writeInput);
    rowTuple.apply(transform);

    // Run pipeline
    writePipeline.run().waitUntilFinish();

    // Create read transform provider with read configuration data
    TFRecordReadSchemaTransformProvider readProvider = new TFRecordReadSchemaTransformProvider();
    TFRecordReadSchemaTransformConfiguration readConfiguration =
        TFRecordReadSchemaTransformConfiguration.builder()
            .setValidate(true)
            .setCompression(readCompression.toString())
            .setFilePattern(baseFilenameViaWrite + "*")
            .build();
    TFRecordReadSchemaTransform readTransform =
        (TFRecordReadSchemaTransform) readProvider.from(readConfiguration);

    // Create PCollectionRowTuples input data and apply transform to read
    PCollectionRowTuple readInput = PCollectionRowTuple.empty(readPipeline);
    PCollectionRowTuple reads = readInput.apply(readTransform);

    // Assert that read output data matches expected rows
    PAssert.that(reads.get("output")).containsInAnyOrder(rows);

    readPipeline.run().waitUntilFinish();
  }

  /*
   * Make some line data for processing.
   */
  private static Iterable<String> makeLines(int n, int minRecordSize) {
    List<String> ret = Lists.newArrayList();
    StringBuilder recordBuilder = new StringBuilder();
    for (int i = 0; i < minRecordSize; i++) {
      recordBuilder.append("x");
    }
    String record = recordBuilder.toString();
    for (int i = 0; i < n; ++i) {
      ret.add(record + " " + i);
    }
    return ret;
  }
}
