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
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for TFRecordIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
public class TFRecordIOTest {

  /*
  From https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/tfrecordio_test.py
  Created by running following code in python:
  >>> import tensorflow as tf
  >>> import base64
  >>> writer = tf.python_io.TFRecordWriter('/tmp/python_foo.tfrecord')
  >>> writer.write('foo')
  >>> writer.close()
  >>> with open('/tmp/python_foo.tfrecord', 'rb') as f:
  ...   data =  base64.b64encode(f.read())
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
  private static final Iterable<String> LARGE = makeLines(1000);

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public TestPipeline readPipeline = TestPipeline.create();

  @Rule
  public TestPipeline writePipeline = TestPipeline.create();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testReadNamed() {
    writePipeline.enableAbandonedNodeEnforcement(false);

    assertEquals(
        "TFRecordIO.Read/Read.out",
        writePipeline.apply(TFRecordIO.read().from("foo.*").withoutValidation()).getName());
    assertEquals(
        "MyRead/Read.out",
        writePipeline
            .apply("MyRead", TFRecordIO.read().from("foo.*").withoutValidation())
            .getName());
  }

  @Test
  public void testReadDisplayData() {
    TFRecordIO.Read read = TFRecordIO.read()
        .from("foo.*")
        .withCompression(GZIP)
        .withoutValidation();

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("filePattern", "foo.*"));
    assertThat(displayData, hasDisplayItem("compressionType", GZIP.toString()));
    assertThat(displayData, hasDisplayItem("validation", false));
  }

  @Test
  public void testWriteDisplayData() {
    TFRecordIO.Write write = TFRecordIO.write()
        .to("/foo")
        .withSuffix("bar")
        .withShardNameTemplate("-SS-of-NN-")
        .withNumShards(100)
        .withCompression(GZIP);

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("filePrefix", "/foo"));
    assertThat(displayData, hasDisplayItem("fileSuffix", "bar"));
    assertThat(displayData, hasDisplayItem("shardNameTemplate", "-SS-of-NN-"));
    assertThat(displayData, hasDisplayItem("numShards", 100));
    assertThat(displayData, hasDisplayItem("compressionType", GZIP.toString()));
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
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Not a valid TFRecord. Fewer than 12 bytes.");
    System.out.println("abr".getBytes().length);
    runTestRead("bar".getBytes(), new String[0]);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadInvalidLengthMask() throws Exception {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Mismatch of length mask");
    byte[] data = BaseEncoding.base64().decode(FOO_RECORD_BASE64);
    data[9] += 1;
    runTestRead(data, FOO_RECORDS);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadInvalidDataMask() throws Exception {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Mismatch of data mask");
    byte[] data = BaseEncoding.base64().decode(FOO_RECORD_BASE64);
    data[16] += 1;
    runTestRead(data, FOO_RECORDS);
  }

  private void runTestRead(String base64, String[] expected) throws IOException {
    runTestRead(BaseEncoding.base64().decode(base64), expected);
  }

  private void runTestRead(byte[] data, String[] expected) throws IOException {
    File tmpFile =
        Files.createTempFile(tempFolder.getRoot().toPath(), "file", ".tfrecords").toFile();
    String filename = tmpFile.getPath();

    try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
      fos.write(data);
    }

    TFRecordIO.Read read = TFRecordIO.read().from(filename);
    PCollection<String> output = writePipeline.apply(read).apply(ParDo.of(new ByteArrayToString()));

    PAssert.that(output).containsInAnyOrder(expected);
    writePipeline.run();
  }

  private void runTestWrite(String[] elems, String ...base64) throws IOException {
    File tmpFile =
        Files.createTempFile(tempFolder.getRoot().toPath(), "file", ".tfrecords").toFile();
    String filename = tmpFile.getPath();

    PCollection<byte[]> input = writePipeline.apply(Create.of(Arrays.asList(elems)))
        .apply(ParDo.of(new StringToByteArray()));

    TFRecordIO.Write write = TFRecordIO.write().to(filename).withoutSharding();
    input.apply(write);

    writePipeline.run();

    FileInputStream fis = new FileInputStream(tmpFile);
    String written = BaseEncoding.base64().encode(ByteStreams.toByteArray(fis));
    // bytes written may vary depending the order of elems
    assertThat(written, isIn(base64));
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
    runTestRoundTrip(LARGE, 10, ".tfrecords", DEFLATE, DEFLATE);
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

  private void runTestRoundTrip(Iterable<String> elems,
                                int numShards,
                                String suffix,
                                Compression writeCompression,
                                Compression readCompression) throws IOException {
    Path baseDir = Files.createTempDirectory(tempFolder.getRoot().toPath(), "test-rt");
    String outputNameViaWrite = "via-write";
    String baseFilenameViaWrite = baseDir.resolve(outputNameViaWrite).toString();
    String outputNameViaSink = "via-sink";
    String baseFilenameViaSink = baseDir.resolve(outputNameViaSink).toString();

    PCollection<byte[]> data =
        writePipeline.apply(Create.of(elems).withCoder(StringUtf8Coder.of()))
            .apply(ParDo.of(new StringToByteArray()));
    data
        .apply(
            "Write via TFRecordIO.write",
            TFRecordIO.write()
                .to(baseFilenameViaWrite)
                .withNumShards(numShards)
                .withSuffix(suffix)
                .withCompression(writeCompression));

    data.apply(
        "Write via TFRecordIO.sink",
        FileIO.<byte[]>write()
            .via(TFRecordIO.sink())
            .to(baseDir.toString())
            .withPrefix(outputNameViaSink)
            .withSuffix(suffix)
            .withCompression(writeCompression)
            .withIgnoreWindowing());
    writePipeline.run();

    PAssert.that(
            readPipeline
                .apply(
                    "Read written by TFRecordIO.write",
                    TFRecordIO.read()
                        .from(baseFilenameViaWrite + "*")
                        .withCompression(readCompression))
                .apply("To string first", ParDo.of(new ByteArrayToString())))
        .containsInAnyOrder(elems);
    PAssert.that(
            readPipeline
                .apply(
                    "Read written by TFRecordIO.sink",
                    TFRecordIO.read()
                        .from(baseFilenameViaSink + "*")
                        .withCompression(readCompression))
                .apply("To string second", ParDo.of(new ByteArrayToString())))
        .containsInAnyOrder(elems);
    readPipeline.run();
  }

  private static Iterable<String> makeLines(int n) {
    List<String> ret = Lists.newArrayList();
    for (int i = 0; i < n; ++i) {
      ret.add("word" + i);
    }
    return ret;
  }

  static class ByteArrayToString extends DoFn<byte[], String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new String(c.element()));
    }
  }

  static class StringToByteArray extends DoFn<String, byte[]> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().getBytes());
    }
  }

}
