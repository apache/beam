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

import static org.apache.beam.sdk.io.TFRecordIO.CompressionType;
import static org.apache.beam.sdk.io.TFRecordIO.CompressionType.AUTO;
import static org.apache.beam.sdk.io.TFRecordIO.CompressionType.GZIP;
import static org.apache.beam.sdk.io.TFRecordIO.CompressionType.NONE;
import static org.apache.beam.sdk.io.TFRecordIO.CompressionType.ZLIB;
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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
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
  private static final Iterable<String> LARGE = makeLines(5000);

  private static Path tempFolder;


  public TestPipeline p = TestPipeline.create();

  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TestPipeline p2 = TestPipeline.create();

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(expectedException).around(p);

  @BeforeClass
  public static void setupClass() throws IOException {
    tempFolder = Files.createTempDirectory("TFRecordIOTest");
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    Files.walkFileTree(tempFolder, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  @Test
  public void testReadNamed() {
    p.enableAbandonedNodeEnforcement(false);

    assertEquals(
        "TFRecordIO.Read/Read.out",
        p.apply(TFRecordIO.read().from("foo.*").withoutValidation()).getName());
    assertEquals(
        "MyRead/Read.out",
        p.apply("MyRead", TFRecordIO.read().from("foo.*").withoutValidation()).getName());
  }

  @Test
  public void testReadDisplayData() {
    TFRecordIO.Read read = TFRecordIO.read()
        .from("foo.*")
        .withCompressionType(GZIP)
        .withoutValidation();

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("filePattern", "foo.*"));
    assertThat(displayData, hasDisplayItem("compressionType", GZIP.toString()));
    assertThat(displayData, hasDisplayItem("validation", false));
  }

  @Test
  public void testWriteDisplayData() {
    TFRecordIO.Write write = TFRecordIO.write()
        .to("foo")
        .withSuffix("bar")
        .withShardNameTemplate("-SS-of-NN-")
        .withNumShards(100)
        .withCompressionType(GZIP);

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("filePrefix", "foo"));
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
    File tmpFile = Files.createTempFile(tempFolder, "file", ".tfrecords").toFile();
    String filename = tmpFile.getPath();

    FileOutputStream fos = new FileOutputStream(tmpFile);
    fos.write(data);
    fos.close();

    TFRecordIO.Read read = TFRecordIO.read().from(filename);
    PCollection<String> output = p.apply(read).apply(ParDo.of(new ByteArrayToString()));

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  private void runTestWrite(String[] elems, String ...base64) throws IOException {
    File tmpFile = Files.createTempFile(tempFolder, "file", ".tfrecords").toFile();
    String filename = tmpFile.getPath();

    PCollection<byte[]> input = p.apply(Create.of(Arrays.asList(elems)))
        .apply(ParDo.of(new StringToByteArray()));

    TFRecordIO.Write write = TFRecordIO.write().to(filename).withoutSharding();
    input.apply(write);

    p.run();

    FileInputStream fis = new FileInputStream(tmpFile);
    String written = BaseEncoding.base64().encode(ByteStreams.toByteArray(fis));
    // bytes written may vary depending the order of elems
    assertThat(written, isIn(base64));
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTrip() throws IOException {
    runTestRoundTrip(LARGE, 10, ".tfrecords", NONE, NONE);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripWithEmptyData() throws IOException {
    runTestRoundTrip(EMPTY, 10, ".tfrecords", NONE, NONE);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripWithOneShards() throws IOException {
    runTestRoundTrip(LARGE, 1, ".tfrecords", NONE, NONE);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripWithSuffix() throws IOException {
    runTestRoundTrip(LARGE, 10, ".suffix", NONE, NONE);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripGzip() throws IOException {
    runTestRoundTrip(LARGE, 10, ".tfrecords", GZIP, GZIP);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripZlib() throws IOException {
    runTestRoundTrip(LARGE, 10, ".tfrecords", ZLIB, ZLIB);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripUncompressedFilesWithAuto() throws IOException {
    runTestRoundTrip(LARGE, 10, ".tfrecords", NONE, AUTO);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripGzipFilesWithAuto() throws IOException {
    runTestRoundTrip(LARGE, 10, ".tfrecords", GZIP, AUTO);
  }

  @Test
  @Category(NeedsRunner.class)
  public void runTestRoundTripZlibFilesWithAuto() throws IOException {
    runTestRoundTrip(LARGE, 10, ".tfrecords", ZLIB, AUTO);
  }

  private void runTestRoundTrip(Iterable<String> elems,
                                int numShards,
                                String suffix,
                                CompressionType writeCompressionType,
                                CompressionType readCompressionType) throws IOException {
    String outputName = "file";
    Path baseDir = Files.createTempDirectory(tempFolder, "test-rt");
    String baseFilename = baseDir.resolve(outputName).toString();

    TFRecordIO.Write write = TFRecordIO.write().to(baseFilename)
        .withNumShards(numShards)
        .withSuffix(suffix)
        .withCompressionType(writeCompressionType);
    p.apply(Create.of(elems).withCoder(StringUtf8Coder.of()))
        .apply(ParDo.of(new StringToByteArray()))
        .apply(write);
    p.run();

    TFRecordIO.Read read = TFRecordIO.read().from(baseFilename + "*")
        .withCompressionType(readCompressionType);
    PCollection<String> output = p2.apply(read).apply(ParDo.of(new ByteArrayToString()));

    PAssert.that(output).containsInAnyOrder(elems);
    p2.run();
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
