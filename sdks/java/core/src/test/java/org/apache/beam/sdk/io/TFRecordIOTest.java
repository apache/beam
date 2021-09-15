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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeFalse;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.TFRecordIO.TFRecordCodec;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for TFRecordIO Read and Write transforms. */
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
  private static final Iterable<String> LARGE = makeLines(1000, 4);
  private static final Iterable<String> LARGE_RECORDS = makeLines(100, 100000);

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testReadNamed() {
    readPipeline.enableAbandonedNodeEnforcement(false);

    assertThat(
        readPipeline.apply(TFRecordIO.read().from("foo.*").withoutValidation()).getName(),
        startsWith("TFRecordIO.Read/Read"));
    assertThat(
        readPipeline.apply("MyRead", TFRecordIO.read().from("foo.*").withoutValidation()).getName(),
        startsWith("MyRead/Read"));
  }

  @Test
  public void testReadFilesNamed() {
    readPipeline.enableAbandonedNodeEnforcement(false);

    Metadata metadata =
        Metadata.builder()
            .setResourceId(FileSystems.matchNewResource("file", false /* isDirectory */))
            .setIsReadSeekEfficient(true)
            .setSizeBytes(1024)
            .build();
    Create.Values<ReadableFile> create = Create.of(new ReadableFile(metadata, Compression.AUTO));

    assertEquals(
        "TFRecordIO.ReadFiles/Read all via FileBasedSource/Read ranges/ParMultiDo(ReadFileRanges).output",
        readPipeline.apply(create).apply(TFRecordIO.readFiles()).getName());
    assertEquals(
        "MyRead/Read all via FileBasedSource/Read ranges/ParMultiDo(ReadFileRanges).output",
        readPipeline.apply(create).apply("MyRead", TFRecordIO.readFiles()).getName());
  }

  @Test
  public void testReadDisplayData() {
    TFRecordIO.Read read =
        TFRecordIO.read().from("foo.*").withCompression(GZIP).withoutValidation();

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("filePattern", "foo.*"));
    assertThat(displayData, hasDisplayItem("compressionType", GZIP.toString()));
    assertThat(displayData, hasDisplayItem("validation", false));
  }

  @Test
  public void testWriteDisplayData() {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10739
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    TFRecordIO.Write write =
        TFRecordIO.write()
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
    expectedException.expectMessage("Not a valid TFRecord. Fewer than 12 bytes.");
    runTestRead("bar".getBytes(Charsets.UTF_8), new String[0]);
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

  /** Tests both {@link TFRecordIO.Read} and {@link TFRecordIO.ReadFiles}. */
  private void runTestRead(byte[] data, String[] expected) throws IOException {
    File tmpFile =
        Files.createTempFile(tempFolder.getRoot().toPath(), "file", ".tfrecords").toFile();
    String filename = tmpFile.getPath();

    try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
      fos.write(data);
    }

    TFRecordIO.Read read = TFRecordIO.read().from(filename);
    PCollection<String> output = readPipeline.apply(read).apply(ParDo.of(new ByteArrayToString()));
    PAssert.that(output).containsInAnyOrder(expected);

    Compression compression = AUTO;
    PAssert.that(
            readPipeline
                .apply("Create_Paths_ReadFiles_" + tmpFile, Create.of(tmpFile.getPath()))
                .apply("Match_" + tmpFile, FileIO.matchAll())
                .apply("ReadMatches_" + tmpFile, FileIO.readMatches().withCompression(compression))
                .apply("ReadFiles_" + compression.toString(), TFRecordIO.readFiles())
                .apply("ToString", ParDo.of(new ByteArrayToString())))
        .containsInAnyOrder(expected);

    readPipeline.run();
  }

  private void runTestWrite(String[] elems, String... base64) throws IOException {
    File tmpFile =
        Files.createTempFile(tempFolder.getRoot().toPath(), "file", ".tfrecords").toFile();
    String filename = tmpFile.getPath();

    PCollection<byte[]> input =
        writePipeline
            .apply(Create.of(Arrays.asList(elems)))
            .apply(ParDo.of(new StringToByteArray()));

    TFRecordIO.Write write = TFRecordIO.write().to(filename).withoutSharding();
    input.apply(write);

    writePipeline.run();

    FileInputStream fis = new FileInputStream(tmpFile);
    String written = BaseEncoding.base64().encode(ByteStreams.toByteArray(fis));
    // bytes written may vary depending the order of elems
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
    Path baseDir = Files.createTempDirectory(tempFolder.getRoot().toPath(), "test-rt");
    String outputNameViaWrite = "via-write";
    String baseFilenameViaWrite = baseDir.resolve(outputNameViaWrite).toString();
    String outputNameViaSink = "via-sink";
    String baseFilenameViaSink = baseDir.resolve(outputNameViaSink).toString();

    PCollection<byte[]> data =
        writePipeline
            .apply(Create.of(elems).withCoder(StringUtf8Coder.of()))
            .apply(ParDo.of(new StringToByteArray()));
    data.apply(
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
                .apply("To string read from write", ParDo.of(new ByteArrayToString())))
        .containsInAnyOrder(elems);
    PAssert.that(
            readPipeline
                .apply(
                    "Read written by TFRecordIO.sink",
                    TFRecordIO.read()
                        .from(baseFilenameViaSink + "*")
                        .withCompression(readCompression))
                .apply("To string read from sink", ParDo.of(new ByteArrayToString())))
        .containsInAnyOrder(elems);
    PAssert.that(
            readPipeline
                .apply(
                    "Create_Paths_ReadFiles_" + baseFilenameViaWrite,
                    Create.of(baseFilenameViaWrite + "*"))
                .apply("Match_" + baseFilenameViaWrite, FileIO.matchAll())
                .apply(
                    "ReadMatches_" + baseFilenameViaWrite,
                    FileIO.readMatches().withCompression(readCompression))
                .apply("ReadFiles written by TFRecordIO.write", TFRecordIO.readFiles())
                .apply("To string readFiles from write", ParDo.of(new ByteArrayToString())))
        .containsInAnyOrder(elems);
    PAssert.that(
            readPipeline
                .apply(
                    "ReadFiles written by TFRecordIO.sink",
                    TFRecordIO.read()
                        .from(baseFilenameViaSink + "*")
                        .withCompression(readCompression))
                .apply("To string readFiles from sink", ParDo.of(new ByteArrayToString())))
        .containsInAnyOrder(elems);
    readPipeline.run();
  }

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

  static class ByteArrayToString extends DoFn<byte[], String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new String(c.element(), Charsets.UTF_8));
    }
  }

  static class StringToByteArray extends DoFn<String, byte[]> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().getBytes(Charsets.UTF_8));
    }
  }

  static boolean maybeThisTime() {
    return ThreadLocalRandom.current().nextBoolean();
  }

  static class PickyReadChannel extends FilterInputStream implements ReadableByteChannel {
    protected PickyReadChannel(InputStream in) {
      super(in);
    }

    @Override
    public int read(byte[] b, int off, int len) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (!maybeThisTime() || !dst.hasRemaining()) {
        return 0;
      }
      int n = read();
      if (n == -1) {
        return -1;
      }
      dst.put((byte) n);
      return 1;
    }

    @Override
    public boolean isOpen() {
      throw new UnsupportedOperationException();
    }
  }

  static class PickyWriteChannel extends FilterOutputStream implements WritableByteChannel {
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      throw new UnsupportedOperationException();
    }

    public PickyWriteChannel(OutputStream out) {
      super(out);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      if (!maybeThisTime() || !src.hasRemaining()) {
        return 0;
      }
      write(src.get());
      return 1;
    }

    @Override
    public boolean isOpen() {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void testReadFully() throws IOException {
    byte[] data = "Hello World".getBytes(StandardCharsets.UTF_8);
    ReadableByteChannel chan = new PickyReadChannel(new ByteArrayInputStream(data));

    ByteBuffer buffer = ByteBuffer.allocate(data.length);
    TFRecordCodec.readFully(chan, buffer);

    assertArrayEquals(data, buffer.array());
  }

  @Test
  public void testReadFullyFail() throws IOException {
    byte[] trunc = "Hello Wo".getBytes(StandardCharsets.UTF_8);
    ReadableByteChannel chan = new PickyReadChannel(new ByteArrayInputStream(trunc));
    ByteBuffer buffer = ByteBuffer.allocate(trunc.length + 1);

    expectedException.expect(IOException.class);
    expectedException.expectMessage("expected 9, but got 8");
    TFRecordCodec.readFully(chan, buffer);
  }

  @Test
  public void testWriteFully() throws IOException {
    byte[] data = "Hello World".getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    WritableByteChannel chan = new PickyWriteChannel(baos);

    ByteBuffer buffer = ByteBuffer.wrap(data);
    TFRecordCodec.writeFully(chan, buffer);

    assertArrayEquals(data, baos.toByteArray());
  }

  @Test
  public void testTFRecordCodec() throws IOException {
    Decoder b64 = Base64.getDecoder();
    TFRecordCodec codec = new TFRecordCodec();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PickyWriteChannel outChan = new PickyWriteChannel(baos);

    codec.write(outChan, "foo".getBytes(StandardCharsets.UTF_8));
    assertArrayEquals(b64.decode(FOO_RECORD_BASE64), baos.toByteArray());
    codec.write(outChan, "bar".getBytes(StandardCharsets.UTF_8));
    assertArrayEquals(b64.decode(FOO_BAR_RECORD_BASE64), baos.toByteArray());

    PickyReadChannel inChan = new PickyReadChannel(new ByteArrayInputStream(baos.toByteArray()));
    byte[] foo = codec.read(inChan);
    byte[] bar = codec.read(inChan);
    assertNull(codec.read(inChan));

    assertEquals("foo", new String(foo, StandardCharsets.UTF_8));
    assertEquals("bar", new String(bar, StandardCharsets.UTF_8));
  }
}
