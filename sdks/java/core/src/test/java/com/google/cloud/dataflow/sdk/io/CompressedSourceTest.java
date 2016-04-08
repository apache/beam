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
package com.google.cloud.dataflow.sdk.io;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.CompressedSource.CompressionMode;
import com.google.cloud.dataflow.sdk.io.CompressedSource.DecompressingChannelFactory;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.PAssert;
import com.google.cloud.dataflow.sdk.testing.SourceTestUtils;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import javax.annotation.Nullable;

/**
 * Tests for CompressedSource.
 */
@RunWith(JUnit4.class)
public class CompressedSourceTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * Test reading nonempty input with gzip.
   */
  @Test
  public void testReadGzip() throws Exception {
    byte[] input = generateInput(5000);
    runReadTest(input, CompressionMode.GZIP);
  }

  /**
   * Test reading nonempty input with bzip2.
   */
  @Test
  public void testReadBzip2() throws Exception {
    byte[] input = generateInput(5000);
    runReadTest(input, CompressionMode.BZIP2);
  }

  /**
   * Test reading empty input with gzip.
   */
  @Test
  public void testEmptyReadGzip() throws Exception {
    byte[] input = generateInput(0);
    runReadTest(input, CompressionMode.GZIP);
  }

  private static byte[] compressGzip(byte[] input) throws IOException {
    ByteArrayOutputStream res = new ByteArrayOutputStream();
    try (GZIPOutputStream gzipStream = new GZIPOutputStream(res)) {
      gzipStream.write(input);
    }
    return res.toByteArray();
  }

  private static byte[] concat(byte[] first, byte[] second) {
    byte[] res = new byte[first.length + second.length];
    System.arraycopy(first, 0, res, 0, first.length);
    System.arraycopy(second, 0, res, first.length, second.length);
    return res;
  }

  /**
   * Test a concatenation of gzip files is correctly decompressed.
   *
   * <p>A concatenation of gzip files as one file is a valid gzip file and should decompress
   * to be the concatenation of those individual files.
   */
  @Test
  public void testReadConcatenatedGzip() throws IOException {
    byte[] header = "a,b,c\n".getBytes(StandardCharsets.UTF_8);
    byte[] body = "1,2,3\n4,5,6\n7,8,9\n".getBytes(StandardCharsets.UTF_8);
    byte[] expected = concat(header, body);
    byte[] totalGz = concat(compressGzip(header), compressGzip(body));
    File tmpFile = tmpFolder.newFile();
    try (FileOutputStream os = new FileOutputStream(tmpFile)) {
      os.write(totalGz);
    }

    Pipeline p = TestPipeline.create();

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(tmpFile.getAbsolutePath(), 1))
            .withDecompression(CompressionMode.GZIP);
    PCollection<Byte> output = p.apply(Read.from(source));

    PAssert.that(output).containsInAnyOrder(Bytes.asList(expected));
    p.run();
  }

  /**
   * Test reading empty input with bzip2.
   */
  @Test
  public void testCompressedReadBzip2() throws Exception {
    byte[] input = generateInput(0);
    runReadTest(input, CompressionMode.BZIP2);
  }

  /**
   * Test reading according to filepattern when the file is bzipped.
   */
  @Test
  public void testCompressedAccordingToFilepatternGzip() throws Exception {
    byte[] input = generateInput(100);
    File tmpFile = tmpFolder.newFile("test.gz");
    writeFile(tmpFile, input, CompressionMode.GZIP);
    verifyReadContents(input, tmpFile, null /* default auto decompression factory */);
  }

  /**
   * Test reading according to filepattern when the file is gzipped.
   */
  @Test
  public void testCompressedAccordingToFilepatternBzip2() throws Exception {
    byte[] input = generateInput(100);
    File tmpFile = tmpFolder.newFile("test.bz2");
    writeFile(tmpFile, input, CompressionMode.BZIP2);
    verifyReadContents(input, tmpFile, null /* default auto decompression factory */);
  }

  /**
   * Test reading multiple files with different compression.
   */
  @Test
  public void testHeterogeneousCompression() throws Exception {
    String baseName = "test-input";

    // Expected data
    byte[] generated = generateInput(1000);
    List<Byte> expected = new ArrayList<>();

    // Every sort of compression
    File uncompressedFile = tmpFolder.newFile(baseName + ".bin");
    generated = generateInput(1000);
    Files.write(generated, uncompressedFile);
    expected.addAll(Bytes.asList(generated));

    File gzipFile = tmpFolder.newFile(baseName + ".gz");
    generated = generateInput(1000);
    writeFile(gzipFile, generated, CompressionMode.GZIP);
    expected.addAll(Bytes.asList(generated));

    File bzip2File = tmpFolder.newFile(baseName + ".bz2");
    generated = generateInput(1000);
    writeFile(bzip2File, generateInput(1000), CompressionMode.BZIP2);
    expected.addAll(Bytes.asList(generated));

    String filePattern = new File(tmpFolder.getRoot().toString(), baseName + ".*").toString();

    Pipeline p = TestPipeline.create();

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(filePattern, 1));
    PCollection<Byte> output = p.apply(Read.from(source));

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testUncompressedFileIsSplittable() throws Exception {
    String baseName = "test-input";

    File uncompressedFile = tmpFolder.newFile(baseName + ".bin");
    Files.write(generateInput(10), uncompressedFile);

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(uncompressedFile.getPath(), 1));
    assertTrue(source.isSplittable());
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testGzipFileIsNotSplittable() throws Exception {
    String baseName = "test-input";

    File compressedFile = tmpFolder.newFile(baseName + ".gz");
    writeFile(compressedFile, generateInput(10), CompressionMode.GZIP);

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(compressedFile.getPath(), 1));
    assertFalse(source.isSplittable());
  }

  @Test
  public void testBzip2FileIsNotSplittable() throws Exception {
    String baseName = "test-input";

    File compressedFile = tmpFolder.newFile(baseName + ".bz2");
    writeFile(compressedFile, generateInput(10), CompressionMode.BZIP2);

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(compressedFile.getPath(), 1));
    assertFalse(source.isSplittable());
  }

  /**
   * Test reading an uncompressed file with {@link CompressionMode#GZIP}, since we must support
   * this due to properties of services that we read from.
   */
  @Test
  public void testFalseGzipStream() throws Exception {
    byte[] input = generateInput(1000);
    File tmpFile = tmpFolder.newFile("test.gz");
    Files.write(input, tmpFile);
    verifyReadContents(input, tmpFile, CompressionMode.GZIP);
  }

  /**
   * Test reading an uncompressed file with {@link CompressionMode#BZIP2}, and show that
   * we fail.
   */
  @Test
  public void testFalseBzip2Stream() throws Exception {
    byte[] input = generateInput(1000);
    File tmpFile = tmpFolder.newFile("test.bz2");
    Files.write(input, tmpFile);
    thrown.expectCause(Matchers.allOf(
        instanceOf(IOException.class),
        ThrowableMessageMatcher.hasMessage(
            containsString("Stream is not in the BZip2 format"))));
    verifyReadContents(input, tmpFile, CompressionMode.BZIP2);
  }

  /**
   * Test reading an empty input file with gzip; it must be interpreted as uncompressed because
   * the gzip header is two bytes.
   */
  @Test
  public void testEmptyReadGzipUncompressed() throws Exception {
    byte[] input = generateInput(0);
    File tmpFile = tmpFolder.newFile("test.gz");
    Files.write(input, tmpFile);
    verifyReadContents(input, tmpFile, CompressionMode.GZIP);
  }

  /**
   * Test reading single byte input with gzip; it must be interpreted as uncompressed because
   * the gzip header is two bytes.
   */
  @Test
  public void testOneByteReadGzipUncompressed() throws Exception {
    byte[] input = generateInput(1);
    File tmpFile = tmpFolder.newFile("test.gz");
    Files.write(input, tmpFile);
    verifyReadContents(input, tmpFile, CompressionMode.GZIP);
  }

  /**
   * Test reading multiple files.
   */
  @Test
  public void testCompressedReadMultipleFiles() throws Exception {
    int numFiles = 10;
    String baseName = "test_input-";
    String filePattern = new File(tmpFolder.getRoot().toString(), baseName + "*").toString();
    List<Byte> expected = new ArrayList<>();

    for (int i = 0; i < numFiles; i++) {
      byte[] generated = generateInput(1000);
      File tmpFile = tmpFolder.newFile(baseName + i);
      writeFile(tmpFile, generated, CompressionMode.GZIP);
      expected.addAll(Bytes.asList(generated));
    }

    Pipeline p = TestPipeline.create();

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(filePattern, 1))
            .withDecompression(CompressionMode.GZIP);
    PCollection<Byte> output = p.apply(Read.from(source));

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  /**
   * Generate byte array of given size.
   */
  private byte[] generateInput(int size) {
    // Arbitrary but fixed seed
    Random random = new Random(285930);
    byte[] buff = new byte[size];
    for (int i = 0; i < size; i++) {
      buff[i] = (byte) (random.nextInt() % Byte.MAX_VALUE);
    }
    return buff;
  }

  /**
   * Get a compressing stream for a given compression mode.
   */
  private OutputStream getOutputStreamForMode(CompressionMode mode, OutputStream stream)
      throws IOException {
    switch (mode) {
      case GZIP:
        return new GzipCompressorOutputStream(stream);
      case BZIP2:
        return new BZip2CompressorOutputStream(stream);
      default:
        throw new RuntimeException("Unexpected compression mode");
    }
  }

  /**
   * Writes a single output file.
   */
  private void writeFile(File file, byte[] input, CompressionMode mode) throws IOException {
    try (OutputStream os = getOutputStreamForMode(mode, new FileOutputStream(file))) {
      os.write(input);
    }
  }

  /**
   * Run a single read test, writing and reading back input with the given compression mode.
   */
  private void runReadTest(byte[] input,
      CompressionMode inputCompressionMode,
      @Nullable DecompressingChannelFactory decompressionFactory)
      throws IOException {
    File tmpFile = tmpFolder.newFile();
    writeFile(tmpFile, input, inputCompressionMode);
    verifyReadContents(input, tmpFile, decompressionFactory);
  }

  private void verifyReadContents(byte[] expected, File inputFile,
      @Nullable DecompressingChannelFactory decompressionFactory) {
    Pipeline p = TestPipeline.create();
    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(inputFile.toPath().toString(), 1));
    if (decompressionFactory != null) {
      source = source.withDecompression(decompressionFactory);
    }
    PCollection<Byte> output = p.apply(Read.from(source));
    PAssert.that(output).containsInAnyOrder(Bytes.asList(expected));
    p.run();
  }

  /**
   * Run a single read test, writing and reading back input with the given compression mode.
   */
  private void runReadTest(byte[] input, CompressionMode mode) throws IOException {
    runReadTest(input, mode, mode);
  }

  /**
   * Dummy source for use in tests.
   */
  private static class ByteSource extends FileBasedSource<Byte> {
    public ByteSource(String fileOrPatternSpec, long minBundleSize) {
      super(fileOrPatternSpec, minBundleSize);
    }

    public ByteSource(String fileName, long minBundleSize, long startOffset, long endOffset) {
      super(fileName, minBundleSize, startOffset, endOffset);
    }

    @Override
    protected FileBasedSource<Byte> createForSubrangeOfFile(String fileName, long start, long end) {
      return new ByteSource(fileName, getMinBundleSize(), start, end);
    }

    @Override
    protected FileBasedReader<Byte> createSingleFileReader(PipelineOptions options) {
      return new ByteReader(this);
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public Coder<Byte> getDefaultOutputCoder() {
      return SerializableCoder.of(Byte.class);
    }

    private static class ByteReader extends FileBasedReader<Byte> {
      ByteBuffer buff = ByteBuffer.allocate(1);
      Byte current;
      long offset = -1;
      ReadableByteChannel channel;

      public ByteReader(ByteSource source) {
        super(source);
      }

      @Override
      public Byte getCurrent() throws NoSuchElementException {
        return current;
      }

      @Override
      protected boolean isAtSplitPoint() {
        return true;
      }

      @Override
      protected void startReading(ReadableByteChannel channel) throws IOException {
        this.channel = channel;
      }

      @Override
      protected boolean readNextRecord() throws IOException {
        buff.clear();
        if (channel.read(buff) != 1) {
          return false;
        }
        current = new Byte(buff.get(0));
        offset += 1;
        return true;
      }

      @Override
      protected long getCurrentOffset() {
        return offset;
      }
    }
  }
}
