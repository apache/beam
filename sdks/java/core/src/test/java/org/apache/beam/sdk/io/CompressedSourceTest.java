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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includesDisplayDataFor;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.CompressedSource.CompressedReader;
import org.apache.beam.sdk.io.CompressedSource.CompressionMode;
import org.apache.beam.sdk.io.CompressedSource.DecompressingChannelFactory;
import org.apache.beam.sdk.io.FileBasedSource.FileBasedReader;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.LzoCompression;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultiset;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Bytes;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for CompressedSource. */
@RunWith(JUnit4.class)
public class CompressedSourceTest {

  private final double delta = 1e-6;

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public ExpectedException thrown = ExpectedException.none();

  /** Test reading nonempty input with gzip. */
  @Test
  public void testReadGzip() throws Exception {
    byte[] input = generateInput(5000);
    runReadTest(input, CompressionMode.GZIP);
  }

  /** Test reading nonempty input with lzo. */
  @Test
  public void testReadLzo() throws Exception {
    byte[] input = generateInput(5000);
    runReadTest(input, CompressionMode.LZO);
  }

  /** Test reading nonempty input with lzop. */
  @Test
  public void testReadLzop() throws Exception {
    byte[] input = generateInput(5000);
    runReadTest(input, CompressionMode.LZOP);
  }

  /** Test splittability of files in AUTO mode. */
  @Test
  public void testAutoSplittable() throws Exception {
    CompressedSource<Byte> source;

    // GZip files are not splittable
    source = CompressedSource.from(new ByteSource("input.gz", 1));
    assertFalse(source.isSplittable());
    source = CompressedSource.from(new ByteSource("input.GZ", 1));
    assertFalse(source.isSplittable());

    // BZ2 files are not splittable
    source = CompressedSource.from(new ByteSource("input.bz2", 1));
    assertFalse(source.isSplittable());
    source = CompressedSource.from(new ByteSource("input.BZ2", 1));
    assertFalse(source.isSplittable());

    // ZIP files are not splittable
    source = CompressedSource.from(new ByteSource("input.zip", 1));
    assertFalse(source.isSplittable());
    source = CompressedSource.from(new ByteSource("input.ZIP", 1));
    assertFalse(source.isSplittable());

    // ZSTD files are not splittable
    source = CompressedSource.from(new ByteSource("input.zst", 1));
    assertFalse(source.isSplittable());
    source = CompressedSource.from(new ByteSource("input.ZST", 1));
    assertFalse(source.isSplittable());
    source = CompressedSource.from(new ByteSource("input.zstd", 1));
    assertFalse(source.isSplittable());

    // LZO files are not splittable
    source = CompressedSource.from(new ByteSource("input.lzo_deflate", 1));
    assertFalse(source.isSplittable());
    source = CompressedSource.from(new ByteSource("input.LZO_DEFLATE", 1));
    assertFalse(source.isSplittable());
    // LZOP files are not splittable
    source = CompressedSource.from(new ByteSource("input.lzo", 1));
    assertFalse(source.isSplittable());
    source = CompressedSource.from(new ByteSource("input.LZO", 1));
    assertFalse(source.isSplittable());

    // DEFLATE files are not splittable
    source = CompressedSource.from(new ByteSource("input.deflate", 1));
    assertFalse(source.isSplittable());
    source = CompressedSource.from(new ByteSource("input.DEFLATE", 1));
    assertFalse(source.isSplittable());

    // Other extensions are assumed to be splittable.
    source = CompressedSource.from(new ByteSource("input.txt", 1));
    assertTrue(source.isSplittable());
    source = CompressedSource.from(new ByteSource("input.csv", 1));
    assertTrue(source.isSplittable());
  }

  /** Test splittability of files in GZIP mode -- none should be splittable. */
  @Test
  public void testGzipSplittable() throws Exception {
    CompressedSource<Byte> source;

    // GZip files are not splittable
    source =
        CompressedSource.from(new ByteSource("input.gz", 1))
            .withDecompression(CompressionMode.GZIP);

    assertFalse(source.isSplittable());
    source =
        CompressedSource.from(new ByteSource("input.GZ", 1))
            .withDecompression(CompressionMode.GZIP);
    assertFalse(source.isSplittable());

    // Other extensions are also not splittable.
    source =
        CompressedSource.from(new ByteSource("input.txt", 1))
            .withDecompression(CompressionMode.GZIP);
    assertFalse(source.isSplittable());
    source =
        CompressedSource.from(new ByteSource("input.csv", 1))
            .withDecompression(CompressionMode.GZIP);
    assertFalse(source.isSplittable());
  }

  /** Test splittability of files in LZO mode -- none should be splittable. */
  @Test
  public void testLzoSplittable() throws Exception {
    CompressedSource<Byte> source;

    // LZO files are not splittable
    source =
        CompressedSource.from(new ByteSource("input.lzo_deflate", 1))
            .withDecompression(CompressionMode.LZO);
    assertFalse(source.isSplittable());

    // Other extensions are also not splittable.
    source =
        CompressedSource.from(new ByteSource("input.txt", 1))
            .withDecompression(CompressionMode.LZO);
    assertFalse(source.isSplittable());
    source =
        CompressedSource.from(new ByteSource("input.csv", 1))
            .withDecompression(CompressionMode.LZO);
    assertFalse(source.isSplittable());
  }

  /** Test splittability of files in LZOP mode -- none should be splittable. */
  @Test
  public void testLzopSplittable() throws Exception {
    CompressedSource<Byte> source;

    // LZO files are not splittable
    source =
        CompressedSource.from(new ByteSource("input.lzo", 1))
            .withDecompression(CompressionMode.LZOP);
    assertFalse(source.isSplittable());

    // Other extensions are also not splittable.
    source =
        CompressedSource.from(new ByteSource("input.txt", 1))
            .withDecompression(CompressionMode.LZOP);
    assertFalse(source.isSplittable());
    source =
        CompressedSource.from(new ByteSource("input.csv", 1))
            .withDecompression(CompressionMode.LZOP);
    assertFalse(source.isSplittable());
  }

  /** Test reading nonempty input with bzip2. */
  @Test
  public void testReadBzip2() throws Exception {
    byte[] input = generateInput(5000);
    runReadTest(input, CompressionMode.BZIP2);
  }

  /** Test reading nonempty input with zip. */
  @Test
  public void testReadZip() throws Exception {
    byte[] input = generateInput(5000);
    runReadTest(input, CompressionMode.ZIP);
  }

  /** Test reading nonempty input with deflate. */
  @Test
  public void testReadDeflate() throws Exception {
    byte[] input = generateInput(5000);
    runReadTest(input, CompressionMode.DEFLATE);
  }

  /** Test reading empty input with gzip. */
  @Test
  public void testEmptyReadGzip() throws Exception {
    byte[] input = generateInput(0);
    runReadTest(input, CompressionMode.GZIP);
  }

  /** Test reading empty input with zstd. */
  @Test
  public void testEmptyReadZstd() throws Exception {
    byte[] input = generateInput(0);
    runReadTest(input, CompressionMode.ZSTD);
  }

  /** Test reading empty input with lzo. */
  @Test
  public void testEmptyReadLzo() throws Exception {
    byte[] input = generateInput(0);
    runReadTest(input, CompressionMode.LZO);
  }

  /** Test reading empty input with lzop. */
  @Test
  public void testEmptyReadLzop() throws Exception {
    byte[] input = generateInput(0);
    runReadTest(input, CompressionMode.LZOP);
  }

  private static byte[] compressGzip(byte[] input) throws IOException {
    ByteArrayOutputStream res = new ByteArrayOutputStream();
    try (GZIPOutputStream gzipStream = new GZIPOutputStream(res)) {
      gzipStream.write(input);
    }
    return res.toByteArray();
  }

  private static byte[] compressLzo(byte[] input) throws IOException {
    ByteArrayOutputStream res = new ByteArrayOutputStream();
    try (OutputStream lzoStream = LzoCompression.createLzoOutputStream(res)) {
      lzoStream.write(input);
    }
    return res.toByteArray();
  }

  private static byte[] compressLzop(byte[] input) throws IOException {
    ByteArrayOutputStream res = new ByteArrayOutputStream();
    try (OutputStream lzopStream = LzoCompression.createLzopOutputStream(res)) {
      lzopStream.write(input);
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
   * <p>A concatenation of gzip files as one file is a valid gzip file and should decompress to be
   * the concatenation of those individual files.
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

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(tmpFile.getAbsolutePath(), 1))
            .withDecompression(CompressionMode.GZIP);
    List<Byte> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
    assertEquals(Bytes.asList(expected), actual);
  }

  /**
   * Using Lzo Codec Test a concatenation of lzo files is correctly decompressed.
   *
   * <p>A concatenation of lzo files as one file is a valid lzo file and should decompress to be the
   * concatenation of those individual files.
   */
  @Test
  public void testReadConcatenatedLzo() throws IOException {
    byte[] header = "a,b,c\n".getBytes(StandardCharsets.UTF_8);
    byte[] body = "1,2,3\n4,5,6\n7,8,9\n".getBytes(StandardCharsets.UTF_8);
    byte[] expected = concat(header, body);
    byte[] totalLzo = concat(compressLzo(header), compressLzo(body));
    File tmpFile = tmpFolder.newFile();
    try (FileOutputStream os = new FileOutputStream(tmpFile)) {
      os.write(totalLzo);
    }

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(tmpFile.getAbsolutePath(), 1))
            .withDecompression(CompressionMode.LZO);
    List<Byte> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
    assertEquals(Bytes.asList(expected), actual);
  }

  /**
   * Using Lzop Codec Test a concatenation of lzop files is not correctly decompressed.
   *
   * <p>The current behaviour of LZOP codec returns the contents of the first file only
   */
  @Test
  public void testFalseReadConcatenatedLzop() throws IOException {
    byte[] header = "a,b,c\n".getBytes(StandardCharsets.UTF_8);
    byte[] body = "1,2,3\n4,5,6\n7,8,9\n".getBytes(StandardCharsets.UTF_8);
    byte[] expected = concat(header, body);
    byte[] totalLzop = concat(compressLzop(header), compressLzop(body));
    File tmpFile = tmpFolder.newFile();
    try (FileOutputStream os = new FileOutputStream(tmpFile)) {
      os.write(totalLzop);
    }

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(tmpFile.getAbsolutePath(), 1))
            .withDecompression(CompressionMode.LZOP);
    List<Byte> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
    assertNotEquals(Bytes.asList(expected), actual);
  }

  /**
   * Test a bzip2 file containing multiple streams is correctly decompressed.
   *
   * <p>A bzip2 file may contain multiple streams and should decompress as the concatenation of
   * those streams.
   */
  @Test
  public void testReadMultiStreamBzip2() throws IOException {
    CompressionMode mode = CompressionMode.BZIP2;
    byte[] input1 = generateInput(5, 587973);
    byte[] input2 = generateInput(5, 387374);

    ByteArrayOutputStream stream1 = new ByteArrayOutputStream();
    try (OutputStream os = getOutputStreamForMode(mode, stream1)) {
      os.write(input1);
    }

    ByteArrayOutputStream stream2 = new ByteArrayOutputStream();
    try (OutputStream os = getOutputStreamForMode(mode, stream2)) {
      os.write(input2);
    }

    File tmpFile = tmpFolder.newFile();
    try (OutputStream os = new FileOutputStream(tmpFile)) {
      os.write(stream1.toByteArray());
      os.write(stream2.toByteArray());
    }

    byte[] output = Bytes.concat(input1, input2);
    verifyReadContents(output, tmpFile, mode);
  }

  /**
   * Test a lzo file containing multiple streams is correctly decompressed.
   *
   * <p>A lzo file may contain multiple streams and should decompress as the concatenation of those
   * streams.
   */
  @Test
  public void testReadMultiStreamLzo() throws IOException {
    CompressionMode mode = CompressionMode.LZO;
    byte[] input1 = generateInput(5, 587973);
    byte[] input2 = generateInput(5, 387374);

    ByteArrayOutputStream stream1 = new ByteArrayOutputStream();
    try (OutputStream os = getOutputStreamForMode(mode, stream1)) {
      os.write(input1);
    }

    ByteArrayOutputStream stream2 = new ByteArrayOutputStream();
    try (OutputStream os = getOutputStreamForMode(mode, stream2)) {
      os.write(input2);
    }

    File tmpFile = tmpFolder.newFile();
    try (OutputStream os = new FileOutputStream(tmpFile)) {
      os.write(stream1.toByteArray());
      os.write(stream2.toByteArray());
    }

    byte[] output = Bytes.concat(input1, input2);
    verifyReadContents(output, tmpFile, mode);
  }

  /**
   * Test a lzop file containing multiple streams is not correctly decompressed. The current
   * behavior is it only reads the contents of the first file.
   */
  @Test
  public void testFalseReadMultiStreamLzop() throws IOException {
    CompressionMode mode = CompressionMode.LZOP;
    byte[] input1 = generateInput(5, 587973);
    byte[] input2 = generateInput(5, 387374);

    ByteArrayOutputStream stream1 = new ByteArrayOutputStream();
    try (OutputStream os = getOutputStreamForMode(mode, stream1)) {
      os.write(input1);
    }

    ByteArrayOutputStream stream2 = new ByteArrayOutputStream();
    try (OutputStream os = getOutputStreamForMode(mode, stream2)) {
      os.write(input2);
    }

    File tmpFile = tmpFolder.newFile();
    try (OutputStream os = new FileOutputStream(tmpFile)) {
      os.write(stream1.toByteArray());
      os.write(stream2.toByteArray());
    }

    byte[] output = Bytes.concat(input1, input2);
    thrown.expectMessage("expected");
    verifyReadContents(output, tmpFile, mode);
  }

  /** Test reading empty input with bzip2. */
  @Test
  public void testCompressedReadBzip2() throws Exception {
    byte[] input = generateInput(0);
    runReadTest(input, CompressionMode.BZIP2);
  }

  /** Test reading empty input with zstd. */
  @Test
  public void testCompressedReadZstd() throws Exception {
    byte[] input = generateInput(0);
    runReadTest(input, CompressionMode.ZSTD);
  }

  /** Test reading according to filepattern when the file is gzipped. */
  @Test
  public void testCompressedAccordingToFilepatternGzip() throws Exception {
    byte[] input = generateInput(100);
    File tmpFile = tmpFolder.newFile("test.gz");
    writeFile(tmpFile, input, CompressionMode.GZIP);
    verifyReadContents(input, tmpFile, null /* default auto decompression factory */);
  }

  /** Test reading according to filepattern when the file is bzipped. */
  @Test
  public void testCompressedAccordingToFilepatternBzip2() throws Exception {
    byte[] input = generateInput(100);
    File tmpFile = tmpFolder.newFile("test.bz2");
    writeFile(tmpFile, input, CompressionMode.BZIP2);
    verifyReadContents(input, tmpFile, null /* default auto decompression factory */);
  }

  /** Test reading according to filepattern when the file is zstd compressed. */
  @Test
  public void testCompressedAccordingToFilepatternZstd() throws Exception {
    byte[] input = generateInput(100);
    File tmpFile = tmpFolder.newFile("test.zst");
    writeFile(tmpFile, input, CompressionMode.ZSTD);
    verifyReadContents(input, tmpFile, null /* default auto decompression factory */);
  }

  /** Test reading according to filepattern when the file is lzo compressed using LZO Codec. */
  @Test
  public void testCompressedAccordingToFilepatternLzo() throws Exception {
    byte[] input = generateInput(100);
    File tmpFile = tmpFolder.newFile("test.lzo_deflate");
    writeFile(tmpFile, input, CompressionMode.LZO);
    verifyReadContents(input, tmpFile, null /* default auto decompression factory */);
  }

  /** Test reading according to filepattern when the file is lzo compressed using LZOP Codec. */
  @Test
  public void testCompressedAccordingToFilepatternLzop() throws Exception {
    byte[] input = generateInput(100);
    File tmpFile = tmpFolder.newFile("test.lzo");
    writeFile(tmpFile, input, CompressionMode.LZOP);
    verifyReadContents(input, tmpFile, null /* default auto decompression factory */);
  }

  /** Test reading multiple files with different compression. */
  @Test
  public void testHeterogeneousCompression() throws Exception {
    String baseName = "test-input";

    // Expected data
    byte[] generated = generateInput(1000);
    List<Byte> expected = new ArrayList<>();

    // Every sort of compression
    File uncompressedFile = tmpFolder.newFile(baseName + ".bin");
    generated = generateInput(1000, 1);
    Files.write(generated, uncompressedFile);
    expected.addAll(Bytes.asList(generated));

    File gzipFile = tmpFolder.newFile(baseName + ".gz");
    generated = generateInput(1000, 2);
    writeFile(gzipFile, generated, CompressionMode.GZIP);
    expected.addAll(Bytes.asList(generated));

    File bzip2File = tmpFolder.newFile(baseName + ".bz2");
    generated = generateInput(1000, 3);
    writeFile(bzip2File, generated, CompressionMode.BZIP2);
    expected.addAll(Bytes.asList(generated));

    File zstdFile = tmpFolder.newFile(baseName + ".zst");
    generated = generateInput(1000, 4);
    writeFile(zstdFile, generated, CompressionMode.ZSTD);
    expected.addAll(Bytes.asList(generated));

    File lzoFile = tmpFolder.newFile(baseName + ".lzo_deflate");
    generated = generateInput(1000, 4);
    writeFile(lzoFile, generated, CompressionMode.LZO);
    expected.addAll(Bytes.asList(generated));

    String filePattern = new File(tmpFolder.getRoot().toString(), baseName + ".*").toString();

    CompressedSource<Byte> source = CompressedSource.from(new ByteSource(filePattern, 1));
    List<Byte> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
    assertEquals(HashMultiset.create(actual), HashMultiset.create(expected));
  }

  @Test
  public void testUncompressedFileWithAutoIsSplittable() throws Exception {
    String baseName = "test-input";

    File uncompressedFile = tmpFolder.newFile(baseName + ".bin");
    Files.write(generateInput(10), uncompressedFile);

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(uncompressedFile.getPath(), 1));
    assertTrue(source.isSplittable());
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testUncompressedFileWithUncompressedIsSplittable() throws Exception {
    String baseName = "test-input";

    File uncompressedFile = tmpFolder.newFile(baseName + ".bin");
    Files.write(generateInput(10), uncompressedFile);

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(uncompressedFile.getPath(), 1))
            .withDecompression(CompressionMode.UNCOMPRESSED);
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

  @Test
  public void testZstdFileIsNotSplittable() throws Exception {
    String baseName = "test-input";

    File compressedFile = tmpFolder.newFile(baseName + ".zst");
    writeFile(compressedFile, generateInput(10), CompressionMode.ZSTD);

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(compressedFile.getPath(), 1));
    assertFalse(source.isSplittable());
  }

  @Test
  public void testLzoFileIsNotSplittable() throws Exception {
    String baseName = "test-input";

    File compressedFile = tmpFolder.newFile(baseName + ".lzo_deflate");
    writeFile(compressedFile, generateInput(10), CompressionMode.LZO);

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(compressedFile.getPath(), 1));
    assertFalse(source.isSplittable());
  }

  @Test
  public void testLzopFileIsNotSplittable() throws Exception {
    String baseName = "test-input";

    File compressedFile = tmpFolder.newFile(baseName + ".lzo");
    writeFile(compressedFile, generateInput(10), CompressionMode.LZOP);

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(compressedFile.getPath(), 1));
    assertFalse(source.isSplittable());
  }

  /**
   * Test reading an uncompressed file with {@link CompressionMode#GZIP}, since we must support this
   * due to properties of services that we read from.
   */
  @Test
  public void testFalseGzipStream() throws Exception {
    byte[] input = generateInput(1000);
    File tmpFile = tmpFolder.newFile("test.gz");
    Files.write(input, tmpFile);
    verifyReadContents(input, tmpFile, CompressionMode.GZIP);
  }

  /**
   * Test reading an uncompressed file with {@link CompressionMode#BZIP2}, and show that we fail.
   */
  @Test
  public void testFalseBzip2Stream() throws Exception {
    byte[] input = generateInput(1000);
    File tmpFile = tmpFolder.newFile("test.bz2");
    Files.write(input, tmpFile);
    thrown.expectMessage("Stream is not in the BZip2 format");
    verifyReadContents(input, tmpFile, CompressionMode.BZIP2);
  }

  /** Test reading an uncompressed file with {@link Compression#ZSTD}, and show that we fail. */
  @Test
  public void testFalseZstdStream() throws Exception {
    byte[] input = generateInput(1000);
    File tmpFile = tmpFolder.newFile("test.zst");
    Files.write(input, tmpFile);
    thrown.expectMessage("Decompression error: Unknown frame descriptor");
    verifyReadContents(input, tmpFile, CompressionMode.ZSTD);
  }

  /** Test reading an uncompressed file with {@link Compression#LZO}, and show that we fail. */
  @Test
  public void testFalseLzoStream() throws Exception {
    byte[] input = generateInput(1000);
    File tmpFile = tmpFolder.newFile("test.lzo_deflate");
    Files.write(input, tmpFile);
    thrown.expectMessage("expected:");
    verifyReadContents(input, tmpFile, CompressionMode.LZO);
  }

  /** Test reading an uncompressed file with {@link Compression#LZOP}, and show that we fail. */
  @Test
  public void testFalseLzopStream() throws Exception {
    byte[] input = generateInput(1000);
    File tmpFile = tmpFolder.newFile("test.lzo");
    Files.write(input, tmpFile);
    thrown.expectMessage("Not an LZOP file");
    verifyReadContents(input, tmpFile, CompressionMode.LZOP);
  }

  /**
   * Test reading an empty input file with gzip; it must be interpreted as uncompressed because the
   * gzip header is two bytes.
   */
  @Test
  public void testEmptyReadGzipUncompressed() throws Exception {
    byte[] input = generateInput(0);
    File tmpFile = tmpFolder.newFile("test.gz");
    Files.write(input, tmpFile);
    verifyReadContents(input, tmpFile, CompressionMode.GZIP);
  }

  /**
   * Test reading single byte input with gzip; it must be interpreted as uncompressed because the
   * gzip header is two bytes.
   */
  @Test
  public void testOneByteReadGzipUncompressed() throws Exception {
    byte[] input = generateInput(1);
    File tmpFile = tmpFolder.newFile("test.gz");
    Files.write(input, tmpFile);
    verifyReadContents(input, tmpFile, CompressionMode.GZIP);
  }

  /** Test reading multiple files. */
  @Test
  public void testCompressedReadMultipleFiles() throws Exception {
    int numFiles = 3;
    String baseName = "test_input-";
    String filePattern = new File(tmpFolder.getRoot().toString(), baseName + "*").toString();
    List<Byte> expected = new ArrayList<>();

    for (int i = 0; i < numFiles; i++) {
      byte[] generated = generateInput(100);
      File tmpFile = tmpFolder.newFile(baseName + i);
      writeFile(tmpFile, generated, CompressionMode.GZIP);
      expected.addAll(Bytes.asList(generated));
    }

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(filePattern, 1))
            .withDecompression(CompressionMode.GZIP);
    List<Byte> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
    assertEquals(HashMultiset.create(expected), HashMultiset.create(actual));
  }

  /** Test reading multiple files. LZO Codec */
  @Test
  public void testCompressedReadMultipleLzoFiles() throws Exception {
    int numFiles = 3;
    String baseName = "test_input-";
    String filePattern = new File(tmpFolder.getRoot().toString(), baseName + "*").toString();
    List<Byte> expected = new ArrayList<>();

    for (int i = 0; i < numFiles; i++) {
      byte[] generated = generateInput(100);
      File tmpFile = tmpFolder.newFile(baseName + i);
      writeFile(tmpFile, generated, CompressionMode.LZO);
      expected.addAll(Bytes.asList(generated));
    }

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(filePattern, 1))
            .withDecompression(CompressionMode.LZO);
    List<Byte> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
    assertEquals(HashMultiset.create(expected), HashMultiset.create(actual));
  }

  /** Test reading multiple files. LZOP Codec */
  @Test
  public void testCompressedReadMultipleLzopFiles() throws Exception {
    int numFiles = 3;
    String baseName = "test_input-";
    String filePattern = new File(tmpFolder.getRoot().toString(), baseName + "*").toString();
    List<Byte> expected = new ArrayList<>();

    for (int i = 0; i < numFiles; i++) {
      byte[] generated = generateInput(100);
      File tmpFile = tmpFolder.newFile(baseName + i);
      writeFile(tmpFile, generated, CompressionMode.LZOP);
      expected.addAll(Bytes.asList(generated));
    }

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(filePattern, 1))
            .withDecompression(CompressionMode.LZOP);
    List<Byte> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
    assertEquals(HashMultiset.create(expected), HashMultiset.create(actual));
  }

  @Test
  public void testDisplayData() {
    ByteSource inputSource =
        new ByteSource("foobar.txt", 1) {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo", "bar"));
          }
        };

    CompressedSource<?> compressedSource = CompressedSource.from(inputSource);
    CompressedSource<?> gzipSource = compressedSource.withDecompression(CompressionMode.GZIP);

    DisplayData compressedSourceDisplayData = DisplayData.from(compressedSource);
    DisplayData gzipDisplayData = DisplayData.from(gzipSource);

    assertThat(compressedSourceDisplayData, hasDisplayItem("compressionMode"));
    assertThat(gzipDisplayData, hasDisplayItem("compressionMode", CompressionMode.GZIP.toString()));
    assertThat(compressedSourceDisplayData, hasDisplayItem("source", inputSource.getClass()));
    assertThat(compressedSourceDisplayData, includesDisplayDataFor("source", inputSource));
  }

  /** Generate byte array of given size. */
  private byte[] generateInput(int size) {
    // Arbitrary but fixed seed
    return generateInput(size, 285930);
  }

  /** Generate byte array of given size. */
  private byte[] generateInput(int size, int seed) {
    // Arbitrary but fixed seed
    Random random = new Random(seed);
    byte[] buff = new byte[size];
    random.nextBytes(buff);
    return buff;
  }

  /** Get a compressing stream for a given compression mode. */
  private OutputStream getOutputStreamForMode(CompressionMode mode, OutputStream stream)
      throws IOException {
    switch (mode) {
      case GZIP:
        return new GzipCompressorOutputStream(stream);
      case BZIP2:
        return new BZip2CompressorOutputStream(stream);
      case ZIP:
        return new TestZipOutputStream(stream);
      case ZSTD:
        return new ZstdCompressorOutputStream(stream);
      case DEFLATE:
        return new DeflateCompressorOutputStream(stream);
      case LZO:
        return LzoCompression.createLzoOutputStream(stream);
      case LZOP:
        return LzoCompression.createLzopOutputStream(stream);
      default:
        throw new RuntimeException("Unexpected compression mode");
    }
  }

  /** Extend of {@link ZipOutputStream} that splits up bytes into multiple entries. */
  private static class TestZipOutputStream extends OutputStream {

    private ZipOutputStream zipOutputStream;
    private long offset = 0;
    private int entry = 0;

    public TestZipOutputStream(OutputStream stream) throws IOException {
      super();
      zipOutputStream = new ZipOutputStream(stream);
      zipOutputStream.putNextEntry(new ZipEntry(String.format("entry-%05d", entry)));
    }

    @Override
    public void write(int b) throws IOException {
      zipOutputStream.write(b);
      offset++;
      if (offset % 100 == 0) {
        entry++;
        zipOutputStream.putNextEntry(new ZipEntry(String.format("entry-%05d", entry)));
      }
    }

    @Override
    public void close() throws IOException {
      zipOutputStream.closeEntry();
      super.close();
    }
  }

  /** Writes a single output file. */
  private void writeFile(File file, byte[] input, CompressionMode mode) throws IOException {
    try (OutputStream os = getOutputStreamForMode(mode, new FileOutputStream(file))) {
      os.write(input);
    }
  }

  /** Run a single read test, writing and reading back input with the given compression mode. */
  private void runReadTest(
      byte[] input,
      CompressionMode inputCompressionMode,
      @Nullable DecompressingChannelFactory decompressionFactory)
      throws IOException {
    File tmpFile = tmpFolder.newFile();
    writeFile(tmpFile, input, inputCompressionMode);
    verifyReadContents(input, tmpFile, decompressionFactory);
  }

  private void verifyReadContents(
      byte[] expected, File inputFile, @Nullable DecompressingChannelFactory decompressionFactory)
      throws IOException {
    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(inputFile.toPath().toString(), 1));
    if (decompressionFactory != null) {
      source = source.withDecompression(decompressionFactory);
    }
    List<KV<Long, Byte>> actualOutput = Lists.newArrayList();
    try (BoundedReader<Byte> reader = source.createReader(PipelineOptionsFactory.create())) {
      for (boolean more = reader.start(); more; more = reader.advance()) {
        actualOutput.add(KV.of(reader.getCurrentTimestamp().getMillis(), reader.getCurrent()));
      }
    }
    List<KV<Long, Byte>> expectedOutput = Lists.newArrayList();
    for (int i = 0; i < expected.length; i++) {
      expectedOutput.add(KV.of((long) i, expected[i]));
    }
    assertEquals(expectedOutput, actualOutput);
  }

  /** Run a single read test, writing and reading back input with the given compression mode. */
  private void runReadTest(byte[] input, CompressionMode mode) throws IOException {
    runReadTest(input, mode, mode);
  }

  /** Dummy source for use in tests. */
  private static class ByteSource extends FileBasedSource<Byte> {
    public ByteSource(String fileOrPatternSpec, long minBundleSize) {
      super(StaticValueProvider.of(fileOrPatternSpec), minBundleSize);
    }

    public ByteSource(Metadata metadata, long minBundleSize, long startOffset, long endOffset) {
      super(metadata, minBundleSize, startOffset, endOffset);
    }

    @Override
    protected ByteSource createForSubrangeOfFile(Metadata metadata, long start, long end) {
      return new ByteSource(metadata, getMinBundleSize(), start, end);
    }

    @Override
    protected FileBasedReader<Byte> createSingleFileReader(PipelineOptions options) {
      return new ByteReader(this);
    }

    @Override
    public Coder<Byte> getOutputCoder() {
      return SerializableCoder.of(Byte.class);
    }

    private static class ByteReader extends FileBasedReader<Byte> {
      ByteBuffer buff = ByteBuffer.allocate(1);
      Byte current;
      long offset;
      ReadableByteChannel channel;

      public ByteReader(ByteSource source) {
        super(source);
        offset = source.getStartOffset() - 1;
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
        current = buff.get(0);
        offset += 1;
        return true;
      }

      @Override
      protected long getCurrentOffset() {
        return offset;
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return new Instant(getCurrentOffset());
      }
    }
  }

  private static class ExtractIndexFromTimestamp extends DoFn<Byte, KV<Long, Byte>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(KV.of(context.timestamp().getMillis(), context.element()));
    }
  }

  @Test
  public void testEmptyGzipProgress() throws IOException {
    File tmpFile = tmpFolder.newFile("empty.gz");
    String filename = tmpFile.toPath().toString();
    writeFile(tmpFile, new byte[0], CompressionMode.GZIP);

    PipelineOptions options = PipelineOptionsFactory.create();
    CompressedSource<Byte> source = CompressedSource.from(new ByteSource(filename, 1));
    try (BoundedReader<Byte> readerOrig = source.createReader(options)) {
      assertThat(readerOrig, instanceOf(CompressedReader.class));
      CompressedReader<Byte> reader = (CompressedReader<Byte>) readerOrig;
      // before starting
      assertEquals(0.0, reader.getFractionConsumed(), delta);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(1, reader.getSplitPointsRemaining());

      // confirm empty
      assertFalse(reader.start());

      // after reading empty source
      assertEquals(1.0, reader.getFractionConsumed(), delta);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(0, reader.getSplitPointsRemaining());
    }
  }

  @Test
  public void testGzipProgress() throws IOException {
    int numRecords = 3;
    File tmpFile = tmpFolder.newFile("nonempty.gz");
    String filename = tmpFile.toPath().toString();
    writeFile(tmpFile, new byte[numRecords], CompressionMode.GZIP);

    PipelineOptions options = PipelineOptionsFactory.create();
    CompressedSource<Byte> source = CompressedSource.from(new ByteSource(filename, 1));
    try (BoundedReader<Byte> readerOrig = source.createReader(options)) {
      assertThat(readerOrig, instanceOf(CompressedReader.class));
      CompressedReader<Byte> reader = (CompressedReader<Byte>) readerOrig;
      // before starting
      assertEquals(0.0, reader.getFractionConsumed(), delta);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(1, reader.getSplitPointsRemaining());

      // confirm has three records
      for (int i = 0; i < numRecords; ++i) {
        if (i == 0) {
          assertTrue(reader.start());
        } else {
          assertTrue(reader.advance());
        }
        assertEquals(0, reader.getSplitPointsConsumed());
        assertEquals(1, reader.getSplitPointsRemaining());
      }
      assertFalse(reader.advance());

      // after reading source
      assertEquals(1.0, reader.getFractionConsumed(), delta);
      assertEquals(1, reader.getSplitPointsConsumed());
      assertEquals(0, reader.getSplitPointsRemaining());
    }
  }

  @Test
  public void testEmptyLzoProgress() throws IOException {
    File tmpFile = tmpFolder.newFile("empty.lzo_deflate");
    String filename = tmpFile.toPath().toString();
    writeFile(tmpFile, new byte[0], CompressionMode.LZO);

    PipelineOptions options = PipelineOptionsFactory.create();
    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(filename, 1)).withDecompression(CompressionMode.LZO);
    try (BoundedReader<Byte> readerOrig = source.createReader(options)) {
      assertThat(readerOrig, instanceOf(CompressedReader.class));
      CompressedReader<Byte> reader = (CompressedReader<Byte>) readerOrig;
      // before starting
      assertEquals(0.0, reader.getFractionConsumed(), delta);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(1, reader.getSplitPointsRemaining());
      // confirm empty
      assertFalse(reader.start());
      // after reading empty source
      assertEquals(1.0, reader.getFractionConsumed(), delta);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(0, reader.getSplitPointsRemaining());
    }
  }

  @Test
  public void testLzoProgress() throws IOException {
    int numRecords = 3;
    File tmpFile = tmpFolder.newFile("nonempty.lzo");
    String filename = tmpFile.toPath().toString();
    writeFile(tmpFile, new byte[numRecords], CompressionMode.LZO);

    PipelineOptions options = PipelineOptionsFactory.create();
    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(filename, 1)).withDecompression(CompressionMode.LZO);
    try (BoundedReader<Byte> readerOrig = source.createReader(options)) {
      assertThat(readerOrig, instanceOf(CompressedReader.class));
      CompressedReader<Byte> reader = (CompressedReader<Byte>) readerOrig;
      // before starting
      assertEquals(0.0, reader.getFractionConsumed(), delta);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(1, reader.getSplitPointsRemaining());

      // confirm has three records
      for (int i = 0; i < numRecords; ++i) {
        if (i == 0) {
          assertTrue(reader.start());
        } else {
          assertTrue(reader.advance());
        }
        assertEquals(0, reader.getSplitPointsConsumed());
        assertEquals(1, reader.getSplitPointsRemaining());
      }
      assertFalse(reader.advance());

      // after reading source
      assertEquals(1.0, reader.getFractionConsumed(), delta);
      assertEquals(1, reader.getSplitPointsConsumed());
      assertEquals(0, reader.getSplitPointsRemaining());
    }
  }

  @Test
  public void testEmptyLzopProgress() throws IOException {
    File tmpFile = tmpFolder.newFile("empty.lzo");
    String filename = tmpFile.toPath().toString();
    writeFile(tmpFile, new byte[0], CompressionMode.LZOP);

    PipelineOptions options = PipelineOptionsFactory.create();
    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(filename, 1)).withDecompression(CompressionMode.LZOP);
    try (BoundedReader<Byte> readerOrig = source.createReader(options)) {
      assertThat(readerOrig, instanceOf(CompressedReader.class));
      CompressedReader<Byte> reader = (CompressedReader<Byte>) readerOrig;
      // before starting
      assertEquals(0.0, reader.getFractionConsumed(), delta);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(1, reader.getSplitPointsRemaining());

      // confirm empty
      assertFalse(reader.start());

      // after reading empty source
      assertEquals(1.0, reader.getFractionConsumed(), delta);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(0, reader.getSplitPointsRemaining());
    }
  }

  @Test
  public void testLzopProgress() throws IOException {
    int numRecords = 3;
    File tmpFile = tmpFolder.newFile("nonempty.lzo");
    String filename = tmpFile.toPath().toString();
    writeFile(tmpFile, new byte[numRecords], CompressionMode.LZOP);

    PipelineOptions options = PipelineOptionsFactory.create();
    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(filename, 1)).withDecompression(CompressionMode.LZOP);
    try (BoundedReader<Byte> readerOrig = source.createReader(options)) {
      assertThat(readerOrig, instanceOf(CompressedReader.class));
      CompressedReader<Byte> reader = (CompressedReader<Byte>) readerOrig;
      // before starting
      assertEquals(0.0, reader.getFractionConsumed(), delta);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(1, reader.getSplitPointsRemaining());

      // confirm has three records
      for (int i = 0; i < numRecords; ++i) {
        if (i == 0) {
          assertTrue(reader.start());
        } else {
          assertTrue(reader.advance());
        }
        assertEquals(0, reader.getSplitPointsConsumed());
        assertEquals(1, reader.getSplitPointsRemaining());
      }
      assertFalse(reader.advance());

      // after reading source
      assertEquals(1.0, reader.getFractionConsumed(), delta);
      assertEquals(1, reader.getSplitPointsConsumed());
      assertEquals(0, reader.getSplitPointsRemaining());
    }
  }

  @Test
  public void testUnsplittable() throws IOException {
    String baseName = "test-input";
    File compressedFile = tmpFolder.newFile(baseName + ".gz");
    byte[] input = generateInput(10000);
    writeFile(compressedFile, input, CompressionMode.GZIP);

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(compressedFile.getPath(), 1));
    List<Byte> expected = Lists.newArrayList();
    for (byte i : input) {
      expected.add(i);
    }

    PipelineOptions options = PipelineOptionsFactory.create();
    BoundedReader<Byte> reader = source.createReader(options);

    List<Byte> actual = Lists.newArrayList();
    for (boolean hasNext = reader.start(); hasNext; hasNext = reader.advance()) {
      actual.add(reader.getCurrent());
      // checkpoint every 9 elements
      if (actual.size() % 9 == 0) {
        Double fractionConsumed = reader.getFractionConsumed();
        assertNotNull(fractionConsumed);
        assertNull(reader.splitAtFraction(fractionConsumed));
      }
    }
    assertEquals(expected.size(), actual.size());
    assertEquals(Sets.newHashSet(expected), Sets.newHashSet(actual));
  }

  @Test
  public void testSplittableProgress() throws IOException {
    File tmpFile = tmpFolder.newFile("nonempty.txt");
    String filename = tmpFile.toPath().toString();
    Files.write(new byte[2], tmpFile);

    PipelineOptions options = PipelineOptionsFactory.create();
    CompressedSource<Byte> source = CompressedSource.from(new ByteSource(filename, 1));
    try (BoundedReader<Byte> readerOrig = source.createReader(options)) {
      assertThat(readerOrig, not(instanceOf(CompressedReader.class)));
      assertThat(readerOrig, instanceOf(FileBasedReader.class));
      FileBasedReader<Byte> reader = (FileBasedReader<Byte>) readerOrig;

      // Check preconditions before starting
      assertEquals(0.0, reader.getFractionConsumed(), delta);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

      // First record: none consumed, unknown remaining.
      assertTrue(reader.start());
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

      // Second record: 1 consumed, know that we're on the last record.
      assertTrue(reader.advance());
      assertEquals(1, reader.getSplitPointsConsumed());
      assertEquals(1, reader.getSplitPointsRemaining());

      // Confirm empty and check post-conditions
      assertFalse(reader.advance());
      assertEquals(1.0, reader.getFractionConsumed(), delta);
      assertEquals(2, reader.getSplitPointsConsumed());
      assertEquals(0, reader.getSplitPointsRemaining());
    }
  }
}
