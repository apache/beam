/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.CompressedSource.CompressionMode;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.primitives.Bytes;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

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

  /**
   * Test reading empty input with bzip2.
   */
  @Test
  public void testCompressedReadBzip2() throws Exception {
    byte[] input = generateInput(0);
    runReadTest(input, CompressionMode.BZIP2);
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

    DataflowAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  /**
   * Generate byte array of given size.
   */
  private byte[] generateInput(int size) {
    byte[] buff = new byte[size];
    for (int i = 0; i < size; i++) {
      buff[i] = (byte) (i % Byte.MAX_VALUE);
    }
    return buff;
  }

  /**
   * Get a compressing stream for a given compression mode.
   */
  private OutputStream getStreamForMode(CompressionMode mode, OutputStream stream)
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
    try (OutputStream os = getStreamForMode(mode, new FileOutputStream(file))) {
      os.write(input);
    }
  }

  /**
   * Run a single read test, writing and reading back input with the given compression mode.
   */
  private void runReadTest(byte[] input, CompressionMode mode) throws IOException {
    File tmpFile = tmpFolder.newFile();
    writeFile(tmpFile, input, mode);

    Pipeline p = TestPipeline.create();

    CompressedSource<Byte> source =
        CompressedSource.from(new ByteSource(tmpFile.toPath().toString(), 1))
            .withDecompression(mode);
    PCollection<Byte> output = p.apply(Read.from(source));

    DataflowAssert.that(output).containsInAnyOrder(Bytes.asList(input));
    p.run();
  }

  /**
   * Dummy source for use in tests.
   */
  private static class ByteSource extends FileBasedSource<Byte> {
    private static final long serialVersionUID = 0;

    public ByteSource(String fileOrPatternSpec, long minBundleSize) {
      super(fileOrPatternSpec, minBundleSize);
    }

    public ByteSource(String fileName, long minBundleSize, long startOffset, long endOffset) {
      super(fileName, minBundleSize, startOffset, endOffset);
    }

    @Override
    public FileBasedSource<Byte> createForSubrangeOfFile(String fileName, long start, long end) {
      return new ByteSource(fileName, getMinBundleSize(), start, end);
    }

    @Override
    public ByteReader createSingleFileReader(
        PipelineOptions options, ExecutionContext executionContext) {
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
      private static final long serialVersionUID = 0;
      ByteBuffer buff = ByteBuffer.allocate(1);
      Byte current;
      long offset = 0;
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
