/*
 * Copyright (C) 2016 Google Inc.
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
package com.google.cloud.dataflow.sdk.runners.dataflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.CountingSource;
import com.google.cloud.dataflow.sdk.io.FileBasedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.dataflow.DataflowUnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter;
import com.google.cloud.dataflow.sdk.runners.dataflow.DataflowUnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter.Checkpoint;
import com.google.cloud.dataflow.sdk.runners.dataflow.DataflowUnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter.CheckpointCoder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.Min;
import com.google.cloud.dataflow.sdk.transforms.RemoveDuplicates;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Unit tests for {@link DataflowUnboundedReadFromBoundedSource}.
 */
@RunWith(JUnit4.class)
public class DataflowUnboundedReadFromBoundedSourceTest {

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCheckpointCoderNulls() throws Exception {
    CheckpointCoder<String> coder = new CheckpointCoder<>(StringUtf8Coder.of());
    Checkpoint<String> emptyCheckpoint = new Checkpoint<>(null, null);
    Checkpoint<String> decodedEmptyCheckpoint = CoderUtils.decodeFromByteArray(
        coder,
        CoderUtils.encodeToByteArray(coder, emptyCheckpoint));
    assertNull(decodedEmptyCheckpoint.getResidualElements());
    assertNull(decodedEmptyCheckpoint.getResidualSource());
  }

  @Test
  @Category(RunnableOnService.class)
  public void testBoundedToUnboundedSourceAdapter() throws Exception {
    long numElements = 100;
    BoundedSource<Long> boundedSource = CountingSource.upTo(numElements);
    UnboundedSource<Long, Checkpoint<Long>> unboundedSource =
        new BoundedToUnboundedSourceAdapter<>(boundedSource);

    Pipeline p = TestPipeline.create();

    PCollection<Long> output =
        p.apply(Read.from(unboundedSource).withMaxNumRecords(numElements));

    // Count == numElements
    DataflowAssert
      .thatSingleton(output.apply("Count", Count.<Long>globally()))
      .isEqualTo(numElements);
    // Unique count == numElements
    DataflowAssert
      .thatSingleton(output.apply(RemoveDuplicates.<Long>create())
                          .apply("UniqueCount", Count.<Long>globally()))
      .isEqualTo(numElements);
    // Min == 0
    DataflowAssert
      .thatSingleton(output.apply("Min", Min.<Long>globally()))
      .isEqualTo(0L);
    // Max == numElements-1
    DataflowAssert
      .thatSingleton(output.apply("Max", Max.<Long>globally()))
      .isEqualTo(numElements - 1);
    p.run();
  }

  @Test
  public void testCountingSourceToUnboundedCheckpoint() throws Exception {
    long numElements = 100;
    BoundedSource<Long> countingSource = CountingSource.upTo(numElements);
    List<Long> expected = Lists.newArrayList();
    for (long i = 0; i < numElements; ++i) {
      expected.add(i);
    }
    testBoundedToUnboundedSourceAdapterCheckpoint(countingSource, expected);
  }

  @Test
  public void testUnsplittableSourceToUnboundedCheckpoint() throws Exception {
    String baseName = "test-input";
    File compressedFile = tmpFolder.newFile(baseName + ".gz");
    byte[] input = generateInput(100);
    writeFile(compressedFile, input);

    BoundedSource<Byte> source = new UnsplittableSource(compressedFile.getPath(), 1);
    List<Byte> expected = Lists.newArrayList();
    for (byte i : input) {
      expected.add(i);
    }
    testBoundedToUnboundedSourceAdapterCheckpoint(source, expected);
  }

  private <T> void testBoundedToUnboundedSourceAdapterCheckpoint(
      BoundedSource<T> boundedSource,
      List<T> expectedElements) throws Exception {
    BoundedToUnboundedSourceAdapter<T> unboundedSource =
        new BoundedToUnboundedSourceAdapter<>(boundedSource);

    PipelineOptions options = PipelineOptionsFactory.create();
    BoundedToUnboundedSourceAdapter<T>.Reader reader =
        unboundedSource.createReader(options, null);

    List<T> actual = Lists.newArrayList();
    for (boolean hasNext = reader.start(); hasNext; hasNext = reader.advance()) {
      actual.add(reader.getCurrent());
      // checkpoint every 9 elements
      if (actual.size() % 9 == 0) {
        Checkpoint<T> checkpoint = reader.getCheckpointMark();
        checkpoint.finalizeCheckpoint();
      }
    }
    Checkpoint<T> checkpointDone = reader.getCheckpointMark();
    assertTrue(checkpointDone.getResidualElements() == null
        || checkpointDone.getResidualElements().isEmpty());

    assertEquals(expectedElements.size(), actual.size());
    assertEquals(Sets.newHashSet(expectedElements), Sets.newHashSet(actual));
  }

  @Test
  public void testCountingSourceToUnboundedCheckpointRestart() throws Exception {
    long numElements = 100;
    BoundedSource<Long> countingSource = CountingSource.upTo(numElements);
    List<Long> expected = Lists.newArrayList();
    for (long i = 0; i < numElements; ++i) {
      expected.add(i);
    }
    testBoundedToUnboundedSourceAdapterCheckpointRestart(countingSource, expected);
  }

  @Test
  public void testUnsplittableSourceToUnboundedCheckpointRestart() throws Exception {
    String baseName = "test-input";
    File compressedFile = tmpFolder.newFile(baseName + ".gz");
    byte[] input = generateInput(1000);
    writeFile(compressedFile, input);

    BoundedSource<Byte> source = new UnsplittableSource(compressedFile.getPath(), 1);
    List<Byte> expected = Lists.newArrayList();
    for (byte i : input) {
      expected.add(i);
    }
    testBoundedToUnboundedSourceAdapterCheckpointRestart(source, expected);
  }

  private <T> void testBoundedToUnboundedSourceAdapterCheckpointRestart(
      BoundedSource<T> boundedSource,
      List<T> expectedElements) throws Exception {
    BoundedToUnboundedSourceAdapter<T> unboundedSource =
        new BoundedToUnboundedSourceAdapter<>(boundedSource);

    PipelineOptions options = PipelineOptionsFactory.create();
    BoundedToUnboundedSourceAdapter<T>.Reader reader =
        unboundedSource.createReader(options, null);

    List<T> actual = Lists.newArrayList();
    for (boolean hasNext = reader.start(); hasNext;) {
      actual.add(reader.getCurrent());
      // checkpoint every 9 elements
      if (actual.size() % 9 == 0) {
        Checkpoint<T> checkpoint = reader.getCheckpointMark();
        Coder<Checkpoint<T>> checkpointCoder = unboundedSource.getCheckpointMarkCoder();
        Checkpoint<T> decodedCheckpoint = CoderUtils.decodeFromByteArray(
            checkpointCoder,
            CoderUtils.encodeToByteArray(checkpointCoder, checkpoint));
        reader.close();
        checkpoint.finalizeCheckpoint();

        BoundedToUnboundedSourceAdapter<T>.Reader restarted =
            unboundedSource.createReader(options, decodedCheckpoint);
        reader = restarted;
        hasNext = reader.start();
      } else {
        hasNext = reader.advance();
      }
    }
    Checkpoint<T> checkpointDone = reader.getCheckpointMark();
    assertTrue(checkpointDone.getResidualElements() == null
        || checkpointDone.getResidualElements().isEmpty());

    assertEquals(expectedElements.size(), actual.size());
    assertEquals(Sets.newHashSet(expectedElements), Sets.newHashSet(actual));
  }

  @Test
  public void testReadBeforeStart() throws Exception {
    thrown.expect(NoSuchElementException.class);

    BoundedSource<Long> countingSource = CountingSource.upTo(100);
    BoundedToUnboundedSourceAdapter<Long> unboundedSource =
        new BoundedToUnboundedSourceAdapter<>(countingSource);
    PipelineOptions options = PipelineOptionsFactory.create();

    unboundedSource.createReader(options, null).getCurrent();
  }

  @Test
  public void testReadFromCheckpointBeforeStart() throws Exception {
    thrown.expect(NoSuchElementException.class);

    BoundedSource<Long> countingSource = CountingSource.upTo(100);
    BoundedToUnboundedSourceAdapter<Long> unboundedSource =
        new BoundedToUnboundedSourceAdapter<>(countingSource);
    PipelineOptions options = PipelineOptionsFactory.create();

    List<TimestampedValue<Long>> elements =
        ImmutableList.of(TimestampedValue.of(1L, new Instant(1L)));
    Checkpoint<Long> checkpoint = new Checkpoint<>(elements, countingSource);
    unboundedSource.createReader(options, checkpoint).getCurrent();
  }

  @Test
  public void testKind() {
    DataflowUnboundedReadFromBoundedSource<?> read = new
        DataflowUnboundedReadFromBoundedSource<>(new NoopNamedSource());

    assertEquals("Read(NoopNamedSource)", read.getKindString());
  }

  @Test
  public void testKindAnonymousSource() {
    NoopNamedSource anonSource = new NoopNamedSource() {};
    DataflowUnboundedReadFromBoundedSource<?> read = new
        DataflowUnboundedReadFromBoundedSource<>(anonSource);

    assertEquals("Read(AnonymousSource)", read.getKindString());
  }

  /** Source implementation only useful for its identity. */
  static class NoopNamedSource extends BoundedSource<String> {
    @Override
    public List<? extends BoundedSource<String>> splitIntoBundles(long desiredBundleSizeBytes,
        PipelineOptions options) throws Exception {
      return null;
    }
    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }
    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }
    @Override
    public BoundedReader<String> createReader(
        PipelineOptions options) throws IOException {
      return null;
    }
    @Override
    public void validate() {

    }
    @Override
    public Coder<String> getDefaultOutputCoder() {
      return null;
    }
  }

  /**
   * Generate byte array of given size.
   */
  private static byte[] generateInput(int size) {
    // Arbitrary but fixed seed
    Random random = new Random(285930);
    byte[] buff = new byte[size];
    random.nextBytes(buff);
    return buff;
  }

  /**
   * Writes a single output file.
   */
  private static void writeFile(File file, byte[] input) throws IOException {
    try (OutputStream os = new FileOutputStream(file)) {
      os.write(input);
    }
  }

  /**
   * Unsplittable source for use in tests.
   */
  private static class UnsplittableSource extends FileBasedSource<Byte> {
    public UnsplittableSource(String fileOrPatternSpec, long minBundleSize) {
      super(fileOrPatternSpec, minBundleSize);
    }

    public UnsplittableSource(
        String fileName, long minBundleSize, long startOffset, long endOffset) {
      super(fileName, minBundleSize, startOffset, endOffset);
    }

    @Override
    protected FileBasedSource<Byte> createForSubrangeOfFile(String fileName, long start, long end) {
      return new UnsplittableSource(fileName, getMinBundleSize(), start, end);
    }

    @Override
    protected FileBasedReader<Byte> createSingleFileReader(PipelineOptions options) {
      return new UnsplittableReader(this);
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public boolean allowsDynamicSplitting() {
      return false;
    }

    @Override
    public Coder<Byte> getDefaultOutputCoder() {
      return SerializableCoder.of(Byte.class);
    }

    private static class UnsplittableReader extends FileBasedReader<Byte> {
      ByteBuffer buff = ByteBuffer.allocate(1);
      Byte current;
      long offset;
      ReadableByteChannel channel;

      public UnsplittableReader(UnsplittableSource source) {
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
