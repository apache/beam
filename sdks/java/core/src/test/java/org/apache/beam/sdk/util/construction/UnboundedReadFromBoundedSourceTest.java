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
package org.apache.beam.sdk.util.construction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.construction.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter;
import org.apache.beam.sdk.util.construction.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter.Checkpoint;
import org.apache.beam.sdk.util.construction.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter.CheckpointCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for {@link UnboundedReadFromBoundedSource}. */
@RunWith(JUnit4.class)
public class UnboundedReadFromBoundedSourceTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(UnboundedReadFromBoundedSourceTest.class);

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  public void testCheckpointCoderNulls() throws Exception {
    CheckpointCoder<String> coder = new CheckpointCoder<>(StringUtf8Coder.of());
    Checkpoint<String> emptyCheckpoint = new Checkpoint<>(null, null);
    Checkpoint<String> decodedEmptyCheckpoint =
        CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, emptyCheckpoint));
    assertNull(decodedEmptyCheckpoint.getResidualElements());
    assertNull(decodedEmptyCheckpoint.getResidualSource());
  }

  @Test
  public void testCheckpointCoderIsSerializableWithWellKnownCoderType() throws Exception {
    CoderProperties.coderSerializable(new CheckpointCoder<>(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBoundedToUnboundedSourceAdapter() throws Exception {
    long numElements = 100;
    BoundedSource<Long> boundedSource = CountingSource.upTo(numElements);
    UnboundedSource<Long, Checkpoint<Long>> unboundedSource =
        new BoundedToUnboundedSourceAdapter<>(boundedSource);

    PCollection<Long> output = p.apply(Read.from(unboundedSource).withMaxNumRecords(numElements));

    // Count == numElements
    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(numElements);
    // Unique count == numElements
    PAssert.thatSingleton(output.apply(Distinct.create()).apply("UniqueCount", Count.globally()))
        .isEqualTo(numElements);
    // Min == 0
    PAssert.thatSingleton(output.apply("Min", Min.globally())).isEqualTo(0L);
    // Max == numElements-1
    PAssert.thatSingleton(output.apply("Max", Max.globally())).isEqualTo(numElements - 1);
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
      BoundedSource<T> boundedSource, List<T> expectedElements) throws Exception {
    BoundedToUnboundedSourceAdapter<T> unboundedSource =
        new BoundedToUnboundedSourceAdapter<>(boundedSource);

    PipelineOptions options = PipelineOptionsFactory.create();
    BoundedToUnboundedSourceAdapter<T>.Reader reader = unboundedSource.createReader(options, null);

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
    assertTrue(
        checkpointDone.getResidualElements() == null
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
      BoundedSource<T> boundedSource, List<T> expectedElements) throws Exception {
    BoundedToUnboundedSourceAdapter<T> unboundedSource =
        new BoundedToUnboundedSourceAdapter<>(boundedSource);

    PipelineOptions options = PipelineOptionsFactory.create();
    BoundedToUnboundedSourceAdapter<T>.Reader reader = unboundedSource.createReader(options, null);

    List<T> actual = Lists.newArrayList();
    for (boolean hasNext = reader.start(); hasNext; ) {
      actual.add(reader.getCurrent());
      // checkpoint every 9 elements
      if (actual.size() % 9 == 0) {
        Checkpoint<T> checkpoint = reader.getCheckpointMark();
        Coder<Checkpoint<T>> checkpointCoder = unboundedSource.getCheckpointMarkCoder();
        Checkpoint<T> decodedCheckpoint =
            CoderUtils.decodeFromByteArray(
                checkpointCoder, CoderUtils.encodeToByteArray(checkpointCoder, checkpoint));
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
    assertTrue(
        checkpointDone.getResidualElements() == null
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
  public void testInvokesSplitWithDefaultNumSplitsTooLarge() throws Exception {
    UnboundedSource<Long, ?> unboundedCountingSource =
        new BoundedToUnboundedSourceAdapter<Long>(CountingSource.upTo(1));
    PipelineOptions options = PipelineOptionsFactory.create();
    List<?> splits = unboundedCountingSource.split(100, options);
    assertEquals(1, splits.size());
    assertNotEquals(splits.get(0), unboundedCountingSource);
  }

  @Test
  public void testInvokingSplitProducesAtLeastOneSplit() throws Exception {
    UnboundedSource<Long, ?> unboundedCountingSource =
        new BoundedToUnboundedSourceAdapter<Long>(CountingSource.upTo(0));
    PipelineOptions options = PipelineOptionsFactory.create();
    List<?> splits = unboundedCountingSource.split(100, options);
    assertEquals(1, splits.size());
    assertNotEquals(splits.get(0), unboundedCountingSource);
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
  public void testReadersClosedProperly() throws IOException {
    ManagedReaderBoundedSource boundedSource = new ManagedReaderBoundedSource(0, 10);
    BoundedToUnboundedSourceAdapter<Integer> unboundedSource =
        new BoundedToUnboundedSourceAdapter<>(boundedSource);
    PipelineOptions options = PipelineOptionsFactory.create();

    BoundedToUnboundedSourceAdapter<Integer>.Reader reader =
        unboundedSource.createReader(options, new Checkpoint<Integer>(null, boundedSource));

    for (int i = 0; i < 3; ++i) {
      if (i == 0) {
        assertTrue(reader.start());
      } else {
        assertTrue(reader.advance());
      }
      assertEquals(i, (int) reader.getCurrent());
    }
    Checkpoint<Integer> checkpoint = reader.getCheckpointMark();
    List<TimestampedValue<Integer>> residualElements = checkpoint.getResidualElements();
    for (int i = 0; i < 7; ++i) {
      TimestampedValue<Integer> element = residualElements.get(i);
      assertEquals(i + 3, (int) element.getValue());
    }
    for (int i = 0; i < 100; ++i) {
      // A WeakReference of an object that no other objects reference are not immediately added to
      // ReferenceQueue. To test this, we should run System.gc() multiple times.
      // If a reader is GCed without closing, `cleanQueue` throws a RuntimeException.
      boundedSource.cleanQueue();
    }
  }

  /** Generate byte array of given size. */
  private static byte[] generateInput(int size) {
    // Arbitrary but fixed seed
    Random random = new Random(285930);
    byte[] buff = new byte[size];
    random.nextBytes(buff);
    return buff;
  }

  /** Writes a single output file. */
  private static void writeFile(File file, byte[] input) throws IOException {
    try (OutputStream os = new FileOutputStream(file)) {
      os.write(input);
    }
  }

  /** Unsplittable source for use in tests. */
  private static class UnsplittableSource extends FileBasedSource<Byte> {

    public UnsplittableSource(String fileOrPatternSpec, long minBundleSize) {
      super(StaticValueProvider.of(fileOrPatternSpec), minBundleSize);
    }

    public UnsplittableSource(
        Metadata metadata, long minBundleSize, long startOffset, long endOffset) {
      super(metadata, minBundleSize, startOffset, endOffset);
    }

    @Override
    protected UnsplittableSource createForSubrangeOfFile(Metadata metadata, long start, long end) {
      return new UnsplittableSource(metadata, getMinBundleSize(), start, end);
    }

    @Override
    protected UnsplittableReader createSingleFileReader(PipelineOptions options) {
      return new UnsplittableReader(this);
    }

    @Override
    public Coder<Byte> getOutputCoder() {
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
      public boolean allowsDynamicSplitting() {
        return false;
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
    }
  }

  /**
   * An integer generating bounded source. This source class checks if readers are closed properly.
   * For that, it manages weak references of readers, and checks at `createReader` and `cleanQueue`
   * if readers were closed before GCed. The `cleanQueue` does not change the state in
   * `ManagedReaderBoundedSource`, but throws an exception if it finds a reader GCed without
   * closing.
   */
  private static class ManagedReaderBoundedSource extends BoundedSource<Integer> {

    private final int from;
    private final int to; // exclusive

    private transient ReferenceQueue<ManagedReader> refQueue;
    private transient Map<Reference<ManagedReader>, CloseStatus> cloesStatusMap;

    public ManagedReaderBoundedSource(int from, int to) {
      if (from > to) {
        throw new RuntimeException(
            String.format("`from` <= `to`, but got from: %d, to: %d", from, to));
      }
      this.from = from;
      this.to = to;
    }

    @Override
    public List<? extends BoundedSource<Integer>> split(
        long desiredBundleSizeBytes, PipelineOptions options) {
      return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return (to - from) * 4L;
    }

    @Override
    public BoundedReader<Integer> createReader(PipelineOptions options) {
      // Add weak reference to queue to monitor GCed readers. If `CloseStatus` associated with
      // reader is not closed, it means a reader was GCed without closing properly. The CloseStatus
      // check for GCed readers are done at cleanQueue().
      if (refQueue == null) {
        refQueue = new ReferenceQueue<>();
        cloesStatusMap = new HashMap<>();
      }
      cleanQueue();

      CloseStatus status = new CloseStatus();
      ManagedReader reader = new ManagedReader(status);
      WeakReference<ManagedReader> reference = new WeakReference<>(reader, refQueue);
      cloesStatusMap.put(reference, status);
      LOG.info("Add reference {} for reader {}", reference, reader);
      return reader;
    }

    public void cleanQueue() {
      System.gc();

      Reference<? extends ManagedReader> reference;
      while ((reference = refQueue.poll()) != null) {
        CloseStatus closeStatus = cloesStatusMap.get(reference);
        LOG.info("Poll reference: {}, closed: {}", reference, closeStatus.closed);
        closeStatus.throwIfNotClosed();
      }
    }

    class CloseStatus {

      private final RuntimeException allocationStacktrace;

      private boolean closed;

      public CloseStatus() {
        allocationStacktrace =
            new RuntimeException("Previous reader was not closed properly. Reader allocation was");
        closed = false;
      }

      void close() {
        cleanQueue();
        closed = true;
      }

      void throwIfNotClosed() {
        if (!closed) {
          throw allocationStacktrace;
        }
      }
    }

    class ManagedReader extends BoundedReader<Integer> {

      private final CloseStatus status;

      int current;

      public ManagedReader(CloseStatus status) {
        this.status = status;
      }

      @Override
      public boolean start() {
        if (from < to) {
          current = from;
          return true;
        } else {
          return false;
        }
      }

      @Override
      public boolean advance() {
        if (current + 1 < to) {
          ++current;
          return true;
        } else {
          return false;
        }
      }

      @Override
      public Integer getCurrent() {
        return current;
      }

      @Override
      public void close() {
        status.close();
      }

      @Override
      public BoundedSource<Integer> getCurrentSource() {
        return ManagedReaderBoundedSource.this;
      }
    }
  }
}
