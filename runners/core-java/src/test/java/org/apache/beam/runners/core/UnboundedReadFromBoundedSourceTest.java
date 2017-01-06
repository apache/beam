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
package org.apache.beam.runners.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import org.apache.beam.runners.core.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter;
import org.apache.beam.runners.core.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter.Checkpoint;
import org.apache.beam.runners.core.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter.CheckpointCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link UnboundedReadFromBoundedSource}.
 */
@RunWith(JUnit4.class)
public class UnboundedReadFromBoundedSourceTest {

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Rule
  public TestPipeline p = TestPipeline.create();

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

    PCollection<Long> output =
        p.apply(Read.from(unboundedSource).withMaxNumRecords(numElements));

    // Count == numElements
    PAssert
      .thatSingleton(output.apply("Count", Count.<Long>globally()))
      .isEqualTo(numElements);
    // Unique count == numElements
    PAssert
      .thatSingleton(output.apply(Distinct.<Long>create())
                          .apply("UniqueCount", Count.<Long>globally()))
      .isEqualTo(numElements);
    // Min == 0
    PAssert
      .thatSingleton(output.apply("Min", Min.<Long>globally()))
      .isEqualTo(0L);
    // Max == numElements-1
    PAssert
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
}
