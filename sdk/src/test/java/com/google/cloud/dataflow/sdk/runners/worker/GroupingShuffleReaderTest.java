/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.api.client.util.Base64.encodeBase64URLSafeString;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.forkRequestAtPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.positionFromForkResult;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.toCloudPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.toForkRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.api.services.dataflow.model.Position;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.Reiterable;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Lists;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * Tests for GroupingShuffleReader.
 */
@RunWith(JUnit4.class)
public class GroupingShuffleReaderTest {
  private static final List<KV<Integer, List<String>>> NO_KVS = Collections.emptyList();

  private static final Instant timestamp = new Instant(123000);
  private static final IntervalWindow window = new IntervalWindow(timestamp, timestamp.plus(1000));

  private static final List<KV<Integer, List<String>>> KVS = Arrays.asList(
      KV.of(1, Arrays.asList("in 1a", "in 1b")), KV.of(2, Arrays.asList("in 2a", "in 2b")),
      KV.of(3, Arrays.asList("in 3")), KV.of(4, Arrays.asList("in 4a", "in 4b", "in 4c", "in 4d")),
      KV.of(5, Arrays.asList("in 5")));

  /** How many of the values with each key are to be read. */
  private enum ValuesToRead {
    /** Don't even ask for the values iterator. */
    SKIP_VALUES,
    /** Get the iterator, but don't read any values. */
    READ_NO_VALUES,
    /** Read just the first value. */
    READ_ONE_VALUE,
    /** Read all the values. */
    READ_ALL_VALUES
  }

  private void runTestReadFromShuffle(
      List<KV<Integer, List<String>>> input, ValuesToRead valuesToRead) throws Exception {
    Coder<WindowedValue<KV<Integer, String>>> sinkElemCoder = WindowedValue.getFullCoder(
        KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()), IntervalWindow.getCoder());

    Coder<WindowedValue<KV<Integer, Iterable<String>>>> sourceElemCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(BigEndianIntegerCoder.of(), IterableCoder.of(StringUtf8Coder.of())),
            IntervalWindow.getCoder());

    // Write to shuffle with GROUP_KEYS ShuffleSink.
    ShuffleSink<KV<Integer, String>> shuffleSink = new ShuffleSink<>(
        PipelineOptionsFactory.create(), null, ShuffleSink.ShuffleKind.GROUP_KEYS, sinkElemCoder);

    TestShuffleWriter shuffleWriter = new TestShuffleWriter();

    int kvCount = 0;
    List<Long> actualSizes = new ArrayList<>();
    try (Sink.SinkWriter<WindowedValue<KV<Integer, String>>> shuffleSinkWriter =
        shuffleSink.writer(shuffleWriter)) {
      for (KV<Integer, List<String>> kvs : input) {
        Integer key = kvs.getKey();
        for (String value : kvs.getValue()) {
          ++kvCount;
          actualSizes.add(shuffleSinkWriter.add(
              WindowedValue.of(KV.of(key, value), timestamp, Lists.newArrayList(window))));
        }
      }
    }
    List<ShuffleEntry> records = shuffleWriter.getRecords();
    assertEquals(kvCount, records.size());
    assertEquals(shuffleWriter.getSizes(), actualSizes);

    // Read from shuffle with GroupingShuffleReader.
    BatchModeExecutionContext context = new BatchModeExecutionContext();
    GroupingShuffleReader<Integer, String> groupingShuffleReader = new GroupingShuffleReader<>(
        PipelineOptionsFactory.create(), null, null, null, sourceElemCoder, context);
    ExecutorTestUtils.TestReaderObserver observer =
        new ExecutorTestUtils.TestReaderObserver(groupingShuffleReader);

    TestShuffleReader shuffleReader = new TestShuffleReader();
    List<Integer> expectedSizes = new ArrayList<>();
    for (ShuffleEntry record : records) {
      expectedSizes.add(record.length());
      shuffleReader.addEntry(record);
    }

    List<KV<Integer, List<String>>> actual = new ArrayList<>();
    try (Reader.ReaderIterator<WindowedValue<KV<Integer, Reiterable<String>>>> iter =
        groupingShuffleReader.iterator(shuffleReader)) {
      Iterable<String> prevValuesIterable = null;
      Iterator<String> prevValuesIterator = null;
      while (iter.hasNext()) {
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());

        WindowedValue<KV<Integer, Reiterable<String>>> windowedValue = iter.next();
        // Verify value is in an empty windows.
        assertEquals(Long.MIN_VALUE, windowedValue.getTimestamp().getMillis());
        assertEquals(0, windowedValue.getWindows().size());

        KV<Integer, Reiterable<String>> elem = windowedValue.getValue();
        Integer key = elem.getKey();
        List<String> values = new ArrayList<>();
        if (valuesToRead.ordinal() > ValuesToRead.SKIP_VALUES.ordinal()) {
          if (prevValuesIterable != null) {
            prevValuesIterable.iterator(); // Verifies that this does not throw.
          }
          if (prevValuesIterator != null) {
            prevValuesIterator.hasNext(); // Verifies that this does not throw.
          }

          Iterable<String> valuesIterable = elem.getValue();
          Iterator<String> valuesIterator = valuesIterable.iterator();

          if (valuesToRead.ordinal() >= ValuesToRead.READ_ONE_VALUE.ordinal()) {
            while (valuesIterator.hasNext()) {
              assertTrue(valuesIterator.hasNext());
              assertTrue(valuesIterator.hasNext());
              assertEquals("BatchModeExecutionContext key", key, context.getKey());
              values.add(valuesIterator.next());
              if (valuesToRead == ValuesToRead.READ_ONE_VALUE) {
                break;
              }
            }
            if (valuesToRead == ValuesToRead.READ_ALL_VALUES) {
              assertFalse(valuesIterator.hasNext());
              assertFalse(valuesIterator.hasNext());

              try {
                valuesIterator.next();
                fail("Expected NoSuchElementException");
              } catch (NoSuchElementException exn) {
                // As expected.
              }
              valuesIterable.iterator(); // Verifies that this does not throw.
            }
          }

          prevValuesIterable = valuesIterable;
          prevValuesIterator = valuesIterator;
        }

        actual.add(KV.of(key, values));
      }
      assertFalse(iter.hasNext());
      assertFalse(iter.hasNext());
      try {
        iter.next();
        fail("Expected NoSuchElementException");
      } catch (NoSuchElementException exn) {
        // As expected.
      }
    }

    List<KV<Integer, List<String>>> expected = new ArrayList<>();
    for (KV<Integer, List<String>> kvs : input) {
      Integer key = kvs.getKey();
      List<String> values = new ArrayList<>();
      if (valuesToRead.ordinal() >= ValuesToRead.READ_ONE_VALUE.ordinal()) {
        for (String value : kvs.getValue()) {
          values.add(value);
          if (valuesToRead == ValuesToRead.READ_ONE_VALUE) {
            break;
          }
        }
      }
      expected.add(KV.of(key, values));
    }
    assertEquals(expected, actual);
    assertEquals(expectedSizes, observer.getActualSizes());
  }

  @Test
  public void testReadEmptyShuffleData() throws Exception {
    runTestReadFromShuffle(NO_KVS, ValuesToRead.READ_ALL_VALUES);
  }

  @Test
  public void testReadEmptyShuffleDataSkippingValues() throws Exception {
    runTestReadFromShuffle(NO_KVS, ValuesToRead.SKIP_VALUES);
  }

  @Test
  public void testReadNonEmptyShuffleData() throws Exception {
    runTestReadFromShuffle(KVS, ValuesToRead.READ_ALL_VALUES);
  }

  @Test
  public void testReadNonEmptyShuffleDataReadingOneValue() throws Exception {
    runTestReadFromShuffle(KVS, ValuesToRead.READ_ONE_VALUE);
  }

  @Test
  public void testReadNonEmptyShuffleDataReadingNoValues() throws Exception {
    runTestReadFromShuffle(KVS, ValuesToRead.READ_NO_VALUES);
  }

  @Test
  public void testReadNonEmptyShuffleDataSkippingValues() throws Exception {
    runTestReadFromShuffle(KVS, ValuesToRead.SKIP_VALUES);
  }

  static byte[] fabricatePosition(int shard, @Nullable byte[] key) throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    dos.writeInt(shard);
    if (key != null) {
      dos.writeInt(Arrays.hashCode(key));
    }
    return os.toByteArray();
  }

  @Test
  public void testReadFromEmptyShuffleDataAndRequestFork() throws Exception {
    BatchModeExecutionContext context = new BatchModeExecutionContext();
    GroupingShuffleReader<Integer, Integer> groupingShuffleReader = new GroupingShuffleReader<>(
        PipelineOptionsFactory.create(), null, null, null,
        WindowedValue.getFullCoder(
            KvCoder.of(BigEndianIntegerCoder.of(), IterableCoder.of(BigEndianIntegerCoder.of())),
            IntervalWindow.getCoder()),
        context);
    TestShuffleReader shuffleReader = new TestShuffleReader();
    try (Reader.ReaderIterator<WindowedValue<KV<Integer, Reiterable<Integer>>>> iter =
        groupingShuffleReader.iterator(shuffleReader)) {
      // Can fork, the source range spans the entire interval.
      Position proposedForkPosition = new Position();
      String stop = encodeBase64URLSafeString(fabricatePosition(0, null));
      proposedForkPosition.setShufflePosition(stop);

      Reader.ForkResult forkResult =
          iter.requestFork(toForkRequest(createApproximateProgress(proposedForkPosition)));
      Reader.Position acceptedForkPosition =
          ((Reader.ForkResultWithPosition) forkResult).getAcceptedPosition();
      assertEquals(stop, toCloudPosition(acceptedForkPosition).getShufflePosition());


      // Cannot fork at a position >= the current stop position
      stop = encodeBase64URLSafeString(fabricatePosition(1, null));
      proposedForkPosition.setShufflePosition(stop);

      try {
        iter.requestFork(toForkRequest(createApproximateProgress(proposedForkPosition)));
        fail("IllegalArgumentException expected");
      } catch (IllegalArgumentException e) {
        assertThat(e.getMessage(), Matchers.containsString(
            "Fork requested at a shuffle position beyond the end of the current range"));
      }
    }
  }

  @Test
  public void testReadFromShuffleDataAndFailToFork() throws Exception {
    BatchModeExecutionContext context = new BatchModeExecutionContext();
    final int kFirstShard = 0;

    TestShuffleReader shuffleReader = new TestShuffleReader();
    final int kNumRecords = 2;
    for (int i = 0; i < kNumRecords; ++i) {
      byte[] key = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), i);
      shuffleReader.addEntry(new ShuffleEntry(fabricatePosition(kFirstShard, key), key, null, key));
    }

    // Note that TestShuffleReader start/end positions are in the
    // space of keys not the positions (TODO: should probably always
    // use positions instead).
    String stop = encodeBase64URLSafeString(fabricatePosition(kNumRecords, null));
    GroupingShuffleReader<Integer, Integer> groupingShuffleReader = new GroupingShuffleReader<>(
        PipelineOptionsFactory.create(), null, null, stop,
        WindowedValue.getFullCoder(
            KvCoder.of(BigEndianIntegerCoder.of(), IterableCoder.of(BigEndianIntegerCoder.of())),
            IntervalWindow.getCoder()),
        context);

    try (Reader.ReaderIterator<WindowedValue<KV<Integer, Reiterable<Integer>>>> iter =
        groupingShuffleReader.iterator(shuffleReader)) {
       // Cannot fork since the value provided is past the current stop position.
       try {
        iter.requestFork(forkRequestAtPosition(makeShufflePosition(kNumRecords + 1, null)));
        fail("IllegalArgumentException expected");
      } catch (IllegalArgumentException e) {
        assertThat(e.getMessage(), Matchers.containsString(
            "Fork requested at a shuffle position beyond the end of the current range"));
      }

      int i = 0;
      for (; iter.hasNext(); ++i) {
        iter.next().getValue(); // ignored
        if (i == 0) {
          // First record
          byte[] key = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), i);
          // Cannot fork since the fork position is identical with the position of the record
          // that was just returned.
          assertNull(
              iter.requestFork(forkRequestAtPosition(makeShufflePosition(kFirstShard, key))));

          // Cannot fork since the requested fork position comes before current position
          assertNull(
              iter.requestFork(forkRequestAtPosition(makeShufflePosition(kFirstShard, null))));
        }
      }
      assertEquals(kNumRecords, i);

      // Cannot fork since all input was consumed.
      assertNull(
          iter.requestFork(forkRequestAtPosition(makeShufflePosition(kFirstShard, null))));
    }
  }

  private Position makeShufflePosition(int shard, byte[] key) throws Exception {
    return new Position().setShufflePosition(
        encodeBase64URLSafeString(fabricatePosition(shard, key)));
  }

  @Test
  public void testReadFromShuffleAndFork() throws Exception {
    BatchModeExecutionContext context = new BatchModeExecutionContext();
    GroupingShuffleReader<Integer, Integer> groupingShuffleReader = new GroupingShuffleReader<>(
        PipelineOptionsFactory.create(), null, null, null,
        WindowedValue.getFullCoder(
            KvCoder.of(BigEndianIntegerCoder.of(), IterableCoder.of(BigEndianIntegerCoder.of())),
            IntervalWindow.getCoder()),
        context);

    TestShuffleReader shuffleReader = new TestShuffleReader();
    final int kNumRecords = 10;
    final int kFirstShard = 0;
    final int kSecondShard = 1;

    // Setting up two shards with kNumRecords each; keys are unique
    // (hence groups of values for the same key are singletons)
    // therefore each record comes with a unique position constructed.
    for (int i = 0; i < kNumRecords; ++i) {
      byte[] keyByte = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), i);
      ShuffleEntry entry =
          new ShuffleEntry(fabricatePosition(kFirstShard, keyByte), keyByte, null, keyByte);
      shuffleReader.addEntry(entry);
    }

    for (int i = kNumRecords; i < 2 * kNumRecords; ++i) {
      byte[] keyByte = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), i);

      ShuffleEntry entry =
          new ShuffleEntry(fabricatePosition(kSecondShard, keyByte), keyByte, null, keyByte);
      shuffleReader.addEntry(entry);
    }

    int i = 0;
    try (Reader.ReaderIterator<WindowedValue<KV<Integer, Reiterable<Integer>>>> iter =
        groupingShuffleReader.iterator(shuffleReader)) {
      assertNull(iter.requestFork(forkRequestAtPosition(new Position())));

      // Fork at the shard boundary
      Reader.ForkResult forkResult =
          iter.requestFork(forkRequestAtPosition(makeShufflePosition(kSecondShard, null)));
      assertEquals(
          encodeBase64URLSafeString(fabricatePosition(kSecondShard, null)),
          positionFromForkResult(forkResult).getShufflePosition());

      while (iter.hasNext()) {
        // iter.hasNext() is supposed to be side-effect-free and give the same result if called
        // repeatedly. Test that this is indeed the case.
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());

        KV<Integer, Reiterable<Integer>> elem = iter.next().getValue();
        int key = elem.getKey();
        assertEquals(key, i);

        Iterable<Integer> valuesIterable = elem.getValue();
        Iterator<Integer> valuesIterator = valuesIterable.iterator();

        int j = 0;
        while (valuesIterator.hasNext()) {
          assertTrue(valuesIterator.hasNext());
          assertTrue(valuesIterator.hasNext());

          int value = valuesIterator.next();
          assertEquals(value, i);
          ++j;
        }
        assertFalse(valuesIterator.hasNext());
        assertFalse(valuesIterator.hasNext());
        assertEquals(j, 1);
        ++i;
      }
      assertFalse(iter.hasNext());
    }
    assertEquals(i, kNumRecords);
  }

  @Test
  public void testGetApproximateProgress() throws Exception {
    // Store the positions of all KVs returned.
    List<byte[]> positionsList = new ArrayList<byte[]>();

    BatchModeExecutionContext context = new BatchModeExecutionContext();
    GroupingShuffleReader<Integer, Integer> groupingShuffleReader = new GroupingShuffleReader<>(
        PipelineOptionsFactory.create(), null, null, null,
        WindowedValue.getFullCoder(
            KvCoder.of(BigEndianIntegerCoder.of(), IterableCoder.of(BigEndianIntegerCoder.of())),
            IntervalWindow.getCoder()),
        context);

    TestShuffleReader shuffleReader = new TestShuffleReader();
    final int kNumRecords = 10;

    for (int i = 0; i < kNumRecords; ++i) {
      byte[] position = fabricatePosition(i, null);
      byte[] keyByte = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), i);
      positionsList.add(position);
      ShuffleEntry entry = new ShuffleEntry(position, keyByte, null, keyByte);
      shuffleReader.addEntry(entry);
    }

    try (Reader.ReaderIterator<WindowedValue<KV<Integer, Reiterable<Integer>>>> readerIterator =
        groupingShuffleReader.iterator(shuffleReader)) {
      Integer i = 0;
      while (readerIterator.hasNext()) {
        assertTrue(readerIterator.hasNext());
        ApproximateProgress progress = readerProgressToCloudProgress(readerIterator.getProgress());
        assertNotNull(progress.getPosition().getShufflePosition());

        // Compare returned position with the expected position.
        assertEquals(
            ByteArrayShufflePosition.of(positionsList.get(i)).encodeBase64(),
            progress.getPosition().getShufflePosition());

        WindowedValue<KV<Integer, Reiterable<Integer>>> elem = readerIterator.next();
        assertEquals(i, elem.getValue().getKey());
        i++;
      }
      assertFalse(readerIterator.hasNext());

      // Cannot fork since all input was consumed.
      Position proposedForkPosition = new Position();
      String stop = encodeBase64URLSafeString(fabricatePosition(0, null));
      proposedForkPosition.setShufflePosition(stop);
      assertNull(
          readerIterator.requestFork(
              toForkRequest(createApproximateProgress(proposedForkPosition))));
    }
  }

  private ApproximateProgress createApproximateProgress(Position position) {
    return new ApproximateProgress().setPosition(position);
  }
}
