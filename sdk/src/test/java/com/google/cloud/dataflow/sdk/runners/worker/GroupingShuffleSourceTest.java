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
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudProgressToSourceProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.sourcePositionToCloudPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.sourceProgressToCloudProgress;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.api.services.dataflow.model.Position;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.InstantCoder;
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
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;
import com.google.cloud.dataflow.sdk.util.common.worker.Source;
import com.google.cloud.dataflow.sdk.util.common.worker.Source.SourceIterator;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Lists;

import org.joda.time.Instant;
import org.junit.Assert;
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

/**
 * Tests for GroupingShuffleSource.
 */
@RunWith(JUnit4.class)
public class GroupingShuffleSourceTest {
  private static final List<KV<Integer, List<String>>> NO_KVS = Collections.emptyList();

  private static final Instant timestamp = new Instant(123000);
  private static final IntervalWindow window = new IntervalWindow(timestamp, timestamp.plus(1000));

  private static final List<KV<Integer, List<String>>> KVS = Arrays.asList(
      KV.of(1, Arrays.asList("in 1a", "in 1b")),
      KV.of(2, Arrays.asList("in 2a", "in 2b")),
      KV.of(3, Arrays.asList("in 3")),
      KV.of(4, Arrays.asList("in 4a", "in 4b", "in 4c", "in 4d")),
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

  private void runTestReadShuffleSource(List<KV<Integer, List<String>>> input,
                                ValuesToRead valuesToRead)
      throws Exception {
    Coder<WindowedValue<String>> elemCoder =
        WindowedValue.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());
    BatchModeExecutionContext context = new BatchModeExecutionContext();
    GroupingShuffleSource<Integer, WindowedValue<String>> shuffleSource =
        new GroupingShuffleSource<>(
            PipelineOptionsFactory.create(),
            null, null, null,
            WindowedValue.getFullCoder(
                KvCoder.of(
                    BigEndianIntegerCoder.of(),
                    IterableCoder.of(
                        WindowedValue.getFullCoder(StringUtf8Coder.of(),
                        IntervalWindow.getCoder()))),
                IntervalWindow.getCoder()),
            context);
    ExecutorTestUtils.TestSourceObserver observer =
        new ExecutorTestUtils.TestSourceObserver(shuffleSource);

    TestShuffleReader shuffleReader = new TestShuffleReader();
    List<Integer> expectedSizes = new ArrayList<>();
    for (KV<Integer, List<String>> kvs : input) {
      Integer key = kvs.getKey();
      byte[] keyByte = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), key);

      for (String value : kvs.getValue()) {
        byte[] valueByte = CoderUtils.encodeToByteArray(
            elemCoder, WindowedValue.of(value, timestamp, Lists.newArrayList(window)));
        byte[] skey =  CoderUtils.encodeToByteArray(InstantCoder.of(), timestamp);
        ShuffleEntry shuffleEntry = new ShuffleEntry(keyByte, skey, valueByte);
        shuffleReader.addEntry(shuffleEntry);
        expectedSizes.add(shuffleEntry.length());
      }
    }

    List<KV<Integer, List<WindowedValue<String>>>> actual = new ArrayList<>();
    try (SourceIterator<WindowedValue<KV<Integer, Reiterable<WindowedValue<String>>>>> iter =
             shuffleSource.iterator(shuffleReader)) {
      Iterable<WindowedValue<String>> prevValuesIterable = null;
      Iterator<WindowedValue<String>> prevValuesIterator = null;
      while (iter.hasNext()) {
        Assert.assertTrue(iter.hasNext());
        Assert.assertTrue(iter.hasNext());

        KV<Integer, Reiterable<WindowedValue<String>>> elem = iter.next().getValue();
        Integer key = elem.getKey();
        List<WindowedValue<String>> values = new ArrayList<>();
        if (valuesToRead.ordinal() > ValuesToRead.SKIP_VALUES.ordinal()) {
          if (prevValuesIterable != null) {
            prevValuesIterable.iterator();  // Verifies that this does not throw.
          }
          if (prevValuesIterator != null) {
            prevValuesIterator.hasNext();  // Verifies that this does not throw.
          }

          Iterable<WindowedValue<String>> valuesIterable = elem.getValue();
          Iterator<WindowedValue<String>> valuesIterator = valuesIterable.iterator();

          if (valuesToRead.ordinal() >= ValuesToRead.READ_ONE_VALUE.ordinal()) {
            while (valuesIterator.hasNext()) {
              Assert.assertTrue(valuesIterator.hasNext());
              Assert.assertTrue(valuesIterator.hasNext());
              Assert.assertEquals("BatchModeExecutionContext key",
                                  key, context.getKey());
              values.add(valuesIterator.next());
              if (valuesToRead == ValuesToRead.READ_ONE_VALUE) {
                break;
              }
            }
            if (valuesToRead == ValuesToRead.READ_ALL_VALUES) {
              Assert.assertFalse(valuesIterator.hasNext());
              Assert.assertFalse(valuesIterator.hasNext());

              try {
                valuesIterator.next();
                Assert.fail("Expected NoSuchElementException");
              } catch (NoSuchElementException exn) {
                // As expected.
              }
              valuesIterable.iterator();  // Verifies that this does not throw.
            }
          }

          prevValuesIterable = valuesIterable;
          prevValuesIterator = valuesIterator;
        }

        actual.add(KV.of(key, values));
      }
      Assert.assertFalse(iter.hasNext());
      Assert.assertFalse(iter.hasNext());
      try {
        iter.next();
        Assert.fail("Expected NoSuchElementException");
      } catch (NoSuchElementException exn) {
        // As expected.
      }
    }

    List<KV<Integer, List<WindowedValue<String>>>> expected = new ArrayList<>();
    for (KV<Integer, List<String>> kvs : input) {
      Integer key = kvs.getKey();
      List<WindowedValue<String>> values = new ArrayList<>();
      if (valuesToRead.ordinal() >= ValuesToRead.READ_ONE_VALUE.ordinal()) {
        for (String value : kvs.getValue()) {
          values.add(WindowedValue.of(value, timestamp, Lists.newArrayList(window)));
          if (valuesToRead == ValuesToRead.READ_ONE_VALUE) {
            break;
          }
        }
      }
      expected.add(KV.of(key, values));
    }
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expectedSizes, observer.getActualSizes());
  }

  @Test
  public void testReadEmptyShuffleSource() throws Exception {
    runTestReadShuffleSource(NO_KVS, ValuesToRead.READ_ALL_VALUES);
  }

  @Test
  public void testReadEmptyShuffleSourceSkippingValues() throws Exception {
    runTestReadShuffleSource(NO_KVS, ValuesToRead.SKIP_VALUES);
  }

  @Test
  public void testReadNonEmptyShuffleSource() throws Exception {
    runTestReadShuffleSource(KVS, ValuesToRead.READ_ALL_VALUES);
  }

  @Test
  public void testReadNonEmptyShuffleSourceReadingOneValue() throws Exception {
    runTestReadShuffleSource(KVS, ValuesToRead.READ_ONE_VALUE);
  }

  @Test
  public void testReadNonEmptyShuffleSourceReadingNoValues() throws Exception {
    runTestReadShuffleSource(KVS, ValuesToRead.READ_NO_VALUES);
  }

  @Test
  public void testReadNonEmptyShuffleSourceSkippingValues() throws Exception {
    runTestReadShuffleSource(KVS, ValuesToRead.SKIP_VALUES);
  }

  static byte[] fabricatePosition(int shard, byte[] key) throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    dos.writeInt(shard);
    if (key != null) {
      dos.writeInt(Arrays.hashCode(key));
    }
    return os.toByteArray();
  }

  @Test
  public void testReadFromEmptyShuffleSourceAndUpdateStopPosition()
      throws Exception {
    BatchModeExecutionContext context = new BatchModeExecutionContext();
    GroupingShuffleSource<Integer, Integer> shuffleSource =
        new GroupingShuffleSource<>(
            PipelineOptionsFactory.create(),
            null, null, null,
            WindowedValue.getFullCoder(
                KvCoder.of(
                    BigEndianIntegerCoder.of(),
                    IterableCoder.of(BigEndianIntegerCoder.of())),
                IntervalWindow.getCoder()),
            context);
    TestShuffleReader shuffleReader = new TestShuffleReader();
    try (Source.SourceIterator<WindowedValue<KV<Integer, Reiterable<Integer>>>> iter =
        shuffleSource.iterator(shuffleReader)) {


      // Can update the stop position, the source range spans all interval
      Position proposedStopPosition = new Position();
      String stop = encodeBase64URLSafeString(fabricatePosition(0, null));
      proposedStopPosition.setShufflePosition(stop);

      Assert.assertEquals(
          stop,
          sourcePositionToCloudPosition(
              iter.updateStopPosition(
                  cloudProgressToSourceProgress(
                      createApproximateProgress(proposedStopPosition))))
          .getShufflePosition());


      // Cannot update stop position to a position >= the current stop position
      stop = encodeBase64URLSafeString(fabricatePosition(1, null));
      proposedStopPosition.setShufflePosition(stop);

      Assert.assertEquals(null, iter.updateStopPosition(
          cloudProgressToSourceProgress(
              createApproximateProgress(proposedStopPosition))));
    }
  }

  @Test
  public void testReadFromShuffleSourceAndFailToUpdateStopPosition()
      throws Exception {
    BatchModeExecutionContext context = new BatchModeExecutionContext();
    final int kFirstShard = 0;

    TestShuffleReader shuffleReader = new TestShuffleReader();
    final int kNumRecords = 2;
    for (int i = 0; i < kNumRecords; ++i) {
      byte[] key = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), i);
      shuffleReader.addEntry(new ShuffleEntry(
          fabricatePosition(kFirstShard, key), key, null, key));
    }

    // Note that TestShuffleReader start/end positions are in the
    // space of keys not the positions (TODO: should probably always
    // use positions instead).
    String stop = encodeBase64URLSafeString(
        fabricatePosition(kNumRecords, null));
    GroupingShuffleSource<Integer, Integer> shuffleSource =
        new GroupingShuffleSource<>(
            PipelineOptionsFactory.create(),
            null, null, stop,
            WindowedValue.getFullCoder(
                KvCoder.of(
                    BigEndianIntegerCoder.of(),
                    IterableCoder.of(BigEndianIntegerCoder.of())),
                IntervalWindow.getCoder()),
            context);

    try (Source.SourceIterator<WindowedValue<KV<Integer, Reiterable<Integer>>>> iter =
        shuffleSource.iterator(shuffleReader)) {

      Position proposedStopPosition = new Position();
      proposedStopPosition.setShufflePosition(
          encodeBase64URLSafeString(fabricatePosition(kNumRecords + 1, null)));

      // Cannot update the stop position since the value provided is
      // past the current stop position.
      Assert.assertEquals(null, iter.updateStopPosition(
          cloudProgressToSourceProgress(createApproximateProgress(proposedStopPosition))));

      int i = 0;
      for (; iter.hasNext(); ++i) {
        iter.next().getValue(); // ignored
        if (i == 0) {
          // First record
          byte[] key = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), i);
          proposedStopPosition.setShufflePosition(
              encodeBase64URLSafeString(fabricatePosition(kFirstShard, key)));
          // Cannot update stop position since it is identical with
          // the position of the record that was just returned.
          Assert.assertEquals(null, iter.updateStopPosition(
              cloudProgressToSourceProgress(createApproximateProgress(proposedStopPosition))));

          proposedStopPosition.setShufflePosition(
              encodeBase64URLSafeString(fabricatePosition(kFirstShard, null)));
          // Cannot update stop position since it comes before current position
          Assert.assertEquals(null, iter.updateStopPosition(
              cloudProgressToSourceProgress(createApproximateProgress(proposedStopPosition))));
        }
      }
      Assert.assertEquals(kNumRecords, i);

      proposedStopPosition.setShufflePosition(
          encodeBase64URLSafeString(fabricatePosition(kFirstShard, null)));
      // Cannot update stop position since all input was consumed.
      Assert.assertEquals(null, iter.updateStopPosition(
          cloudProgressToSourceProgress(createApproximateProgress(proposedStopPosition))));
    }
  }

  @Test
  public void testReadFromShuffleSourceAndUpdateStopPosition()
      throws Exception {
    BatchModeExecutionContext context = new BatchModeExecutionContext();
    GroupingShuffleSource<Integer, Integer> shuffleSource =
        new GroupingShuffleSource<>(
            PipelineOptionsFactory.create(),
            null, null, null,
            WindowedValue.getFullCoder(
                KvCoder.of(
                    BigEndianIntegerCoder.of(),
                    IterableCoder.of(BigEndianIntegerCoder.of())),
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
      byte[] keyByte = CoderUtils.encodeToByteArray(
          BigEndianIntegerCoder.of(), i);
      ShuffleEntry entry = new ShuffleEntry(
          fabricatePosition(kFirstShard, keyByte), keyByte, null, keyByte);
      shuffleReader.addEntry(entry);
    }

    for (int i = kNumRecords; i < 2 * kNumRecords; ++i) {
      byte[] keyByte = CoderUtils.encodeToByteArray(
          BigEndianIntegerCoder.of(), i);

      ShuffleEntry entry = new ShuffleEntry(
          fabricatePosition(kSecondShard, keyByte), keyByte, null, keyByte);
      shuffleReader.addEntry(entry);
    }

    int i = 0;
    try (Source.SourceIterator<WindowedValue<KV<Integer, Reiterable<Integer>>>> iter =
        shuffleSource.iterator(shuffleReader)) {

      Position proposedStopPosition = new Position();

      Assert.assertNull(iter.updateStopPosition(
          cloudProgressToSourceProgress(createApproximateProgress(proposedStopPosition))));

      // Stop at the shard boundary
      String stop = encodeBase64URLSafeString(fabricatePosition(kSecondShard, null));
      proposedStopPosition.setShufflePosition(stop);

      Assert.assertEquals(
          stop,
          sourcePositionToCloudPosition(
              iter.updateStopPosition(
                  cloudProgressToSourceProgress(createApproximateProgress(proposedStopPosition))))
          .getShufflePosition());

      while (iter.hasNext()) {
        Assert.assertTrue(iter.hasNext());
        Assert.assertTrue(iter.hasNext());

        KV<Integer, Reiterable<Integer>> elem = iter.next().getValue();
        int key = elem.getKey();
        Assert.assertEquals(key, i);

        Iterable<Integer> valuesIterable = elem.getValue();
        Iterator<Integer> valuesIterator = valuesIterable.iterator();

        int j = 0;
        while (valuesIterator.hasNext()) {
          Assert.assertTrue(valuesIterator.hasNext());
          Assert.assertTrue(valuesIterator.hasNext());

          int value = valuesIterator.next();
          Assert.assertEquals(value, i);
          ++j;
        }
        Assert.assertEquals(j, 1);
        ++i;
      }
    }
    Assert.assertEquals(i, kNumRecords);
  }

  @Test
  public void testGetApproximateProgress() throws Exception {
    // Store the positions of all KVs returned.
    List<byte[]> positionsList = new ArrayList<byte[]>();

    BatchModeExecutionContext context = new BatchModeExecutionContext();
    GroupingShuffleSource<Integer, Integer> shuffleSource =
        new GroupingShuffleSource<>(
            PipelineOptionsFactory.create(),
            null, null, null,
            WindowedValue.getFullCoder(
                KvCoder.of(
                    BigEndianIntegerCoder.of(),
                    IterableCoder.of(BigEndianIntegerCoder.of())),
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

    try (Source.SourceIterator<WindowedValue<KV<Integer, Reiterable<Integer>>>> sourceIterator =
        shuffleSource.iterator(shuffleReader)) {
      Integer i = 0;
      while (sourceIterator.hasNext()) {
        Assert.assertTrue(sourceIterator.hasNext());
        ApproximateProgress progress = sourceProgressToCloudProgress(sourceIterator.getProgress());
        Assert.assertNotNull(progress.getPosition().getShufflePosition());

        // Compare returned position with the expected position.
        Assert.assertEquals(ByteArrayShufflePosition.of(positionsList.get(i)).encodeBase64(),
            progress.getPosition().getShufflePosition());

        WindowedValue<KV<Integer, Reiterable<Integer>>> elem = sourceIterator.next();
        Assert.assertEquals(i, elem.getValue().getKey());
        i++;
      }
      Assert.assertFalse(sourceIterator.hasNext());

      // Cannot update stop position since all input was consumed.
      Position proposedStopPosition = new Position();
      String stop = encodeBase64URLSafeString(fabricatePosition(0, null));
      proposedStopPosition.setShufflePosition(stop);
      Assert.assertEquals(null, sourceIterator.updateStopPosition(
          cloudProgressToSourceProgress(createApproximateProgress(proposedStopPosition))));
    }
  }

  private ApproximateProgress createApproximateProgress(
      com.google.api.services.dataflow.model.Position position) {
    return new ApproximateProgress().setPosition(position);
  }
}
