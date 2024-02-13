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
package org.apache.beam.runners.dataflow.worker;

import static com.google.api.client.util.Base64.encodeBase64URLSafeString;
import static org.apache.beam.runners.dataflow.worker.ReaderTestUtils.approximateSplitRequestAtPosition;
import static org.apache.beam.runners.dataflow.worker.ReaderTestUtils.consumedParallelismFromProgress;
import static org.apache.beam.runners.dataflow.worker.ReaderTestUtils.positionFromSplitResult;
import static org.apache.beam.runners.dataflow.worker.ReaderTestUtils.splitRequestAtPosition;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.toDynamicSplitRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import com.google.api.services.dataflow.model.Position;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.ExperimentContext.Experiment;
import org.apache.beam.runners.dataflow.worker.GroupingShuffleReader.GroupingShuffleReaderIterator;
import org.apache.beam.runners.dataflow.worker.ShuffleSink.ShuffleKind;
import org.apache.beam.runners.dataflow.worker.TestOperationContext.TestDataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterBackedElementByteSizeObserver;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ByteArrayShufflePosition;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ExecutorTestUtils;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShufflePosition;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleReadCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleReadCounterFactory;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.Reiterable;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for GroupingShuffleReader. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class GroupingShuffleReaderTest {
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  private static final List<KV<Integer, List<KV<Integer, Integer>>>> NO_KVS =
      Collections.emptyList();

  private static final Instant timestamp = new Instant(123000);
  private static final IntervalWindow window =
      new IntervalWindow(timestamp, timestamp.plus(Duration.millis(1000)));

  private final ExecutionStateSampler sampler = ExecutionStateSampler.newForTest();
  private final ExecutionStateTracker tracker = new ExecutionStateTracker(sampler);
  private Closeable trackerCleanup;

  // As Shuffle records, {@code KV} is encoded as 10 records. Each records uses an integer as key
  // (4 bytes), and a {@code KV} of an integer key and value (each 4 bytes).
  // Overall {@code KV}s have a byte size of 25 * 4 = 100. Note that we also encode the
  // timestamp into the secondary key adding another 100 bytes.
  private static final List<KV<Integer, List<KV<Integer, Integer>>>> KVS =
      Arrays.asList(
          KV.of(1, Arrays.asList(KV.of(1, 11), KV.of(2, 12))),
          KV.of(2, Arrays.asList(KV.of(1, 21), KV.of(2, 22))),
          KV.of(3, Arrays.asList(KV.of(1, 31))),
          KV.of(
              4,
              Arrays.asList(
                  KV.of(1, 41), KV.of(2, 42),
                  KV.of(3, 43), KV.of(4, 44))),
          KV.of(5, Arrays.asList(KV.of(1, 51))));

  private static final String MOCK_STAGE_NAME = "mockStageName";
  private static final String MOCK_ORIGINAL_NAME_FOR_EXECUTING_STEP1 = "mockOriginalName1";
  private static final String MOCK_ORIGINAL_NAME_FOR_EXECUTING_STEP2 = "mockOriginalName2";
  private static final String MOCK_SYSTEM_NAME = "mockSystemName";
  private static final String MOCK_USER_NAME = "mockUserName";
  private static final String ORIGINAL_SHUFFLE_STEP_NAME = "originalName";

  /**
   * How many of the values with each key are to be read. Note that the order matters as the
   * conversion to ordinal is used below.
   */
  private enum ValuesToRead {
    /** Don't even ask for the values iterator. */
    SKIP_VALUES,
    /** Get the iterator, but don't read any values. */
    READ_NO_VALUES,
    /** Read just the first value. */
    READ_ONE_VALUE,
    /** Read all the values. */
    READ_ALL_VALUES,
    /** Read all the values twice. */
    READ_ALL_VALUES_TWICE
  }

  private void setCurrentExecutionState(String mockOriginalName) {
    DataflowExecutionState state =
        new TestDataflowExecutionState(
            NameContext.create(MOCK_STAGE_NAME, mockOriginalName, MOCK_SYSTEM_NAME, MOCK_USER_NAME),
            "activity");
    tracker.enterState(state);
  }

  @Before
  public void setUp() {
    trackerCleanup = tracker.activate();
    setCurrentExecutionState(MOCK_ORIGINAL_NAME_FOR_EXECUTING_STEP1);
  }

  @After
  public void tearDown() throws IOException {
    trackerCleanup.close();
  }

  private List<ShuffleEntry> writeShuffleEntries(
      List<KV<Integer, List<KV<Integer, Integer>>>> input, boolean sortValues) throws Exception {
    Coder<WindowedValue<KV<Integer, KV<Integer, Integer>>>> sinkElemCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(
                BigEndianIntegerCoder.of(),
                KvCoder.of(BigEndianIntegerCoder.of(), BigEndianIntegerCoder.of())),
            IntervalWindow.getCoder());
    // Write to shuffle with GROUP_KEYS ShuffleSink.
    BatchModeExecutionContext executionContext =
        BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "STAGE");
    ShuffleSink<KV<Integer, KV<Integer, Integer>>> shuffleSink =
        new ShuffleSink<>(
            PipelineOptionsFactory.create(),
            null,
            sortValues ? ShuffleKind.GROUP_KEYS_AND_SORT_VALUES : ShuffleKind.GROUP_KEYS,
            sinkElemCoder,
            executionContext,
            TestOperationContext.create());

    TestShuffleWriter shuffleWriter = new TestShuffleWriter();

    int kvCount = 0;
    List<Long> actualSizes = new ArrayList<>();
    try (Sink.SinkWriter<WindowedValue<KV<Integer, KV<Integer, Integer>>>> shuffleSinkWriter =
        shuffleSink.writer(shuffleWriter, "dataset")) {
      for (KV<Integer, List<KV<Integer, Integer>>> kvs : input) {
        Integer key = kvs.getKey();
        for (KV<Integer, Integer> value : kvs.getValue()) {
          ++kvCount;
          actualSizes.add(
              shuffleSinkWriter.add(
                  WindowedValue.of(
                      KV.of(key, value),
                      timestamp,
                      Lists.newArrayList(window),
                      PaneInfo.NO_FIRING)));
        }
      }
    }
    List<ShuffleEntry> records = shuffleWriter.getRecords();
    assertEquals(kvCount, records.size());
    assertEquals(shuffleWriter.getSizes(), actualSizes);
    return records;
  }

  @SuppressWarnings("ReturnValueIgnored")
  private List<KV<Integer, List<KV<Integer, Integer>>>> runIterationOverGroupingShuffleReader(
      BatchModeExecutionContext context,
      TestShuffleReader shuffleReader,
      GroupingShuffleReader<Integer, KV<Integer, Integer>> groupingShuffleReader,
      Coder<WindowedValue<KV<Integer, Iterable<KV<Integer, Integer>>>>> coder,
      ValuesToRead valuesToRead)
      throws Exception {
    CounterSet counterSet = new CounterSet();
    Counter<Long, ?> elementByteSizeCounter =
        counterSet.longSum(CounterName.named("element-byte-size-counter"));
    CounterBackedElementByteSizeObserver elementObserver =
        new CounterBackedElementByteSizeObserver(elementByteSizeCounter);
    List<KV<Integer, List<KV<Integer, Integer>>>> actual = new ArrayList<>();
    assertFalse(shuffleReader.isClosed());
    try (GroupingShuffleReaderIterator<Integer, KV<Integer, Integer>> iter =
        groupingShuffleReader.iterator(shuffleReader)) {
      Iterable<KV<Integer, Integer>> prevValuesIterable = null;
      Iterator<KV<Integer, Integer>> prevValuesIterator = null;
      for (boolean more = iter.start(); more; more = iter.advance()) {
        // Should not fail.
        iter.getCurrent();
        iter.getCurrent();

        // safe co-variant cast from Reiterable to Iterable
        @SuppressWarnings({
          "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
          "unchecked"
        })
        WindowedValue<KV<Integer, Iterable<KV<Integer, Integer>>>> windowedValue =
            (WindowedValue) iter.getCurrent();
        // Verify that the byte size observer is lazy for every value the GroupingShuffleReader
        // produces.
        coder.registerByteSizeObserver(windowedValue, elementObserver);
        assertTrue(elementObserver.getIsLazy());

        // Verify value is in an empty windows.
        assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE, windowedValue.getTimestamp());
        assertEquals(0, windowedValue.getWindows().size());

        KV<Integer, Iterable<KV<Integer, Integer>>> elem = windowedValue.getValue();
        Integer key = elem.getKey();
        List<KV<Integer, Integer>> values = new ArrayList<>();
        if (valuesToRead.ordinal() > ValuesToRead.SKIP_VALUES.ordinal()) {
          if (prevValuesIterable != null) {
            prevValuesIterable.iterator(); // Verifies that this does not throw.
          }
          if (prevValuesIterator != null) {
            prevValuesIterator.hasNext(); // Verifies that this does not throw.
          }

          Iterable<KV<Integer, Integer>> valuesIterable = elem.getValue();
          Iterator<KV<Integer, Integer>> valuesIterator = valuesIterable.iterator();

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
            if (valuesToRead.ordinal() >= ValuesToRead.READ_ALL_VALUES.ordinal()) {
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
          if (valuesToRead == ValuesToRead.READ_ALL_VALUES_TWICE) {
            // Create new iterator;
            valuesIterator = valuesIterable.iterator();

            while (valuesIterator.hasNext()) {
              assertTrue(valuesIterator.hasNext());
              assertTrue(valuesIterator.hasNext());
              assertEquals("BatchModeExecutionContext key", key, context.getKey());
              valuesIterator.next();
            }
            assertFalse(valuesIterator.hasNext());
            assertFalse(valuesIterator.hasNext());
            try {
              valuesIterator.next();
              fail("Expected NoSuchElementException");
            } catch (NoSuchElementException exn) {
              // As expected.
            }
          }

          prevValuesIterable = valuesIterable;
          prevValuesIterator = valuesIterator;
        }

        actual.add(KV.of(key, values));
      }
      assertFalse(iter.advance());
      assertFalse(iter.advance());
      try {
        iter.getCurrent();
        fail("Expected NoSuchElementException");
      } catch (NoSuchElementException exn) {
        // As expected.
      }
    }
    assertTrue(shuffleReader.isClosed());
    return actual;
  }

  private void runTestReadFromShuffle(
      List<KV<Integer, List<KV<Integer, Integer>>>> input,
      boolean sortValues,
      ValuesToRead valuesToRead)
      throws Exception {
    Coder<WindowedValue<KV<Integer, Iterable<KV<Integer, Integer>>>>> sourceElemCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(
                BigEndianIntegerCoder.of(),
                IterableCoder.of(
                    KvCoder.of(BigEndianIntegerCoder.of(), BigEndianIntegerCoder.of()))),
            IntervalWindow.getCoder());

    List<ShuffleEntry> records = writeShuffleEntries(input, sortValues);

    PipelineOptions options = PipelineOptionsFactory.create();
    BatchModeExecutionContext context = BatchModeExecutionContext.forTesting(options, "testStage");
    GroupingShuffleReader<Integer, KV<Integer, Integer>> groupingShuffleReader =
        new GroupingShuffleReader<>(
            options,
            null,
            null,
            null,
            sourceElemCoder,
            context,
            TestOperationContext.create(),
            ShuffleReadCounterFactory.INSTANCE,
            sortValues);
    ExecutorTestUtils.TestReaderObserver observer =
        new ExecutorTestUtils.TestReaderObserver(groupingShuffleReader);

    TestShuffleReader shuffleReader = new TestShuffleReader();
    List<Integer> expectedSizes = new ArrayList<>();
    for (ShuffleEntry record : records) {
      expectedSizes.add(record.length());
      shuffleReader.addEntry(record);
    }

    List<KV<Integer, List<KV<Integer, Integer>>>> actual =
        runIterationOverGroupingShuffleReader(
            context, shuffleReader, groupingShuffleReader, sourceElemCoder, valuesToRead);

    List<KV<Integer, List<KV<Integer, Integer>>>> expected = new ArrayList<>();
    for (KV<Integer, List<KV<Integer, Integer>>> kvs : input) {
      Integer key = kvs.getKey();
      List<KV<Integer, Integer>> values = new ArrayList<>();
      if (valuesToRead.ordinal() >= ValuesToRead.READ_ONE_VALUE.ordinal()) {
        for (KV<Integer, Integer> value : kvs.getValue()) {
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
    runTestReadFromShuffle(NO_KVS, false /* do not sort values */, ValuesToRead.READ_ALL_VALUES);
    runTestReadFromShuffle(NO_KVS, true /* sort values */, ValuesToRead.READ_ALL_VALUES);
  }

  @Test
  public void testReadEmptyShuffleDataSkippingValues() throws Exception {
    runTestReadFromShuffle(NO_KVS, false /* do not sort values */, ValuesToRead.SKIP_VALUES);
    runTestReadFromShuffle(NO_KVS, true /* sort values */, ValuesToRead.SKIP_VALUES);
  }

  @Test
  public void testReadNonEmptyShuffleData() throws Exception {
    runTestReadFromShuffle(KVS, false /* do not sort values */, ValuesToRead.READ_ALL_VALUES);
    runTestReadFromShuffle(KVS, true /* sort values */, ValuesToRead.READ_ALL_VALUES);
  }

  @Test
  public void testReadNonEmptyShuffleDataTwice() throws Exception {
    runTestReadFromShuffle(KVS, false /* do not sort values */, ValuesToRead.READ_ALL_VALUES_TWICE);
    runTestReadFromShuffle(KVS, true /* sort values */, ValuesToRead.READ_ALL_VALUES_TWICE);
  }

  @Test
  public void testReadNonEmptyShuffleDataReadingOneValue() throws Exception {
    runTestReadFromShuffle(KVS, false /* do not sort values */, ValuesToRead.READ_ONE_VALUE);
    runTestReadFromShuffle(KVS, true /* sort values */, ValuesToRead.READ_ONE_VALUE);
  }

  @Test
  public void testReadNonEmptyShuffleDataReadingNoValues() throws Exception {
    runTestReadFromShuffle(KVS, false /* do not sort values */, ValuesToRead.READ_NO_VALUES);
    runTestReadFromShuffle(KVS, true /* sort values */, ValuesToRead.READ_NO_VALUES);
  }

  @Test
  public void testReadNonEmptyShuffleDataSkippingValues() throws Exception {
    runTestReadFromShuffle(KVS, false /* do not sort values */, ValuesToRead.SKIP_VALUES);
    runTestReadFromShuffle(KVS, true /* sort values */, ValuesToRead.SKIP_VALUES);
  }

  private void expectShuffleReadCounterEquals(
      TestShuffleReadCounterFactory factory, long expectedReadBytes) {
    Map<String, Long> expectedReadBytesMap = new HashMap<>();
    expectedReadBytesMap.put(MOCK_ORIGINAL_NAME_FOR_EXECUTING_STEP1, expectedReadBytes);
    expectShuffleReadCounterEquals(factory, expectedReadBytesMap);
  }

  private void expectShuffleReadCounterEquals(
      TestShuffleReadCounterFactory factory, Map<String, Long> expectedReadBytesForOriginal) {
    ShuffleReadCounter src = factory.getOnlyShuffleReadCounterOrNull();
    assertNotNull(src);
    // If the experiment is enabled then the legacyPerOperationPerDatasetBytesCounter
    // should not be set.
    if (src.legacyPerOperationPerDatasetBytesCounter != null) {
      assertEquals(0, (long) src.legacyPerOperationPerDatasetBytesCounter.getAggregate());
    }
    // Verify that each executing step used when reading from the GroupingShuffleReader
    // has a counter with a bytes read value.
    assertEquals(expectedReadBytesForOriginal.size(), (long) src.counterSet.size());
    Iterator it = expectedReadBytesForOriginal.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Long> pair = (Map.Entry) it.next();
      Counter counter =
          src.counterSet.getExistingCounter(
              ShuffleReadCounter.generateCounterName(ORIGINAL_SHUFFLE_STEP_NAME, pair.getKey()));
      assertEquals(pair.getValue(), counter.getAggregate());
    }
  }

  private void runTestBytesReadCounterForOptions(
      PipelineOptions options,
      List<KV<Integer, List<KV<Integer, Integer>>>> input,
      boolean useSecondaryKey,
      ValuesToRead valuesToRead,
      long expectedReadBytes)
      throws Exception {
    // Create a shuffle reader with the shuffle values provided as input.
    List<ShuffleEntry> records = writeShuffleEntries(input, useSecondaryKey);
    TestShuffleReader shuffleReader = new TestShuffleReader();
    for (ShuffleEntry record : records) {
      shuffleReader.addEntry(record);
    }

    TestShuffleReadCounterFactory shuffleReadCounterFactory = new TestShuffleReadCounterFactory();

    Coder<WindowedValue<KV<Integer, Iterable<KV<Integer, Integer>>>>> sourceElemCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(
                BigEndianIntegerCoder.of(),
                IterableCoder.of(
                    KvCoder.of(BigEndianIntegerCoder.of(), BigEndianIntegerCoder.of()))),
            IntervalWindow.getCoder());
    // Read from shuffle with GroupingShuffleReader.
    BatchModeExecutionContext context = BatchModeExecutionContext.forTesting(options, "testStage");
    TestOperationContext operationContext = TestOperationContext.create();
    GroupingShuffleReader<Integer, KV<Integer, Integer>> groupingShuffleReader =
        new GroupingShuffleReader<>(
            options,
            null,
            null,
            null,
            sourceElemCoder,
            context,
            operationContext,
            shuffleReadCounterFactory,
            useSecondaryKey);
    groupingShuffleReader.perOperationPerDatasetBytesCounter =
        operationContext
            .counterFactory()
            .longSum(CounterName.named("dax-shuffle-test-wf-read-bytes"));

    runIterationOverGroupingShuffleReader(
        context, shuffleReader, groupingShuffleReader, sourceElemCoder, valuesToRead);

    if (ExperimentContext.parseFrom(options).isEnabled(Experiment.IntertransformIO)) {
      expectShuffleReadCounterEquals(shuffleReadCounterFactory, expectedReadBytes);
    } else {
      assertEquals(
          expectedReadBytes,
          (long) groupingShuffleReader.perOperationPerDatasetBytesCounter.getAggregate());
    }
  }

  private void runTestBytesReadCounter(
      List<KV<Integer, List<KV<Integer, Integer>>>> input,
      boolean useSecondaryKey,
      ValuesToRead valuesToRead,
      long expectedReadBytes)
      throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    runTestBytesReadCounterForOptions(
        options, input, useSecondaryKey, valuesToRead, expectedReadBytes);

    // TODO: Remove experimental worker code once inter-transform IO has shipped.
    options
        .as(DataflowPipelineDebugOptions.class)
        .setExperiments(Lists.newArrayList(Experiment.IntertransformIO.getName()));
    runTestBytesReadCounterForOptions(
        options, input, useSecondaryKey, valuesToRead, expectedReadBytes);
  }

  @Test
  public void testBytesReadNonEmptyShuffleDataUnsorted() throws Exception {
    runTestBytesReadCounter(
        KVS, false /* do not sort values */, ValuesToRead.READ_ALL_VALUES, 200L);
  }

  @Test
  public void testBytesReadNonEmptyShuffleDataSorted() throws Exception {
    runTestBytesReadCounter(KVS, true /* sort values */, ValuesToRead.READ_ALL_VALUES, 200L);
  }

  @Test
  public void testBytesReadNonEmptyShuffleDataTwiceUnsorted() throws Exception {
    runTestBytesReadCounter(
        KVS, false /* do not sort values */, ValuesToRead.READ_ALL_VALUES_TWICE, 200L);
  }

  @Test
  public void testBytesReadNonEmptyShuffleDataTwiceSorted() throws Exception {
    runTestBytesReadCounter(KVS, true /* sort values */, ValuesToRead.READ_ALL_VALUES_TWICE, 200L);
  }

  @Test
  public void testBytesReadNonEmptyShuffleDataReadingOneValueUnsorted() throws Exception {
    runTestBytesReadCounter(KVS, false /* do not sort values */, ValuesToRead.READ_ONE_VALUE, 200L);
  }

  @Test
  public void testBytesReadNonEmptyShuffleDataReadingOneValueSorted() throws Exception {
    runTestBytesReadCounter(KVS, true /* sort values */, ValuesToRead.READ_ONE_VALUE, 200L);
  }

  @Test
  public void testBytesReadNonEmptyShuffleDataSkippingValuesUnsorted() throws Exception {
    runTestBytesReadCounter(KVS, false /* do not sort values */, ValuesToRead.SKIP_VALUES, 200L);
  }

  @Test
  public void testBytesReadNonEmptyShuffleDataSkippingValuesSorted() throws Exception {
    runTestBytesReadCounter(KVS, true /* sort values */, ValuesToRead.SKIP_VALUES, 200L);
  }

  @Test
  public void testBytesReadEmptyShuffleData() throws Exception {
    runTestBytesReadCounter(
        NO_KVS, false /* do not sort values */, ValuesToRead.READ_ALL_VALUES, 0L);
    runTestBytesReadCounter(NO_KVS, true /* sort values */, ValuesToRead.READ_ALL_VALUES, 0L);
  }

  static ByteArrayShufflePosition fabricatePosition(int shard) throws Exception {
    return fabricatePosition(shard, (Integer) null);
  }

  static ByteArrayShufflePosition fabricatePosition(int shard, byte @Nullable [] key)
      throws Exception {
    return fabricatePosition(shard, key == null ? null : Arrays.hashCode(key));
  }

  static ShuffleEntry newShuffleEntry(
      ShufflePosition position, byte[] key, byte[] secondaryKey, byte[] value) {
    return new ShuffleEntry(
        position,
        key == null ? null : ByteString.copyFrom(key),
        secondaryKey == null ? null : ByteString.copyFrom(secondaryKey),
        value == null ? null : ByteString.copyFrom(value));
  }

  static ByteArrayShufflePosition fabricatePosition(int shard, @Nullable Integer keyHash)
      throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    dos.writeInt(shard);
    if (keyHash != null) {
      dos.writeInt(keyHash);
    }
    return ByteArrayShufflePosition.of(os.toByteArray());
  }

  @Test
  public void testReadFromShuffleDataAndFailToSplit() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    BatchModeExecutionContext context = BatchModeExecutionContext.forTesting(options, "testStage");
    final int kFirstShard = 0;

    TestShuffleReader shuffleReader = new TestShuffleReader();
    final int kNumRecords = 2;
    for (int i = 0; i < kNumRecords; ++i) {
      byte[] key = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), i);
      shuffleReader.addEntry(
          newShuffleEntry(fabricatePosition(kFirstShard, key), key, EMPTY_BYTE_ARRAY, key));
    }

    // Note that TestShuffleReader start/end positions are in the
    // space of keys not the positions (TODO: should probably always
    // use positions instead).
    String stop =
        encodeBase64URLSafeString(fabricatePosition(kNumRecords).getPosition().toByteArray());
    TestOperationContext operationContext = TestOperationContext.create();
    GroupingShuffleReader<Integer, Integer> groupingShuffleReader =
        new GroupingShuffleReader<>(
            options,
            null,
            null,
            stop,
            WindowedValue.getFullCoder(
                KvCoder.of(
                    BigEndianIntegerCoder.of(), IterableCoder.of(BigEndianIntegerCoder.of())),
                IntervalWindow.getCoder()),
            context,
            operationContext,
            ShuffleReadCounterFactory.INSTANCE,
            false /* do not sort values */);

    assertFalse(shuffleReader.isClosed());
    try (GroupingShuffleReaderIterator<Integer, Integer> iter =
        groupingShuffleReader.iterator(shuffleReader)) {
      // Poke the iterator so we can test dynamic splitting.
      assertTrue(iter.start());

      // Cannot split since the value provided is past the current stop position.
      assertNull(
          iter.requestDynamicSplit(
              splitRequestAtPosition(makeShufflePosition(kNumRecords + 1, null))));

      byte[] key = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), 0);
      // Cannot split since the split position is identical with the position of the record
      // that was just returned.
      assertNull(
          iter.requestDynamicSplit(splitRequestAtPosition(makeShufflePosition(kFirstShard, key))));

      // Cannot split since the requested split position comes before current position
      assertNull(
          iter.requestDynamicSplit(splitRequestAtPosition(makeShufflePosition(kFirstShard, null))));
      int numRecordsReturned = 1; // including start() above.
      for (; iter.advance(); ++numRecordsReturned) {
        iter.getCurrent().getValue(); // ignored
      }
      assertEquals(kNumRecords, numRecordsReturned);

      // Cannot split since all input was consumed.
      assertNull(
          iter.requestDynamicSplit(splitRequestAtPosition(makeShufflePosition(kFirstShard, null))));
    }
    assertTrue(shuffleReader.isClosed());
  }

  @Test
  public void testConsumedParallelism() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    BatchModeExecutionContext context = BatchModeExecutionContext.forTesting(options, "testStage");
    final int kFirstShard = 0;

    TestShuffleReader shuffleReader = new TestShuffleReader();
    final int kNumRecords = 5;
    for (int i = 0; i < kNumRecords; ++i) {
      byte[] key = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), i);
      ShuffleEntry entry =
          newShuffleEntry(fabricatePosition(kFirstShard, i), key, EMPTY_BYTE_ARRAY, key);
      shuffleReader.addEntry(entry);
    }

    TestOperationContext operationContext = TestOperationContext.create();
    GroupingShuffleReader<Integer, Integer> groupingShuffleReader =
        new GroupingShuffleReader<>(
            options,
            null,
            null,
            null,
            WindowedValue.getFullCoder(
                KvCoder.of(
                    BigEndianIntegerCoder.of(), IterableCoder.of(BigEndianIntegerCoder.of())),
                IntervalWindow.getCoder()),
            context,
            operationContext,
            ShuffleReadCounterFactory.INSTANCE,
            false /* do not sort values */);

    assertFalse(shuffleReader.isClosed());
    try (GroupingShuffleReaderIterator<Integer, Integer> iter =
        groupingShuffleReader.iterator(shuffleReader)) {

      // Iterator hasn't started; consumed parallelism is 0.
      assertEquals(0.0, consumedParallelismFromProgress(iter.getProgress()), 0);

      // The only way to set a stop *position* in tests is via a split. To do that,
      // we must call hasNext() first.

      // Should return entry at key 0.
      assertTrue(iter.start());

      // Iterator just started; consumed parallelism is 0.
      assertEquals(
          0.0,
          readerProgressToCloudProgress(iter.getProgress()).getConsumedParallelism().getValue(),
          0);
      assertNotNull(
          iter.requestDynamicSplit(
              splitRequestAtPosition(
                  makeShufflePosition(
                      fabricatePosition(kFirstShard, 2).immediateSuccessor().getPosition()))));
      // Split does not affect consumed parallelism; consumed parallelism is still 0.
      assertEquals(0.0, consumedParallelismFromProgress(iter.getProgress()), 0);

      // Should return entry at key 1.
      assertTrue(iter.advance());
      assertEquals(1.0, consumedParallelismFromProgress(iter.getProgress()), 0);

      // Should return entry at key 2 (last key, because the stop position
      // is its immediate successor.) Consumed parallelism increments by one to 2.
      assertTrue(iter.advance());
      assertEquals(2.0, consumedParallelismFromProgress(iter.getProgress()), 0);

      // Iterator advanced by one and consumes one more split point (total consumed: 3).
      assertFalse(iter.advance());
      assertEquals(3.0, consumedParallelismFromProgress(iter.getProgress()), 0);
    }
    assertTrue(shuffleReader.isClosed());
  }

  private Position makeShufflePosition(int shard, byte[] key) throws Exception {
    return new Position()
        .setShufflePosition(
            encodeBase64URLSafeString(fabricatePosition(shard, key).getPosition().toByteArray()));
  }

  private Position makeShufflePosition(ByteString position) throws Exception {
    return new Position().setShufflePosition(encodeBase64URLSafeString(position.toByteArray()));
  }

  @Test
  public void testReadFromShuffleAndDynamicSplit() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    BatchModeExecutionContext context = BatchModeExecutionContext.forTesting(options, "testStage");
    TestOperationContext operationContext = TestOperationContext.create();
    GroupingShuffleReader<Integer, Integer> groupingShuffleReader =
        new GroupingShuffleReader<>(
            options,
            null,
            null,
            null,
            WindowedValue.getFullCoder(
                KvCoder.of(
                    BigEndianIntegerCoder.of(), IterableCoder.of(BigEndianIntegerCoder.of())),
                IntervalWindow.getCoder()),
            context,
            operationContext,
            ShuffleReadCounterFactory.INSTANCE,
            false /* do not sort values */);
    groupingShuffleReader.perOperationPerDatasetBytesCounter =
        operationContext
            .counterFactory()
            .longSum(CounterName.named("dax-shuffle-test-wf-read-bytes"));

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
          newShuffleEntry(
              fabricatePosition(kFirstShard, keyByte), keyByte, EMPTY_BYTE_ARRAY, keyByte);
      shuffleReader.addEntry(entry);
    }

    for (int i = kNumRecords; i < 2 * kNumRecords; ++i) {
      byte[] keyByte = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), i);

      ShuffleEntry entry =
          newShuffleEntry(
              fabricatePosition(kSecondShard, keyByte), keyByte, EMPTY_BYTE_ARRAY, keyByte);
      shuffleReader.addEntry(entry);
    }

    int i = 0;
    assertFalse(shuffleReader.isClosed());
    try (GroupingShuffleReaderIterator<Integer, Integer> iter =
        groupingShuffleReader.iterator(shuffleReader)) {
      // Poke the iterator so we can test dynamic splitting.
      assertTrue(iter.start());
      ++i;

      assertNull(iter.requestDynamicSplit(splitRequestAtPosition(new Position())));

      // Split at the shard boundary
      NativeReader.DynamicSplitResult dynamicSplitResult =
          iter.requestDynamicSplit(splitRequestAtPosition(makeShufflePosition(kSecondShard, null)));
      assertNotNull(dynamicSplitResult);
      assertEquals(
          encodeBase64URLSafeString(fabricatePosition(kSecondShard).getPosition().toByteArray()),
          positionFromSplitResult(dynamicSplitResult).getShufflePosition());

      for (; iter.advance(); ++i) {
        // iter.getCurrent() is supposed to be side-effect-free and give the same result if called
        // repeatedly. Test that this is indeed the case.
        iter.getCurrent();
        iter.getCurrent();

        KV<Integer, Reiterable<Integer>> elem = iter.getCurrent().getValue();
        int key = elem.getKey();
        assertEquals(key, i);

        Reiterable<Integer> valuesIterable = elem.getValue();
        Reiterator<Integer> valuesIterator = valuesIterable.iterator();

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
        assertEquals(1, j);
      }
      assertFalse(iter.advance());
    }
    assertTrue(shuffleReader.isClosed());
    assertEquals(i, kNumRecords);
    // There are 10 Shuffle records that each encode an integer key (4 bytes) and integer value (4
    // bytes). We therefore expect to read 80 bytes.
    assertEquals(
        80L, (long) groupingShuffleReader.perOperationPerDatasetBytesCounter.getAggregate());
  }

  @Test
  public void testGetApproximateProgress() throws Exception {
    // Store the positions of all KVs returned.
    List<ByteArrayShufflePosition> positionsList = new ArrayList<>();

    PipelineOptions options = PipelineOptionsFactory.create();
    BatchModeExecutionContext context = BatchModeExecutionContext.forTesting(options, "testStage");
    TestOperationContext operationContext = TestOperationContext.create();
    GroupingShuffleReader<Integer, Integer> groupingShuffleReader =
        new GroupingShuffleReader<>(
            options,
            null,
            null,
            null,
            WindowedValue.getFullCoder(
                KvCoder.of(
                    BigEndianIntegerCoder.of(), IterableCoder.of(BigEndianIntegerCoder.of())),
                IntervalWindow.getCoder()),
            context,
            operationContext,
            ShuffleReadCounterFactory.INSTANCE,
            false /* do not sort values */);

    TestShuffleReader shuffleReader = new TestShuffleReader();
    final int kNumRecords = 10;

    for (int i = 0; i < kNumRecords; ++i) {
      ByteArrayShufflePosition position = fabricatePosition(i);
      byte[] keyByte = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), i);
      positionsList.add(position);
      ShuffleEntry entry = newShuffleEntry(position, keyByte, EMPTY_BYTE_ARRAY, keyByte);
      shuffleReader.addEntry(entry);
    }

    assertFalse(shuffleReader.isClosed());
    try (GroupingShuffleReaderIterator<Integer, Integer> iter =
        groupingShuffleReader.iterator(shuffleReader)) {
      Integer i = 0;
      for (boolean more = iter.start(); more; more = iter.advance()) {
        ApproximateReportedProgress progress = readerProgressToCloudProgress(iter.getProgress());
        assertNotNull(progress.getPosition().getShufflePosition());

        // Compare returned position with the expected position.
        assertEquals(
            positionsList.get(i).encodeBase64(), progress.getPosition().getShufflePosition());

        WindowedValue<KV<Integer, Reiterable<Integer>>> elem = iter.getCurrent();
        assertEquals(i, elem.getValue().getKey());
        i++;
      }
      assertFalse(iter.advance());

      // Cannot split since all input was consumed.
      Position proposedSplitPosition = new Position();
      String stop = encodeBase64URLSafeString(fabricatePosition(0).getPosition().toByteArray());
      proposedSplitPosition.setShufflePosition(stop);
      assertNull(
          iter.requestDynamicSplit(
              toDynamicSplitRequest(approximateSplitRequestAtPosition(proposedSplitPosition))));
    }
    assertTrue(shuffleReader.isClosed());
  }

  @Test
  public void testShuffleReadCounterMultipleExecutingSteps() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(DataflowPipelineDebugOptions.class)
        .setExperiments(Lists.newArrayList(Experiment.IntertransformIO.getName()));
    BatchModeExecutionContext context = BatchModeExecutionContext.forTesting(options, "testStage");
    final int kFirstShard = 0;

    TestShuffleReader shuffleReader = new TestShuffleReader();
    final int kNumRecords = 10;
    for (int i = 0; i < kNumRecords; ++i) {
      byte[] key = CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), i);
      shuffleReader.addEntry(
          newShuffleEntry(fabricatePosition(kFirstShard, key), key, EMPTY_BYTE_ARRAY, key));
    }

    TestShuffleReadCounterFactory shuffleReadCounterFactory = new TestShuffleReadCounterFactory();
    // Note that TestShuffleReader start/end positions are in the
    // space of keys not the positions (TODO: should probably always
    // use positions instead).
    String stop =
        encodeBase64URLSafeString(fabricatePosition(kNumRecords).getPosition().toByteArray());
    TestOperationContext operationContext = TestOperationContext.create();
    GroupingShuffleReader<Integer, Integer> groupingShuffleReader =
        new GroupingShuffleReader<>(
            options,
            null,
            null,
            stop,
            WindowedValue.getFullCoder(
                KvCoder.of(
                    BigEndianIntegerCoder.of(), IterableCoder.of(BigEndianIntegerCoder.of())),
                IntervalWindow.getCoder()),
            context,
            operationContext,
            shuffleReadCounterFactory,
            false /* do not sort values */);

    assertFalse(shuffleReader.isClosed());
    try (GroupingShuffleReaderIterator<Integer, Integer> iter =
        groupingShuffleReader.iterator(shuffleReader)) {
      // Poke the iterator so we can test dynamic splitting.
      assertTrue(iter.start());
      int numRecordsReturned = 1; // including start() above.

      for (; iter.advance(); ++numRecordsReturned) {
        if (numRecordsReturned > 5) {
          setCurrentExecutionState(MOCK_ORIGINAL_NAME_FOR_EXECUTING_STEP2);
        }
        iter.getCurrent().getValue(); // ignored
      }
      assertEquals(kNumRecords, numRecordsReturned);
    }
    assertTrue(shuffleReader.isClosed());

    Map<String, Long> expectedReadBytesMap = new HashMap<>();
    expectedReadBytesMap.put(MOCK_ORIGINAL_NAME_FOR_EXECUTING_STEP1, 48L);
    expectedReadBytesMap.put(MOCK_ORIGINAL_NAME_FOR_EXECUTING_STEP2, 32L);
    expectShuffleReadCounterEquals(shuffleReadCounterFactory, expectedReadBytesMap);
  }
}
