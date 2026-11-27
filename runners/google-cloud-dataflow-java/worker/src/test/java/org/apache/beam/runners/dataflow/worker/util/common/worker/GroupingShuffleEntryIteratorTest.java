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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Observer;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.ExperimentContext.Experiment;
import org.apache.beam.runners.dataflow.worker.TestOperationContext.TestDataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link GroupingShuffleEntryIterator}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class GroupingShuffleEntryIteratorTest {
  private static final ByteArrayShufflePosition START_POSITION =
      ByteArrayShufflePosition.of("aaa".getBytes(StandardCharsets.UTF_8));
  private static final ByteArrayShufflePosition END_POSITION =
      ByteArrayShufflePosition.of("zzz".getBytes(StandardCharsets.UTF_8));

  private static final String MOCK_STAGE_NAME = "mockStageName";
  private static final String MOCK_ORIGINAL_NAME_FOR_EXECUTING_STEP1 = "mockOriginalName1";
  private static final String MOCK_SYSTEM_NAME = "mockSystemName";
  private static final String MOCK_USER_NAME = "mockUserName";
  private static final String ORIGINAL_SHUFFLE_STEP_NAME = "originalName";

  @Mock private ShuffleEntryReader reader;
  private GroupingShuffleEntryIterator iterator;

  private final ExecutionStateSampler sampler = ExecutionStateSampler.newForTest();
  private final ExecutionStateTracker tracker = new ExecutionStateTracker(sampler);
  private Closeable trackerCleanup;

  @Before
  public void setUp() {
    trackerCleanup = tracker.activate();
  }

  @After
  public void tearDown() throws IOException {
    trackerCleanup.close();
  }

  private static class ListReiterator<T> implements Reiterator<T> {
    protected final List<T> entries;
    protected int nextIndex;

    public ListReiterator(List<T> entries, int nextIndex) {
      this.entries = entries;
      this.nextIndex = nextIndex;
    }

    @Override
    public Reiterator<T> copy() {
      return new ListReiterator<T>(entries, nextIndex);
    }

    @Override
    public boolean hasNext() {
      return nextIndex < entries.size();
    }

    @Override
    public T next() {
      T res = entries.get(nextIndex);
      nextIndex++;
      return res;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private void setCurrentExecutionState(String mockOriginalName) {
    DataflowExecutionState state =
        new TestDataflowExecutionState(
            NameContext.create(MOCK_STAGE_NAME, mockOriginalName, MOCK_SYSTEM_NAME, MOCK_USER_NAME),
            "activity");
    tracker.enterState(state);
  }

  private static ShuffleEntry shuffleEntry(String key, String value) {
    return new ShuffleEntry(
        /* use key itself as position */
        ByteArrayShufflePosition.of(key.getBytes(StandardCharsets.UTF_8)),
        ByteString.copyFrom(key.getBytes(StandardCharsets.UTF_8)),
        ByteString.copyFrom(new byte[0]),
        ByteString.copyFrom(value.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testCopyValuesIterator() throws Exception {
    setCurrentExecutionState(MOCK_ORIGINAL_NAME_FOR_EXECUTING_STEP1);

    MockitoAnnotations.initMocks(this);
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(DataflowPipelineDebugOptions.class)
        .setExperiments(Lists.newArrayList(Experiment.IntertransformIO.getName()));

    ArrayList<ShuffleEntry> entries = new ArrayList<>();
    entries.add(shuffleEntry("k1", "v11"));
    entries.add(shuffleEntry("k1", "v12"));
    entries.add(shuffleEntry("k1", "v13"));
    when(reader.read(START_POSITION, END_POSITION)).thenReturn(new ListReiterator<>(entries, 0));

    final ShuffleReadCounter shuffleReadCounter =
        new ShuffleReadCounter(ORIGINAL_SHUFFLE_STEP_NAME, true, null);
    iterator =
        new GroupingShuffleEntryIterator(reader, START_POSITION, END_POSITION) {
          @Override
          protected void notifyElementRead(long byteSize) {
            // nothing
          }

          @Override
          protected void commitBytesRead(long bytes) {
            shuffleReadCounter.addBytesRead(bytes);
          }
        };
    assertTrue(iterator.advance());
    KeyGroupedShuffleEntries k1Entries = iterator.getCurrent();
    Reiterator<ShuffleEntry> values1 = k1Entries.values.iterator();
    assertTrue(values1.hasNext());
    Reiterator<ShuffleEntry> values1Copy1 = values1.copy();
    Reiterator<ShuffleEntry> values1Copy2 = values1.copy();
    ShuffleEntry expectedEntry = values1.next();
    assertFalse(iterator.advance()); // Advance the iterator again to record bytes read.

    // Test that the copy works two ways: 1) if we call hasNext, and 2) if we don't.
    assertTrue(values1Copy1.hasNext());
    assertEquals(expectedEntry, values1Copy1.next());

    assertEquals(expectedEntry, values1Copy2.next());

    Map<String, Long> expectedReadBytesMap = new HashMap<>();
    expectedReadBytesMap.put(MOCK_ORIGINAL_NAME_FOR_EXECUTING_STEP1, 15L);

    // Verify that each executing step used when reading from the GroupingShuffleReader
    // has a counter with a bytes read value.
    assertEquals(expectedReadBytesMap.size(), (long) shuffleReadCounter.counterSet.size());
    Iterator it = expectedReadBytesMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Long> pair = (Map.Entry) it.next();
      Counter counter =
          shuffleReadCounter.counterSet.getExistingCounter(
              ShuffleReadCounter.generateCounterName(ORIGINAL_SHUFFLE_STEP_NAME, pair.getKey()));
      assertEquals(pair.getValue(), counter.getAggregate());
    }
  }

  /** A ShuffleEntryReader that asserts that its iterators never go backwards ("reiterate"). */
  private static class ForwardOnlyShuffleEntryReader implements ShuffleEntryReader {
    private final List<ShuffleEntry> entries;
    private int minNextIndex = 0; // The smallest index that iterators are allowed to advance.

    public ForwardOnlyShuffleEntryReader(List<ShuffleEntry> entries) {
      this.entries = entries;
    }

    @Override
    public Reiterator<ShuffleEntry> read(
        @Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition) {
      return new MinIndexAssertingListReiterator(entries, 0);
    }

    @Override
    public void close() {}

    private class MinIndexAssertingListReiterator<T> extends ListReiterator<T> {
      public MinIndexAssertingListReiterator(List<T> entries, int nextIndex) {
        super(entries, nextIndex);
      }

      @Override
      public Reiterator<T> copy() {
        return new MinIndexAssertingListReiterator<T>(entries, nextIndex);
      }

      @Override
      public T next() {
        assertTrue("Reiteration unexpected.", nextIndex >= minNextIndex);
        minNextIndex = nextIndex;
        return super.next();
      }
    }
  }

  /**
   * Tests that GroupingShuffleEntryIterator does not reiterate the underlying shuffle iterator when
   * the returned value iterators are iterated over (i.e., that fast-forwarding works properly).
   */
  @Test
  public void testNoReiteration() throws Exception {
    ArrayList<ShuffleEntry> entries = new ArrayList<>();
    entries.add(shuffleEntry("k1", "v11"));
    entries.add(shuffleEntry("k1", "v12"));
    entries.add(shuffleEntry("k1", "v13"));
    entries.add(shuffleEntry("k2", "v21"));
    entries.add(shuffleEntry("k2", "v22"));
    entries.add(shuffleEntry("k2", "v23"));
    ForwardOnlyShuffleEntryReader reader = new ForwardOnlyShuffleEntryReader(entries);

    iterator =
        new GroupingShuffleEntryIterator(reader, START_POSITION, END_POSITION) {
          @Override
          protected void notifyElementRead(long byteSize) {
            // nothing
          }

          @Override
          protected void commitBytesRead(long bytes) {
            // nothing
          }
        };
    int totalKeys = 0;
    int totalValues = 0;
    while (iterator.advance()) {
      ++totalKeys;
      Reiterator<ShuffleEntry> values = iterator.getCurrent().values.iterator();
      while (values.hasNext()) {
        values.next();
        ++totalValues;
      }
    }
    assertEquals(2, totalKeys);
    assertEquals(6, totalValues);

    // We expect that AssertionException in MinIndexAssertingListReiterator.next() is not thrown.
  }

  @Test
  public void testReiterationCopyAndPartialSkip() throws Exception {
    ArrayList<ShuffleEntry> entries = new ArrayList<>();
    ShuffleEntry k1v1 = shuffleEntry("k1", "v11");
    ShuffleEntry k2v1 = shuffleEntry("k2", "v21");
    ShuffleEntry k2v2 = shuffleEntry("k2", "v22");
    ShuffleEntry k2v3 = shuffleEntry("k2", "v23");
    ShuffleEntry k3v1 = shuffleEntry("k3", "v31");
    ShuffleEntry k3v2 = shuffleEntry("k3", "v32");
    ShuffleEntry k4v1 = shuffleEntry("k4", "v41");
    entries.add(k1v1);
    entries.add(k2v1);
    entries.add(k2v2);
    entries.add(k2v3);
    entries.add(k3v1);
    entries.add(k3v2);
    entries.add(k4v1);

    long expectedTotalBytes = entries.stream().mapToLong(ShuffleEntry::length).sum();
    long[] notifyTotals = new long[1];
    ArrayList<Long> committed = new ArrayList<>();

    ShuffleEntryReader reader =
        new ShuffleEntryReader() {
          @Override
          public Reiterator<ShuffleEntry> read(
              @Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition) {
            return new ListReiterator<>(entries, 0);
          }

          @Override
          public void close() {}
        };

    iterator =
        new GroupingShuffleEntryIterator(reader, START_POSITION, END_POSITION) {
          @Override
          protected void notifyElementRead(long byteSize) {
            notifyTotals[0] += byteSize;
          }

          @Override
          protected void commitBytesRead(long bytes) {
            committed.add(bytes);
          }
        };

    // Key 1 (single value): copy before reading, read twice, copy after exhaustion.
    assertTrue(iterator.advance());
    assertEquals("k1", iterator.getCurrent().key.toStringUtf8());
    Reiterator<ShuffleEntry> k1First = iterator.getCurrent().values.iterator();
    Reiterator<ShuffleEntry> k1CopyBeforeNext = k1First.copy();
    assertTrue(k1First.hasNext());
    Reiterator<ShuffleEntry> k1CopyAfterHasNext = k1First.copy();
    assertEquals("v11", k1First.next().getValue().toStringUtf8());
    assertFalse(k1First.hasNext());
    // Copy AFTER exhaustion (valuesIterator donated and nulled out).
    Reiterator<ShuffleEntry> k1ExhaustedCopy = k1First.copy();
    assertFalse(k1ExhaustedCopy.hasNext());
    try {
      k1ExhaustedCopy.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException expected) {
    }
    assertEquals("v11", k1CopyAfterHasNext.next().getValue().toStringUtf8());
    assertFalse(k1CopyAfterHasNext.hasNext());
    assertEquals("v11", k1CopyBeforeNext.next().getValue().toStringUtf8());
    assertFalse(k1CopyBeforeNext.hasNext());
    Reiterator<ShuffleEntry> k1Second = iterator.getCurrent().values.iterator();
    assertEquals("v11", k1Second.next().getValue().toStringUtf8());
    assertFalse(k1Second.hasNext());

    // Key 2 (3 values): read ONLY v21 (do NOT exhaust any copy) so advance() genuinely takes
    // the same-key skip loop in GroupingShuffleEntryIterator.advance().
    assertTrue(iterator.advance());
    assertEquals("k2", iterator.getCurrent().key.toStringUtf8());
    Reiterator<ShuffleEntry> k2Iter = iterator.getCurrent().values.iterator();
    assertEquals("v21", k2Iter.next().getValue().toStringUtf8());
    Reiterator<ShuffleEntry> k2MidCopy = k2Iter.copy();

    // Key 3 (2 values): completely unconsumed values iterable.
    assertTrue(iterator.advance());
    assertEquals("k3", iterator.getCurrent().key.toStringUtf8());

    // Draining the older k2 iterators AFTER parent advanced past k2 must yield v22, v23 cleanly
    // and must NOT corrupt parent.shuffleIterator positioned at k3.
    assertEquals("v22", k2Iter.next().getValue().toStringUtf8());
    assertEquals("v23", k2Iter.next().getValue().toStringUtf8());
    assertFalse(k2Iter.hasNext());
    assertEquals("v22", k2MidCopy.next().getValue().toStringUtf8());
    assertEquals("v23", k2MidCopy.next().getValue().toStringUtf8());
    assertFalse(k2MidCopy.hasNext());

    // Key 4 (1 value): advance() skips unconsumed k3 entries and lands on k4.
    assertTrue(iterator.advance());
    assertEquals("k4", iterator.getCurrent().key.toStringUtf8());
    Reiterator<ShuffleEntry> k4Iter = iterator.getCurrent().values.iterator();
    assertEquals("v41", k4Iter.next().getValue().toStringUtf8());
    assertFalse(k4Iter.hasNext());

    assertFalse(iterator.advance());
    // Advancing again after finished stays false and doesn't commit extra bytes.
    assertFalse(iterator.advance());

    // Every ShuffleEntry must be reported to notifyElementRead exactly once.
    assertEquals(expectedTotalBytes, notifyTotals[0]);
    // Committed totals: [0 (before k1), k1, k2, k3, k4].
    assertEquals(
        Arrays.asList(
            0L,
            (long) k1v1.length(),
            (long) k2v1.length() + k2v2.length() + k2v3.length(),
            (long) k3v1.length() + k3v2.length(),
            (long) k4v1.length()),
        committed);
  }

  @Test
  public void testEmptyKeyEmptyStreamSplitAndObserver() throws Exception {
    // Empty input stream.
    ShuffleEntryReader emptyReader =
        new ShuffleEntryReader() {
          @Override
          public Reiterator<ShuffleEntry> read(
              @Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition) {
            return new ListReiterator<>(new ArrayList<>(), 0);
          }

          @Override
          public void close() {}
        };
    ArrayList<Long> emptyCommitted = new ArrayList<>();
    GroupingShuffleEntryIterator emptyIter =
        new GroupingShuffleEntryIterator(emptyReader, START_POSITION, END_POSITION) {
          @Override
          protected void notifyElementRead(long byteSize) {
            fail("No entries to read");
          }

          @Override
          protected void commitBytesRead(long bytes) {
            emptyCommitted.add(bytes);
          }
        };
    assertFalse(emptyIter.advance());
    assertEquals(Collections.singletonList(0L), emptyCommitted);

    // Zero-byte key (ByteString.EMPTY), fast-forward, dynamic split stop before k2, and observer.
    ShuffleEntry eEmpty1 =
        new ShuffleEntry(
            ByteArrayShufflePosition.of("a".getBytes(StandardCharsets.UTF_8)),
            ByteString.EMPTY,
            ByteString.EMPTY,
            ByteString.copyFromUtf8("v1"));
    ShuffleEntry eEmpty2 =
        new ShuffleEntry(
            ByteArrayShufflePosition.of("b".getBytes(StandardCharsets.UTF_8)),
            ByteString.EMPTY,
            ByteString.EMPTY,
            ByteString.copyFromUtf8("v22"));
    ShuffleEntry eSplit =
        new ShuffleEntry(
            ByteArrayShufflePosition.of("z".getBytes(StandardCharsets.UTF_8)),
            ByteString.copyFromUtf8("k2"),
            ByteString.EMPTY,
            ByteString.copyFromUtf8("v3"));
    ArrayList<ShuffleEntry> entries = new ArrayList<>();
    entries.add(eEmpty1);
    entries.add(eEmpty2);
    entries.add(eSplit);

    ShuffleEntryReader reader =
        new ShuffleEntryReader() {
          @Override
          public Reiterator<ShuffleEntry> read(
              @Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition) {
            return new ListReiterator<>(entries, 0);
          }

          @Override
          public void close() {}
        };
    ArrayList<Long> committed = new ArrayList<>();
    GroupingShuffleEntryIterator it =
        new GroupingShuffleEntryIterator(
            reader,
            ByteArrayShufflePosition.of("0".getBytes(StandardCharsets.UTF_8)),
            ByteArrayShufflePosition.of("zz".getBytes(StandardCharsets.UTF_8))) {
          @Override
          protected void notifyElementRead(long byteSize) {}

          @Override
          protected void commitBytesRead(long bytes) {
            committed.add(bytes);
          }
        };

    assertTrue(it.advance());
    assertEquals(0, it.getCurrent().key.size());
    ArrayList<Long> observed = new ArrayList<>();
    @SuppressWarnings("deprecation")
    Observer observer = (o, arg) -> observed.add((Long) arg);
    ((ElementByteSizeObservableIterable<?, ?>) it.getCurrent().values).addObserver(observer);
    Reiterator<ShuffleEntry> values = it.getCurrent().values.iterator();
    assertEquals("v1", values.next().getValue().toStringUtf8());
    // Copy must still forward progress through original iterator's observer.
    Reiterator<ShuffleEntry> valuesCopy = values.copy();
    assertEquals("v22", valuesCopy.next().getValue().toStringUtf8());
    assertFalse(valuesCopy.hasNext());
    assertEquals(Arrays.asList((long) eEmpty1.length(), (long) eEmpty2.length()), observed);

    // Removing the observer turns off notifications on subsequent reiterations/elements.
    Reiterator<ShuffleEntry> values2 = it.getCurrent().values.iterator();
    @SuppressWarnings("deprecation")
    ElementByteSizeObservableIterator<?> observable2 =
        (ElementByteSizeObservableIterator<?>) values2;
    ArrayList<Long> secondObserved = new ArrayList<>();
    @SuppressWarnings("deprecation")
    Observer observer2 = (o, arg) -> secondObserved.add((Long) arg);
    observable2.addObserver(observer2);
    assertEquals("v1", values2.next().getValue().toStringUtf8());
    observable2.deleteObserver(observer2);
    assertEquals("v22", values2.next().getValue().toStringUtf8());
    assertFalse(values2.hasNext());
    assertEquals(Collections.singletonList((long) eEmpty1.length()), secondObserved);

    Reiterator<ShuffleEntry> values3 = it.getCurrent().values.iterator();
    ElementByteSizeObservableIterator<?> observable3 =
        (ElementByteSizeObservableIterator<?>) values3;
    ArrayList<Long> thirdObserved = new ArrayList<>();
    observable3.addObserver((o, arg) -> thirdObserved.add((Long) arg));
    observable3.deleteObservers();
    assertEquals("v1", values3.next().getValue().toStringUtf8());
    assertEquals("v22", values3.next().getValue().toStringUtf8());
    assertTrue(thirdObserved.isEmpty());

    // Split BEFORE "z" so the buffered nextKeyEntry is rejected by rangeTracker.
    assertTrue(
        it.trySplitAtPosition(ByteArrayShufflePosition.of("m".getBytes(StandardCharsets.UTF_8))));
    assertFalse(it.advance());
    assertEquals(Arrays.asList(0L, (long) eEmpty1.length() + eEmpty2.length()), committed);
  }
}
