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
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.BatchModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.ExperimentContext.Experiment;
import org.apache.beam.runners.dataflow.worker.TestOperationContext.TestDataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link GroupingShuffleEntryIterator}. */
@RunWith(JUnit4.class)
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
        ByteArrayShufflePosition.of(key.getBytes(Charsets.UTF_8)),
        key.getBytes(Charsets.UTF_8),
        new byte[0],
        value.getBytes(Charsets.UTF_8));
  }

  @Test
  public void testCopyValuesIterator() throws Exception {
    setCurrentExecutionState(MOCK_ORIGINAL_NAME_FOR_EXECUTING_STEP1);

    MockitoAnnotations.initMocks(this);
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(DataflowPipelineDebugOptions.class)
        .setExperiments(Lists.newArrayList(Experiment.IntertransformIO.getName()));
    BatchModeExecutionContext spyExecutionContext =
        Mockito.spy(BatchModeExecutionContext.forTesting(options, "STAGE"));

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
}
