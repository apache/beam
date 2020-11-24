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

import static org.apache.beam.runners.dataflow.worker.DataflowMatchers.ByteStringMatcher.byteStringEq;
import static org.apache.beam.sdk.testing.SystemNanoTimeSleeper.sleepMillis;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterables;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaceForTest;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.WindmillStateInternals.IdTracker;
import org.apache.beam.runners.dataflow.worker.WindmillStateInternals.WindmillOrderedList;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagBag;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagSortedListUpdateRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.RangeSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.SettableFuture;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link WindmillStateInternals}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class WindmillStateInternalsTest {

  private static final StateNamespace NAMESPACE = new StateNamespaceForTest("ns");
  private static final String STATE_FAMILY = "family";

  private static final StateTag<CombiningState<Integer, int[], Integer>> COMBINING_ADDR =
      StateTags.combiningValueFromInputInternal("combining", VarIntCoder.of(), Sum.ofIntegers());
  private static final ByteString COMBINING_KEY = key(NAMESPACE, "combining");
  private final Coder<int[]> accumCoder =
      Sum.ofIntegers().getAccumulatorCoder(null, VarIntCoder.of());
  private long workToken = 0;

  DataflowWorkerHarnessOptions options;

  @Mock private WindmillStateReader mockReader;

  private WindmillStateInternals<String> underTest;
  private WindmillStateInternals<String> underTestNewKey;
  private WindmillStateCache cache;

  @Mock private Supplier<Closeable> readStateSupplier;

  private static ByteString key(StateNamespace namespace, String addrId) {
    return ByteString.copyFromUtf8(namespace.stringKey() + "+u" + addrId);
  }

  private static ByteString systemKey(StateNamespace namespace, String addrId) {
    return ByteString.copyFromUtf8(namespace.stringKey() + "+s" + addrId);
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    options = PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    cache = new WindmillStateCache(options.getWorkerCacheMb());
    resetUnderTest();
  }

  public void resetUnderTest() {
    workToken++;
    underTest =
        new WindmillStateInternals<>(
            "dummyKey",
            STATE_FAMILY,
            mockReader,
            false,
            cache
                .forComputation("comp")
                .forKey(
                    WindmillComputationKey.create("comp", ByteString.EMPTY, 123),
                    STATE_FAMILY,
                    17L,
                    workToken),
            readStateSupplier);
    underTestNewKey =
        new WindmillStateInternals<String>(
            "dummyNewKey",
            STATE_FAMILY,
            mockReader,
            true,
            cache
                .forComputation("comp")
                .forKey(
                    WindmillComputationKey.create("comp", ByteString.EMPTY, 123),
                    STATE_FAMILY,
                    17L,
                    workToken),
            readStateSupplier);
  }

  @After
  public void tearDown() throws Exception {
    // Make sure no WindmillStateReader (a per-WorkItem object) escapes into the cache
    // (a global object).
    WindmillStateTestUtils.assertNoReference(cache, WindmillStateReader.class);
  }

  private <T> void waitAndSet(final SettableFuture<T> future, final T value, final long millis) {
    new Thread(
            () -> {
              try {
                sleepMillis(millis);
              } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted before setting", e);
              }
              future.set(value);
            })
        .run();
  }

  private WindmillStateReader.WeightedList<String> weightedList(String... elems) {
    WindmillStateReader.WeightedList<String> result =
        new WindmillStateReader.WeightedList<>(new ArrayList<String>(elems.length));
    for (String elem : elems) {
      result.addWeighted(elem, elem.length());
    }
    return result;
  }

  public static final Range<Long> FULL_ORDERED_LIST_RANGE =
      Range.closedOpen(WindmillOrderedList.MIN_TS_MICROS, WindmillOrderedList.MAX_TS_MICROS);

  @Test
  public void testOrderedListAddBeforeRead() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<TimestampedValue<String>>> future = SettableFuture.create();
    when(mockReader.orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of()))
        .thenReturn(future);

    orderedList.readLater();

    final TimestampedValue<String> helloValue =
        TimestampedValue.of("hello", Instant.ofEpochMilli(100));
    final TimestampedValue<String> worldValue =
        TimestampedValue.of("world", Instant.ofEpochMilli(75));
    final TimestampedValue<String> goodbyeValue =
        TimestampedValue.of("goodbye", Instant.ofEpochMilli(50));

    orderedList.add(helloValue);
    waitAndSet(future, Arrays.asList(worldValue), 200);
    assertThat(orderedList.read(), Matchers.contains(worldValue, helloValue));

    orderedList.add(goodbyeValue);
    assertThat(orderedList.read(), Matchers.contains(goodbyeValue, worldValue, helloValue));
  }

  @Test
  public void testOrderedListClearBeforeRead() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedListState = underTest.state(NAMESPACE, addr);

    final TimestampedValue<String> helloElement = TimestampedValue.of("hello", Instant.EPOCH);
    orderedListState.clear();
    orderedListState.add(helloElement);
    assertThat(orderedListState.read(), Matchers.containsInAnyOrder(helloElement));

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testOrderedListIsEmptyFalse() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<TimestampedValue<String>>> future = SettableFuture.create();
    when(mockReader.orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of()))
        .thenReturn(future);
    ReadableState<Boolean> result = orderedList.isEmpty().readLater();
    Mockito.verify(mockReader)
        .orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of());

    waitAndSet(future, Arrays.asList(TimestampedValue.of("world", Instant.EPOCH)), 200);
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testOrderedListIsEmptyTrue() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<TimestampedValue<String>>> future = SettableFuture.create();
    when(mockReader.orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of()))
        .thenReturn(future);
    ReadableState<Boolean> result = orderedList.isEmpty().readLater();
    Mockito.verify(mockReader)
        .orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of());

    waitAndSet(future, Collections.emptyList(), 200);
    assertThat(result.read(), Matchers.is(true));
  }

  @Test
  public void testOrderedListIsEmptyAfterClear() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTest.state(NAMESPACE, addr);

    orderedList.clear();
    ReadableState<Boolean> result = orderedList.isEmpty();
    Mockito.verify(mockReader, never())
        .orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of());
    assertThat(result.read(), Matchers.is(true));

    orderedList.add(TimestampedValue.of("hello", Instant.EPOCH));
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testOrderedListAddPersist() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTest.state(NAMESPACE, addr);

    SettableFuture<Map<Range<Instant>, RangeSet<Long>>> orderedListFuture = SettableFuture.create();
    orderedListFuture.set(null);
    SettableFuture<Map<Range<Instant>, RangeSet<Instant>>> deletionsFuture =
        SettableFuture.create();
    deletionsFuture.set(null);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.IDS_AVAILABLE_STR),
            STATE_FAMILY,
            IdTracker.IDS_AVAILABLE_CODER))
        .thenReturn(orderedListFuture);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.DELETIONS_STR),
            STATE_FAMILY,
            IdTracker.SUBRANGE_DELETIONS_CODER))
        .thenReturn(deletionsFuture);

    orderedList.add(TimestampedValue.of("hello", Instant.ofEpochMilli(1)));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getSortedListUpdatesCount());
    TagSortedListUpdateRequest updates = commitBuilder.getSortedListUpdates(0);
    assertEquals(key(NAMESPACE, "orderedList"), updates.getTag());
    assertEquals(1, updates.getInsertsCount());
    assertEquals(1, updates.getInserts(0).getEntriesCount());

    assertEquals("hello", updates.getInserts(0).getEntries(0).getValue().toStringUtf8());
    assertEquals(1000, updates.getInserts(0).getEntries(0).getSortKey());
    assertEquals(IdTracker.MIN_ID, updates.getInserts(0).getEntries(0).getId());
  }

  @Test
  public void testOrderedListClearPersist() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedListState = underTest.state(NAMESPACE, addr);

    orderedListState.add(TimestampedValue.of("hello", Instant.ofEpochMilli(1)));
    orderedListState.clear();
    orderedListState.add(TimestampedValue.of("world", Instant.ofEpochMilli(2)));
    orderedListState.add(TimestampedValue.of("world", Instant.ofEpochMilli(2)));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getSortedListUpdatesCount());
    TagSortedListUpdateRequest updates = commitBuilder.getSortedListUpdates(0);
    assertEquals(STATE_FAMILY, updates.getStateFamily());
    assertEquals(key(NAMESPACE, "orderedList"), updates.getTag());
    assertEquals(1, updates.getInsertsCount());
    assertEquals(2, updates.getInserts(0).getEntriesCount());

    assertEquals("world", updates.getInserts(0).getEntries(0).getValue().toStringUtf8());
    assertEquals("world", updates.getInserts(0).getEntries(1).getValue().toStringUtf8());
    assertEquals(2000, updates.getInserts(0).getEntries(0).getSortKey());
    assertEquals(2000, updates.getInserts(0).getEntries(1).getSortKey());
    assertEquals(IdTracker.MIN_ID, updates.getInserts(0).getEntries(0).getId());
    assertEquals(IdTracker.MIN_ID + 1, updates.getInserts(0).getEntries(1).getId());
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testOrderedListDeleteRangePersist() {
    SettableFuture<Map<Range<Instant>, RangeSet<Long>>> orderedListFuture = SettableFuture.create();
    orderedListFuture.set(null);
    SettableFuture<Map<Range<Instant>, RangeSet<Instant>>> deletionsFuture =
        SettableFuture.create();
    deletionsFuture.set(null);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.IDS_AVAILABLE_STR),
            STATE_FAMILY,
            IdTracker.IDS_AVAILABLE_CODER))
        .thenReturn(orderedListFuture);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.DELETIONS_STR),
            STATE_FAMILY,
            IdTracker.SUBRANGE_DELETIONS_CODER))
        .thenReturn(deletionsFuture);

    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedListState = underTest.state(NAMESPACE, addr);

    orderedListState.add(TimestampedValue.of("hello", Instant.ofEpochMilli(1)));
    orderedListState.add(TimestampedValue.of("hello", Instant.ofEpochMilli(2)));
    orderedListState.add(TimestampedValue.of("hello", Instant.ofEpochMilli(2)));
    orderedListState.add(TimestampedValue.of("world", Instant.ofEpochMilli(3)));
    orderedListState.add(TimestampedValue.of("world", Instant.ofEpochMilli(4)));
    orderedListState.clearRange(Instant.ofEpochMilli(2), Instant.ofEpochMilli(4));
    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getSortedListUpdatesCount());
    TagSortedListUpdateRequest updates = commitBuilder.getSortedListUpdates(0);
    assertEquals(STATE_FAMILY, updates.getStateFamily());
    assertEquals(key(NAMESPACE, "orderedList"), updates.getTag());
    assertEquals(1, updates.getInsertsCount());
    assertEquals(2, updates.getInserts(0).getEntriesCount());

    assertEquals("hello", updates.getInserts(0).getEntries(0).getValue().toStringUtf8());
    assertEquals("world", updates.getInserts(0).getEntries(1).getValue().toStringUtf8());
    assertEquals(1000, updates.getInserts(0).getEntries(0).getSortKey());
    assertEquals(4000, updates.getInserts(0).getEntries(1).getSortKey());
    assertEquals(IdTracker.MIN_ID, updates.getInserts(0).getEntries(0).getId());
    assertEquals(IdTracker.MIN_ID + 1, updates.getInserts(0).getEntries(1).getId());
  }

  @Test
  public void testOrderedListMergePendingAdds() {
    SettableFuture<Map<Range<Instant>, RangeSet<Long>>> orderedListFuture = SettableFuture.create();
    orderedListFuture.set(null);
    SettableFuture<Map<Range<Instant>, RangeSet<Instant>>> deletionsFuture =
        SettableFuture.create();
    deletionsFuture.set(null);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.IDS_AVAILABLE_STR),
            STATE_FAMILY,
            IdTracker.IDS_AVAILABLE_CODER))
        .thenReturn(orderedListFuture);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.DELETIONS_STR),
            STATE_FAMILY,
            IdTracker.SUBRANGE_DELETIONS_CODER))
        .thenReturn(deletionsFuture);

    SettableFuture<Iterable<TimestampedValue<String>>> fromStorage = SettableFuture.create();
    when(mockReader.orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of()))
        .thenReturn(fromStorage);

    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedListState = underTest.state(NAMESPACE, addr);

    orderedListState.add(TimestampedValue.of("second", Instant.ofEpochMilli(1)));
    orderedListState.add(TimestampedValue.of("third", Instant.ofEpochMilli(2)));
    orderedListState.add(TimestampedValue.of("fourth", Instant.ofEpochMilli(2)));
    orderedListState.add(TimestampedValue.of("eighth", Instant.ofEpochMilli(10)));
    orderedListState.add(TimestampedValue.of("ninth", Instant.ofEpochMilli(15)));

    fromStorage.set(
        ImmutableList.of(
            TimestampedValue.of("first", Instant.ofEpochMilli(-1)),
            TimestampedValue.of("fifth", Instant.ofEpochMilli(5)),
            TimestampedValue.of("sixth", Instant.ofEpochMilli(5)),
            TimestampedValue.of("seventh", Instant.ofEpochMilli(5)),
            TimestampedValue.of("tenth", Instant.ofEpochMilli(20))));

    TimestampedValue[] expected =
        Iterables.toArray(
            ImmutableList.of(
                TimestampedValue.of("first", Instant.ofEpochMilli(-1)),
                TimestampedValue.of("second", Instant.ofEpochMilli(1)),
                TimestampedValue.of("third", Instant.ofEpochMilli(2)),
                TimestampedValue.of("fourth", Instant.ofEpochMilli(2)),
                TimestampedValue.of("fifth", Instant.ofEpochMilli(5)),
                TimestampedValue.of("sixth", Instant.ofEpochMilli(5)),
                TimestampedValue.of("seventh", Instant.ofEpochMilli(5)),
                TimestampedValue.of("eighth", Instant.ofEpochMilli(10)),
                TimestampedValue.of("ninth", Instant.ofEpochMilli(15)),
                TimestampedValue.of("tenth", Instant.ofEpochMilli(20))),
            TimestampedValue.class);

    TimestampedValue[] read = Iterables.toArray(orderedListState.read(), TimestampedValue.class);
    assertArrayEquals(expected, read);
  }

  @Test
  public void testOrderedListPersistEmpty() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedListState = underTest.state(NAMESPACE, addr);

    orderedListState.clear();

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    // 1 bag update = the clear
    assertEquals(1, commitBuilder.getSortedListUpdatesCount());
    TagSortedListUpdateRequest updates = commitBuilder.getSortedListUpdates(0);
    assertEquals(1, updates.getDeletesCount());
    assertEquals(WindmillOrderedList.MIN_TS_MICROS, updates.getDeletes(0).getRange().getStart());
    assertEquals(WindmillOrderedList.MAX_TS_MICROS, updates.getDeletes(0).getRange().getLimit());
  }

  @Test
  public void testNewOrderedListNoFetch() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTestNewKey.state(NAMESPACE, addr);

    assertThat(orderedList.read(), Matchers.emptyIterable());

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  // test ordered list cleared before read
  // test fetch + add + read
  // test ids

  @Test
  public void testBagAddBeforeRead() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);

    bag.readLater();

    bag.add("hello");
    waitAndSet(future, Arrays.asList("world"), 200);
    assertThat(bag.read(), Matchers.containsInAnyOrder("hello", "world"));

    bag.add("goodbye");
    assertThat(bag.read(), Matchers.containsInAnyOrder("hello", "world", "goodbye"));
  }

  @Test
  public void testBagClearBeforeRead() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    bag.clear();
    bag.add("hello");
    assertThat(bag.read(), Matchers.containsInAnyOrder("hello"));

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testBagIsEmptyFalse() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);
    ReadableState<Boolean> result = bag.isEmpty().readLater();
    Mockito.verify(mockReader).bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of());

    waitAndSet(future, Arrays.asList("world"), 200);
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testBagIsEmptyTrue() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);
    ReadableState<Boolean> result = bag.isEmpty().readLater();
    Mockito.verify(mockReader).bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of());

    waitAndSet(future, Arrays.<String>asList(), 200);
    assertThat(result.read(), Matchers.is(true));
  }

  @Test
  public void testBagIsEmptyAfterClear() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    bag.clear();
    ReadableState<Boolean> result = bag.isEmpty();
    Mockito.verify(mockReader, never())
        .bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of());
    assertThat(result.read(), Matchers.is(true));

    bag.add("hello");
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testBagAddPersist() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    bag.add("hello");

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getBagUpdatesCount());

    TagBag bagUpdates = commitBuilder.getBagUpdates(0);
    assertEquals(key(NAMESPACE, "bag"), bagUpdates.getTag());
    assertEquals(1, bagUpdates.getValuesCount());
    assertEquals("hello", bagUpdates.getValues(0).toStringUtf8());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testBagClearPersist() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    bag.add("hello");
    bag.clear();
    bag.add("world");

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getBagUpdatesCount());

    TagBag tagBag = commitBuilder.getBagUpdates(0);
    assertEquals(key(NAMESPACE, "bag"), tagBag.getTag());
    assertEquals(STATE_FAMILY, tagBag.getStateFamily());
    assertTrue(tagBag.getDeleteAll());
    assertEquals(1, tagBag.getValuesCount());
    assertEquals("world", tagBag.getValues(0).toStringUtf8());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testBagPersistEmpty() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    bag.clear();

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    // 1 bag update = the clear
    assertEquals(1, commitBuilder.getBagUpdatesCount());
  }

  @Test
  public void testNewBagNoFetch() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTestNewKey.state(NAMESPACE, addr);

    assertThat(bag.read(), Matchers.emptyIterable());

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  @SuppressWarnings("ArraysAsListPrimitiveArray")
  public void testCombiningAddBeforeRead() throws Exception {
    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    SettableFuture<Iterable<int[]>> future = SettableFuture.create();
    when(mockReader.bagFuture(eq(COMBINING_KEY), eq(STATE_FAMILY), Mockito.<Coder<int[]>>any()))
        .thenReturn(future);

    value.readLater();

    value.add(5);
    value.add(6);
    waitAndSet(future, Arrays.asList(new int[] {8}, new int[] {10}), 200);
    assertThat(value.read(), Matchers.equalTo(29));

    // That get "compressed" the combiner. So, the underlying future should change:
    future.set(Arrays.asList(new int[] {29}));

    value.add(2);
    assertThat(value.read(), Matchers.equalTo(31));
  }

  @Test
  public void testCombiningClearBeforeRead() throws Exception {
    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.clear();
    value.readLater();

    value.add(5);
    value.add(6);
    assertThat(value.read(), Matchers.equalTo(11));

    value.add(2);
    assertThat(value.read(), Matchers.equalTo(13));

    // Shouldn't need to read from windmill for this because we immediately cleared..
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  @SuppressWarnings("ArraysAsListPrimitiveArray")
  public void testCombiningIsEmpty() throws Exception {
    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    SettableFuture<Iterable<int[]>> future = SettableFuture.create();
    when(mockReader.bagFuture(eq(COMBINING_KEY), eq(STATE_FAMILY), Mockito.<Coder<int[]>>any()))
        .thenReturn(future);
    ReadableState<Boolean> result = value.isEmpty().readLater();
    ArgumentCaptor<ByteString> byteString = ArgumentCaptor.forClass(ByteString.class);

    // Note that we do expect the third argument - the coder - to be equal to accumCoder, but that
    // is possibly overspecified and currently trips an issue in the SDK where identical coders are
    // not #equals().
    //
    // What matters is that a future is created, hence a Windmill RPC sent.
    Mockito.verify(mockReader)
        .bagFuture(byteString.capture(), eq(STATE_FAMILY), Mockito.<Coder<int[]>>any());
    assertThat(byteString.getValue(), byteStringEq(COMBINING_KEY));

    waitAndSet(future, Arrays.asList(new int[] {29}), 200);
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testCombiningIsEmptyAfterClear() throws Exception {
    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.clear();
    ReadableState<Boolean> result = value.isEmpty();
    Mockito.verify(mockReader, never()).bagFuture(COMBINING_KEY, STATE_FAMILY, accumCoder);
    assertThat(result.read(), Matchers.is(true));

    value.add(87);
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testCombiningAddPersist() throws Exception {
    disableCompactOnWrite();

    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.add(5);
    value.add(6);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getBagUpdatesCount());

    TagBag bagUpdates = commitBuilder.getBagUpdates(0);
    assertEquals(COMBINING_KEY, bagUpdates.getTag());
    assertEquals(1, bagUpdates.getValuesCount());
    assertEquals(
        11, CoderUtils.decodeFromByteArray(accumCoder, bagUpdates.getValues(0).toByteArray())[0]);

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testCombiningAddPersistWithCompact() throws Exception {
    forceCompactOnWrite();

    Mockito.when(
            mockReader.bagFuture(
                org.mockito.Matchers.<ByteString>any(),
                org.mockito.Matchers.<String>any(),
                org.mockito.Matchers.<Coder<int[]>>any()))
        .thenReturn(
            Futures.<Iterable<int[]>>immediateFuture(
                ImmutableList.of(new int[] {40}, new int[] {60})));

    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.add(5);
    value.add(6);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getBagUpdatesCount());
    TagBag bagUpdates = commitBuilder.getBagUpdates(0);
    assertEquals(COMBINING_KEY, bagUpdates.getTag());
    assertEquals(1, bagUpdates.getValuesCount());
    assertTrue(bagUpdates.getDeleteAll());
    assertEquals(
        111, CoderUtils.decodeFromByteArray(accumCoder, bagUpdates.getValues(0).toByteArray())[0]);
  }

  @Test
  public void testCombiningClearPersist() throws Exception {
    disableCompactOnWrite();

    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.clear();
    value.add(5);
    value.add(6);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getBagUpdatesCount());

    TagBag tagBag = commitBuilder.getBagUpdates(0);
    assertEquals(COMBINING_KEY, tagBag.getTag());
    assertEquals(STATE_FAMILY, tagBag.getStateFamily());
    assertTrue(tagBag.getDeleteAll());
    assertEquals(1, tagBag.getValuesCount());
    assertEquals(
        11, CoderUtils.decodeFromByteArray(accumCoder, tagBag.getValues(0).toByteArray())[0]);

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testNewCombiningNoFetch() throws Exception {
    GroupingState<Integer, Integer> value = underTestNewKey.state(NAMESPACE, COMBINING_ADDR);

    assertThat(value.isEmpty().read(), Matchers.is(true));
    assertThat(value.read(), Matchers.is(Sum.ofIntegers().identity()));
    assertThat(value.isEmpty().read(), Matchers.is(false));

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testWatermarkAddBeforeReadEarliest() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);
    WatermarkHoldState bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);

    bag.readLater();

    bag.add(new Instant(3000));
    waitAndSet(future, new Instant(2000), 200);
    assertThat(bag.read(), Matchers.equalTo(new Instant(2000)));

    Mockito.verify(mockReader, times(2)).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);

    // Adding another value doesn't create another future, but does update the result.
    bag.add(new Instant(1000));
    assertThat(bag.read(), Matchers.equalTo(new Instant(1000)));
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkAddBeforeReadLatest() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.LATEST);
    WatermarkHoldState bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);

    // Suggesting we will read it later should get a future from the underlying WindmillStateReader
    bag.readLater();

    // Actually reading it will request another future, and get the same one, from
    // WindmillStateReader
    bag.add(new Instant(3000));
    waitAndSet(future, new Instant(2000), 200);
    assertThat(bag.read(), Matchers.equalTo(new Instant(3000)));

    Mockito.verify(mockReader, times(2)).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);

    // Adding another value doesn't create another future, but does update the result.
    bag.add(new Instant(3000));
    assertThat(bag.read(), Matchers.equalTo(new Instant(3000)));
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkAddBeforeReadEndOfWindow() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.END_OF_WINDOW);
    WatermarkHoldState bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);

    // Requests a future once
    bag.readLater();

    bag.add(new Instant(3000));
    waitAndSet(future, new Instant(3000), 200);
    // read() requests a future again, receiving the same one
    assertThat(bag.read(), Matchers.equalTo(new Instant(3000)));

    Mockito.verify(mockReader, times(2)).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);

    // Adding another value doesn't create another future, but does update the result.
    bag.add(new Instant(3000));
    assertThat(bag.read(), Matchers.equalTo(new Instant(3000)));
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkClearBeforeRead() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);

    WatermarkHoldState bag = underTest.state(NAMESPACE, addr);

    bag.clear();
    assertThat(bag.read(), Matchers.nullValue());

    bag.add(new Instant(300));
    assertThat(bag.read(), Matchers.equalTo(new Instant(300)));

    // Shouldn't need to read from windmill because the value is already available.
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersistEarliest() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);
    WatermarkHoldState bag = underTest.state(NAMESPACE, addr);

    bag.add(new Instant(1000));
    bag.add(new Instant(2000));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getWatermarkHoldsCount());

    Windmill.WatermarkHold watermarkHold = commitBuilder.getWatermarkHolds(0);
    assertEquals(key(NAMESPACE, "watermark"), watermarkHold.getTag());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(1000), watermarkHold.getTimestamps(0));

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersistLatestEmpty() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.LATEST);
    WatermarkHoldState hold = underTest.state(NAMESPACE, addr);

    hold.add(new Instant(1000));
    hold.add(new Instant(2000));

    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY))
        .thenReturn(Futures.<Instant>immediateFuture(null));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getWatermarkHoldsCount());

    Windmill.WatermarkHold watermarkHold = commitBuilder.getWatermarkHolds(0);
    assertEquals(key(NAMESPACE, "watermark"), watermarkHold.getTag());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(2000), watermarkHold.getTimestamps(0));

    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersistLatestWindmillWins() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.LATEST);
    WatermarkHoldState hold = underTest.state(NAMESPACE, addr);

    hold.add(new Instant(1000));
    hold.add(new Instant(2000));

    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY))
        .thenReturn(Futures.<Instant>immediateFuture(new Instant(4000)));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getWatermarkHoldsCount());

    Windmill.WatermarkHold watermarkHold = commitBuilder.getWatermarkHolds(0);
    assertEquals(key(NAMESPACE, "watermark"), watermarkHold.getTag());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(4000), watermarkHold.getTimestamps(0));

    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersistLatestLocalAdditionsWin() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.LATEST);
    WatermarkHoldState hold = underTest.state(NAMESPACE, addr);

    hold.add(new Instant(1000));
    hold.add(new Instant(2000));

    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY))
        .thenReturn(Futures.<Instant>immediateFuture(new Instant(500)));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getWatermarkHoldsCount());

    Windmill.WatermarkHold watermarkHold = commitBuilder.getWatermarkHolds(0);
    assertEquals(key(NAMESPACE, "watermark"), watermarkHold.getTag());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(2000), watermarkHold.getTimestamps(0));

    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersistEndOfWindow() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.END_OF_WINDOW);
    WatermarkHoldState hold = underTest.state(NAMESPACE, addr);

    hold.add(new Instant(2000));
    hold.add(new Instant(2000));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getWatermarkHoldsCount());

    Windmill.WatermarkHold watermarkHold = commitBuilder.getWatermarkHolds(0);
    assertEquals(key(NAMESPACE, "watermark"), watermarkHold.getTag());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(2000), watermarkHold.getTimestamps(0));

    // Blind adds should not need to read the future.
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkClearPersist() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);
    WatermarkHoldState hold = underTest.state(NAMESPACE, addr);

    hold.add(new Instant(500));
    hold.clear();
    hold.add(new Instant(1000));
    hold.add(new Instant(2000));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getWatermarkHoldsCount());

    Windmill.WatermarkHold clearAndUpdate = commitBuilder.getWatermarkHolds(0);
    assertEquals(key(NAMESPACE, "watermark"), clearAndUpdate.getTag());
    assertEquals(1, clearAndUpdate.getTimestampsCount());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(1000), clearAndUpdate.getTimestamps(0));

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersistEmpty() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);
    WatermarkHoldState bag = underTest.state(NAMESPACE, addr);

    bag.add(new Instant(500));
    bag.clear();

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    // 1 bag update corresponds to deletion. There shouldn't be a bag update adding items.
    assertEquals(1, commitBuilder.getWatermarkHoldsCount());
  }

  @Test
  public void testNewWatermarkNoFetch() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);

    WatermarkHoldState bag = underTestNewKey.state(NAMESPACE, addr);
    assertThat(bag.read(), Matchers.nullValue());

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testValueSetBeforeRead() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    value.write("Hello");

    assertEquals("Hello", value.read());
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testValueClearBeforeRead() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    value.clear();

    assertEquals(null, value.read());
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testValueRead() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    SettableFuture<String> future = SettableFuture.create();
    when(mockReader.valueFuture(key(NAMESPACE, "value"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);
    waitAndSet(future, "World", 200);

    assertEquals("World", value.read());
  }

  @Test
  public void testValueSetPersist() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    value.write("Hi");

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getValueUpdatesCount());
    TagValue valueUpdate = commitBuilder.getValueUpdates(0);
    assertEquals(key(NAMESPACE, "value"), valueUpdate.getTag());
    assertEquals("Hi", valueUpdate.getValue().getData().toStringUtf8());
    assertTrue(valueUpdate.isInitialized());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testValueClearPersist() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    value.write("Hi");
    value.clear();

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getValueUpdatesCount());
    TagValue valueUpdate = commitBuilder.getValueUpdates(0);
    assertEquals(key(NAMESPACE, "value"), valueUpdate.getTag());
    assertEquals(0, valueUpdate.getValue().getData().size());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testValueNoChangePersist() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    underTest.state(NAMESPACE, addr);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(0, commitBuilder.getValueUpdatesCount());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testNewValueNoFetch() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTestNewKey.state(NAMESPACE, addr);

    assertEquals(null, value.read());

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testCachedValue() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    assertEquals(0, cache.getWeight());

    value.write("Hi");
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(126, cache.getWeight());

    resetUnderTest();
    value = underTest.state(NAMESPACE, addr);
    assertEquals("Hi", value.read());
    value.clear();
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(124, cache.getWeight());

    resetUnderTest();
    value = underTest.state(NAMESPACE, addr);
    assertEquals(null, value.read());
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testCachedBag() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    assertEquals(0, cache.getWeight());

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);

    bag.readLater();

    assertEquals(0, cache.getWeight());

    bag.add("hello");
    waitAndSet(future, weightedList("world"), 200);
    Iterable<String> readResult1 = bag.read();
    assertThat(readResult1, Matchers.containsInAnyOrder("hello", "world"));

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(134, cache.getWeight());

    resetUnderTest();
    bag = underTest.state(NAMESPACE, addr);
    bag.add("goodbye");

    // Make sure that cached iterables have not changed after persist+add.
    assertThat(readResult1, Matchers.containsInAnyOrder("hello", "world"));

    Iterable<String> readResult2 = bag.read();
    assertThat(readResult2, Matchers.containsInAnyOrder("hello", "world", "goodbye"));
    bag.clear();
    // Make sure that cached iterables have not changed after clear.
    assertThat(readResult2, Matchers.containsInAnyOrder("hello", "world", "goodbye"));
    bag.add("new");
    // Make sure that cached iterables have not changed after clear+add.
    assertThat(readResult2, Matchers.containsInAnyOrder("hello", "world", "goodbye"));

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(127, cache.getWeight());

    resetUnderTest();
    bag = underTest.state(NAMESPACE, addr);
    bag.add("new2");
    assertThat(bag.read(), Matchers.containsInAnyOrder("new", "new2"));
    bag.clear();
    bag.add("new3");

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(128, cache.getWeight());

    resetUnderTest();
    bag = underTest.state(NAMESPACE, addr);
    assertThat(bag.read(), Matchers.containsInAnyOrder("new3"));
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    Mockito.verify(mockReader, times(2))
        .bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of());
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testCachedWatermarkHold() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);
    WatermarkHoldState hold = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);

    assertEquals(0, cache.getWeight());

    hold.readLater();

    hold.add(new Instant(3000));
    waitAndSet(future, new Instant(2000), 200);
    assertThat(hold.read(), Matchers.equalTo(new Instant(2000)));

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(132, cache.getWeight());

    resetUnderTest();
    hold = underTest.state(NAMESPACE, addr);
    assertThat(hold.read(), Matchers.equalTo(new Instant(2000)));
    hold.clear();

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(132, cache.getWeight());

    resetUnderTest();
    hold = underTest.state(NAMESPACE, addr);
    assertEquals(null, hold.read());
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    Mockito.verify(mockReader, times(2)).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  @SuppressWarnings("ArraysAsListPrimitiveArray")
  public void testCachedCombining() throws Exception {
    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    SettableFuture<Iterable<int[]>> future = SettableFuture.create();
    when(mockReader.bagFuture(
            eq(key(NAMESPACE, "combining")), eq(STATE_FAMILY), Mockito.<Coder<int[]>>any()))
        .thenReturn(future);

    assertEquals(0, cache.getWeight());

    value.readLater();

    value.add(1);
    waitAndSet(future, Arrays.asList(new int[] {2}), 200);
    assertThat(value.read(), Matchers.equalTo(3));

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(125, cache.getWeight());

    resetUnderTest();
    value = underTest.state(NAMESPACE, COMBINING_ADDR);
    assertThat(value.read(), Matchers.equalTo(3));
    value.add(3);
    assertThat(value.read(), Matchers.equalTo(6));
    value.clear();

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(124, cache.getWeight());

    resetUnderTest();
    value = underTest.state(NAMESPACE, COMBINING_ADDR);
    assertThat(value.read(), Matchers.equalTo(0));
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    // Note that we do expect the third argument - the coder - to be equal to accumCoder, but that
    // is possibly overspecified and currently trips an issue in the SDK where identical coders are
    // not #equals().
    //
    // What matters is the number of futures created, hence Windmill RPCs.
    Mockito.verify(mockReader, times(2))
        .bagFuture(eq(key(NAMESPACE, "combining")), eq(STATE_FAMILY), Mockito.<Coder<int[]>>any());
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  private void disableCompactOnWrite() {
    WindmillStateInternals.COMPACT_NOW.set(() -> false);
  }

  private void forceCompactOnWrite() {
    WindmillStateInternals.COMPACT_NOW.set(() -> true);
  }
}
