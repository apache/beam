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
package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.DataflowMatchers.ByteStringMatcher.byteStringEq;
import static com.google.cloud.dataflow.sdk.testing.SystemNanoTimeSleeper.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.TagList;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.TagValue;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFns;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.util.state.BagState;
import com.google.cloud.dataflow.sdk.util.state.CombiningValueState;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaceForTest;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.util.state.ValueState;
import com.google.cloud.dataflow.sdk.util.state.WatermarkStateInternal;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link WindmillStateInternals}.
 */
@RunWith(JUnit4.class)
public class WindmillStateInternalsTest {
  private static final StateNamespace NAMESPACE = new StateNamespaceForTest("ns");
  private static final String STATE_FAMILY = "family";

  private static final StateTag<CombiningValueState<Integer, Integer>> COMBINING_ADDR =
      StateTags.combiningValueFromInputInternal(
          "combining", VarIntCoder.of(), new Sum.SumIntegerFn());
  private static final ByteString COMBINING_KEY = key(NAMESPACE, "combining");
  private final Coder<int[]> accumCoder =
      new Sum.SumIntegerFn().getAccumulatorCoder(null, VarIntCoder.of());

  @Mock
  private WindmillStateReader mockReader;

  private WindmillStateInternals underTest;
  private WindmillStateCache cache;

  @Mock
  private Supplier<StateSampler.ScopedState> readStateSupplier;

  private static ByteString key(StateNamespace namespace, String addrId) {
    return key("", namespace, addrId);
  }

  private static ByteString key(String prefix, StateNamespace namespace, String addrId) {
    return ByteString.copyFromUtf8(prefix + namespace.stringKey() + "+u" + addrId);
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    cache = new WindmillStateCache();
    underTest = new WindmillStateInternals(STATE_FAMILY, mockReader,
        cache.forComputation("comp").forKey(ByteString.EMPTY, STATE_FAMILY, 17L),
        readStateSupplier);
  }

  private <T> void waitAndSet(final SettableFuture<T> future, final T value, final long millis) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          sleepMillis(millis);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted before setting", e);
        }
        future.set(value);
      }
    }).run();
  }

  private WindmillStateReader.WeightedList<String> weightedList(String... elems) {
    WindmillStateReader.WeightedList<String> result =
        new WindmillStateReader.WeightedList<String>(new ArrayList<String>(elems.length));
    for (String elem : elems) {
      result.addWeighted(elem, elem.length());
    }
    return result;
  }

  @Test
  public void testBagAddBeforeRead() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.listFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);

    StateContents<Iterable<String>> result = bag.get();

    bag.add("hello");
    waitAndSet(future, Arrays.asList("world"), 200);
    assertThat(result.read(), Matchers.containsInAnyOrder("hello", "world"));

    bag.add("goodbye");
    assertThat(result.read(), Matchers.containsInAnyOrder("hello", "world", "goodbye"));
  }

  @Test
  public void testBagClearBeforeRead() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    bag.clear();
    bag.add("hello");
    assertThat(bag.get().read(), Matchers.containsInAnyOrder("hello"));

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testBagIsEmptyFalse() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.listFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);
    StateContents<Boolean> result = bag.isEmpty();
    Mockito.verify(mockReader)
        .listFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of());

    waitAndSet(future, Arrays.asList("world"), 200);
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testBagIsEmptyTrue() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.listFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);
    StateContents<Boolean> result = bag.isEmpty();
    Mockito.verify(mockReader)
        .listFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of());

    waitAndSet(future, Arrays.<String>asList(), 200);
    assertThat(result.read(), Matchers.is(true));
  }

  @Test
  public void testBagIsEmptyAfterClear() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    bag.clear();
    StateContents<Boolean> result = bag.isEmpty();
    Mockito.verify(mockReader, never())
        .listFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of());
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

    assertEquals(1, commitBuilder.getListUpdatesCount());

    TagList listUpdates = commitBuilder.getListUpdates(0);
    assertEquals(key(NAMESPACE, "bag"), listUpdates.getTag());
    assertEquals(1, listUpdates.getValuesCount());
    assertEquals("hello", listUpdates.getValues(0).getData().substring(1).toStringUtf8());

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

    assertEquals(2, commitBuilder.getListUpdatesCount());

    TagList listClear = commitBuilder.getListUpdates(0);
    assertEquals(key(NAMESPACE, "bag"), listClear.getTag());
    assertEquals(Long.MAX_VALUE, listClear.getEndTimestamp());
    assertEquals(0, listClear.getValuesCount());

    TagList listUpdates = commitBuilder.getListUpdates(1);
    assertEquals(key(NAMESPACE, "bag"), listUpdates.getTag());
    assertEquals(1, listUpdates.getValuesCount());
    assertEquals("world", listUpdates.getValues(0).getData().substring(1).toStringUtf8());

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

    // 1 list update = the clear
    assertEquals(1, commitBuilder.getListUpdatesCount());
  }

  @Test
  public void testCombiningAddBeforeRead() throws Exception {
    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    SettableFuture<Iterable<int[]>> future = SettableFuture.create();
    when(mockReader.listFuture(COMBINING_KEY, STATE_FAMILY, accumCoder))
        .thenReturn(future);

    StateContents<Integer> result = value.get();

    value.add(5);
    value.add(6);
    waitAndSet(future, Arrays.asList(new int[] {8}, new int[] {10}), 200);
    assertThat(result.read(), Matchers.equalTo(29));

    // That get "compressed" the combiner. So, the underlying future should change:
    future.set(Arrays.asList(new int[] {29}));

    value.add(2);
    assertThat(result.read(), Matchers.equalTo(31));
  }

  @Test
  public void testCombiningClearBeforeRead() throws Exception {
    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.clear();

    StateContents<Integer> result = value.get();
    value.add(5);
    value.add(6);
    assertThat(result.read(), Matchers.equalTo(11));

    value.add(2);
    assertThat(result.read(), Matchers.equalTo(13));

    // Shouldn't need to read from windmill for this because we immediately cleared..
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testCombiningIsEmpty() throws Exception {
    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    SettableFuture<Iterable<int[]>> future = SettableFuture.create();
    when(mockReader.listFuture(COMBINING_KEY, STATE_FAMILY, accumCoder))
        .thenReturn(future);
    StateContents<Boolean> result = value.isEmpty();
    ArgumentCaptor<ByteString> byteString = ArgumentCaptor.forClass(ByteString.class);
    Mockito.verify(mockReader).listFuture(byteString.capture(), eq(STATE_FAMILY), eq(accumCoder));
    assertThat(byteString.getValue(), byteStringEq(COMBINING_KEY));

    waitAndSet(future, Arrays.asList(new int[] {29}), 200);
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testCombiningIsEmptyAfterClear() throws Exception {
    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.clear();
    StateContents<Boolean> result = value.isEmpty();
    Mockito.verify(mockReader, never())
        .listFuture(COMBINING_KEY, STATE_FAMILY, accumCoder);
    assertThat(result.read(), Matchers.is(true));

    value.add(87);
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testCombiningAddPersist() throws Exception {
    disableCompactOnWrite();

    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.add(5);
    value.add(6);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getListUpdatesCount());

    TagList listUpdates = commitBuilder.getListUpdates(0);
    assertEquals(COMBINING_KEY, listUpdates.getTag());
    assertEquals(1, listUpdates.getValuesCount());
    assertEquals(
        11,
        CoderUtils.decodeFromByteArray(
            accumCoder, listUpdates.getValues(0).getData().substring(1).toByteArray())[0]);

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testCombiningAddPersistWithCompact() throws Exception {
    forceCompactOnWrite();

    Mockito.stub(
            mockReader.listFuture(
                org.mockito.Matchers.<ByteString>any(),
                org.mockito.Matchers.<String>any(),
                org.mockito.Matchers.<Coder<int[]>>any()))
        .toReturn(
            Futures.<Iterable<int[]>>immediateFuture(
                ImmutableList.of(new int[] {40}, new int[] {60})));

    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.add(5);
    value.add(6);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(2, commitBuilder.getListUpdatesCount());
    assertEquals(0, commitBuilder.getListUpdates(0).getValuesCount());

    TagList listUpdates = commitBuilder.getListUpdates(1);
    assertEquals(COMBINING_KEY, listUpdates.getTag());
    assertEquals(1, listUpdates.getValuesCount());
    assertEquals(
        111,
        CoderUtils.decodeFromByteArray(
                accumCoder, listUpdates.getValues(0).getData().substring(1).toByteArray())[
            0]);
  }

  @Test
  public void testCombiningClearPersist() throws Exception {
    disableCompactOnWrite();

    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.clear();
    value.add(5);
    value.add(6);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(2, commitBuilder.getListUpdatesCount());

    TagList listClear = commitBuilder.getListUpdates(0);
    assertEquals(COMBINING_KEY, listClear.getTag());
    assertEquals(Long.MAX_VALUE, listClear.getEndTimestamp());
    assertEquals(0, listClear.getValuesCount());

    TagList listUpdates = commitBuilder.getListUpdates(1);
    assertEquals(COMBINING_KEY, listUpdates.getTag());
    assertEquals(1, listUpdates.getValuesCount());
    assertEquals(
        11,
        CoderUtils.decodeFromByteArray(
            accumCoder, listUpdates.getValues(0).getData().substring(1).toByteArray())[0]);

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkAddBeforeReadEarliest() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtEarliestInputTimestamp());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);

    StateContents<Instant> result = bag.get();

    bag.add(new Instant(3000));
    waitAndSet(future, new Instant(2000), 200);
    assertThat(result.read(), Matchers.equalTo(new Instant(2000)));

    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);

    // Adding another value doesn't create another future, but does update the result.
    bag.add(new Instant(1000));
    assertThat(result.read(), Matchers.equalTo(new Instant(1000)));
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkAddBeforeReadLatest() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtLatestInputTimestamp());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);

    StateContents<Instant> result = bag.get();

    bag.add(new Instant(3000));
    waitAndSet(future, new Instant(2000), 200);
    assertThat(result.read(), Matchers.equalTo(new Instant(3000)));

    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);

    // Adding another value doesn't create another future, but does update the result.
    bag.add(new Instant(3000));
    assertThat(result.read(), Matchers.equalTo(new Instant(3000)));
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkAddBeforeReadEndOfWindow() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtEndOfWindow());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);

    StateContents<Instant> result = bag.get();

    bag.add(new Instant(3000));
    waitAndSet(future, new Instant(3000), 200);
    assertThat(result.read(), Matchers.equalTo(new Instant(3000)));

    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);

    // Adding another value doesn't create another future, but does update the result.
    bag.add(new Instant(3000));
    assertThat(result.read(), Matchers.equalTo(new Instant(3000)));
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkClearBeforeRead() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtEarliestInputTimestamp());

    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    bag.clear();
    assertThat(bag.get().read(), Matchers.nullValue());

    bag.add(new Instant(300));
    assertThat(bag.get().read(), Matchers.equalTo(new Instant(300)));

    // Shouldn't need to read from windmill because the value is already available.
    Mockito.verifyNoMoreInteractions(mockReader);
  }


  /*
  @Test
  public void testWatermarkIsEmptyWindmillHasData() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtEarliestInputTimestamp());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);
    StateContents<Boolean> result = bag.isEmpty();
    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);

    waitAndSet(future, new Instant(1000), 200);
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testWatermarkIsEmpty() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtEarliestInputTimestamp());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);
    StateContents<Boolean> result = bag.isEmpty();
    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);

    waitAndSet(future, null, 200);
    assertThat(result.read(), Matchers.is(true));
  }

  @Test
  public void testWatermarkIsEmptyAfterClear() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtEarliestInputTimestamp());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    bag.clear();
    StateContents<Boolean> result = bag.isEmpty();
    Mockito.verify(mockReader, never()).watermarkFuture(key(NAMESPACE, addr.getId()), STATE_FAMILY);
    assertThat(result.read(), Matchers.is(true));

    bag.add(new Instant(1000));
    assertThat(result.read(), Matchers.is(false));
  }
  */

  @Test
  public void testWatermarkPersistEarliest() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtEarliestInputTimestamp());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

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
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtLatestInputTimestamp());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    bag.add(new Instant(1000));
    bag.add(new Instant(2000));

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
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtLatestInputTimestamp());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    bag.add(new Instant(1000));
    bag.add(new Instant(2000));

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
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtLatestInputTimestamp());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    bag.add(new Instant(1000));
    bag.add(new Instant(2000));

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
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtEndOfWindow());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    bag.add(new Instant(2000));
    bag.add(new Instant(2000));

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
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtEarliestInputTimestamp());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    bag.add(new Instant(500));
    bag.clear();
    bag.add(new Instant(1000));
    bag.add(new Instant(2000));

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
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtEarliestInputTimestamp());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    bag.add(new Instant(500));
    bag.clear();

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    // 1 list update corresponds to deletion. There shouldn't be a list update adding items.
    assertEquals(1, commitBuilder.getWatermarkHoldsCount());
  }

  @Test
  public void testValueSetBeforeRead() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    value.set("Hello");

    assertEquals("Hello", value.get().read());
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testValueClearBeforeRead() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    value.clear();

    assertEquals(null, value.get().read());
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

    assertEquals("World", value.get().read());
  }

  @Test
  public void testValueSetPersist() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    value.set("Hi");

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

    value.set("Hi");
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
  public void testCachedValue() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    assertEquals(0, cache.getWeight());

    value.set("Hi");
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(2, cache.getWeight());

    value = underTest.state(NAMESPACE, addr);
    assertEquals("Hi", value.get().read());
    value.clear();
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(0, cache.getWeight());

    value = underTest.state(NAMESPACE, addr);
    assertEquals(null, value.get().read());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testCachedBag() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    assertEquals(0, cache.getWeight());

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.listFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);

    StateContents<Iterable<String>> result = bag.get();

    assertEquals(0, cache.getWeight());

    bag.add("hello");
    waitAndSet(future, weightedList("world"), 200);
    assertThat(result.read(), Matchers.containsInAnyOrder("hello", "world"));

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(10, cache.getWeight());

    bag = underTest.state(NAMESPACE, addr);
    bag.add("goodbye");
    assertThat(bag.get().read(), Matchers.containsInAnyOrder("hello", "world", "goodbye"));
    bag.clear();
    bag.add("new");

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(3, cache.getWeight());

    bag = underTest.state(NAMESPACE, addr);
    bag.add("new2");
    assertThat(bag.get().read(), Matchers.containsInAnyOrder("new", "new2"));
    bag.clear();
    bag.add("new3");

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(4, cache.getWeight());

    bag = underTest.state(NAMESPACE, addr);
    assertThat(bag.get().read(), Matchers.containsInAnyOrder("new3"));

    Mockito.verify(mockReader)
        .listFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of());
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testCachedWatermarkHold() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal(
        "watermark", OutputTimeFns.outputAtEarliestInputTimestamp());
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);

    assertEquals(0, cache.getWeight());

    StateContents<Instant> result = bag.get();

    bag.add(new Instant(3000));
    waitAndSet(future, new Instant(2000), 200);
    assertThat(result.read(), Matchers.equalTo(new Instant(2000)));

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(8, cache.getWeight());

    bag = underTest.state(NAMESPACE, addr);
    assertThat(bag.get().read(), Matchers.equalTo(new Instant(2000)));
    bag.clear();

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(8, cache.getWeight());

    bag = underTest.state(NAMESPACE, addr);
    assertEquals(null, bag.get().read());

    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testCachedCombining() throws Exception {
    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    SettableFuture<Iterable<int[]>> future = SettableFuture.create();
    when(mockReader.listFuture(key(NAMESPACE, "combining"), STATE_FAMILY, accumCoder))
        .thenReturn(future);

    assertEquals(0, cache.getWeight());

    StateContents<Integer> result = value.get();

    value.add(1);
    waitAndSet(future, Arrays.asList(new int[]{2}), 200);
    assertThat(result.read(), Matchers.equalTo(3));

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(1, cache.getWeight());

    value = underTest.state(NAMESPACE, COMBINING_ADDR);
    assertThat(value.get().read(), Matchers.equalTo(3));
    value.add(3);
    assertThat(value.get().read(), Matchers.equalTo(6));
    value.clear();

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(0, cache.getWeight());

    value = underTest.state(NAMESPACE, COMBINING_ADDR);
    assertThat(value.get().read(), Matchers.equalTo(0));

    Mockito.verify(mockReader)
        .listFuture(key(NAMESPACE, "combining"), STATE_FAMILY, accumCoder);
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  private void disableCompactOnWrite() {
    WindmillStateInternals.COMPACT_NOW.set(
        new Supplier<Boolean>() {
          @Override
          public Boolean get() {
            return false;
          }
        });
  }

  private void forceCompactOnWrite() {
    WindmillStateInternals.COMPACT_NOW.set(
        new Supplier<Boolean>() {
          @Override
          public Boolean get() {
            return true;
          }
        });
  }
}
