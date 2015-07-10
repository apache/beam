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
package com.google.cloud.dataflow.sdk.util.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.TagList;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.TagValue;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link WindmillStateInternals}.
 */
@RunWith(JUnit4.class)
public class WindmillStateInternalsTest {

  private static final StateNamespace NAMESPACE = new StateNamespaceForTest("ns");
  private static final String MANGLED_PREFIX = "mangled";

  private static final StateTag<CombiningValueState<Integer, Integer>> COMBINING_ADDR =
      StateTags.combiningValue("combining", VarIntCoder.of(), new Sum.SumIntegerFn());
  private final Coder<int[]> accumCoder =
      new Sum.SumIntegerFn().getAccumulatorCoder(null, VarIntCoder.of());

  @Mock
  private WindmillStateReader mockReader;

  private WindmillStateInternals underTest;

  private ByteString key(StateNamespace namespace, String addrId) {
    return ByteString.copyFromUtf8(MANGLED_PREFIX + "/" + namespace.stringKey() + "/" + addrId);
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    underTest = new WindmillStateInternals(MANGLED_PREFIX, mockReader);
  }

  private <T> void waitAndSet(
      final SettableFuture<T> future, final T value, final long millis) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(millis);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted before setting", e);
        }
        future.set(value);
      }
    }).run();
  }

  @Test
  public void testBagAddBeforeRead() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.listFuture(key(NAMESPACE, "bag"), StringUtf8Coder.of())).thenReturn(future);

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

    // Blind adds should not need to read the future.
    Mockito.verify(mockReader).startBatchAndBlock();
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

    // Clear should need to read the future.
    Mockito.verify(mockReader).listFuture(key(NAMESPACE, "bag"), StringUtf8Coder.of());
    Mockito.verify(mockReader).startBatchAndBlock();
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testCombiningAddBeforeRead() throws Exception {
    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    SettableFuture<Iterable<int[]>> future = SettableFuture.create();
    when(mockReader.listFuture(key(NAMESPACE, COMBINING_ADDR.getId()), accumCoder))
        .thenReturn(future);

    StateContents<Integer> result = value.get();

    value.add(5);
    value.add(6);
    waitAndSet(future, Arrays.asList(new int[]{8}, new int[]{10}), 200);
    assertThat(result.read(), Matchers.equalTo(29));

    // That get "compressed" the combiner. So, the underlying future should change:
    future.set(Arrays.asList(new int[]{29}));

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
  public void testCombiningAddPersist() throws Exception {
    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.add(5);
    value.add(6);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getListUpdatesCount());

    TagList listUpdates = commitBuilder.getListUpdates(0);
    assertEquals(key(NAMESPACE, COMBINING_ADDR.getId()), listUpdates.getTag());
    assertEquals(1, listUpdates.getValuesCount());
    assertEquals(11,
        CoderUtils.decodeFromByteArray(accumCoder,
            listUpdates.getValues(0).getData().substring(1).toByteArray())[0]);

    // Blind adds should not need to read the future.
    Mockito.verify(mockReader).startBatchAndBlock();
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testCombiningClearPersist() throws Exception {
    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.clear();
    value.add(5);
    value.add(6);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(2, commitBuilder.getListUpdatesCount());

    TagList listClear = commitBuilder.getListUpdates(0);
    assertEquals(key(NAMESPACE, COMBINING_ADDR.getId()), listClear.getTag());
    assertEquals(Long.MAX_VALUE, listClear.getEndTimestamp());
    assertEquals(0, listClear.getValuesCount());

    TagList listUpdates = commitBuilder.getListUpdates(1);
    assertEquals(key(NAMESPACE, COMBINING_ADDR.getId()), listUpdates.getTag());
    assertEquals(1, listUpdates.getValuesCount());
    assertEquals(11,
        CoderUtils.decodeFromByteArray(accumCoder,
            listUpdates.getValues(0).getData().substring(1).toByteArray())[0]);

    // Blind adds should not need to read the future.
    Mockito.verify(mockReader).listFuture(key(NAMESPACE, COMBINING_ADDR.getId()), accumCoder);
    Mockito.verify(mockReader).startBatchAndBlock();
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkAddBeforeRead() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal("watermark");
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"))).thenReturn(future);

    StateContents<Instant> result = bag.get();

    bag.add(new Instant(3000));
    waitAndSet(future, new Instant(2000), 200);
    assertThat(result.read(), Matchers.equalTo(new Instant(2000)));

    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"));
    Mockito.verifyNoMoreInteractions(mockReader);

    // Adding another value doesn't create another future, but does update the result.
    bag.add(new Instant(1000));
    assertThat(result.read(), Matchers.equalTo(new Instant(1000)));
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkClearBeforeRead() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal("watermark");
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    bag.clear();
    assertThat(bag.get().read(), Matchers.nullValue());

    bag.add(new Instant(300));
    assertThat(bag.get().read(), Matchers.equalTo(new Instant(300)));

    // Shouldn't need to read from windmill because the value is already available.
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersist() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal("watermark");
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    bag.add(new Instant(1000));
    bag.add(new Instant(2000));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getListUpdatesCount());

    TagList listUpdates = commitBuilder.getListUpdates(0);
    assertEquals(key(NAMESPACE, "watermark"), listUpdates.getTag());
    assertEquals(1, listUpdates.getValuesCount());
    // Just the zero-byte.
    assertEquals(1, listUpdates.getValues(0).getData().size());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(1000), listUpdates.getValues(0).getTimestamp());

    // Blind adds should not need to read the future.
    Mockito.verify(mockReader).startBatchAndBlock();
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkClearPersist() throws Exception {
    StateTag<WatermarkStateInternal> addr = StateTags.watermarkStateInternal("watermark");
    WatermarkStateInternal bag = underTest.state(NAMESPACE, addr);

    bag.add(new Instant(500));
    bag.clear();
    bag.add(new Instant(1000));
    bag.add(new Instant(2000));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(2, commitBuilder.getListUpdatesCount());

    TagList listClear = commitBuilder.getListUpdates(0);
    assertEquals(key(NAMESPACE, "watermark"), listClear.getTag());
    assertEquals(Long.MAX_VALUE, listClear.getEndTimestamp());
    assertEquals(0, listClear.getValuesCount());

    TagList listUpdates = commitBuilder.getListUpdates(1);
    assertEquals(key(NAMESPACE, "watermark"), listUpdates.getTag());
    assertEquals(1, listUpdates.getValuesCount());
    // Just the zero-byte.
    assertEquals(1, listUpdates.getValues(0).getData().size());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(1000), listUpdates.getValues(0).getTimestamp());

    // Clearing requires reading the future.
    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"));
    Mockito.verify(mockReader).startBatchAndBlock();
    Mockito.verifyNoMoreInteractions(mockReader);
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
    when(mockReader.valueFuture(key(NAMESPACE, "value"), StringUtf8Coder.of())).thenReturn(future);
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

    // Setting a value requires a read to prevent blind writes.
    Mockito.verify(mockReader).valueFuture(key(NAMESPACE, "value"), StringUtf8Coder.of());
    Mockito.verify(mockReader).startBatchAndBlock();
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

    // Setting a value requires a read to prevent blind writes.
    Mockito.verify(mockReader).valueFuture(key(NAMESPACE, "value"), StringUtf8Coder.of());
    Mockito.verify(mockReader).startBatchAndBlock();
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

    // No changes shouldn't require getting any futures
    Mockito.verify(mockReader).startBatchAndBlock();
    Mockito.verifyNoMoreInteractions(mockReader);
  }
}
