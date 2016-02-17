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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CopyOnAccessInMemoryStateInternals}.
 */
@RunWith(JUnit4.class)
public class CopyOnAccessInMemoryStateInternalsTest {
  private String key = "foo";
  @Test
  public void testGetWithEmpty() {
    CopyOnAccessInMemoryStateInternals<String> internals =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, null);
    StateNamespace namespace = new StateNamespaceForTest("foo");
    StateTag<Object, BagState<String>> bagTag = StateTags.bag("foo", StringUtf8Coder.of());
    BagState<String> stringBag = internals.state(namespace, bagTag);
    assertThat(stringBag.get().read(), emptyIterable());

    stringBag.add("bar");
    stringBag.add("baz");
    assertThat(stringBag.get().read(), containsInAnyOrder("baz", "bar"));

    BagState<String> reReadStringBag = internals.state(namespace, bagTag);
    assertThat(reReadStringBag.get().read(), containsInAnyOrder("baz", "bar"));
  }

  @Test
  public void testGetWithAbsentInUnderlying() {
    CopyOnAccessInMemoryStateInternals<String> underlying =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, null);
    CopyOnAccessInMemoryStateInternals<String> internals =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, underlying);

    StateNamespace namespace = new StateNamespaceForTest("foo");
    StateTag<Object, BagState<String>> bagTag = StateTags.bag("foo", StringUtf8Coder.of());
    BagState<String> stringBag = internals.state(namespace, bagTag);
    assertThat(stringBag.get().read(), emptyIterable());

    stringBag.add("bar");
    stringBag.add("baz");
    assertThat(stringBag.get().read(), containsInAnyOrder("baz", "bar"));

    BagState<String> reReadVoidBag = internals.state(namespace, bagTag);
    assertThat(reReadVoidBag.get().read(), containsInAnyOrder("baz", "bar"));

    BagState<String> underlyingState = underlying.state(namespace, bagTag);
    assertThat(underlyingState.get().read(), emptyIterable());
  }

  /**
   * Tests that retrieving state with an underlying StateInternals with an existing value returns
   * a value that initially has equal value to the provided state but can be modified without
   * modifying the existing state.
   */
  @Test
  public void testGetWithPresentInUnderlying() {
    CopyOnAccessInMemoryStateInternals<String> underlying =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, null);

    StateNamespace namespace = new StateNamespaceForTest("foo");
    StateTag<Object, ValueState<String>> valueTag = StateTags.value("foo", StringUtf8Coder.of());
    ValueState<String> underlyingValue = underlying.state(namespace, valueTag);
    assertThat(underlyingValue.get().read(), nullValue(String.class));

    underlyingValue.set("bar");
    assertThat(underlyingValue.get().read(), equalTo("bar"));

    CopyOnAccessInMemoryStateInternals<String> internals =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, underlying);
    ValueState<String> copyOnAccessState = internals.state(namespace, valueTag);
    assertThat(copyOnAccessState.get().read(), equalTo("bar"));

    copyOnAccessState.set("baz");
    assertThat(copyOnAccessState.get().read(), equalTo("baz"));
    assertThat(underlyingValue.get().read(), equalTo("bar"));

    ValueState<String> reReadUnderlyingValue = underlying.state(namespace, valueTag);
    assertThat(underlyingValue.get().read(), equalTo(reReadUnderlyingValue.get().read()));
  }

  @Test
  public void testBagStateWithUnderlying() {
    CopyOnAccessInMemoryStateInternals<String> underlying =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, null);

    StateNamespace namespace = new StateNamespaceForTest("foo");
    StateTag<Object, BagState<Integer>> valueTag = StateTags.bag("foo", VarIntCoder.of());
    BagState<Integer> underlyingValue = underlying.state(namespace, valueTag);
    assertThat(underlyingValue.get().read(), emptyIterable());

    underlyingValue.add(1);
    assertThat(underlyingValue.get().read(), containsInAnyOrder(1));

    CopyOnAccessInMemoryStateInternals<String> internals =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, underlying);
    BagState<Integer> copyOnAccessState = internals.state(namespace, valueTag);
    assertThat(copyOnAccessState.get().read(), containsInAnyOrder(1));

    copyOnAccessState.add(4);
    assertThat(copyOnAccessState.get().read(), containsInAnyOrder(4, 1));
    assertThat(underlyingValue.get().read(), containsInAnyOrder(1));

    BagState<Integer> reReadUnderlyingValue = underlying.state(namespace, valueTag);
    assertThat(underlyingValue.get().read(), equalTo(reReadUnderlyingValue.get().read()));
  }

  @Test
  public void testCombiningValueStateInternalWithUnderlying() throws CannotProvideCoderException {
    CopyOnAccessInMemoryStateInternals<String> underlying =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, null);
    CombineFn<Long, long[], Long> sumLongFn = new Sum.SumLongFn();

    StateNamespace namespace = new StateNamespaceForTest("foo");
    CoderRegistry reg = TestPipeline.create().getCoderRegistry();
    StateTag<Object, CombiningValueStateInternal<Long, long[], Long>> stateTag =
        StateTags.combiningValue("summer",
            sumLongFn.getAccumulatorCoder(reg, reg.getDefaultCoder(Long.class)), sumLongFn);
    CombiningValueState<Long, Long> underlyingValue = underlying.state(namespace, stateTag);
    assertThat(underlyingValue.get().read(), equalTo(0L));

    underlyingValue.add(1L);
    assertThat(underlyingValue.get().read(), equalTo(1L));

    CopyOnAccessInMemoryStateInternals<String> internals =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, underlying);
    CombiningValueState<Long, Long> copyOnAccessState = internals.state(namespace, stateTag);
    assertThat(copyOnAccessState.get().read(), equalTo(1L));

    copyOnAccessState.add(4L);
    assertThat(copyOnAccessState.get().read(), equalTo(5L));
    assertThat(underlyingValue.get().read(), equalTo(1L));

    CombiningValueState<Long, Long> reReadUnderlyingValue = underlying.state(namespace, stateTag);
    assertThat(underlyingValue.get().read(), equalTo(reReadUnderlyingValue.get().read()));
  }

  @Test
  public void testKeyedCombiningValueStateInternalWithUnderlying() throws Exception {
    CopyOnAccessInMemoryStateInternals<String> underlying =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, null);
    KeyedCombineFn<String, Long, long[], Long> sumLongFn = new Sum.SumLongFn().asKeyedFn();

    StateNamespace namespace = new StateNamespaceForTest("foo");
    CoderRegistry reg = TestPipeline.create().getCoderRegistry();
    StateTag<String, CombiningValueStateInternal<Long, long[], Long>> stateTag =
        StateTags.keyedCombiningValue(
            "summer",
            sumLongFn.getAccumulatorCoder(
                reg, StringUtf8Coder.of(), reg.getDefaultCoder(Long.class)),
            sumLongFn);
    CombiningValueState<Long, Long> underlyingValue = underlying.state(namespace, stateTag);
    assertThat(underlyingValue.get().read(), equalTo(0L));

    underlyingValue.add(1L);
    assertThat(underlyingValue.get().read(), equalTo(1L));

    CopyOnAccessInMemoryStateInternals<String> internals =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, underlying);
    CombiningValueState<Long, Long> copyOnAccessState = internals.state(namespace, stateTag);
    assertThat(copyOnAccessState.get().read(), equalTo(1L));

    copyOnAccessState.add(4L);
    assertThat(copyOnAccessState.get().read(), equalTo(5L));
    assertThat(underlyingValue.get().read(), equalTo(1L));

    CombiningValueState<Long, Long> reReadUnderlyingValue = underlying.state(namespace, stateTag);
    assertThat(underlyingValue.get().read(), equalTo(reReadUnderlyingValue.get().read()));
  }

  @Test
  public void testWatermarkStateInternalWithUnderlying() {
    CopyOnAccessInMemoryStateInternals<String> underlying =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, null);

    @SuppressWarnings("unchecked")
    OutputTimeFn<BoundedWindow> outputTimeFn = (OutputTimeFn<BoundedWindow>)
        TestPipeline.create().apply(Create.of("foo")).getWindowingStrategy().getOutputTimeFn();

    StateNamespace namespace = new StateNamespaceForTest("foo");
    StateTag<Object, WatermarkStateInternal<BoundedWindow>> stateTag =
        StateTags.watermarkStateInternal("wmstate", outputTimeFn);
    WatermarkStateInternal<?> underlyingValue = underlying.state(namespace, stateTag);
    assertThat(underlyingValue.get().read(), nullValue());

    underlyingValue.add(new Instant(250L));
    assertThat(underlyingValue.get().read(), equalTo(new Instant(250L)));

    CopyOnAccessInMemoryStateInternals<String> internals =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, underlying);
    WatermarkStateInternal<BoundedWindow> copyOnAccessState = internals.state(namespace, stateTag);
    assertThat(copyOnAccessState.get().read(), equalTo(new Instant(250L)));

    copyOnAccessState.add(new Instant(100L));
    assertThat(copyOnAccessState.get().read(), equalTo(new Instant(100L)));
    assertThat(underlyingValue.get().read(), equalTo(new Instant(250L)));

    copyOnAccessState.add(new Instant(500L));
    assertThat(copyOnAccessState.get().read(), equalTo(new Instant(100L)));

    WatermarkStateInternal<BoundedWindow> reReadUnderlyingValue =
        underlying.state(namespace, stateTag);
    assertThat(underlyingValue.get().read(), equalTo(reReadUnderlyingValue.get().read()));
  }

  @Test
  public void testCommitWithoutUnderlying() {
    CopyOnAccessInMemoryStateInternals<String> internals =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, null);
    StateNamespace namespace = new StateNamespaceForTest("foo");
    StateTag<Object, BagState<String>> bagTag = StateTags.bag("foo", StringUtf8Coder.of());
    BagState<String> stringBag = internals.state(namespace, bagTag);
    assertThat(stringBag.get().read(), emptyIterable());

    stringBag.add("bar");
    stringBag.add("baz");
    assertThat(stringBag.get().read(), containsInAnyOrder("baz", "bar"));

    internals.commit();

    BagState<String> reReadStringBag = internals.state(namespace, bagTag);
    assertThat(reReadStringBag.get().read(), containsInAnyOrder("baz", "bar"));
  }

  @Test
  public void testCommitWithUnderlying() {
    CopyOnAccessInMemoryStateInternals<String> underlying =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, null);
    CopyOnAccessInMemoryStateInternals<String> internals =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, underlying);

    StateNamespace namespace = new StateNamespaceForTest("foo");
    StateTag<Object, BagState<String>> bagTag = StateTags.bag("foo", StringUtf8Coder.of());
    BagState<String> stringBag = underlying.state(namespace, bagTag);
    assertThat(stringBag.get().read(), emptyIterable());

    stringBag.add("bar");
    stringBag.add("baz");

    internals.commit();
    BagState<String> reReadStringBag = internals.state(namespace, bagTag);
    assertThat(reReadStringBag.get().read(), containsInAnyOrder("baz", "bar"));

    reReadStringBag.add("spam");

    BagState<String> underlyingState = underlying.state(namespace, bagTag);
    assertThat(underlyingState.get().read(), containsInAnyOrder("spam", "bar", "baz"));
    assertThat(underlyingState, is(theInstance(stringBag)));
  }

  @Test
  public void testCommitWithOverwrittenUnderlying() {
    CopyOnAccessInMemoryStateInternals<String> underlying =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, null);
    CopyOnAccessInMemoryStateInternals<String> internals =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, underlying);

    StateNamespace namespace = new StateNamespaceForTest("foo");
    StateTag<Object, BagState<String>> bagTag = StateTags.bag("foo", StringUtf8Coder.of());
    BagState<String> stringBag = underlying.state(namespace, bagTag);
    assertThat(stringBag.get().read(), emptyIterable());

    stringBag.add("bar");
    stringBag.add("baz");

    BagState<String> internalsState = internals.state(namespace, bagTag);
    internalsState.add("eggs");
    internalsState.add("ham");
    internalsState.add("0x00ff00");
    internalsState.add("&");

    internals.commit();

    BagState<String> reReadInternalState = internals.state(namespace, bagTag);
    assertThat(
        reReadInternalState.get().read(),
        containsInAnyOrder("bar", "baz", "0x00ff00", "eggs", "&", "ham"));
    BagState<String> reReadUnderlyingState = underlying.state(namespace, bagTag);
    assertThat(reReadUnderlyingState.get().read(), containsInAnyOrder("bar", "baz"));
  }

  @Test
  public void testCommitWithAddedUnderlying() {
    CopyOnAccessInMemoryStateInternals<String> underlying =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, null);
    CopyOnAccessInMemoryStateInternals<String> internals =
        CopyOnAccessInMemoryStateInternals.withUnderlying(key, underlying);

    internals.commit();

    StateNamespace namespace = new StateNamespaceForTest("foo");
    StateTag<Object, BagState<String>> bagTag = StateTags.bag("foo", StringUtf8Coder.of());
    BagState<String> stringBag = underlying.state(namespace, bagTag);
    assertThat(stringBag.get().read(), emptyIterable());

    stringBag.add("bar");
    stringBag.add("baz");

    BagState<String> internalState = internals.state(namespace, bagTag);
    assertThat(internalState.get().read(), emptyIterable());

    BagState<String> reReadUnderlyingState = underlying.state(namespace, bagTag);
    assertThat(reReadUnderlyingState.get().read(), containsInAnyOrder("bar", "baz"));
  }
}
