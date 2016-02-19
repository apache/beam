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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessEvaluationContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.util.InProcessBundle;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey.GroupByKeyOnly;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link GroupByKeyEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class GroupByKeyEvaluatorFactoryTest {
  @Test
  public void testInMemoryEvaluator() throws Exception {
    TestPipeline p = TestPipeline.create();
    KV<String, Integer> firstFoo = KV.of("foo", -1);
    KV<String, Integer> secondFoo = KV.of("foo", 1);
    KV<String, Integer> thirdFoo = KV.of("foo", 3);
    KV<String, Integer> firstBar = KV.of("bar", 22);
    KV<String, Integer> secondBar = KV.of("bar", 12);
    KV<String, Integer> firstBaz = KV.of("baz", Integer.MAX_VALUE);
    PCollection<KV<String, Integer>> kvs =
        p.apply(Create.of(firstFoo, firstBar, secondFoo, firstBaz, secondBar, thirdFoo));
    PCollection<KV<String, Iterable<Integer>>> groupedKvs =
        kvs.apply(new GroupByKeyOnly<String, Integer>());

    CommittedBundle<KV<String, Integer>> inputBundle =
        InProcessBundle.unkeyed(kvs).commit(BoundedWindow.TIMESTAMP_MAX_VALUE);
    InProcessEvaluationContext evaluationContext = mock(InProcessEvaluationContext.class);

    UncommittedBundle<KV<String, Iterable<Integer>>> fooBundle =
        InProcessBundle.keyed(groupedKvs, "foo");
    UncommittedBundle<KV<String, Iterable<Integer>>> barBundle =
        InProcessBundle.keyed(groupedKvs, "bar");
    UncommittedBundle<KV<String, Iterable<Integer>>> bazBundle =
        InProcessBundle.keyed(groupedKvs, "baz");

    when(evaluationContext.createKeyedBundle(inputBundle, "foo", groupedKvs)).thenReturn(fooBundle);
    when(evaluationContext.createKeyedBundle(inputBundle, "bar", groupedKvs)).thenReturn(barBundle);
    when(evaluationContext.createKeyedBundle(inputBundle, "baz", groupedKvs)).thenReturn(bazBundle);

    // The input to a GroupByKey is assumed to be a KvCoder
    @SuppressWarnings("unchecked")
    Coder<String> keyCoder = ((KvCoder<String, Integer>) kvs.getCoder()).getKeyCoder();
    TransformEvaluator<KV<String, Integer>> evaluator =
        new GroupByKeyEvaluatorFactory().forApplication(
            groupedKvs.getProducingTransformInternal(), inputBundle, evaluationContext);

    evaluator.processElement(WindowedValue.valueInGlobalWindow(firstFoo));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(secondFoo));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(thirdFoo));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(firstBar));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(secondBar));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(firstBaz));

    evaluator.finishBundle();

    assertThat(
        fooBundle.commit(Instant.now()).getElements(),
        contains(new KIterVMatcher<String, Integer>(
            KV.<String, Iterable<Integer>>of("foo", ImmutableSet.of(-1, 1, 3)), keyCoder)));
    assertThat(
        barBundle.commit(Instant.now()).getElements(),
        contains(new KIterVMatcher<String, Integer>(
            KV.<String, Iterable<Integer>>of("bar", ImmutableSet.of(12, 22)), keyCoder)));
    assertThat(
        bazBundle.commit(Instant.now()).getElements(),
        contains(new KIterVMatcher<String, Integer>(
            KV.<String, Iterable<Integer>>of("baz", ImmutableSet.of(Integer.MAX_VALUE)),
            keyCoder)));
  }

  private static class KIterVMatcher<K, V> extends BaseMatcher<WindowedValue<KV<K, Iterable<V>>>> {
    private final KV<K, Iterable<V>> myKv;
    private final Coder<K> keyCoder;

    public KIterVMatcher(KV<K, Iterable<V>> myKv, Coder<K> keyCoder) {
      this.myKv = myKv;
      this.keyCoder = keyCoder;
    }

    @Override
    public boolean matches(Object item) {
      if (item == null || !(item instanceof WindowedValue)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      WindowedValue<KV<K, Iterable<V>>> that = (WindowedValue<KV<K, Iterable<V>>>) item;
      Multiset<V> myValues = HashMultiset.create();
      Multiset<V> thatValues = HashMultiset.create();
      for (V value : myKv.getValue()) {
        myValues.add(value);
      }
      for (V value : that.getValue().getValue()) {
        thatValues.add(value);
      }
      try {
        return myValues.equals(thatValues)
            && keyCoder.structuralValue(myKv.getKey())
                   .equals(keyCoder.structuralValue(that.getValue().getKey()));
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("KV<K, Iterable<V>> containing key ")
          .appendValue(myKv.getKey())
          .appendText(" and values ")
          .appendValueList("[", ", ", "]", myKv.getValue());
    }
  }
}

