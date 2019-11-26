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
package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupByKeyOnly;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultiset;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multiset;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GroupByKeyOnlyEvaluatorFactory}. */
@RunWith(JUnit4.class)
public class GroupByKeyOnlyEvaluatorFactoryTest {
  private BundleFactory bundleFactory = ImmutableListBundleFactory.create();

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testInMemoryEvaluator() throws Exception {
    KV<String, Integer> firstFoo = KV.of("foo", -1);
    KV<String, Integer> secondFoo = KV.of("foo", 1);
    KV<String, Integer> thirdFoo = KV.of("foo", 3);
    KV<String, Integer> firstBar = KV.of("bar", 22);
    KV<String, Integer> secondBar = KV.of("bar", 12);
    KV<String, Integer> firstBaz = KV.of("baz", Integer.MAX_VALUE);
    PCollection<KV<String, Integer>> values =
        p.apply(Create.of(firstFoo, firstBar, secondFoo, firstBaz, secondBar, thirdFoo));
    PCollection<KeyedWorkItem<String, Integer>> groupedKvs =
        values.apply(new DirectGroupByKeyOnly<>());

    CommittedBundle<KV<String, Integer>> inputBundle =
        bundleFactory.createBundle(values).commit(Instant.now());
    EvaluationContext evaluationContext = mock(EvaluationContext.class);

    StructuralKey<String> fooKey = StructuralKey.of("foo", StringUtf8Coder.of());
    UncommittedBundle<KeyedWorkItem<String, Integer>> fooBundle =
        bundleFactory.createKeyedBundle(fooKey, groupedKvs);
    StructuralKey<String> barKey = StructuralKey.of("bar", StringUtf8Coder.of());
    UncommittedBundle<KeyedWorkItem<String, Integer>> barBundle =
        bundleFactory.createKeyedBundle(barKey, groupedKvs);
    StructuralKey<String> bazKey = StructuralKey.of("baz", StringUtf8Coder.of());
    UncommittedBundle<KeyedWorkItem<String, Integer>> bazBundle =
        bundleFactory.createKeyedBundle(bazKey, groupedKvs);

    when(evaluationContext.createKeyedBundle(fooKey, groupedKvs)).thenReturn(fooBundle);
    when(evaluationContext.createKeyedBundle(barKey, groupedKvs)).thenReturn(barBundle);
    when(evaluationContext.createKeyedBundle(bazKey, groupedKvs)).thenReturn(bazBundle);

    // The input to a GroupByKey is assumed to be a KvCoder
    @SuppressWarnings("unchecked")
    Coder<String> keyCoder = ((KvCoder<String, Integer>) values.getCoder()).getKeyCoder();
    TransformEvaluator<KV<String, Integer>> evaluator =
        new GroupByKeyOnlyEvaluatorFactory(evaluationContext)
            .forApplication(DirectGraphs.getProducer(groupedKvs), inputBundle);

    evaluator.processElement(WindowedValue.valueInGlobalWindow(firstFoo));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(secondFoo));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(thirdFoo));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(firstBar));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(secondBar));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(firstBaz));

    evaluator.finishBundle();

    assertThat(
        fooBundle.commit(Instant.now()).getElements(),
        contains(
            new KeyedWorkItemMatcher<>(
                KeyedWorkItems.elementsWorkItem(
                    "foo",
                    ImmutableSet.of(
                        WindowedValue.valueInGlobalWindow(-1),
                        WindowedValue.valueInGlobalWindow(1),
                        WindowedValue.valueInGlobalWindow(3))),
                keyCoder)));
    assertThat(
        barBundle.commit(Instant.now()).getElements(),
        contains(
            new KeyedWorkItemMatcher<>(
                KeyedWorkItems.elementsWorkItem(
                    "bar",
                    ImmutableSet.of(
                        WindowedValue.valueInGlobalWindow(12),
                        WindowedValue.valueInGlobalWindow(22))),
                keyCoder)));
    assertThat(
        bazBundle.commit(Instant.now()).getElements(),
        contains(
            new KeyedWorkItemMatcher<>(
                KeyedWorkItems.elementsWorkItem(
                    "baz", ImmutableSet.of(WindowedValue.valueInGlobalWindow(Integer.MAX_VALUE))),
                keyCoder)));
  }

  private <K, V> KV<K, WindowedValue<V>> gwValue(KV<K, V> kv) {
    return KV.of(kv.getKey(), WindowedValue.valueInGlobalWindow(kv.getValue()));
  }

  private static class KeyedWorkItemMatcher<K, V>
      extends BaseMatcher<WindowedValue<KeyedWorkItem<K, V>>> {
    private final KeyedWorkItem<K, V> myWorkItem;
    private final Coder<K> keyCoder;

    public KeyedWorkItemMatcher(KeyedWorkItem<K, V> myWorkItem, Coder<K> keyCoder) {
      this.myWorkItem = myWorkItem;
      this.keyCoder = keyCoder;
    }

    @Override
    public boolean matches(Object item) {
      if (item == null || !(item instanceof WindowedValue)) {
        return false;
      }
      WindowedValue<KeyedWorkItem<K, V>> that = (WindowedValue<KeyedWorkItem<K, V>>) item;
      Multiset<WindowedValue<V>> myValues = HashMultiset.create();
      Multiset<WindowedValue<V>> thatValues = HashMultiset.create();
      for (WindowedValue<V> value : myWorkItem.elementsIterable()) {
        myValues.add(value);
      }
      for (WindowedValue<V> value : that.getValue().elementsIterable()) {
        thatValues.add(value);
      }
      try {
        return myValues.equals(thatValues)
            && keyCoder
                .structuralValue(myWorkItem.key())
                .equals(keyCoder.structuralValue(that.getValue().key()));
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("KeyedWorkItem<K, V> containing key ")
          .appendValue(myWorkItem.key())
          .appendText(" and values ")
          .appendValueList("[", ", ", "]", myWorkItem.elementsIterable());
    }
  }
}
