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
package org.apache.beam.runners.direct.portable;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.fnexecution.wire.LengthPrefixUnknownCoders;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.HashMultiset;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Multiset;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GroupByKeyOnlyEvaluatorFactory}. */
@RunWith(JUnit4.class)
public class GroupByKeyOnlyEvaluatorFactoryTest {
  private BundleFactory bundleFactory = ImmutableListBundleFactory.create();

  @Test
  public void testInMemoryEvaluator() throws Exception {
    KvCoder<String, Integer> javaCoder = KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of());
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environments.createDockerEnvironment("java"));
    String windowingStrategyId =
        sdkComponents.registerWindowingStrategy(WindowingStrategy.globalDefault());
    String coderId = sdkComponents.registerCoder(javaCoder);
    Components.Builder builder = sdkComponents.toComponents().toBuilder();
    String javaWireCoderId =
        LengthPrefixUnknownCoders.addLengthPrefixedCoder(coderId, builder, false);
    String runnerWireCoderId =
        LengthPrefixUnknownCoders.addLengthPrefixedCoder(coderId, builder, true);
    RehydratedComponents components = RehydratedComponents.forComponents(builder.build());
    Coder<KV<String, Integer>> javaWireCoder =
        (Coder<KV<String, Integer>>) components.getCoder(javaWireCoderId);
    Coder<KV<?, ?>> runnerWireCoder = (Coder<KV<?, ?>>) components.getCoder(runnerWireCoderId);

    KV<?, ?> firstFoo = asRunnerKV(javaWireCoder, runnerWireCoder, KV.of("foo", -1));
    KV<?, ?> secondFoo = asRunnerKV(javaWireCoder, runnerWireCoder, KV.of("foo", 1));
    KV<?, ?> thirdFoo = asRunnerKV(javaWireCoder, runnerWireCoder, KV.of("foo", 3));
    KV<?, ?> firstBar = asRunnerKV(javaWireCoder, runnerWireCoder, KV.of("bar", 22));
    KV<?, ?> secondBar = asRunnerKV(javaWireCoder, runnerWireCoder, KV.of("bar", 12));
    KV<?, ?> firstBaz = asRunnerKV(javaWireCoder, runnerWireCoder, KV.of("baz", Integer.MAX_VALUE));

    PTransformNode inputTransform =
        PipelineNode.pTransform(
            "source", PTransform.newBuilder().putOutputs("out", "values").build());
    PCollectionNode values =
        PipelineNode.pCollection(
            "values",
            RunnerApi.PCollection.newBuilder()
                .setUniqueName("values")
                .setCoderId(coderId)
                .setWindowingStrategyId(windowingStrategyId)
                .build());
    PCollectionNode groupedKvs =
        PipelineNode.pCollection(
            "groupedKvs", RunnerApi.PCollection.newBuilder().setUniqueName("groupedKvs").build());
    PTransformNode groupByKeyOnly =
        PipelineNode.pTransform(
            "gbko",
            PTransform.newBuilder()
                .putInputs("input", "values")
                .putOutputs("output", "groupedKvs")
                .setSpec(FunctionSpec.newBuilder().setUrn(DirectGroupByKey.DIRECT_GBKO_URN).build())
                .build());
    Pipeline pipeline =
        Pipeline.newBuilder()
            .addRootTransformIds(inputTransform.getId())
            .addRootTransformIds(groupByKeyOnly.getId())
            .setComponents(
                builder
                    .putTransforms(inputTransform.getId(), inputTransform.getTransform())
                    .putTransforms(groupByKeyOnly.getId(), groupByKeyOnly.getTransform())
                    .putPcollections(values.getId(), values.getPCollection())
                    .putPcollections(groupedKvs.getId(), groupedKvs.getPCollection()))
            .build();

    PortableGraph graph = PortableGraph.forPipeline(pipeline);

    CommittedBundle<KV<String, Integer>> inputBundle =
        bundleFactory.<KV<String, Integer>>createBundle(values).commit(Instant.now());

    TransformEvaluator<KV<?, ?>> evaluator =
        new GroupByKeyOnlyEvaluatorFactory(graph, pipeline.getComponents(), bundleFactory)
            .forApplication(groupByKeyOnly, inputBundle);

    evaluator.processElement(WindowedValue.valueInGlobalWindow(firstFoo));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(secondFoo));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(thirdFoo));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(firstBar));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(secondBar));
    evaluator.processElement(WindowedValue.valueInGlobalWindow(firstBaz));

    TransformResult<KV<?, ?>> result = evaluator.finishBundle();

    // The input to a GroupByKey is assumed to be a KvCoder
    @SuppressWarnings({"rawtypes", "unchecked"})
    Coder runnerKeyCoder = ((KvCoder) runnerWireCoder).getKeyCoder();
    CommittedBundle<?> fooBundle = null;
    CommittedBundle<?> barBundle = null;
    CommittedBundle<?> bazBundle = null;
    StructuralKey fooKey = StructuralKey.of(firstFoo.getKey(), runnerKeyCoder);
    StructuralKey barKey = StructuralKey.of(firstBar.getKey(), runnerKeyCoder);
    StructuralKey bazKey = StructuralKey.of(firstBaz.getKey(), runnerKeyCoder);
    for (UncommittedBundle<?> groupedBundle : result.getOutputBundles()) {
      CommittedBundle<?> groupedCommitted = groupedBundle.commit(Instant.now());
      if (fooKey.equals(groupedCommitted.getKey())) {
        fooBundle = groupedCommitted;
      } else if (barKey.equals(groupedCommitted.getKey())) {
        barBundle = groupedCommitted;
      } else if (bazKey.equals(groupedCommitted.getKey())) {
        bazBundle = groupedCommitted;
      } else {
        throw new IllegalArgumentException(
            String.format("Unknown Key %s", groupedCommitted.getKey()));
      }
    }
    assertThat(
        fooBundle,
        contains(
            new KeyedWorkItemMatcher(
                KeyedWorkItems.elementsWorkItem(
                    fooKey.getKey(),
                    ImmutableSet.of(
                        WindowedValue.valueInGlobalWindow(firstFoo.getValue()),
                        WindowedValue.valueInGlobalWindow(secondFoo.getValue()),
                        WindowedValue.valueInGlobalWindow(thirdFoo.getValue()))),
                runnerKeyCoder)));
    assertThat(
        barBundle,
        contains(
            new KeyedWorkItemMatcher<>(
                KeyedWorkItems.elementsWorkItem(
                    barKey.getKey(),
                    ImmutableSet.of(
                        WindowedValue.valueInGlobalWindow(firstBar.getValue()),
                        WindowedValue.valueInGlobalWindow(secondBar.getValue()))),
                runnerKeyCoder)));
    assertThat(
        bazBundle,
        contains(
            new KeyedWorkItemMatcher<>(
                KeyedWorkItems.elementsWorkItem(
                    bazKey.getKey(),
                    ImmutableSet.of(WindowedValue.valueInGlobalWindow(firstBaz.getValue()))),
                runnerKeyCoder)));
  }

  private KV<?, ?> asRunnerKV(
      Coder<KV<String, Integer>> javaWireCoder,
      Coder<KV<?, ?>> runnerWireCoder,
      KV<String, Integer> value)
      throws org.apache.beam.sdk.coders.CoderException {
    return CoderUtils.decodeFromByteArray(
        runnerWireCoder, CoderUtils.encodeToByteArray(javaWireCoder, value));
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
