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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables.getOnlyElement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components.Builder;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;

/**
 * The {@code DirectRunner} {@link TransformEvaluatorFactory} for the {@link GroupByKeyOnly} {@link
 * PTransform}.
 */
class GroupByKeyOnlyEvaluatorFactory implements TransformEvaluatorFactory {
  private final Components components;

  private final BundleFactory bundleFactory;
  private final ExecutableGraph<PTransformNode, PCollectionNode> graph;

  GroupByKeyOnlyEvaluatorFactory(
      ExecutableGraph<PTransformNode, PCollectionNode> graph,
      Components components,
      BundleFactory bundleFactory) {
    this.components = components;
    this.bundleFactory = bundleFactory;
    this.graph = graph;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application, CommittedBundle<?> inputBundle) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator = (TransformEvaluator) createEvaluator(application);
    return evaluator;
  }

  @Override
  public void cleanup() {}

  private <K, V> TransformEvaluator<KV<K, V>> createEvaluator(final PTransformNode application) {
    return new GroupByKeyOnlyEvaluator<>(application);
  }

  /**
   * A transform evaluator for the pseudo-primitive {@link GroupByKeyOnly}. Windowing is ignored;
   * all input should be in the global window since all output will be as well.
   *
   * @see GroupByKeyViaGroupByKeyOnly
   */
  private class GroupByKeyOnlyEvaluator<K, V> implements TransformEvaluator<KV<K, V>> {
    private final Coder<K> keyCoder;
    private final Map<StructuralKey<K>, List<WindowedValue<V>>> groupingMap;

    private final PCollectionNode outputPCollection;
    private final StepTransformResult.Builder<KV<K, V>> resultBuilder;

    private GroupByKeyOnlyEvaluator(PTransformNode application) {
      keyCoder = getKeyCoder(application);
      groupingMap = new HashMap<>();
      outputPCollection = getOnlyElement(graph.getProduced(application));
      resultBuilder = StepTransformResult.withoutHold(application);
    }

    private Coder<K> getKeyCoder(PTransformNode application) {
      PCollectionNode inputPCollection = getOnlyElement(graph.getPerElementInputs(application));
      try {
        // We know the type restrictions on the input PCollection, and the restrictions on the
        // Wire coder
        Builder builder = GroupByKeyOnlyEvaluatorFactory.this.components.toBuilder();
        String wireCoderId = WireCoders.addRunnerWireCoder(inputPCollection, builder);
        Coder<WindowedValue<KV<K, V>>> wireCoder =
            (Coder<WindowedValue<KV<K, V>>>)
                RehydratedComponents.forComponents(builder.build()).getCoder(wireCoderId);

        checkArgument(
            wireCoder instanceof WindowedValue.WindowedValueCoder,
            "Wire %s must be a %s",
            Coder.class.getSimpleName(),
            WindowedValueCoder.class.getSimpleName());
        WindowedValueCoder<KV<K, V>> windowedValueCoder = (WindowedValueCoder<KV<K, V>>) wireCoder;

        checkArgument(
            windowedValueCoder.getValueCoder() instanceof KvCoder,
            "Input elements to %s must be encoded with a %s",
            DirectGroupByKey.DirectGroupByKeyOnly.class.getSimpleName(),
            KvCoder.class.getSimpleName());
        KvCoder<K, V> kvCoder = (KvCoder<K, V>) windowedValueCoder.getValueCoder();

        return kvCoder.getKeyCoder();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void processElement(WindowedValue<KV<K, V>> element) {
      KV<K, V> kv = element.getValue();
      K key = kv.getKey();
      StructuralKey<K> groupingKey = StructuralKey.of(key, keyCoder);
      List<WindowedValue<V>> values =
          groupingMap.computeIfAbsent(groupingKey, k -> new ArrayList<>());
      values.add(element.withValue(kv.getValue()));
    }

    @Override
    public TransformResult<KV<K, V>> finishBundle() {
      for (Map.Entry<StructuralKey<K>, List<WindowedValue<V>>> groupedEntry :
          groupingMap.entrySet()) {
        K key = groupedEntry.getKey().getKey();
        KeyedWorkItem<K, V> groupedKv =
            KeyedWorkItems.elementsWorkItem(key, groupedEntry.getValue());
        UncommittedBundle<KeyedWorkItem<K, V>> bundle =
            bundleFactory.createKeyedBundle(StructuralKey.of(key, keyCoder), outputPCollection);
        bundle.add(WindowedValue.valueInGlobalWindow(groupedKv));
        resultBuilder.addOutput(bundle);
      }
      return resultBuilder.build();
    }
  }
}
