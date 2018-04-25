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

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.portable.StepTransformResult.Builder;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the
 * {@link GroupByKeyOnly} {@link PTransform}.
 */
class GroupByKeyOnlyEvaluatorFactory implements TransformEvaluatorFactory {
  private final EvaluationContext evaluationContext;

  GroupByKeyOnlyEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application,
      CommittedBundle<?> inputBundle) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator = (TransformEvaluator) createEvaluator(application);
    return evaluator;
  }

  @Override
  public void cleanup() {}

  private <K, V> TransformEvaluator<KV<K, V>> createEvaluator(final PTransformNode application) {
    return new GroupByKeyOnlyEvaluator<>(evaluationContext, application);
  }

  /**
   * A transform evaluator for the pseudo-primitive {@link GroupByKeyOnly}. Windowing is ignored;
   * all input should be in the global window since all output will be as well.
   *
   * @see GroupByKeyViaGroupByKeyOnly
   */
  private static class GroupByKeyOnlyEvaluator<K, V>
      implements TransformEvaluator<KV<K, V>> {
    private final EvaluationContext evaluationContext;

    private final PTransformNode application;
    private final Coder<K> keyCoder;
    private Map<StructuralKey<K>, List<WindowedValue<V>>> groupingMap;

    public GroupByKeyOnlyEvaluator(
        EvaluationContext evaluationContext,
        PTransformNode application) {
      this.evaluationContext = evaluationContext;
      this.application = application;
      this.keyCoder = null;
      this.groupingMap = new HashMap<>();
      throw new UnsupportedOperationException("Not yet migrated");
    }

    private Coder<K> getKeyCoder(Coder<KV<K, V>> coder) {
      checkState(
          coder instanceof KvCoder,
          "%s requires a coder of class %s."
              + " This is an internal error; this is checked during pipeline construction"
              + " but became corrupted.",
          getClass().getSimpleName(),
          KvCoder.class.getSimpleName());
      @SuppressWarnings("unchecked")
      Coder<K> keyCoder = ((KvCoder<K, V>) coder).getKeyCoder();
      return keyCoder;
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
      Builder resultBuilder = StepTransformResult.withoutHold(application);
      for (Map.Entry<StructuralKey<K>, List<WindowedValue<V>>> groupedEntry :
          groupingMap.entrySet()) {
        K key = groupedEntry.getKey().getKey();
        KeyedWorkItem<K, V> groupedKv =
            KeyedWorkItems.elementsWorkItem(key, groupedEntry.getValue());
        PCollectionNode outputNode = null;
        UncommittedBundle<KeyedWorkItem<K, V>> bundle =
            evaluationContext.createKeyedBundle(StructuralKey.of(key, keyCoder), outputNode);
        bundle.add(WindowedValue.valueInGlobalWindow(groupedKv));
        resultBuilder.addOutput(bundle);
      }
      throw new UnsupportedOperationException("Not yet migrated");
    }
  }
}
