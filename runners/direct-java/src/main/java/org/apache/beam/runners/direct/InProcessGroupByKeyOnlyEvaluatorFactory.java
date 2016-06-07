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

import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.runners.direct.InProcessGroupByKey.InProcessGroupByKeyOnly;
import org.apache.beam.runners.direct.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.runners.direct.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.runners.direct.StepTransformResult.Builder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.KeyedWorkItems;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link InProcessPipelineRunner} {@link TransformEvaluatorFactory} for the
 * {@link GroupByKeyOnly} {@link PTransform}.
 */
class InProcessGroupByKeyOnlyEvaluatorFactory implements TransformEvaluatorFactory {
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator =
        createEvaluator(
            (AppliedPTransform) application, (CommittedBundle) inputBundle, evaluationContext);
    return evaluator;
  }

  private <K, V> TransformEvaluator<KV<K, WindowedValue<V>>> createEvaluator(
      final AppliedPTransform<
              PCollection<KV<K, WindowedValue<V>>>, PCollection<KeyedWorkItem<K, V>>,
              InProcessGroupByKeyOnly<K, V>>
          application,
      final CommittedBundle<KV<K, WindowedValue<V>>> inputBundle,
      final InProcessEvaluationContext evaluationContext) {
    return new InProcessGroupByKeyOnlyEvaluator<K, V>(evaluationContext, inputBundle, application);
  }

  /**
   * A transform evaluator for the pseudo-primitive {@link GroupByKeyOnly}. Windowing is ignored;
   * all input should be in the global window since all output will be as well.
   *
   * @see GroupByKeyViaGroupByKeyOnly
   */
  private static class InProcessGroupByKeyOnlyEvaluator<K, V>
      implements TransformEvaluator<KV<K, WindowedValue<V>>> {
    private final InProcessEvaluationContext evaluationContext;

    private final CommittedBundle<KV<K, WindowedValue<V>>> inputBundle;
    private final AppliedPTransform<
            PCollection<KV<K, WindowedValue<V>>>, PCollection<KeyedWorkItem<K, V>>,
            InProcessGroupByKeyOnly<K, V>>
        application;
    private final Coder<K> keyCoder;
    private Map<GroupingKey<K>, List<WindowedValue<V>>> groupingMap;

    public InProcessGroupByKeyOnlyEvaluator(
        InProcessEvaluationContext evaluationContext,
        CommittedBundle<KV<K, WindowedValue<V>>> inputBundle,
        AppliedPTransform<
                PCollection<KV<K, WindowedValue<V>>>, PCollection<KeyedWorkItem<K, V>>,
                InProcessGroupByKeyOnly<K, V>>
            application) {
      this.evaluationContext = evaluationContext;
      this.inputBundle = inputBundle;
      this.application = application;
      this.keyCoder = getKeyCoder(application.getInput().getCoder());
      this.groupingMap = new HashMap<>();
    }

    private Coder<K> getKeyCoder(Coder<KV<K, WindowedValue<V>>> coder) {
      checkState(
          coder instanceof KvCoder,
          "%s requires a coder of class %s."
              + " This is an internal error; this is checked during pipeline construction"
              + " but became corrupted.",
          getClass().getSimpleName(),
          KvCoder.class.getSimpleName());
      @SuppressWarnings("unchecked")
      Coder<K> keyCoder = ((KvCoder<K, WindowedValue<V>>) coder).getKeyCoder();
      return keyCoder;
    }

    @Override
    public void processElement(WindowedValue<KV<K, WindowedValue<V>>> element) {
      KV<K, WindowedValue<V>> kv = element.getValue();
      K key = kv.getKey();
      byte[] encodedKey;
      try {
        encodedKey = encodeToByteArray(keyCoder, key);
      } catch (CoderException exn) {
        // TODO: Put in better element printing:
        // truncate if too long.
        throw new IllegalArgumentException(
            String.format("unable to encode key %s of input to %s using %s", key, this, keyCoder),
            exn);
      }
      GroupingKey<K> groupingKey = new GroupingKey<>(key, encodedKey);
      List<WindowedValue<V>> values = groupingMap.get(groupingKey);
      if (values == null) {
        values = new ArrayList<WindowedValue<V>>();
        groupingMap.put(groupingKey, values);
      }
      values.add(kv.getValue());
    }

    @Override
    public InProcessTransformResult finishBundle() {
      Builder resultBuilder = StepTransformResult.withoutHold(application);
      for (Map.Entry<GroupingKey<K>, List<WindowedValue<V>>> groupedEntry :
          groupingMap.entrySet()) {
        K key = groupedEntry.getKey().key;
        KeyedWorkItem<K, V> groupedKv =
            KeyedWorkItems.elementsWorkItem(key, groupedEntry.getValue());
        UncommittedBundle<KeyedWorkItem<K, V>> bundle = evaluationContext.createKeyedBundle(
            inputBundle,
            StructuralKey.of(key, keyCoder),
            application.getOutput());
        bundle.add(WindowedValue.valueInGlobalWindow(groupedKv));
        resultBuilder.addOutput(bundle);
      }
      return resultBuilder.build();
    }

    private static class GroupingKey<K> {
      private K key;
      private byte[] encodedKey;

      public GroupingKey(K key, byte[] encodedKey) {
        this.key = key;
        this.encodedKey = encodedKey;
      }

      @Override
      public boolean equals(Object o) {
        if (o instanceof GroupingKey) {
          GroupingKey<?> that = (GroupingKey<?>) o;
          return Arrays.equals(this.encodedKey, that.encodedKey);
        } else {
          return false;
        }
      }

      @Override
      public int hashCode() {
        return Arrays.hashCode(encodedKey);
      }
    }
  }
}
