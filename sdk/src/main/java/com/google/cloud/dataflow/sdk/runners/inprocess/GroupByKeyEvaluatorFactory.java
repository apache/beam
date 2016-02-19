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

import static com.google.cloud.dataflow.sdk.util.CoderUtils.encodeToByteArray;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessEvaluationContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.StepTransformResult.Builder;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey.GroupByKeyOnly;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link InProcessPipelineRunner} {@link TransformEvaluatorFactory} for the {@link GroupByKey}
 * {@link PTransform}.
 */
class GroupByKeyEvaluatorFactory implements TransformEvaluatorFactory {
  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    return createEvaluator(
        (AppliedPTransform) application, (CommittedBundle) inputBundle, evaluationContext);
  }

  private <K, V> TransformEvaluator<KV<K, V>> createEvaluator(
      final AppliedPTransform<
              PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKeyOnly<K, V>>
          application,
      final CommittedBundle<KV<K, V>> inputBundle,
      final InProcessEvaluationContext evaluationContext) {
    return new GroupByKeyEvaluator<K, V>(evaluationContext, inputBundle, application);
  }

  private static class GroupByKeyEvaluator<K, V> implements TransformEvaluator<KV<K, V>> {
    private final InProcessEvaluationContext evaluationContext;

    private final CommittedBundle<KV<K, V>> inputBundle;
    private final AppliedPTransform<
            PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKeyOnly<K, V>>
        application;
    private final Coder<K> keyCoder;
    private Map<GroupingKey<K>, List<V>> groupingMap;

    public GroupByKeyEvaluator(
        InProcessEvaluationContext evaluationContext,
        CommittedBundle<KV<K, V>> inputBundle,
        AppliedPTransform<
                PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKeyOnly<K, V>>
            application) {
      this.evaluationContext = evaluationContext;
      this.inputBundle = inputBundle;
      this.application = application;

      PCollection<KV<K, V>> input = application.getInput();
      keyCoder = getKeyCoder(input.getCoder());
      groupingMap = new HashMap<>();
    }

    private Coder<K> getKeyCoder(Coder<KV<K, V>> coder) {
      if (!(coder instanceof KvCoder)) {
        throw new IllegalStateException();
      }
      @SuppressWarnings("unchecked")
      Coder<K> keyCoder = ((KvCoder<K, V>) coder).getKeyCoder();
      return keyCoder;
    }

    @Override
    public void processElement(WindowedValue<KV<K, V>> element) {
      KV<K, V> kv = element.getValue();
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
      if (!groupingMap.containsKey(groupingKey)) {
        groupingMap.put(groupingKey, new ArrayList<V>());
      }
      List<V> values = groupingMap.get(groupingKey);
      values.add(kv.getValue());
    }

    @Override
    public InProcessTransformResult finishBundle() {
      Builder resultBuilder = StepTransformResult.withoutHold(application);
      for (Map.Entry<GroupingKey<K>, List<V>> groupedEntry : groupingMap.entrySet()) {
        K key = groupedEntry.getKey().key;
        KV<K, Iterable<V>> groupedKv = KV.<K, Iterable<V>>of(key, groupedEntry.getValue());
        UncommittedBundle<KV<K, Iterable<V>>> bundle =
            evaluationContext.createKeyedBundle(inputBundle, key, application.getOutput());
        bundle.add(WindowedValue.valueInEmptyWindows(groupedKv));
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
