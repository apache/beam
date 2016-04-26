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
package org.apache.beam.sdk.runners.inprocess;

import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.runners.inprocess.StepTransformResult.Builder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.GroupAlsoByWindowViaWindowSetDoFn;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly.ReifyTimestampsAndWindows;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.KeyedWorkItemCoder;
import org.apache.beam.sdk.util.KeyedWorkItems;
import org.apache.beam.sdk.util.SystemReduceFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import com.google.common.annotations.VisibleForTesting;

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
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator = createEvaluator(
            (AppliedPTransform) application, (CommittedBundle) inputBundle, evaluationContext);
    return evaluator;
  }

  private <K, V> TransformEvaluator<KV<K, WindowedValue<V>>> createEvaluator(
      final AppliedPTransform<
              PCollection<KV<K, WindowedValue<V>>>, PCollection<KeyedWorkItem<K, V>>,
              InProcessGroupByKeyOnly<K, V>>
          application,
      final CommittedBundle<KV<K, V>> inputBundle,
      final InProcessEvaluationContext evaluationContext) {
    return new GroupByKeyEvaluator<K, V>(evaluationContext, inputBundle, application);
  }

  private static class GroupByKeyEvaluator<K, V>
      implements TransformEvaluator<KV<K, WindowedValue<V>>> {
    private final InProcessEvaluationContext evaluationContext;

    private final CommittedBundle<KV<K, V>> inputBundle;
    private final AppliedPTransform<
            PCollection<KV<K, WindowedValue<V>>>, PCollection<KeyedWorkItem<K, V>>,
            InProcessGroupByKeyOnly<K, V>>
        application;
    private final Coder<K> keyCoder;
    private Map<GroupingKey<K>, List<WindowedValue<V>>> groupingMap;

    public GroupByKeyEvaluator(
        InProcessEvaluationContext evaluationContext,
        CommittedBundle<KV<K, V>> inputBundle,
        AppliedPTransform<
                PCollection<KV<K, WindowedValue<V>>>, PCollection<KeyedWorkItem<K, V>>,
                InProcessGroupByKeyOnly<K, V>>
            application) {
      this.evaluationContext = evaluationContext;
      this.inputBundle = inputBundle;
      this.application = application;

      PCollection<KV<K, WindowedValue<V>>> input = application.getInput();
      keyCoder = getKeyCoder(input.getCoder());
      groupingMap = new HashMap<>();
    }

    private Coder<K> getKeyCoder(Coder<KV<K, WindowedValue<V>>> coder) {
      if (!(coder instanceof KvCoder)) {
        throw new IllegalStateException();
      }
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
        UncommittedBundle<KeyedWorkItem<K, V>> bundle =
            evaluationContext.createKeyedBundle(inputBundle, key, application.getOutput());
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

  /**
   * A {@link PTransformOverrideFactory} for {@link GroupByKey} PTransforms.
   */
  public static final class InProcessGroupByKeyOverrideFactory
      implements PTransformOverrideFactory {
    @Override
    public <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> override(
        PTransform<InputT, OutputT> transform) {
      if (transform instanceof GroupByKey) {
        @SuppressWarnings({"rawtypes", "unchecked"})
        PTransform<InputT, OutputT> override = new InProcessGroupByKey((GroupByKey) transform);
        return override;
      }
      return transform;
    }
  }

  /**
   * An in-memory implementation of the {@link GroupByKey} primitive as a composite
   * {@link PTransform}.
   */
  private static final class InProcessGroupByKey<K, V>
      extends ForwardingPTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {
    private final GroupByKey<K, V> original;

    private InProcessGroupByKey(GroupByKey<K, V> from) {
      this.original = from;
    }

    @Override
    public PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> delegate() {
      return original;
    }

    @Override
    public PCollection<KV<K, Iterable<V>>> apply(PCollection<KV<K, V>> input) {
      KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();

      // This operation groups by the combination of key and window,
      // merging windows as needed, using the windows assigned to the
      // key/value input elements and the window merge operation of the
      // window function associated with the input PCollection.
      WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();

      // Use the default GroupAlsoByWindow implementation
      DoFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> groupAlsoByWindow =
          groupAlsoByWindow(windowingStrategy, inputCoder.getValueCoder());

      // By default, implement GroupByKey via a series of lower-level operations.
      return input
          // Make each input element's timestamp and assigned windows
          // explicit, in the value part.
          .apply(new ReifyTimestampsAndWindows<K, V>())

          .apply(new InProcessGroupByKeyOnly<K, V>())
          .setCoder(KeyedWorkItemCoder.of(inputCoder.getKeyCoder(),
              inputCoder.getValueCoder(), input.getWindowingStrategy().getWindowFn().windowCoder()))

          // Group each key's values by window, merging windows as needed.
          .apply("GroupAlsoByWindow", ParDo.of(groupAlsoByWindow))

          // And update the windowing strategy as appropriate.
          .setWindowingStrategyInternal(original.updateWindowingStrategy(windowingStrategy))
          .setCoder(
              KvCoder.of(inputCoder.getKeyCoder(), IterableCoder.of(inputCoder.getValueCoder())));
    }

    private <W extends BoundedWindow>
        DoFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> groupAlsoByWindow(
            final WindowingStrategy<?, W> windowingStrategy, final Coder<V> inputCoder) {
      return GroupAlsoByWindowViaWindowSetDoFn.create(
          windowingStrategy, SystemReduceFn.<K, V, W>buffering(inputCoder));
    }
  }

  /**
   * An implementation primitive to use in the evaluation of a {@link GroupByKey}
   * {@link PTransform}.
   */
  public static final class InProcessGroupByKeyOnly<K, V>
      extends PTransform<PCollection<KV<K, WindowedValue<V>>>, PCollection<KeyedWorkItem<K, V>>> {
    @Override
    public PCollection<KeyedWorkItem<K, V>> apply(PCollection<KV<K, WindowedValue<V>>> input) {
      return PCollection.<KeyedWorkItem<K, V>>createPrimitiveOutputInternal(
          input.getPipeline(), input.getWindowingStrategy(), input.isBounded());
    }

    @VisibleForTesting
    InProcessGroupByKeyOnly() {}
  }
}
