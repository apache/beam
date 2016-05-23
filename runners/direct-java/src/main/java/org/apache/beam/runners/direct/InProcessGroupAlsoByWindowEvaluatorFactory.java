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

import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetDoFn;
import org.apache.beam.runners.direct.InProcessGroupByKey.InProcessGroupAlsoByWindow;
import org.apache.beam.runners.direct.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.SystemReduceFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.collect.ImmutableMap;

import java.util.Collections;

/**
 * The {@link InProcessPipelineRunner} {@link TransformEvaluatorFactory} for the
 * {@link GroupByKeyOnly} {@link PTransform}.
 */
class InProcessGroupAlsoByWindowEvaluatorFactory implements TransformEvaluatorFactory {
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

  private <K, V> TransformEvaluator<KeyedWorkItem<K, V>> createEvaluator(
      AppliedPTransform<
              PCollection<KeyedWorkItem<K, V>>, PCollection<KV<K, Iterable<V>>>,
              InProcessGroupAlsoByWindow<K, V>>
          application,
      CommittedBundle<KeyedWorkItem<K, V>> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    return new InProcessGroupAlsoByWindowEvaluator<K, V>(
        evaluationContext, inputBundle, application);
  }

  /**
   * A transform evaluator for the pseudo-primitive {@link GroupAlsoByWindow}. Windowing is ignored;
   * all input should be in the global window since all output will be as well.
   *
   * @see GroupByKeyViaGroupByKeyOnly
   */
  private static class InProcessGroupAlsoByWindowEvaluator<K, V>
      implements TransformEvaluator<KeyedWorkItem<K, V>> {

    private final TransformEvaluator<KeyedWorkItem<K, V>> gabwParDoEvaluator;

    public InProcessGroupAlsoByWindowEvaluator(
        final InProcessEvaluationContext evaluationContext,
        CommittedBundle<KeyedWorkItem<K, V>> inputBundle,
        final AppliedPTransform<
                PCollection<KeyedWorkItem<K, V>>, PCollection<KV<K, Iterable<V>>>,
                InProcessGroupAlsoByWindow<K, V>>
            application) {

      Coder<V> valueCoder =
          application.getTransform().getValueCoder(inputBundle.getPCollection().getCoder());

      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) application.getTransform().getWindowingStrategy();

      DoFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> gabwDoFn =
          GroupAlsoByWindowViaWindowSetDoFn.create(
              windowingStrategy,
              SystemReduceFn.<K, V, BoundedWindow>buffering(valueCoder));

      TupleTag<KV<K, Iterable<V>>> mainOutputTag = new TupleTag<KV<K, Iterable<V>>>() {};

      // Not technically legit, as the application is not a ParDo
      this.gabwParDoEvaluator =
          ParDoInProcessEvaluator.create(
              evaluationContext,
              inputBundle,
              application,
              gabwDoFn,
              Collections.<PCollectionView<?>>emptyList(),
              mainOutputTag,
              Collections.<TupleTag<?>>emptyList(),
              ImmutableMap.<TupleTag<?>, PCollection<?>>of(mainOutputTag, application.getOutput()));
    }

    @Override
    public void processElement(WindowedValue<KeyedWorkItem<K, V>> element) throws Exception {
      gabwParDoEvaluator.processElement(element);
    }

    @Override
    public InProcessTransformResult finishBundle() throws Exception {
      return gabwParDoEvaluator.finishBundle();
    }
  }
}
