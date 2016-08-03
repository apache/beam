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
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupAlsoByWindow;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly.GroupAlsoByWindow;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.SystemReduceFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateInternalsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.collect.ImmutableMap;

import java.util.Collections;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the
 * {@link GroupByKeyOnly} {@link PTransform}.
 */
class GroupAlsoByWindowEvaluatorFactory implements TransformEvaluatorFactory {
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      CommittedBundle<?> inputBundle,
      EvaluationContext evaluationContext) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator =
        createEvaluator(
            (AppliedPTransform) application, (CommittedBundle) inputBundle, evaluationContext);
    return evaluator;
  }

  private <K, V> TransformEvaluator<KeyedWorkItem<K, V>> createEvaluator(
      AppliedPTransform<
              PCollection<KeyedWorkItem<K, V>>,
              PCollection<KV<K, Iterable<V>>>,
              DirectGroupAlsoByWindow<K, V>> application,
      CommittedBundle<KeyedWorkItem<K, V>> inputBundle,
      EvaluationContext evaluationContext) {
    return new GroupAlsoByWindowEvaluator<>(
        evaluationContext, inputBundle, application);
  }

  /**
   * A transform evaluator for the pseudo-primitive {@link GroupAlsoByWindow}. Windowing is ignored;
   * all input should be in the global window since all output will be as well.
   *
   * @see GroupByKeyViaGroupByKeyOnly
   */
  private static class GroupAlsoByWindowEvaluator<K, V>
      implements TransformEvaluator<KeyedWorkItem<K, V>> {

    private final TransformEvaluator<KeyedWorkItem<K, V>> gabwParDoEvaluator;

    public GroupAlsoByWindowEvaluator(
        final EvaluationContext evaluationContext,
        CommittedBundle<KeyedWorkItem<K, V>> inputBundle,
        final AppliedPTransform<
                PCollection<KeyedWorkItem<K, V>>,
                PCollection<KV<K, Iterable<V>>>,
                DirectGroupAlsoByWindow<K, V>> application) {

      Coder<V> valueCoder =
          application.getTransform().getValueCoder(inputBundle.getPCollection().getCoder());

      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) application.getTransform().getWindowingStrategy();

      DirectStepContext stepContext =
          evaluationContext
              .getExecutionContext(application, inputBundle.getKey())
              .getOrCreateStepContext(
                  evaluationContext.getStepName(application), application.getTransform().getName());

      StateInternals<K> stateInternals = (StateInternals<K>) stepContext.stateInternals();

      DoFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> gabwDoFn =
          GroupAlsoByWindowViaWindowSetDoFn.create(
              windowingStrategy,
              new ConstantStateInternalsFactory<K>(stateInternals),
              SystemReduceFn.<K, V, BoundedWindow>buffering(valueCoder));

      TupleTag<KV<K, Iterable<V>>> mainOutputTag = new TupleTag<KV<K, Iterable<V>>>() {};

      // Not technically legit, as the application is not a ParDo
      this.gabwParDoEvaluator =
          ParDoEvaluator.create(
              evaluationContext,
              stepContext,
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
    public TransformResult finishBundle() throws Exception {
      return gabwParDoEvaluator.finishBundle();
    }
  }

  private static final class ConstantStateInternalsFactory<K>
      implements StateInternalsFactory<K> {
    private final StateInternals<K> stateInternals;

    private ConstantStateInternalsFactory(StateInternals<K> stateInternals) {
      this.stateInternals = stateInternals;
    }

    @Override
    @SuppressWarnings("unchecked")
    public StateInternals<K> stateInternalsForKey(K key) {
      return stateInternals;
    }
  }
}
