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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey.GroupByKeyOnly;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.TimerInternals;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

/**
 * An In-Memory implementation of the Dataflow Programming Model. Supports Unbounded
 * {@link PCollection PCollections}.
 */
@Experimental
public class InProcessPipelineRunner {
  private static Map<Class<?>, TransformEvaluatorFactory> defaultEvaluatorFactories =
      new ConcurrentHashMap<>();

  /**
   * Register a default transform evaluator.
   */
  public static <TransformT extends PTransform<?, ?>> void registerTransformEvaluatorFactory(
      Class<TransformT> clazz, TransformEvaluatorFactory evaluator) {
    checkArgument(defaultEvaluatorFactories.put(clazz, evaluator) == null,
        "Defining a default factory %s to evaluate Transforms of type %s multiple times", evaluator,
        clazz);
  }

  /**
   * Part of a {@link PCollection}. Elements are output to a bundle, which will cause them to be
   * executed by {@link PTransform PTransforms} that consume the {@link PCollection} this bundle is
   * a part of at a later point.
   * @param <T>
   */
  public static interface Bundle<T> {
    /**
     * Outputs an element to this bundle.
     *
     * @param element the element to add to this bundle
     * @return this bundle
     */
    Bundle<T> add(WindowedValue<T> element);

    /**
     * @return the PCollection that the elements of this bundle belong to
     */
    PCollection<T> getPCollection();

    /**
     * Returns weather this bundle is keyed. A bundle that is part of a {@link PCollection} that
     * occurs after a {@link GroupByKey} is keyed by the result of the last {@link GroupByKey}.
     */
    boolean isKeyed();

    /**
     * Returns the (possibly null) key that was output in the most recent {@link GroupByKey} in the
     * execution of this bundle.
     */
    @Nullable Object getKey();

    /**
     * @return an {@link Iterable} containing all of the elements that have been added to this
     *         {@link Bundle}
     */
    Iterable<WindowedValue<T>> getElements();
  }

  /**
   * A {@link PCollectionViewWriter} is responsible for writing contents of a {@link PCollection} to
   * a storage mechanism that can be read from while constructing a {@link PCollectionView}.
   * @param <ElemT> the type of elements the input {@link PCollection} contains.
   * @param <ViewT> the type of the PCollectionView this writer writes to.
   */
  public static interface PCollectionViewWriter<ElemT, ViewT> {
    void add(Iterable<WindowedValue<ElemT>> values);
  }

  /**
   * Execution Context for the InMemoryPipelineRunner.
   *
   * This implementation is not thread safe. A new InMemoryExecutionContext must be created for each
   * thread that requires it.
   */
  public static class InProcessExecutionContext
      extends com.google.cloud.dataflow.sdk.util.BaseExecutionContext<InProcessExecutionContext
              .InMemoryStepContext> {
    @Override
    protected InMemoryStepContext createStepContext(
        String stepName, String transformName, StateSampler stateSampler) {
      return new InMemoryStepContext(this, stepName, transformName);
    }

    /**
     * Step Context for the InMemoryPipelineRunner.
     */
    public class InMemoryStepContext
        extends com.google.cloud.dataflow.sdk.util.BaseExecutionContext.StepContext {
      public InMemoryStepContext(
          InProcessExecutionContext executionContext, String stepName, String transformName) {
        super(executionContext, stepName, transformName);
      }

      @Override
      public StateInternals stateInternals() {
        // TODO get or create state for current key.
        throw new UnsupportedOperationException("StateInternals not yet meaningfully supported");
      }

      @Override
      public TimerInternals timerInternals() {
        // TODO: Have the executionContext/evaluationContext pass this in
        throw new UnsupportedOperationException("TimerInternals not yet meaningfully supported");
      }
    }

  }


  /**
   * The evaluation context for the {@link InProcessPipelineRunner}. Contains state shared within
   * the current evaluation.
   */
  public static interface InProcessEvaluationContext {
    /**
     * Create a bundle for use by a source.
     */
    <T> Bundle<T> createRootBundle(PCollection<T> output);

    /**
     * Create a {@link Bundle} whose elements belong to the specified {@link PCollection}.
     */
    <T> Bundle<T> createBundle(Bundle<?> input, PCollection<T> output);

    /**
     * Create a {@link Bundle} with the specified keys at the specified step. For use by
     * {@link GroupByKeyOnly} {@link PTransform PTransforms}.
     */
    <T> Bundle<T> createKeyedBundle(Bundle<?> input, Object key, PCollection<T> output);

    /**
     * Create a bundle whose elements will be used in a PCollectionView.
     */
    <ElemT, ViewT> PCollectionViewWriter<ElemT, ViewT> createPCollectionViewWriter(
        PCollection<ElemT> input, PCollectionView<ViewT> output);

    /**
     * Get the options used by this {@link Pipeline}.
     */
    InProcessPipelineOptions getPipelineOptions();

    /**
     * Get an {@link ExecutionContext} for the provided application.
     */
    InProcessExecutionContext getExecutionContext(AppliedPTransform<?, ?, ?> application);

    /**
     * Get the Step Name for the provided application.
     */
    String getStepName(AppliedPTransform<?, ?, ?> application);

    /**
     * @param sideInputs the {@link PCollectionView PCollectionViews} the result should be able to
     *                   read
     * @return a {@link SideInputReader} that can read all of the provided
     *         {@link PCollectionView PCollectionViews}
     */
    SideInputReader createSideInputReader(List<PCollectionView<?>> sideInputs);

    /**
     * Create a {@link CounterSet} for this {@link Pipeline}. The {@link CounterSet} is independent
     * of all other {@link CounterSet CounterSets} created by this call.
     *
     * The {@link InProcessEvaluationContext} is responsible for unifying the counters present in
     * all created {@link CounterSet CounterSets} when the transforms that call this method
     * complete.
     */
    CounterSet createCounterSet();

    /**
     * Returns all of the counters that have been merged into this context via calls to
     * {@link CounterSet#merge(CounterSet)}.
     */
    CounterSet getCounters();
  }

  /**
   * An executor that schedules and executes {@link AppliedPTransform AppliedPTransforms} for both
   * source and intermediate {@link PTransform PTransforms}.
   */
  public static interface InProcessExecutor {
    /**
     * @param root the root {@link AppliedPTransform} to schedule
     */
    void scheduleRoot(AppliedPTransform<?, ?, ?> root);

    /**
     * @param consumer the {@link AppliedPTransform} to schedule
     * @param bundle the input bundle to the consumer
     */
    void scheduleConsumption(AppliedPTransform<?, ?, ?> consumer, Bundle<?> bundle);

    /**
     * Blocks until the job being executed enters a terminal state. A job is completed after all
     * root {@link AppliedPTransform AppliedPTransforms} have completed, and all
     * {@link Bundle Bundles} have been consumed. Jobs may also terminate abnormally.
     */
    void awaitCompletion();
  }
}
