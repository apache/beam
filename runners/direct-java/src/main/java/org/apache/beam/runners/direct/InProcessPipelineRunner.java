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

import org.apache.beam.runners.direct.InProcessGroupByKey.InProcessGroupByKeyOnly;
import org.apache.beam.runners.direct.ViewEvaluatorFactory.InProcessViewOverrideFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AggregatorPipelineExtractor;
import org.apache.beam.sdk.runners.AggregatorRetrievalException;
import org.apache.beam.sdk.runners.AggregatorValues;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.util.MapAggregatorValues;
import org.apache.beam.sdk.util.TimerInternals.TimerData;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.Counter;
import org.apache.beam.sdk.util.common.CounterSet;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.joda.time.Instant;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * An In-Memory implementation of the Dataflow Programming Model. Supports Unbounded
 * {@link PCollection PCollections}.
 */
@Experimental
public class InProcessPipelineRunner
    extends PipelineRunner<InProcessPipelineRunner.InProcessPipelineResult> {
  /**
   * The default set of transform overrides to use in the {@link InProcessPipelineRunner}.
   *
   * <p>A transform override must have a single-argument constructor that takes an instance of the
   * type of transform it is overriding.
   */
  @SuppressWarnings("rawtypes")
  private static Map<Class<? extends PTransform>, PTransformOverrideFactory>
      defaultTransformOverrides =
          ImmutableMap.<Class<? extends PTransform>, PTransformOverrideFactory>builder()
              .put(GroupByKey.class, new InProcessGroupByKeyOverrideFactory())
              .put(CreatePCollectionView.class, new InProcessViewOverrideFactory())
              .put(AvroIO.Write.Bound.class, new AvroIOShardedWriteFactory())
              .put(TextIO.Write.Bound.class, new TextIOShardedWriteFactory())
              .build();

  /**
   * Part of a {@link PCollection}. Elements are output to a bundle, which will cause them to be
   * executed by {@link PTransform PTransforms} that consume the {@link PCollection} this bundle is
   * a part of at a later point. This is an uncommitted bundle and can have elements added to it.
   *
   * @param <T> the type of elements that can be added to this bundle
   */
  public static interface UncommittedBundle<T> {
    /**
     * Returns the PCollection that the elements of this {@link UncommittedBundle} belong to.
     */
    PCollection<T> getPCollection();

    /**
     * Outputs an element to this bundle.
     *
     * @param element the element to add to this bundle
     * @return this bundle
     */
    UncommittedBundle<T> add(WindowedValue<T> element);

    /**
     * Commits this {@link UncommittedBundle}, returning an immutable {@link CommittedBundle}
     * containing all of the elements that were added to it. The {@link #add(WindowedValue)} method
     * will throw an {@link IllegalStateException} if called after a call to commit.
     * @param synchronizedProcessingTime the synchronized processing time at which this bundle was
     *                                   committed
     */
    CommittedBundle<T> commit(Instant synchronizedProcessingTime);
  }

  /**
   * Part of a {@link PCollection}. Elements are output to an {@link UncommittedBundle}, which will
   * eventually committed. Committed elements are executed by the {@link PTransform PTransforms}
   * that consume the {@link PCollection} this bundle is
   * a part of at a later point.
   * @param <T> the type of elements contained within this bundle
   */
  public static interface CommittedBundle<T> {
    /**
     * Returns the PCollection that the elements of this bundle belong to.
     */
    PCollection<T> getPCollection();

    /**
     * Returns the key that was output in the most recent {@link GroupByKey} in the
     * execution of this bundle.
     */
    StructuralKey<?> getKey();

    /**
     * Returns an {@link Iterable} containing all of the elements that have been added to this
     * {@link CommittedBundle}.
     */
    Iterable<WindowedValue<T>> getElements();

    /**
     * Returns the processing time output watermark at the time the producing {@link PTransform}
     * committed this bundle. Downstream synchronized processing time watermarks cannot progress
     * past this point before consuming this bundle.
     *
     * <p>This value is no greater than the earliest incomplete processing time or synchronized
     * processing time {@link TimerData timer} at the time this bundle was committed, including any
     * timers that fired to produce this bundle.
     */
    Instant getSynchronizedProcessingOutputWatermark();

    /**
     * Return a new {@link CommittedBundle} that is like this one, except calls to
     * {@link #getElements()} will return the provided elements. This bundle is unchanged.
     *
     * <p>
     * The value of the {@link #getSynchronizedProcessingOutputWatermark() synchronized processing
     * output watermark} of the returned {@link CommittedBundle} is equal to the value returned from
     * the current bundle. This is used to ensure a {@link PTransform} that could not complete
     * processing on input elements properly holds the synchronized processing time to the
     * appropriate value.
     */
    CommittedBundle<T> withElements(Iterable<WindowedValue<T>> elements);
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

  ////////////////////////////////////////////////////////////////////////////////////////////////
  private final InProcessPipelineOptions options;

  public static InProcessPipelineRunner fromOptions(PipelineOptions options) {
    return new InProcessPipelineRunner(options.as(InProcessPipelineOptions.class));
  }

  private InProcessPipelineRunner(InProcessPipelineOptions options) {
    this.options = options;
  }

  /**
   * Returns the {@link PipelineOptions} used to create this {@link InProcessPipelineRunner}.
   */
  public InProcessPipelineOptions getPipelineOptions() {
    return options;
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    PTransformOverrideFactory overrideFactory = defaultTransformOverrides.get(transform.getClass());
    if (overrideFactory != null) {
      PTransform<InputT, OutputT> customTransform = overrideFactory.override(transform);

      return super.apply(customTransform, input);
    }
    // If there is no override, or we should not apply the override, apply the original transform
    return super.apply(transform, input);
  }

  @Override
  public InProcessPipelineResult run(Pipeline pipeline) {
    ConsumerTrackingPipelineVisitor consumerTrackingVisitor = new ConsumerTrackingPipelineVisitor();
    pipeline.traverseTopologically(consumerTrackingVisitor);
    for (PValue unfinalized : consumerTrackingVisitor.getUnfinalizedPValues()) {
      unfinalized.finishSpecifying();
    }
    @SuppressWarnings("rawtypes")
    KeyedPValueTrackingVisitor keyedPValueVisitor =
        KeyedPValueTrackingVisitor.create(
            ImmutableSet.<Class<? extends PTransform>>of(
                GroupByKey.class, InProcessGroupByKeyOnly.class));
    pipeline.traverseTopologically(keyedPValueVisitor);

    DisplayDataValidator.validatePipeline(pipeline);

    InProcessEvaluationContext context =
        InProcessEvaluationContext.create(
            getPipelineOptions(),
            createBundleFactory(getPipelineOptions()),
            consumerTrackingVisitor.getRootTransforms(),
            consumerTrackingVisitor.getValueToConsumers(),
            consumerTrackingVisitor.getStepNames(),
            consumerTrackingVisitor.getViews());

    // independent executor service for each run
    ExecutorService executorService =
        context.getPipelineOptions().getExecutorServiceFactory().create();
    InProcessExecutor executor =
        ExecutorServiceParallelExecutor.create(
            executorService,
            consumerTrackingVisitor.getValueToConsumers(),
            keyedPValueVisitor.getKeyedPValues(),
            TransformEvaluatorRegistry.defaultRegistry(),
            defaultModelEnforcements(options),
            context);
    executor.start(consumerTrackingVisitor.getRootTransforms());

    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        new AggregatorPipelineExtractor(pipeline).getAggregatorSteps();
    InProcessPipelineResult result =
        new InProcessPipelineResult(executor, context, aggregatorSteps);
    if (options.isBlockOnRun()) {
      try {
        result.awaitCompletion();
      } catch (UserCodeException userException) {
        throw new PipelineExecutionException(userException.getCause());
      } catch (Throwable t) {
        if (t instanceof RuntimeException) {
          throw (RuntimeException) t;
        }
        throw new RuntimeException(t);
      }
    }
    return result;
  }

  private Map<Class<? extends PTransform>, Collection<ModelEnforcementFactory>>
      defaultModelEnforcements(InProcessPipelineOptions options) {
    ImmutableMap.Builder<Class<? extends PTransform>, Collection<ModelEnforcementFactory>>
        enforcements = ImmutableMap.builder();
    Collection<ModelEnforcementFactory> parDoEnforcements = createParDoEnforcements(options);
    enforcements.put(ParDo.Bound.class, parDoEnforcements);
    enforcements.put(ParDo.BoundMulti.class, parDoEnforcements);
    return enforcements.build();
  }

  private Collection<ModelEnforcementFactory> createParDoEnforcements(
      InProcessPipelineOptions options) {
    ImmutableList.Builder<ModelEnforcementFactory> enforcements = ImmutableList.builder();
    if (options.isTestImmutability()) {
      enforcements.add(ImmutabilityEnforcementFactory.create());
    }
    return enforcements.build();
  }

  private BundleFactory createBundleFactory(InProcessPipelineOptions pipelineOptions) {
    BundleFactory bundleFactory = InProcessBundleFactory.create();
    if (pipelineOptions.isTestImmutability()) {
      bundleFactory = ImmutabilityCheckingBundleFactory.create(bundleFactory);
    }
    return bundleFactory;
  }

  /**
   * The result of running a {@link Pipeline} with the {@link InProcessPipelineRunner}.
   *
   * Throws {@link UnsupportedOperationException} for all methods.
   */
  public static class InProcessPipelineResult implements PipelineResult {
    private final InProcessExecutor executor;
    private final InProcessEvaluationContext evaluationContext;
    private final Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps;
    private State state;

    private InProcessPipelineResult(
        InProcessExecutor executor,
        InProcessEvaluationContext evaluationContext,
        Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps) {
      this.executor = executor;
      this.evaluationContext = evaluationContext;
      this.aggregatorSteps = aggregatorSteps;
      // Only ever constructed after the executor has started.
      this.state = State.RUNNING;
    }

    @Override
    public State getState() {
      return state;
    }

    @Override
    public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator)
        throws AggregatorRetrievalException {
      CounterSet counters = evaluationContext.getCounters();
      Collection<PTransform<?, ?>> steps = aggregatorSteps.get(aggregator);
      Map<String, T> stepValues = new HashMap<>();
      for (AppliedPTransform<?, ?, ?> transform : evaluationContext.getSteps()) {
        if (steps.contains(transform.getTransform())) {
          String stepName =
              String.format(
                  "user-%s-%s", evaluationContext.getStepName(transform), aggregator.getName());
          Counter<T> counter = (Counter<T>) counters.getExistingCounter(stepName);
          if (counter != null) {
            stepValues.put(transform.getFullName(), counter.getAggregate());
          }
        }
      }
      return new MapAggregatorValues<>(stepValues);
    }

    /**
     * Blocks until the {@link Pipeline} execution represented by this
     * {@link InProcessPipelineResult} is complete, returning the terminal state.
     *
     * <p>If the pipeline terminates abnormally by throwing an exception, this will rethrow the
     * exception. Future calls to {@link #getState()} will return
     * {@link org.apache.beam.sdk.PipelineResult.State#FAILED}.
     *
     * <p>NOTE: if the {@link Pipeline} contains an {@link IsBounded#UNBOUNDED unbounded}
     * {@link PCollection}, and the {@link PipelineRunner} was created with
     * {@link InProcessPipelineOptions#isShutdownUnboundedProducersWithMaxWatermark()} set to false,
     * this method will never return.
     *
     * See also {@link InProcessExecutor#awaitCompletion()}.
     */
    public State awaitCompletion() throws Throwable {
      if (!state.isTerminal()) {
        try {
          executor.awaitCompletion();
          state = State.DONE;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw e;
        } catch (Throwable t) {
          state = State.FAILED;
          throw t;
        }
      }
      return state;
    }
  }
}
