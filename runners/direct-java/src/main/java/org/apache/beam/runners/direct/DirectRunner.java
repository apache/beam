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

import com.google.common.base.MoreObjects;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupByKeyOnly;
import org.apache.beam.runners.direct.DirectRunner.DirectPipelineResult;
import org.apache.beam.runners.direct.TestStreamEvaluatorFactory.DirectTestStreamFactory;
import org.apache.beam.runners.direct.ViewEvaluatorFactory.ViewOverrideFactory;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.util.TimerInternals.TimerData;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An In-Memory implementation of the Dataflow Programming Model. Supports Unbounded
 * {@link PCollection PCollections}.
 */
@Experimental
public class DirectRunner
    extends PipelineRunner<DirectPipelineResult> {
  /**
   * The default set of transform overrides to use in the {@link DirectRunner}.
   *
   * <p>A transform override must have a single-argument constructor that takes an instance of the
   * type of transform it is overriding.
   */
  @SuppressWarnings("rawtypes")
  private static Map<Class<? extends PTransform>, PTransformOverrideFactory>
      defaultTransformOverrides =
          ImmutableMap.<Class<? extends PTransform>, PTransformOverrideFactory>builder()
              .put(CreatePCollectionView.class, new ViewOverrideFactory())
              .put(GroupByKey.class, new DirectGroupByKeyOverrideFactory())
              .put(TestStream.class, new DirectTestStreamFactory())
              .put(Write.Bound.class, new WriteWithShardingFactory())
              .build();

  /**
   * Part of a {@link PCollection}. Elements are output to a bundle, which will cause them to be
   * executed by {@link PTransform PTransforms} that consume the {@link PCollection} this bundle is
   * a part of at a later point. This is an uncommitted bundle and can have elements added to it.
   *
   * @param <T> the type of elements that can be added to this bundle
   */
  interface UncommittedBundle<T> {
    /**
     * Returns the PCollection that the elements of this {@link UncommittedBundle} belong to.
     */
    @Nullable
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
  interface CommittedBundle<T> {
    /**
     * Returns the PCollection that the elements of this bundle belong to.
     */
    @Nullable
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
  private final DirectOptions options;
  private Supplier<ExecutorService> executorServiceSupplier = new FixedThreadPoolSupplier();
  private Supplier<Clock> clockSupplier = new NanosOffsetClockSupplier();

  public static DirectRunner fromOptions(PipelineOptions options) {
    return new DirectRunner(options.as(DirectOptions.class));
  }

  private DirectRunner(DirectOptions options) {
    this.options = options;
  }

  /**
   * Returns the {@link PipelineOptions} used to create this {@link DirectRunner}.
   */
  public DirectOptions getPipelineOptions() {
    return options;
  }

  Supplier<Clock> getClockSupplier() {
    return clockSupplier;
  }

  void setClockSupplier(Supplier<Clock> supplier) {
    this.clockSupplier = supplier;
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
  public DirectPipelineResult run(Pipeline pipeline) {
    ConsumerTrackingPipelineVisitor consumerTrackingVisitor = new ConsumerTrackingPipelineVisitor();
    pipeline.traverseTopologically(consumerTrackingVisitor);
    for (PValue unfinalized : consumerTrackingVisitor.getUnfinalizedPValues()) {
      unfinalized.finishSpecifying();
    }
    @SuppressWarnings("rawtypes")
    KeyedPValueTrackingVisitor keyedPValueVisitor =
        KeyedPValueTrackingVisitor.create(
            ImmutableSet.<Class<? extends PTransform>>of(
                GroupByKey.class, DirectGroupByKeyOnly.class));
    pipeline.traverseTopologically(keyedPValueVisitor);

    DisplayDataValidator.validatePipeline(pipeline);

    EvaluationContext context =
        EvaluationContext.create(
            getPipelineOptions(),
            clockSupplier.get(),
            createBundleFactory(getPipelineOptions()),
            consumerTrackingVisitor.getRootTransforms(),
            consumerTrackingVisitor.getValueToConsumers(),
            consumerTrackingVisitor.getStepNames(),
            consumerTrackingVisitor.getViews());

    // independent executor service for each run
    ExecutorService executorService = executorServiceSupplier.get();

    TransformEvaluatorRegistry registry = TransformEvaluatorRegistry.defaultRegistry(context);
    PipelineExecutor executor =
        ExecutorServiceParallelExecutor.create(
            executorService,
            consumerTrackingVisitor.getValueToConsumers(),
            keyedPValueVisitor.getKeyedPValues(),
            registry,
            defaultModelEnforcements(options),
            context);
    executor.start(consumerTrackingVisitor.getRootTransforms());

    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        pipeline.getAggregatorSteps();
    DirectPipelineResult result =
        new DirectPipelineResult(executor, context, aggregatorSteps);
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
      defaultModelEnforcements(DirectOptions options) {
    ImmutableMap.Builder<Class<? extends PTransform>, Collection<ModelEnforcementFactory>>
        enforcements = ImmutableMap.builder();
    Collection<ModelEnforcementFactory> parDoEnforcements = createParDoEnforcements(options);
    enforcements.put(ParDo.Bound.class, parDoEnforcements);
    enforcements.put(ParDo.BoundMulti.class, parDoEnforcements);
    if (options.isEnforceEncodability()) {
      enforcements.put(
          Read.Unbounded.class,
          ImmutableSet.<ModelEnforcementFactory>of(EncodabilityEnforcementFactory.create()));
      enforcements.put(
          Read.Bounded.class,
          ImmutableSet.<ModelEnforcementFactory>of(EncodabilityEnforcementFactory.create()));
    }
    return enforcements.build();
  }

  private Collection<ModelEnforcementFactory> createParDoEnforcements(
      DirectOptions options) {
    ImmutableList.Builder<ModelEnforcementFactory> enforcements = ImmutableList.builder();
    if (options.isEnforceImmutability()) {
      enforcements.add(ImmutabilityEnforcementFactory.create());
    }
    if (options.isEnforceEncodability()) {
      enforcements.add(EncodabilityEnforcementFactory.create());
    }
    return enforcements.build();
  }

  private BundleFactory createBundleFactory(DirectOptions pipelineOptions) {
    BundleFactory bundleFactory = ImmutableListBundleFactory.create();
    if (pipelineOptions.isEnforceImmutability()) {
      bundleFactory = ImmutabilityCheckingBundleFactory.create(bundleFactory);
    }
    return bundleFactory;
  }

  /**
   * The result of running a {@link Pipeline} with the {@link DirectRunner}.
   *
   * Throws {@link UnsupportedOperationException} for all methods.
   */
  public static class DirectPipelineResult implements PipelineResult {
    private final PipelineExecutor executor;
    private final EvaluationContext evaluationContext;
    private final Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps;
    private State state;

    private DirectPipelineResult(
        PipelineExecutor executor,
        EvaluationContext evaluationContext,
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
      AggregatorContainer aggregators = evaluationContext.getAggregatorContainer();
      Collection<PTransform<?, ?>> steps = aggregatorSteps.get(aggregator);
      final Map<String, T> stepValues = new HashMap<>();
      for (AppliedPTransform<?, ?, ?> transform : evaluationContext.getSteps()) {
        if (steps.contains(transform.getTransform())) {
          T aggregate = aggregators.getAggregate(
              evaluationContext.getStepName(transform), aggregator.getName());
          if (aggregate != null) {
            stepValues.put(transform.getFullName(), aggregate);
          }
        }
      }
      return new AggregatorValues<T>() {
        @Override
        public Map<String, T> getValuesAtSteps() {
          return stepValues;
        }

        @Override
        public String toString() {
          return MoreObjects.toStringHelper(this)
              .add("stepValues", stepValues)
              .toString();
        }
      };
    }

    /**
     * Blocks until the {@link Pipeline} execution represented by this
     * {@link DirectPipelineResult} is complete, returning the terminal state.
     *
     * <p>If the pipeline terminates abnormally by throwing an exception, this will rethrow the
     * exception. Future calls to {@link #getState()} will return
     * {@link org.apache.beam.sdk.PipelineResult.State#FAILED}.
     *
     * <p>NOTE: if the {@link Pipeline} contains an {@link IsBounded#UNBOUNDED unbounded}
     * {@link PCollection}, and the {@link PipelineRunner} was created with
     * {@link DirectOptions#isShutdownUnboundedProducersWithMaxWatermark()} set to false,
     * this method will never return.
     *
     * See also {@link PipelineExecutor#awaitCompletion()}.
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

    @Override
    public State cancel() throws IOException {
      throw new UnsupportedOperationException("DirectPipelineResult does not support cancel.");
    }

    @Override
    public State waitUntilFinish() throws IOException {
      return waitUntilFinish(Duration.millis(-1));
    }

    @Override
    public State waitUntilFinish(Duration duration) throws IOException {
      throw new UnsupportedOperationException(
          "DirectPipelineResult does not support waitUntilFinish.");
    }
  }

  /**
   * A {@link Supplier} that creates a {@link ExecutorService} based on
   * {@link Executors#newFixedThreadPool(int)}.
   */
  private static class FixedThreadPoolSupplier implements Supplier<ExecutorService> {
    @Override
    public ExecutorService get() {
      return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }
  }


  /**
   * A {@link Supplier} that creates a {@link NanosOffsetClock}.
   */
  private static class NanosOffsetClockSupplier implements Supplier<Clock> {
    @Override
    public Clock get() {
      return NanosOffsetClock.create();
    }
  }
}
