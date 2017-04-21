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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.SplittableParDo.GBKIntoKeyedWorkItems;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.direct.DirectRunner.DirectPipelineResult;
import org.apache.beam.runners.direct.TestStreamEvaluatorFactory.DirectTestStreamFactory;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An In-Memory implementation of the Dataflow Programming Model. Supports Unbounded
 * {@link PCollection PCollections}.
 */
public class DirectRunner extends PipelineRunner<DirectPipelineResult> {
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
     * Returns the minimum timestamp among all of the elements of this {@link CommittedBundle}.
     */
    Instant getMinTimestamp();

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
     * <p>The value of the {@link #getSynchronizedProcessingOutputWatermark() synchronized
     * processing output watermark} of the returned {@link CommittedBundle} is equal to the value
     * returned from the current bundle. This is used to ensure a {@link PTransform} that could not
     * complete processing on input elements properly holds the synchronized processing time to the
     * appropriate value.
     */
    CommittedBundle<T> withElements(Iterable<WindowedValue<T>> elements);
  }

  /**
   * A {@link PCollectionViewWriter} is responsible for writing contents of a {@link PCollection} to
   * a storage mechanism that can be read from while constructing a {@link PCollectionView}.
   *
   * @param <ElemT> the type of elements the input {@link PCollection} contains.
   * @param <ViewT> the type of the PCollectionView this writer writes to.
   */
  interface PCollectionViewWriter<ElemT, ViewT> {
    void add(Iterable<WindowedValue<ElemT>> values);
  }

  /** The set of {@link PTransform PTransforms} that execute a UDF. Useful for some enforcements. */
  private static final Set<Class<? extends PTransform>> CONTAINS_UDF =
      ImmutableSet.of(
          Read.Bounded.class, Read.Unbounded.class, ParDo.SingleOutput.class, MultiOutput.class);

  enum Enforcement {
    ENCODABILITY {
      @Override
      public boolean appliesTo(PCollection<?> collection, DirectGraph graph) {
        return true;
      }
    },
    IMMUTABILITY {
      @Override
      public boolean appliesTo(PCollection<?> collection, DirectGraph graph) {
        return CONTAINS_UDF.contains(graph.getProducer(collection).getTransform().getClass());
      }
    };

    public abstract boolean appliesTo(PCollection<?> collection, DirectGraph graph);

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Utilities for creating enforcements
    public static Set<Enforcement> enabled(DirectOptions options) {
      EnumSet<Enforcement> enabled = EnumSet.noneOf(Enforcement.class);
      if (options.isEnforceEncodability()) {
        enabled.add(ENCODABILITY);
      }
      if (options.isEnforceImmutability()) {
        enabled.add(IMMUTABILITY);
      }
      return Collections.unmodifiableSet(enabled);
    }

    public static BundleFactory bundleFactoryFor(Set<Enforcement> enforcements, DirectGraph graph) {
      BundleFactory bundleFactory =
          enforcements.contains(Enforcement.ENCODABILITY)
              ? CloningBundleFactory.create()
              : ImmutableListBundleFactory.create();
      if (enforcements.contains(Enforcement.IMMUTABILITY)) {
        bundleFactory = ImmutabilityCheckingBundleFactory.create(bundleFactory, graph);
      }
      return bundleFactory;
    }

    @SuppressWarnings("rawtypes")
    private static Map<Class<? extends PTransform>, Collection<ModelEnforcementFactory>>
        defaultModelEnforcements(Set<Enforcement> enabledEnforcements) {
      ImmutableMap.Builder<Class<? extends PTransform>, Collection<ModelEnforcementFactory>>
          enforcements = ImmutableMap.builder();
      ImmutableList.Builder<ModelEnforcementFactory> enabledParDoEnforcements =
          ImmutableList.builder();
      if (enabledEnforcements.contains(Enforcement.IMMUTABILITY)) {
        enabledParDoEnforcements.add(ImmutabilityEnforcementFactory.create());
      }
      Collection<ModelEnforcementFactory> parDoEnforcements = enabledParDoEnforcements.build();
      enforcements.put(ParDo.SingleOutput.class, parDoEnforcements);
      enforcements.put(MultiOutput.class, parDoEnforcements);
      return enforcements.build();
    }

  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  private final DirectOptions options;
  private final Set<Enforcement> enabledEnforcements;
  private Supplier<Clock> clockSupplier = new NanosOffsetClockSupplier();

  public static DirectRunner fromOptions(PipelineOptions options) {
    return new DirectRunner(options.as(DirectOptions.class));
  }

  private DirectRunner(DirectOptions options) {
    this.options = options;
    this.enabledEnforcements = Enforcement.enabled(options);
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
  public DirectPipelineResult run(Pipeline pipeline) {
    pipeline.replaceAll(defaultTransformOverrides());
    MetricsEnvironment.setMetricsSupported(true);
    DirectGraphVisitor graphVisitor = new DirectGraphVisitor();
    pipeline.traverseTopologically(graphVisitor);

    @SuppressWarnings("rawtypes")
    KeyedPValueTrackingVisitor keyedPValueVisitor = KeyedPValueTrackingVisitor.create();
    pipeline.traverseTopologically(keyedPValueVisitor);

    DisplayDataValidator.validatePipeline(pipeline);

    DirectGraph graph = graphVisitor.getGraph();
    EvaluationContext context =
        EvaluationContext.create(
            getPipelineOptions(),
            clockSupplier.get(),
            Enforcement.bundleFactoryFor(enabledEnforcements, graph),
            graph,
            keyedPValueVisitor.getKeyedPValues());

    RootProviderRegistry rootInputProvider = RootProviderRegistry.defaultRegistry(context);
    TransformEvaluatorRegistry registry = TransformEvaluatorRegistry.defaultRegistry(context);
    PipelineExecutor executor =
        ExecutorServiceParallelExecutor.create(
            options.getTargetParallelism(), graph,
            rootInputProvider,
            registry,
            Enforcement.defaultModelEnforcements(enabledEnforcements),
            context);
    executor.start(graph.getRootTransforms());

    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        pipeline.getAggregatorSteps();
    DirectPipelineResult result = new DirectPipelineResult(executor, context, aggregatorSteps);
    if (options.isBlockOnRun()) {
      try {
        result.waitUntilFinish();
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

  /**
   * The default set of transform overrides to use in the {@link DirectRunner}.
   *
   * <p>The order in which overrides is applied is important, as some overrides are expanded into a
   * composite. If the composite contains {@link PTransform PTransforms} which are also overridden,
   * these PTransforms must occur later in the iteration order. {@link ImmutableMap} has an
   * iteration order based on the order at which elements are added to it.
   */
  @SuppressWarnings("rawtypes")
  private List<PTransformOverride> defaultTransformOverrides() {
    return ImmutableList.<PTransformOverride>builder()
        .add(
            PTransformOverride.of(
                PTransformMatchers.writeWithRunnerDeterminedSharding(),
                new WriteWithShardingFactory())) /* Uses a view internally. */
        .add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(CreatePCollectionView.class),
                new ViewOverrideFactory())) /* Uses pardos and GBKs */
        .add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(TestStream.class),
                new DirectTestStreamFactory(this))) /* primitive */
        // SplittableParMultiDo is implemented in terms of nonsplittable simple ParDos and extra
        // primitives
        .add(
            PTransformOverride.of(
                PTransformMatchers.splittableParDoMulti(), new ParDoMultiOverrideFactory()))
        // state and timer pardos are implemented in terms of simple ParDos and extra primitives
        .add(
            PTransformOverride.of(
                PTransformMatchers.stateOrTimerParDoMulti(), new ParDoMultiOverrideFactory()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(GBKIntoKeyedWorkItems.class),
                new DirectGBKIntoKeyedWorkItemsOverrideFactory())) /* Returns a GBKO */
        .add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(GroupByKey.class),
                new DirectGroupByKeyOverrideFactory())) /* returns two chained primitives. */
        .build();
  }

  /**
   * The result of running a {@link Pipeline} with the {@link DirectRunner}.
   *
   * <p>Throws {@link UnsupportedOperationException} for all methods.
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

    @Override
    public MetricResults metrics() {
      return evaluationContext.getMetrics();
    }

    /**
     * Blocks until the {@link Pipeline} execution represented by this
     * {@link DirectPipelineResult} is complete, returning the terminal state.
     *
     * <p>If the pipeline terminates abnormally by throwing an exception, this will rethrow the
     * exception. Future calls to {@link #getState()} will return
     * {@link org.apache.beam.sdk.PipelineResult.State#FAILED}.
     *
     * <p>See also {@link PipelineExecutor#waitUntilFinish(Duration)}.
     */
    @Override
    public State waitUntilFinish() {
      return waitUntilFinish(Duration.ZERO);
    }

    @Override
    public State cancel() {
      this.state = executor.getPipelineState();
      if (!this.state.isTerminal()) {
        executor.stop();
        this.state = executor.getPipelineState();
      }
      return executor.getPipelineState();
    }

    @Override
    public State waitUntilFinish(Duration duration) {
      State startState = this.state;
      if (!startState.isTerminal()) {
        try {
          state = executor.waitUntilFinish(duration);
        } catch (UserCodeException uce) {
          // Emulates the behavior of Pipeline#run(), where a stack trace caused by a
          // UserCodeException is truncated and replaced with the stack starting at the call to
          // waitToFinish
          throw new Pipeline.PipelineExecutionException(uce.getCause());
        } catch (Exception e) {
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
          if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
          }
          throw new RuntimeException(e);
        }
      }
      return this.state;
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

  private static class ComplexParDoMatcher {
  }
}
