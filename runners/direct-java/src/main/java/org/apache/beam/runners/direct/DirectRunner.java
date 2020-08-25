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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.direct.DirectRunner.DirectPipelineResult;
import org.apache.beam.runners.direct.TestStreamEvaluatorFactory.DirectTestStreamFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;

/**
 * A {@link PipelineRunner} that executes a {@link Pipeline} within the process that constructed the
 * {@link Pipeline}.
 *
 * <p>The {@link DirectRunner} is suitable for running a {@link Pipeline} on small scale, example,
 * and test data, and should be used for ensuring that processing logic is correct. It also is
 * appropriate for executing unit tests and performs additional work to ensure that behavior
 * contained within a {@link Pipeline} does not break assumptions within the Beam model, to improve
 * the ability to execute a {@link Pipeline} at scale on a distributed backend.
 */
public class DirectRunner extends PipelineRunner<DirectPipelineResult> {

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
        return CONTAINS_UDF.contains(
            PTransformTranslation.urnForTransform(graph.getProducer(collection).getTransform()));
      }
    };

    /**
     * The set of {@link PTransform PTransforms} that execute a UDF. Useful for some enforcements.
     */
    private static final Set<String> CONTAINS_UDF =
        ImmutableSet.of(
            PTransformTranslation.READ_TRANSFORM_URN, PTransformTranslation.PAR_DO_TRANSFORM_URN);

    public abstract boolean appliesTo(PCollection<?> collection, DirectGraph graph);

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Utilities for creating enforcements
    static Set<Enforcement> enabled(DirectOptions options) {
      EnumSet<Enforcement> enabled = EnumSet.noneOf(Enforcement.class);
      if (options.isEnforceEncodability()) {
        enabled.add(ENCODABILITY);
      }
      if (options.isEnforceImmutability()) {
        enabled.add(IMMUTABILITY);
      }
      return Collections.unmodifiableSet(enabled);
    }

    static BundleFactory bundleFactoryFor(Set<Enforcement> enforcements, DirectGraph graph) {
      BundleFactory bundleFactory =
          enforcements.contains(Enforcement.ENCODABILITY)
              ? CloningBundleFactory.create()
              : ImmutableListBundleFactory.create();
      if (enforcements.contains(Enforcement.IMMUTABILITY)) {
        bundleFactory = ImmutabilityCheckingBundleFactory.create(bundleFactory, graph);
      }
      return bundleFactory;
    }

    private static Map<String, Collection<ModelEnforcementFactory>> defaultModelEnforcements(
        Set<Enforcement> enabledEnforcements) {
      ImmutableMap.Builder<String, Collection<ModelEnforcementFactory>> enforcements =
          ImmutableMap.builder();
      ImmutableList.Builder<ModelEnforcementFactory> enabledParDoEnforcements =
          ImmutableList.builder();
      if (enabledEnforcements.contains(Enforcement.IMMUTABILITY)) {
        enabledParDoEnforcements.add(ImmutabilityEnforcementFactory.create());
      }
      Collection<ModelEnforcementFactory> parDoEnforcements = enabledParDoEnforcements.build();
      enforcements.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, parDoEnforcements);
      return enforcements.build();
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  private DirectOptions options;
  private final Set<Enforcement> enabledEnforcements;
  private Supplier<Clock> clockSupplier = new NanosOffsetClockSupplier();
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  /** Construct a {@link DirectRunner} from the provided options. */
  public static DirectRunner fromOptions(PipelineOptions options) {
    return new DirectRunner(options.as(DirectOptions.class));
  }

  private DirectRunner(DirectOptions options) {
    this.options = options;
    this.enabledEnforcements = Enforcement.enabled(options);
  }

  Supplier<Clock> getClockSupplier() {
    return clockSupplier;
  }

  void setClockSupplier(Supplier<Clock> supplier) {
    this.clockSupplier = supplier;
  }

  @Override
  public DirectPipelineResult run(Pipeline pipeline) {
    try {
      options =
          MAPPER
              .readValue(MAPPER.writeValueAsBytes(options), PipelineOptions.class)
              .as(DirectOptions.class);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "PipelineOptions specified failed to serialize to JSON.", e);
    }

    pipeline.replaceAll(defaultTransformOverrides());
    MetricsEnvironment.setMetricsSupported(true);
    try {
      DirectGraphVisitor graphVisitor = new DirectGraphVisitor();
      pipeline.traverseTopologically(graphVisitor);

      @SuppressWarnings("rawtypes")
      KeyedPValueTrackingVisitor keyedPValueVisitor = KeyedPValueTrackingVisitor.create();
      pipeline.traverseTopologically(keyedPValueVisitor);

      DisplayDataValidator.validatePipeline(pipeline);
      DisplayDataValidator.validateOptions(options);

      ExecutorService metricsPool =
          Executors.newCachedThreadPool(
              new ThreadFactoryBuilder()
                  .setThreadFactory(MoreExecutors.platformThreadFactory())
                  .setDaemon(false) // otherwise you say you want to leak, please don't!
                  .setNameFormat("direct-metrics-counter-committer")
                  .build());
      DirectGraph graph = graphVisitor.getGraph();
      EvaluationContext context =
          EvaluationContext.create(
              clockSupplier.get(),
              Enforcement.bundleFactoryFor(enabledEnforcements, graph),
              graph,
              keyedPValueVisitor.getKeyedPValues(),
              metricsPool);

      TransformEvaluatorRegistry registry =
          TransformEvaluatorRegistry.javaSdkNativeRegistry(context, options);
      PipelineExecutor executor =
          ExecutorServiceParallelExecutor.create(
              options.getTargetParallelism(),
              registry,
              Enforcement.defaultModelEnforcements(enabledEnforcements),
              context,
              metricsPool);
      executor.start(graph, RootProviderRegistry.javaNativeRegistry(context, options));

      DirectPipelineResult result = new DirectPipelineResult(executor, context);
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
    } finally {
      MetricsEnvironment.setMetricsSupported(false);
    }
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
  @VisibleForTesting
  List<PTransformOverride> defaultTransformOverrides() {
    DirectTestOptions testOptions = options.as(DirectTestOptions.class);
    ImmutableList.Builder<PTransformOverride> builder = ImmutableList.builder();
    if (testOptions.isRunnerDeterminedSharding()) {
      builder.add(
          PTransformOverride.of(
              PTransformMatchers.writeWithRunnerDeterminedSharding(),
              new WriteWithShardingFactory())); /* Uses a view internally. */
    }
    builder =
        builder
            .add(
                PTransformOverride.of(
                    MultiStepCombine.matcher(), MultiStepCombine.Factory.create()))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.urnEqualTo(PTransformTranslation.CREATE_VIEW_TRANSFORM_URN),
                    new ViewOverrideFactory())) /* Uses pardos and GBKs */
            .add(
                PTransformOverride.of(
                    PTransformMatchers.urnEqualTo(PTransformTranslation.TEST_STREAM_TRANSFORM_URN),
                    new DirectTestStreamFactory(this))) /* primitive */
            // SplittableParMultiDo is implemented in terms of nonsplittable simple ParDos and extra
            // primitives
            .add(
                PTransformOverride.of(
                    PTransformMatchers.splittableParDo(), new ParDoMultiOverrideFactory()))
            // state and timer pardos are implemented in terms of simple ParDos and extra primitives
            .add(
                PTransformOverride.of(
                    PTransformMatchers.stateOrTimerParDo(), new ParDoMultiOverrideFactory()))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.urnEqualTo(
                        PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN),
                    new SplittableParDoViaKeyedWorkItems.OverrideFactory()))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.urnEqualTo(SplittableParDo.SPLITTABLE_GBKIKWI_URN),
                    new DirectGBKIntoKeyedWorkItemsOverrideFactory())) /* Returns a GBKO */
            .add(
                PTransformOverride.of(
                    PTransformMatchers.urnEqualTo(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN),
                    new DirectGroupByKeyOverrideFactory())); /* returns two chained primitives. */
    return builder.build();
  }

  /** The result of running a {@link Pipeline} with the {@link DirectRunner}. */
  public static class DirectPipelineResult implements PipelineResult {
    private final PipelineExecutor executor;
    private final EvaluationContext evaluationContext;
    private State state;

    private DirectPipelineResult(PipelineExecutor executor, EvaluationContext evaluationContext) {
      this.executor = executor;
      this.evaluationContext = evaluationContext;
      // Only ever constructed after the executor has started.
      this.state = State.RUNNING;
    }

    @Override
    public State getState() {
      return state;
    }

    @Override
    public MetricResults metrics() {
      return evaluationContext.getMetrics();
    }

    /**
     * {@inheritDoc}.
     *
     * <p>If the pipeline terminates abnormally by throwing an {@link Exception}, this will rethrow
     * the original {@link Exception}. Future calls to {@link #getState()} will return {@link
     * org.apache.beam.sdk.PipelineResult.State#FAILED}.
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

    /**
     * {@inheritDoc}.
     *
     * <p>If the pipeline terminates abnormally by throwing an {@link Exception}, this will rethrow
     * the original {@link Exception}. Future calls to {@link #getState()} will return {@link
     * org.apache.beam.sdk.PipelineResult.State#FAILED}.
     */
    @Override
    public State waitUntilFinish(Duration duration) {
      if (this.state.isTerminal()) {
        return this.state;
      }
      final State endState;
      try {
        endState = executor.waitUntilFinish(duration);
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
      if (endState != null) {
        this.state = endState;
      }
      return endState;
    }
  }

  /** A {@link Supplier} that creates a {@link NanosOffsetClock}. */
  private static class NanosOffsetClockSupplier implements Supplier<Clock> {
    @Override
    public Clock get() {
      return NanosOffsetClock.create();
    }
  }
}
