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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.CachedSideInputReader;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.NoOpStepContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.joda.time.Instant;
import scala.Serializable;

/**
 * Factory to create a {@link DoFnRunner}. The factory supports fusing multiple {@link DoFnRunner
 * runners} into a single one.
 */
abstract class DoFnRunnerFactory<InT, T> implements Serializable {

  interface DoFnRunnerWithTeardown<InT, T> extends DoFnRunner<InT, T> {
    void teardown();
  }

  /**
   * Creates a runner that is ready to process elements.
   *
   * <p>Both, {@link org.apache.beam.sdk.transforms.reflect.DoFnInvoker#invokeSetup setup} and
   * {@link DoFnRunner#startBundle()} are already invoked by the factory.
   */
  abstract DoFnRunnerWithTeardown<InT, T> create(
      PipelineOptions options, MetricsAccumulator metrics, OutputManager output);

  /**
   * Fuses the factory for the following {@link DoFnRunner} into a single factory that processes
   * both DoFns in a single step.
   */
  abstract <T2> DoFnRunnerFactory<InT, T2> fuse(DoFnRunnerFactory<T, T2> next);

  static <InT, T> DoFnRunnerFactory<InT, T> simple(
      AppliedPTransform<PCollection<? extends InT>, ?, ParDo.MultiOutput<InT, T>> appliedPT,
      PCollection<InT> input,
      SideInputReader sideInputReader,
      boolean filterMainOutput) {
    return new SimpleRunnerFactory<>(appliedPT, input, sideInputReader, filterMainOutput);
  }

  /**
   * Factory creating a {@link org.apache.beam.runners.core.SimpleDoFnRunner SimpleRunner} with
   * metrics support.
   */
  private static class SimpleRunnerFactory<InT, T> extends DoFnRunnerFactory<InT, T> {
    private final String stepName;
    private final DoFn<InT, T> doFn;
    private final DoFnSchemaInformation doFnSchema;
    private final Coder<InT> coder;
    private final WindowingStrategy<?, ?> windowingStrategy;
    private final TupleTag<T> mainOutput;
    private final List<TupleTag<?>> additionalOutputs;
    private final Map<TupleTag<?>, Coder<?>> outputCoders;
    private final Map<String, PCollectionView<?>> sideInputs;
    private final SideInputReader sideInputReader;
    private final boolean filterMainOutput;

    SimpleRunnerFactory(
        AppliedPTransform<PCollection<? extends InT>, ?, ParDo.MultiOutput<InT, T>> appliedPT,
        PCollection<InT> input,
        SideInputReader sideInputReader,
        boolean filterMainOutput) {
      this.stepName = appliedPT.getFullName();
      this.doFn = appliedPT.getTransform().getFn();
      this.doFnSchema = ParDoTranslation.getSchemaInformation(appliedPT);
      this.coder = input.getCoder();
      this.windowingStrategy = input.getWindowingStrategy();
      this.mainOutput = appliedPT.getTransform().getMainOutputTag();
      this.additionalOutputs = additionalOutputs(appliedPT.getTransform());
      this.outputCoders = coders(appliedPT.getOutputs(), mainOutput);
      this.sideInputs = appliedPT.getTransform().getSideInputs();
      this.sideInputReader = sideInputReader;
      this.filterMainOutput = filterMainOutput;
    }

    @Override
    <T2> DoFnRunnerFactory<InT, T2> fuse(DoFnRunnerFactory<T, T2> next) {
      return new FusedRunnerFactory<>(Lists.newArrayList(this, next));
    }

    @Override
    DoFnRunnerWithTeardown<InT, T> create(
        PipelineOptions options, MetricsAccumulator metrics, OutputManager output) {
      DoFnRunner<InT, T> simpleRunner =
          DoFnRunners.simpleRunner(
              options,
              doFn,
              CachedSideInputReader.of(sideInputReader, sideInputs.values()),
              filterMainOutput ? new FilteredOutput(output, mainOutput) : output,
              mainOutput,
              additionalOutputs,
              new NoOpStepContext(),
              coder,
              outputCoders,
              windowingStrategy,
              doFnSchema,
              sideInputs);
      DoFnRunnerWithTeardown<InT, T> runner =
          new DoFnRunnerWithMetrics<>(stepName, simpleRunner, metrics);
      // Invoke setup and then startBundle before returning the runner
      DoFnInvokers.tryInvokeSetupFor(doFn, options);
      try {
        runner.startBundle();
      } catch (RuntimeException re) {
        DoFnInvokers.invokerFor(doFn).invokeTeardown();
        throw re;
      }
      return runner;
    }

    /**
     * Delegate {@link OutputManager} that only forwards outputs matching the provided tag. This is
     * used in cases where unused, obsolete outputs get dropped to avoid unnecessary caching.
     */
    private static class FilteredOutput implements OutputManager {
      final OutputManager outputManager;
      final TupleTag<?> tupleTag;

      FilteredOutput(OutputManager outputManager, TupleTag<?> tupleTag) {
        this.outputManager = outputManager;
        this.tupleTag = tupleTag;
      }

      @Override
      public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
        if (tupleTag.equals(tag)) {
          outputManager.output(tag, output);
        }
      }
    }

    private static Map<TupleTag<?>, Coder<?>> coders(
        Map<TupleTag<?>, PCollection<?>> pCols, TupleTag<?> main) {
      if (pCols.size() == 1) {
        return Collections.singletonMap(main, Iterables.getOnlyElement(pCols.values()).getCoder());
      }
      Map<TupleTag<?>, Coder<?>> coders = Maps.newHashMapWithExpectedSize(pCols.size());
      for (Map.Entry<TupleTag<?>, PCollection<?>> e : pCols.entrySet()) {
        coders.put(e.getKey(), e.getValue().getCoder());
      }
      return coders;
    }

    private static List<TupleTag<?>> additionalOutputs(ParDo.MultiOutput<?, ?> transform) {
      List<TupleTag<?>> tags = transform.getAdditionalOutputTags().getAll();
      return tags.isEmpty() ? Collections.emptyList() : new ArrayList<>(tags);
    }
  }

  /**
   * Factory that produces a fused runner consisting of multiple chained {@link DoFn DoFns}. Outputs
   * are directly forwarded to the next runner without buffering inbetween.
   */
  private static class FusedRunnerFactory<InT, T> extends DoFnRunnerFactory<InT, T> {
    private final List<DoFnRunnerFactory<?, ?>> factories;

    FusedRunnerFactory(List<DoFnRunnerFactory<?, ?>> factories) {
      this.factories = factories;
    }

    @Override
    DoFnRunnerWithTeardown<InT, T> create(
        PipelineOptions options, MetricsAccumulator metrics, OutputManager output) {
      return new FusedRunner<>(options, metrics, output, factories);
    }

    @Override
    <T2> DoFnRunnerFactory<InT, T2> fuse(DoFnRunnerFactory<T, T2> next) {
      factories.add(next);
      return (DoFnRunnerFactory<InT, T2>) this;
    }

    private static class FusedRunner<InT, T> implements DoFnRunnerWithTeardown<InT, T> {
      final DoFnRunnerWithTeardown<?, ?>[] runners;

      FusedRunner(
          PipelineOptions options,
          MetricsAccumulator metrics,
          OutputManager output,
          List<DoFnRunnerFactory<?, ?>> factories) {
        runners = new DoFnRunnerWithTeardown<?, ?>[factories.size()];
        runners[runners.length - 1] =
            factories.get(runners.length - 1).create(options, metrics, output);
        for (int i = runners.length - 2; i >= 0; i--) {
          runners[i] = factories.get(i).create(options, metrics, new FusedOutput(runners[i + 1]));
        }
      }

      /** {@link OutputManager} that forwards output directly to the next runner. */
      private static class FusedOutput implements OutputManager {
        final DoFnRunnerWithTeardown<?, ?> runner;

        FusedOutput(DoFnRunnerWithTeardown<?, ?> runner) {
          this.runner = runner;
        }

        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
          runner.processElement((WindowedValue) output);
        }
      }

      @Override
      public void startBundle() {
        for (int i = 0; i < runners.length; i++) {
          runners[i].startBundle();
        }
      }

      @Override
      public void processElement(WindowedValue<InT> elem) {
        runners[0].processElement((WindowedValue) elem);
      }

      @Override
      public <KeyT> void onTimer(
          String timerId,
          String timerFamilyId,
          KeyT key,
          BoundedWindow window,
          Instant timestamp,
          Instant outputTimestamp,
          TimeDomain timeDomain) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void finishBundle() {
        for (int i = 0; i < runners.length; i++) {
          runners[i].finishBundle();
        }
      }

      @Override
      public DoFn<InT, T> getFn() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void teardown() {
        for (int i = 0; i < runners.length; i++) {
          runners[i].teardown();
        }
      }
    }
  }
}
