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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.scalaIterator;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayListWithCapacity;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.NoOpStepContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.SparkSideInputReader;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.SideInputBroadcast;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.CachedSideInputReader;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.Fun1;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.Fun2;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.AbstractIterator;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import scala.collection.Iterator;

/**
 * Encapsulates a {@link DoFn} inside a Spark {@link
 * org.apache.spark.api.java.function.MapPartitionsFunction}.
 */
class DoFnMapPartitionsFactory<InT, OutT> implements Serializable {
  private final String stepName;

  private final DoFn<InT, OutT> doFn;
  private final DoFnSchemaInformation doFnSchema;
  private final SerializablePipelineOptions options;

  private final Coder<InT> coder;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final TupleTag<OutT> mainOutput;
  private final List<TupleTag<?>> additionalOutputs;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;

  private final Map<String, PCollectionView<?>> sideInputs;
  private final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputWindows;
  private final SideInputBroadcast broadcastStateData;

  DoFnMapPartitionsFactory(
      String stepName,
      DoFn<InT, OutT> doFn,
      DoFnSchemaInformation doFnSchema,
      SerializablePipelineOptions options,
      PCollection<InT> input,
      TupleTag<OutT> mainOutput,
      Map<TupleTag<?>, PCollection<?>> outputs,
      Map<String, PCollectionView<?>> sideInputs,
      SideInputBroadcast broadcastStateData) {
    this.stepName = stepName;
    this.doFn = doFn;
    this.doFnSchema = doFnSchema;
    this.options = options;
    this.coder = input.getCoder();
    this.windowingStrategy = input.getWindowingStrategy();
    this.mainOutput = mainOutput;
    this.additionalOutputs = additionalOutputs(outputs, mainOutput);
    this.outputCoders = outputCoders(outputs);
    this.sideInputs = sideInputs;
    this.sideInputWindows = sideInputWindows(sideInputs.values());
    this.broadcastStateData = broadcastStateData;
  }

  /** Create the {@link MapPartitionsFunction} using the provided output function. */
  <OutputT extends @NonNull Object> Fun1<Iterator<WindowedValue<InT>>, Iterator<OutputT>> create(
      Fun2<TupleTag<?>, WindowedValue<?>, OutputT> outputFn) {
    return it ->
        it.hasNext()
            ? scalaIterator(new DoFnPartitionIt<>(outputFn, it))
            : (Iterator<OutputT>) Iterator.empty();
  }

  // FIXME Add support for TimerInternals.TimerData
  /**
   * Partition iterator that lazily processes each element from the (input) iterator on demand
   * producing zero, one or more output elements as output (via an internal buffer).
   *
   * <p>When initializing the iterator for a partition {@code setup} followed by {@code startBundle}
   * is called.
   */
  private class DoFnPartitionIt<FnInT extends InT, OutputT> extends AbstractIterator<OutputT> {
    private final Deque<OutputT> buffer;
    private final DoFnRunner<InT, OutT> doFnRunner;
    private final Iterator<WindowedValue<FnInT>> partitionIt;

    private boolean isBundleFinished;

    DoFnPartitionIt(
        Fun2<TupleTag<?>, WindowedValue<?>, OutputT> outputFn,
        Iterator<WindowedValue<FnInT>> partitionIt) {
      this.buffer = new ArrayDeque<>();
      this.doFnRunner = metricsRunner(simpleRunner(outputFn, buffer));
      this.partitionIt = partitionIt;
      // Before starting to iterate over the partition, invoke setup and then startBundle
      DoFnInvokers.tryInvokeSetupFor(doFn, options.get());
      try {
        doFnRunner.startBundle();
      } catch (RuntimeException re) {
        DoFnInvokers.invokerFor(doFn).invokeTeardown();
        throw re;
      }
    }

    @Override
    protected OutputT computeNext() {
      try {
        while (true) {
          if (!buffer.isEmpty()) {
            return buffer.remove();
          }
          if (partitionIt.hasNext()) {
            // grab the next element and process it.
            doFnRunner.processElement((WindowedValue<InT>) partitionIt.next());
          } else {
            if (!isBundleFinished) {
              isBundleFinished = true;
              doFnRunner.finishBundle();
              continue; // finishBundle can produce more output
            }
            DoFnInvokers.invokerFor(doFn).invokeTeardown();
            return endOfData();
          }
        }
      } catch (RuntimeException re) {
        DoFnInvokers.invokerFor(doFn).invokeTeardown();
        throw re;
      }
    }
  }

  private <OutputT> DoFnRunner<InT, OutT> simpleRunner(
      Fun2<TupleTag<?>, WindowedValue<?>, OutputT> outputFn, Deque<OutputT> buffer) {
    OutputManager outputManager =
        new OutputManager() {
          @Override
          public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
            buffer.add(outputFn.apply(tag, output));
          }
        };
    SideInputReader sideInputReader =
        CachedSideInputReader.of(new SparkSideInputReader(sideInputWindows, broadcastStateData));
    return DoFnRunners.simpleRunner(
        options.get(),
        doFn,
        sideInputReader,
        outputManager,
        mainOutput,
        additionalOutputs,
        new NoOpStepContext(),
        coder,
        outputCoders,
        windowingStrategy,
        doFnSchema,
        sideInputs);
  }

  private DoFnRunner<InT, OutT> metricsRunner(DoFnRunner<InT, OutT> runner) {
    return new DoFnRunnerWithMetrics<>(stepName, runner, MetricsAccumulator.getInstance());
  }

  private static Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputWindows(
      Collection<PCollectionView<?>> views) {
    return views.stream().collect(toMap(identity(), DoFnMapPartitionsFactory::windowingStrategy));
  }

  private static WindowingStrategy<?, ?> windowingStrategy(PCollectionView<?> view) {
    PCollection<?> pc = view.getPCollection();
    if (pc == null) {
      throw new IllegalStateException("PCollection not available for " + view);
    }
    return pc.getWindowingStrategy();
  }

  private static List<TupleTag<?>> additionalOutputs(
      Map<TupleTag<?>, PCollection<?>> outputs, TupleTag<?> mainOutput) {
    return outputs.keySet().stream()
        .filter(t -> !t.equals(mainOutput))
        .collect(toCollection(() -> newArrayListWithCapacity(outputs.size() - 1)));
  }

  private static Map<TupleTag<?>, Coder<?>> outputCoders(Map<TupleTag<?>, PCollection<?>> outputs) {
    return outputs.entrySet().stream()
        .collect(toMap(Map.Entry::getKey, e -> e.getValue().getCoder()));
  }
}
