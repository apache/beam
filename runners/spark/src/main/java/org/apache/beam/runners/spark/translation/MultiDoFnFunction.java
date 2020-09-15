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
package org.apache.beam.runners.spark.translation;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.metrics.MetricsContainerStepMapAccumulator;
import org.apache.beam.runners.spark.util.CachedSideInputReader;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.runners.spark.util.SparkSideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.LinkedListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

/**
 * DoFunctions ignore outputs that are not the main output. MultiDoFunctions deal with additional
 * outputs by enriching the underlying data with multiple TupleTags.
 *
 * @param <InputT> Input type for DoFunction.
 * @param <OutputT> Output type for DoFunction.
 */
public class MultiDoFnFunction<InputT, OutputT>
    implements PairFlatMapFunction<Iterator<WindowedValue<InputT>>, TupleTag<?>, WindowedValue<?>> {

  private final MetricsContainerStepMapAccumulator metricsAccum;
  private final String stepName;
  private final DoFn<InputT, OutputT> doFn;
  private transient boolean wasSetupCalled;
  private final SerializablePipelineOptions options;
  private final TupleTag<OutputT> mainOutputTag;
  private final List<TupleTag<?>> additionalOutputTags;
  private final Coder<InputT> inputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;
  private final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final boolean stateful;
  private final DoFnSchemaInformation doFnSchemaInformation;
  private final Map<String, PCollectionView<?>> sideInputMapping;

  /**
   * @param metricsAccum The Spark {@link AccumulatorV2} that backs the Beam metrics.
   * @param doFn The {@link DoFn} to be wrapped.
   * @param options The {@link SerializablePipelineOptions}.
   * @param mainOutputTag The main output {@link TupleTag}.
   * @param additionalOutputTags Additional {@link TupleTag output tags}.
   * @param inputCoder The coder for the input.
   * @param outputCoders A map of all output coders.
   * @param sideInputs Side inputs used in this {@link DoFn}.
   * @param windowingStrategy Input {@link WindowingStrategy}.
   * @param stateful Stateful {@link DoFn}.
   */
  public MultiDoFnFunction(
      MetricsContainerStepMapAccumulator metricsAccum,
      String stepName,
      DoFn<InputT, OutputT> doFn,
      SerializablePipelineOptions options,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy,
      boolean stateful,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {
    this.metricsAccum = metricsAccum;
    this.stepName = stepName;
    this.doFn = SerializableUtils.clone(doFn);
    this.options = options;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.sideInputs = sideInputs;
    this.windowingStrategy = windowingStrategy;
    this.stateful = stateful;
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideInputMapping = sideInputMapping;
  }

  @Override
  public Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> call(Iterator<WindowedValue<InputT>> iter)
      throws Exception {
    if (!wasSetupCalled && iter.hasNext()) {
      DoFnInvokers.tryInvokeSetupFor(doFn);
      wasSetupCalled = true;
    }

    DoFnOutputManager outputManager = new DoFnOutputManager();

    final InMemoryTimerInternals timerInternals;
    final StepContext context;
    // Now only implements the StatefulParDo in Batch mode.
    Object key = null;

    if (stateful) {
      if (iter.hasNext()) {
        WindowedValue<InputT> currentValue = iter.next();
        key = ((KV) currentValue.getValue()).getKey();
        iter = Iterators.concat(Iterators.singletonIterator(currentValue), iter);
      }
      final InMemoryStateInternals<?> stateInternals = InMemoryStateInternals.forKey(key);
      timerInternals = new InMemoryTimerInternals();
      context =
          new StepContext() {
            @Override
            public StateInternals stateInternals() {
              return stateInternals;
            }

            @Override
            public TimerInternals timerInternals() {
              return timerInternals;
            }
          };
    } else {
      timerInternals = null;
      context = new SparkProcessContext.NoOpStepContext();
    }

    final DoFnRunner<InputT, OutputT> doFnRunner =
        DoFnRunners.simpleRunner(
            options.get(),
            doFn,
            CachedSideInputReader.of(new SparkSideInputReader(sideInputs)),
            outputManager,
            mainOutputTag,
            additionalOutputTags,
            context,
            inputCoder,
            outputCoders,
            windowingStrategy,
            doFnSchemaInformation,
            sideInputMapping);

    DoFnRunnerWithMetrics<InputT, OutputT> doFnRunnerWithMetrics =
        new DoFnRunnerWithMetrics<>(stepName, doFnRunner, metricsAccum);

    return new SparkProcessContext<>(
            doFn,
            doFnRunnerWithMetrics,
            outputManager,
            key,
            stateful ? new TimerDataIterator(timerInternals) : Collections.emptyIterator())
        .processPartition(iter)
        .iterator();
  }

  private static class TimerDataIterator implements Iterator<TimerInternals.TimerData> {

    private final InMemoryTimerInternals timerInternals;
    private boolean hasAdvance;
    private TimerInternals.TimerData timerData;

    TimerDataIterator(InMemoryTimerInternals timerInternals) {
      this.timerInternals = timerInternals;
    }

    @Override
    public boolean hasNext() {

      // Advance
      if (!hasAdvance) {
        try {
          // Finish any pending windows by advancing the input watermark to infinity.
          timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);
          // Finally, advance the processing time to infinity to fire any timers.
          timerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
          timerInternals.advanceSynchronizedProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        hasAdvance = true;
      }

      // Get timer data
      return (timerData = timerInternals.removeNextEventTimer()) != null
          || (timerData = timerInternals.removeNextProcessingTimer()) != null
          || (timerData = timerInternals.removeNextSynchronizedProcessingTimer()) != null;
    }

    @Override
    public TimerInternals.TimerData next() {
      if (timerData == null) {
        throw new NoSuchElementException();
      } else {
        return timerData;
      }
    }

    @Override
    public void remove() {
      throw new RuntimeException("TimerDataIterator not support remove!");
    }
  }

  private class DoFnOutputManager
      implements SparkProcessContext.SparkOutputManager<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final Multimap<TupleTag<?>, WindowedValue<?>> outputs = LinkedListMultimap.create();

    @Override
    public void clear() {
      outputs.clear();
    }

    @Override
    public Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> iterator() {
      Iterator<Map.Entry<TupleTag<?>, WindowedValue<?>>> entryIter = outputs.entries().iterator();
      return Iterators.transform(entryIter, this.entryToTupleFn());
    }

    private <K, V> Function<Map.Entry<K, V>, Tuple2<K, V>> entryToTupleFn() {
      return en -> new Tuple2<>(en.getKey(), en.getValue());
    }

    @Override
    public synchronized <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      outputs.put(tag, output);
    }
  }
}
