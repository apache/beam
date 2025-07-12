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
package org.apache.beam.runners.spark.translation.streaming;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.metrics.MetricsContainerStepMapAccumulator;
import org.apache.beam.runners.spark.stateful.SparkStateInternals;
import org.apache.beam.runners.spark.stateful.SparkTimerInternals;
import org.apache.beam.runners.spark.stateful.StateAndTimers;
import org.apache.beam.runners.spark.translation.AbstractInOutIterator;
import org.apache.beam.runners.spark.translation.DoFnRunnerWithMetrics;
import org.apache.beam.runners.spark.translation.SparkInputDataProcessor;
import org.apache.beam.runners.spark.translation.SparkProcessContext;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.runners.spark.util.SideInputReaderFactory;
import org.apache.beam.runners.spark.util.TimerUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.spark.streaming.State;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.collect.Iterators;
import scala.Option;
import scala.Tuple2;
import scala.runtime.AbstractFunction3;

/**
 * A function to handle stateful processing in Apache Beam's SparkRunner. This class processes
 * stateful DoFn operations by managing state updates in a Spark streaming context.
 *
 * <p>Current Implementation Status:
 *
 * <ul>
 *   <li>State: Fully implemented and supported through {@link SparkStateInternals}
 *   <li>Timers: Processing time timers are now supported through {@link SparkTimerInternals}.
 * </ul>
 *
 * @param <KeyT> The type of the key in the input KV pairs
 * @param <ValueT> The type of the value in the input KV pairs
 * @param <InputT> The input type, must be a KV of KeyT and ValueT
 * @param <OutputT> The output type produced by the DoFn
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ParDoStateUpdateFn<KeyT, ValueT, InputT extends KV<KeyT, ValueT>, OutputT>
    extends AbstractFunction3<
        /*Serialized KeyT*/ ByteArray,
        Option</*Serialized WindowedValue<ValueT>*/ byte[]>,
        /*State*/ State<StateAndTimers>,
        List<Tuple2</*Output Tag*/ TupleTag<?>, /*Serialized WindowedValue<OutputT>*/ byte[]>>>
    implements Serializable {

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(ParDoStateUpdateFn.class);

  private final MetricsContainerStepMapAccumulator metricsAccum;
  private final String stepName;
  private final DoFn<InputT, OutputT> doFn;
  private final Coder<KeyT> keyCoder;
  private final WindowedValues.FullWindowedValueCoder<ValueT> wvCoder;
  private transient boolean wasSetupCalled;
  private final SerializablePipelineOptions options;
  private final TupleTag<?> mainOutputTag;
  private final List<TupleTag<?>> additionalOutputTags;
  private final Coder<InputT> inputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;
  private final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final DoFnSchemaInformation doFnSchemaInformation;
  private final Map<String, PCollectionView<?>> sideInputMapping;
  // for timer
  private final Map<Integer, GlobalWatermarkHolder.SparkWatermarks> watermarks;
  private final List<Integer> sourceIds;
  private final TimerInternals.TimerDataCoderV2 timerDataCoder;
  // for sideInput
  private final boolean useStreamingSideInput;

  public ParDoStateUpdateFn(
      MetricsContainerStepMapAccumulator metricsAccum,
      String stepName,
      DoFn<InputT, OutputT> doFn,
      Coder<KeyT> keyCoder,
      WindowedValues.FullWindowedValueCoder<ValueT> wvCoder,
      SerializablePipelineOptions options,
      TupleTag<?> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping,
      Map<Integer, GlobalWatermarkHolder.SparkWatermarks> watermarks,
      List<Integer> sourceIds,
      boolean useStreamingSideInput) {
    this.metricsAccum = metricsAccum;
    this.stepName = stepName;
    this.doFn = SerializableUtils.clone(doFn);
    this.options = options;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.keyCoder = keyCoder;
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.wvCoder = wvCoder;
    this.sideInputs = sideInputs;
    this.windowingStrategy = windowingStrategy;
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideInputMapping = sideInputMapping;
    this.watermarks = watermarks;
    this.sourceIds = sourceIds;
    this.timerDataCoder =
        TimerInternals.TimerDataCoderV2.of(windowingStrategy.getWindowFn().windowCoder());
    this.useStreamingSideInput = useStreamingSideInput;
  }

  @Override
  public List<Tuple2</*Output Tag*/ TupleTag<?>, /*Serialized WindowedValue<OutputT>*/ byte[]>>
      apply(ByteArray serializedKey, Option<byte[]> serializedValue, State<StateAndTimers> state) {
    if (serializedValue.isEmpty()) {
      return Lists.newArrayList();
    }

    SparkStateInternals<KeyT> stateInternals;
    final KeyT key = CoderHelpers.fromByteArray(serializedKey.getValue(), this.keyCoder);
    final SparkTimerInternals timerInternals =
        SparkTimerInternals.forStreamFromSources(sourceIds, watermarks);

    if (state.exists()) {
      final StateAndTimers stateAndTimers = state.get();
      stateInternals = SparkStateInternals.forKeyAndState(key, stateAndTimers.getState());
      timerInternals.addTimers(
          SparkTimerInternals.deserializeTimers(stateAndTimers.getTimers(), timerDataCoder));
    } else {
      stateInternals = SparkStateInternals.forKey(key);
    }

    if (!wasSetupCalled) {
      DoFnInvokers.tryInvokeSetupFor(this.doFn, this.options.get());
      this.wasSetupCalled = true;
    }

    SparkInputDataProcessor<InputT, OutputT, Tuple2<TupleTag<?>, WindowedValue<?>>> processor =
        SparkInputDataProcessor.createUnbounded();

    final StepContext context =
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

    final Coder<? extends BoundedWindow> windowCoder =
        windowingStrategy.getWindowFn().windowCoder();

    final StatefulDoFnRunner.CleanupTimer<InputT> cleanUpTimer =
        new StatefulDoFnRunner.TimeInternalsCleanupTimer<>(timerInternals, windowingStrategy);

    final StatefulDoFnRunner.StateCleaner<? extends BoundedWindow> stateCleaner =
        new StatefulDoFnRunner.StateInternalsStateCleaner<>(doFn, stateInternals, windowCoder);

    // simple runner --> stateful runner --> metrics runner
    DoFnRunner<InputT, OutputT> doFnRunner =
        DoFnRunners.simpleRunner(
            options.get(),
            doFn,
            SideInputReaderFactory.create(this.useStreamingSideInput, this.sideInputs),
            processor.getOutputManager(),
            (TupleTag<OutputT>) mainOutputTag,
            additionalOutputTags,
            context,
            inputCoder,
            outputCoders,
            windowingStrategy,
            doFnSchemaInformation,
            sideInputMapping);

    doFnRunner =
        DoFnRunners.defaultStatefulDoFnRunner(
            doFn, inputCoder, doFnRunner, context, windowingStrategy, cleanUpTimer, stateCleaner);

    doFnRunner = new DoFnRunnerWithMetrics<>(stepName, doFnRunner, metricsAccum);

    SparkProcessContext<KeyT, InputT, OutputT> ctx =
        new SparkProcessContext<>(
            stepName, doFn, doFnRunner, key, new SparkTimerInternalsIterator(timerInternals));

    final byte[] byteValue = serializedValue.get();
    @Nullable WindowedValue<ValueT> windowedValue;
    @Nullable WindowedValue<KV<KeyT, ValueT>> keyedWindowedValue;
    Iterator<WindowedValue<KV<KeyT, ValueT>>> iterator = Iterators.emptyIterator();
    if (byteValue.length > 0) {
      windowedValue = CoderHelpers.fromByteArray(byteValue, this.wvCoder);
      keyedWindowedValue = windowedValue.withValue(KV.of(key, windowedValue.getValue()));
      iterator = Lists.newArrayList(keyedWindowedValue).iterator();
    }

    final AbstractInOutIterator outputIterator =
        (AbstractInOutIterator) processor.createOutputIterator((Iterator) iterator, ctx);

    final List<Tuple2<TupleTag<?>, WindowedValue<?>>> resultList =
        Lists.newArrayList(outputIterator);

    TimerUtils.triggerExpiredTimers(timerInternals, windowingStrategy, outputIterator);

    final Collection<byte[]> serializedTimers =
        SparkTimerInternals.serializeTimers(timerInternals.getTimers(), timerDataCoder);

    state.update(StateAndTimers.of(stateInternals.getState(), serializedTimers));

    return (List<Tuple2<TupleTag<?>, byte[]>>)
        (List)
            resultList.stream()
                .map(
                    (Tuple2<TupleTag<?>, WindowedValue<?>> e) -> {
                      final TupleTag<OutputT> tupleTag = (TupleTag<OutputT>) e._1();
                      final Coder<OutputT> outputCoder =
                          (Coder<OutputT>) outputCoders.get(tupleTag);

                      @SuppressWarnings("nullness")
                      final WindowedValues.FullWindowedValueCoder<OutputT> outputWindowCoder =
                          WindowedValues.FullWindowedValueCoder.of(outputCoder, windowCoder);

                      return Tuple2.apply(
                          tupleTag,
                          CoderHelpers.toByteArray((WindowedValue) e._2(), outputWindowCoder));
                    })
                .collect(Collectors.toList());
  }

  /**
   * An iterator implementation that processes timers from {@link SparkTimerInternals}. This
   * iterator is used in stateful processing to handle timer-based operations.
   */
  @SuppressWarnings("nullness")
  public static class SparkTimerInternalsIterator implements Iterator<TimerInternals.TimerData> {

    private final SparkTimerInternals delegate;

    private SparkTimerInternalsIterator(SparkTimerInternals sparkTimerInternals) {
      this.delegate = sparkTimerInternals;
    }

    @Override
    public boolean hasNext() {
      return this.delegate.hasNextProcessingTimer();
    }

    @Override
    public TimerInternals.TimerData next() {
      final TimerInternals.TimerData nextTimer = this.delegate.getNextProcessingTimer();
      if (nextTimer == null) {
        throw new NoSuchElementException();
      }

      return nextTimer;
    }

    public void deleteTimer(TimerInternals.TimerData timerData) {
      this.delegate.deleteTimer(timerData);
    }
  }
}
