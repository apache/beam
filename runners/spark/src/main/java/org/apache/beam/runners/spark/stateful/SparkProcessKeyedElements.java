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
package org.apache.beam.runners.spark.stateful;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.SplittableParDo.ProcessKeyedElements;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.stateful.SparkGroupAlsoByWindowViaWindowSet.OutputWindowedValueHolder;
import org.apache.beam.runners.spark.stateful.SparkGroupAlsoByWindowViaWindowSet.StateAndTimers;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.runners.spark.util.TimerUtils;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.dstream.PairDStreamFunctions;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

public class SparkProcessKeyedElements {
  private static final Logger LOG =
      LoggerFactory.getLogger(SparkGroupAlsoByWindowViaWindowSet.class);

  static class UpdateStateByKeyFunction<
          InputT,
          RestrictionT,
          PositionT,
          WatermarkEstimatorStateT,
          OutputT,
          W extends BoundedWindow>
      extends AbstractFunction1<
          Iterator<
              Tuple3<
                  /*byte[]*/ ByteArray,
                  Seq</*WV<KV<I, R>>*/ byte[]>,
                  Option<Tuple2<StateAndTimers, /*WV<O>*/ List<byte[]>>>>>,
          Iterator<Tuple2</*byte[]*/ ByteArray, Tuple2<StateAndTimers, /*WV<O>*/ List<byte[]>>>>>
      implements Serializable {

    private class UpdateStateByKeyOutputIterator
        extends AbstractIterator<
            Tuple2</*byte[]*/ ByteArray, Tuple2<StateAndTimers, /*WV<O>*/ List<byte[]>>>> {

      private final Iterator<
              Tuple3<ByteArray, Seq<byte[]>, Option<Tuple2<StateAndTimers, List<byte[]>>>>>
          input;
      private final SplittableParDoViaKeyedWorkItems.ProcessElements<
              InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
          processElements;

      private SparkStateInternals<byte[]> processPreviousState(
          final Option<Tuple2<StateAndTimers, List<byte[]>>> prevStateAndTimersOpt,
          final byte[] key,
          final SparkTimerInternals timerInternals) {

        final SparkStateInternals<byte[]> stateInternals;

        if (prevStateAndTimersOpt.isEmpty()) {
          // no previous state.
          stateInternals = SparkStateInternals.forKey(key);
        } else {
          // with pre-existing state.
          final StateAndTimers prevStateAndTimers = prevStateAndTimersOpt.get()._1();
          // get state(internals) per key.
          stateInternals = SparkStateInternals.forKeyAndState(key, prevStateAndTimers.getState());

          timerInternals.addTimers(
              SparkTimerInternals.deserializeTimers(
                  prevStateAndTimers.getTimers(), timerDataCoder));
        }

        return stateInternals;
      }

      UpdateStateByKeyOutputIterator(
          final Iterator<
                  Tuple3<ByteArray, Seq<byte[]>, Option<Tuple2<StateAndTimers, List<byte[]>>>>>
              input,
          SplittableParDo.ProcessKeyedElements<
                  InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
              processKeyedElements) {
        this.input = input;
        this.processElements =
            new SplittableParDoViaKeyedWorkItems.ProcessElements(processKeyedElements);
      }

      /**
       * Retrieves the timers that are eligible for processing by {@link
       * org.apache.beam.runners.core.ReduceFnRunner}.
       *
       * @return A collection of timers that are eligible for processing. For a {@link
       *     TimeDomain#EVENT_TIME} timer, this implies that the watermark has passed the timer's
       *     timestamp. For other <code>TimeDomain</code>s (e.g., {@link
       *     TimeDomain#PROCESSING_TIME}), a timer is always considered eligible for processing (no
       *     restrictions).
       */
      private Collection<TimerInternals.TimerData> filterTimersEligibleForProcessing(
          final Collection<TimerInternals.TimerData> timers, final Instant inputWatermark) {
        final Predicate<TimerInternals.TimerData> eligibleForProcessing =
            timer ->
                !timer.getDomain().equals(TimeDomain.EVENT_TIME)
                    || inputWatermark.isAfter(timer.getTimestamp());

        return FluentIterable.from(timers).filter(eligibleForProcessing).toSet();
      }

      @Override
      protected Tuple2</*byte[]*/ ByteArray, Tuple2<StateAndTimers, /*WV<O>*/ List<byte[]>>>
          computeNext() {
        // input iterator is a Spark partition (~bundle), containing keys and their
        // (possibly) previous-state and (possibly) new data.
        while (input.hasNext()) {

          // for each element in the partition:
          final Tuple3<ByteArray, Seq<byte[]>, Option<Tuple2<StateAndTimers, List<byte[]>>>> next =
              input.next();

          final ByteArray encodedKey = next._1();
          final Seq<byte[]> encodedElements = next._2();
          final Option<Tuple2<StateAndTimers, List<byte[]>>> prevStateAndTimersOpt = next._3();

          final byte[] key = CoderHelpers.fromByteArray(encodedKey.getValue(), ByteArrayCoder.of());

          final Map<Integer, GlobalWatermarkHolder.SparkWatermarks> watermarks =
              GlobalWatermarkHolder.get(
                  SparkGroupAlsoByWindowViaWindowSet.getBatchDuration(options));

          final SparkTimerInternals timerInternals =
              SparkTimerInternals.forStreamFromSources(sourceIds, watermarks);

          final SparkStateInternals<byte[]> stateInternals =
              processPreviousState(prevStateAndTimersOpt, key, timerInternals);

          LOG.info(encodedKey.hashCode() + ": timerInternals: " + timerInternals.getTimers());
          LOG.info(encodedKey.hashCode() + ": stateInternals: " + stateInternals.getState());

          final OutputWindowedValueHolder<OutputT> outputHolder = new OutputWindowedValueHolder<>();

          SplittableParDoViaKeyedWorkItems.ProcessFn<
                  InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
              processFn = processElements.newProcessFn(processElements.getFn());
          DoFnInvokers.tryInvokeSetupFor(processFn);
          processFn.setStateInternalsFactory(unusedKey -> stateInternals);
          processFn.setTimerInternalsFactory(unusedKey -> timerInternals);
          processFn.setProcessElementInvoker(
              new OutputAndTimeBoundedSplittableProcessElementInvoker<>(
                  processElements.getFn(),
                  options.get(),
                  outputHolder,
                  NullSideInputReader.empty(),
                  Executors.newSingleThreadScheduledExecutor(Executors.defaultThreadFactory()),
                  10000,
                  org.joda.time.Duration.standardSeconds(10),
                  () -> {
                    throw new UnsupportedOperationException("BundleFinalizer unsupported in Spark");
                  }));

          final StepContext stepContext =
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

          DoFnRunner<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> fnRunner =
              DoFnRunners.simpleRunner(
                  options.get(),
                  processFn,
                  NullSideInputReader.of(Collections.emptyList()),
                  new OutputManager() {
                    @Override
                    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
                      if (processElements.getMainOutputTag().equals(tag)) {
                        outputHolder.outputWindowedValue(
                            (OutputT) output.getValue(),
                            output.getTimestamp(),
                            output.getWindows(),
                            output.getPane());
                      } else {
                        outputHolder.outputWindowedValue(
                            tag,
                            output.getValue(),
                            output.getTimestamp(),
                            output.getWindows(),
                            output.getPane());
                      }
                    }
                  },
                  processElements.getMainOutputTag(),
                  Collections.emptyList(),
                  stepContext,
                  null,
                  Collections.emptyMap(),
                  windowingStrategy,
                  DoFnSchemaInformation.create(),
                  Collections.emptyMap());

          try {
            // advance the watermark to HWM to fire by timers.
            LOG.info(
                encodedKey.hashCode() + ": timerInternals before advance are {}",
                timerInternals.toString());

            // store the highWatermark as the new inputWatermark to calculate triggers
            timerInternals.advanceWatermark();

            final Collection<TimerInternals.TimerData> timersEligibleForProcessing =
                filterTimersEligibleForProcessing(
                    timerInternals.getTimers(), timerInternals.currentInputWatermarkTime());

            LOG.info(
                encodedKey.hashCode() + ": timers eligible for processing are {}",
                timersEligibleForProcessing);

            // Note that at this point, the watermark has already advanced since
            // timerInternals.advanceWatermark() has been called and the highWatermark
            // is now stored as the new inputWatermark, according to which triggers are
            // calculated.
            // Note 2: The implicit contract between the runner and reduceFnRunner is that
            // event_time based triggers are only delivered if the watermark has passed their
            // timestamp.
            // Note 3: Timer cleanups are performed by the GC timer scheduled by reduceFnRunner as
            // part of processing timers.
            // Note 4: Even if a given timer is deemed eligible for processing, it does not
            // necessarily mean that it will actually fire (firing is determined by the trigger
            // itself, not the TimerInternals/TimerData objects).
            for (TimerInternals.TimerData eligibleTimer : timersEligibleForProcessing) {
              fnRunner.processElement(
                  WindowedValue.valueInGlobalWindow(
                      KeyedWorkItems.timersWorkItem(
                          key, Collections.singletonList(eligibleTimer))));
            }
          } catch (final Exception e) {
            throw new RuntimeException("Failed to process ProcessKeyedElements onTimer.", e);
          }

          if (!encodedElements.isEmpty()) {
            // new input for key.
            try {
              final Iterable<WindowedValue<KV<InputT, RestrictionT>>> elements =
                  FluentIterable.from(JavaConversions.asJavaIterable(encodedElements))
                      .transform(bytes -> CoderHelpers.fromByteArray(bytes, wvInputCoder));

              LOG.info(encodedKey.hashCode() + ": input elements: {}", elements);

              fnRunner.processElement(
                  WindowedValue.valueInGlobalWindow(
                      KeyedWorkItems.elementsWorkItem(key, elements)));
            } catch (final Exception e) {
              throw new RuntimeException("Failed to process element with ReduceFnRunner", e);
            }
          } else if (stateInternals.getState().isEmpty()) {
            // no input and no state -> GC evict now.
            continue;
          }

          // obtain output, if fired.
          final List<WindowedValue<OutputT>> outputs = outputHolder.getWindowedValues();

          if (!outputs.isEmpty() || !stateInternals.getState().isEmpty()) {

            TimerUtils.dropExpiredTimers(timerInternals, windowingStrategy);

            // empty outputs are filtered later using DStream filtering
            final StateAndTimers updated =
                new StateAndTimers(
                    stateInternals.getState(),
                    SparkTimerInternals.serializeTimers(
                        timerInternals.getTimers(), timerDataCoder));

            /*
            Not something we want to happen in production, but is very helpful
            when debugging - TRACE.
             */
            LOG.info(
                encodedKey.hashCode() + ": output elements are {}", Joiner.on(", ").join(outputs));

            // persist Spark's state by outputting.
            final List<byte[]> serOutput = CoderHelpers.toByteArrays(outputs, wvOutputCoder);
            return new Tuple2<>(encodedKey, new Tuple2<>(updated, serOutput));
          }
          // an empty state with no output, can be evicted completely - do nothing.
        }
        return endOfData();
      }
    }

    private final ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
        processKeyedElements;
    private final FullWindowedValueCoder<KV<InputT, RestrictionT>> wvInputCoder;
    private final List<Integer> sourceIds;
    private final TimerInternals.TimerDataCoderV2 timerDataCoder;
    private final WindowingStrategy<?, W> windowingStrategy;
    private final SerializablePipelineOptions options;
    private final String logPrefix;
    private final Coder<WindowedValue<OutputT>> wvOutputCoder;

    UpdateStateByKeyFunction(
        final ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
            processKeyedElements,
        final List<Integer> sourceIds,
        final WindowingStrategy<?, W> windowingStrategy,
        final FullWindowedValueCoder<KV<InputT, RestrictionT>> wvInputCoder,
        final FullWindowedValueCoder<OutputT> wvOutputCoder,
        final SerializablePipelineOptions options,
        final String logPrefix) {
      this.processKeyedElements = processKeyedElements;
      this.wvInputCoder = wvInputCoder;
      this.sourceIds = sourceIds;
      this.timerDataCoder = timerDataCoderOf(windowingStrategy);
      this.windowingStrategy = windowingStrategy;
      this.options = options;
      this.logPrefix = logPrefix;
      this.wvOutputCoder = wvOutputCoder;
    }

    @Override
    public Iterator<
            Tuple2</*byte[]*/ ByteArray, Tuple2<StateAndTimers, /*WV<OutputT>*/ List<byte[]>>>>
        apply(
            final Iterator<
                    Tuple3<
                        /*byte[]*/ ByteArray,
                        Seq</*WV<InputT>*/ byte[]>,
                        Option<Tuple2<StateAndTimers, /*WV<OutputT>*/ List<byte[]>>>>>
                input) {
      // --- ACTUAL STATEFUL OPERATION:
      //
      // Input Iterator: the partition (~bundle) of a co-grouping of the input
      // and the previous state (if exists).
      //
      // Output Iterator: the output key, and the updated state.
      //
      // possible input scenarios for (K, Seq, Option<S>):
      // (1) Option<S>.isEmpty: new data with no previous state.
      // (2) Seq.isEmpty: no new data, but evaluating previous state (timer-like behaviour).
      // (3) Seq.nonEmpty && Option<S>.isDefined: new data with previous state.
      return scala.collection.JavaConversions.asScalaIterator(
          new UpdateStateByKeyOutputIterator(input, processKeyedElements));
    }
  }

  private static <W extends BoundedWindow> TimerInternals.TimerDataCoderV2 timerDataCoderOf(
      final WindowingStrategy<?, W> windowingStrategy) {
    return TimerInternals.TimerDataCoderV2.of(windowingStrategy.getWindowFn().windowCoder());
  }

  public static <
          InputT,
          OutputT,
          RestrictionT,
          PositionT,
          WatermarkEstimatorStateT,
          W extends BoundedWindow>
      JavaDStream<WindowedValue<OutputT>> processKeyedElements(
          final ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
              processKeyedElements,
          final JavaDStream<WindowedValue<KV<byte[], KV<InputT, RestrictionT>>>> inputDStream,
          final Coder<WindowedValue<KV<InputT, RestrictionT>>> wvInputCoder,
          final Coder<WindowedValue<OutputT>> wvOutputCoder,
          final WindowingStrategy<?, W> windowingStrategy,
          final SerializablePipelineOptions options,
          final List<Integer> sourceIds,
          final String transformFullName) {

    final PairDStreamFunctions<ByteArray, byte[]> pairDStream =
        SparkGroupAlsoByWindowViaWindowSet.buildPairDStream(
            inputDStream, ByteArrayCoder.of(), wvInputCoder);

    // use updateStateByKey to scan through the state and update elements and timers.
    final UpdateStateByKeyFunction<
            InputT, RestrictionT, PositionT, WatermarkEstimatorStateT, OutputT, W>
        updateFunc =
            new UpdateStateByKeyFunction<>(
                processKeyedElements,
                sourceIds,
                windowingStrategy,
                (FullWindowedValueCoder<KV<InputT, RestrictionT>>) wvInputCoder,
                (FullWindowedValueCoder<OutputT>) wvOutputCoder,
                options,
                transformFullName);

    final DStream<
            Tuple2</*byte[]*/ ByteArray, Tuple2<StateAndTimers, /*WV<OutputT>*/ List<byte[]>>>>
        firedStream =
            pairDStream.updateStateByKey(
                updateFunc,
                pairDStream.defaultPartitioner(pairDStream.defaultPartitioner$default$1()),
                true,
                JavaSparkContext$.MODULE$.fakeClassTag());

    SparkGroupAlsoByWindowViaWindowSet.checkpointIfNeeded(firedStream, options);

    // filter state-only output (nothing to fire) and remove the state from the output.
    return SparkGroupAlsoByWindowViaWindowSet.stripStateValues(
        firedStream, (FullWindowedValueCoder<OutputT>) wvOutputCoder);
  }
}
