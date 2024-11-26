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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.GroupAlsoByWindowsAggregators;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly.GroupAlsoByWindow;
import org.apache.beam.runners.core.LateDataUtils;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.UnsupportedSideInputReader;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.translation.ReifyTimestampsAndWindowsFunction;
import org.apache.beam.runners.spark.translation.TranslationUtils;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.runners.spark.util.TimerUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.construction.TriggerTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
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

/**
 * An implementation of {@link GroupAlsoByWindow} logic for grouping by windows and controlling
 * trigger firings and pane accumulation.
 *
 * <p>This implementation is a composite of Spark transformations revolving around state management
 * using Spark's {@link PairDStreamFunctions#updateStateByKey(scala.Function1,
 * org.apache.spark.Partitioner, boolean, scala.reflect.ClassTag)} to update state with new data and
 * timers.
 *
 * <p>Using updateStateByKey allows to scan through the entire state visiting not just the updated
 * state (new values for key) but also check if timers are ready to fire. Since updateStateByKey
 * bounds the types of state and output to be the same, a (state, output) tuple is used, filtering
 * the state (and output if no firing) in the following steps.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SparkGroupAlsoByWindowViaWindowSet implements Serializable {
  private static final Logger LOG =
      LoggerFactory.getLogger(SparkGroupAlsoByWindowViaWindowSet.class);

  private static class OutputWindowedValueHolder<K, V>
      implements OutputWindowedValue<KV<K, Iterable<V>>> {
    private final List<WindowedValue<KV<K, Iterable<V>>>> windowedValues = new ArrayList<>();

    @Override
    public void outputWindowedValue(
        final KV<K, Iterable<V>> output,
        final Instant timestamp,
        final Collection<? extends BoundedWindow> windows,
        final PaneInfo pane) {
      windowedValues.add(WindowedValue.of(output, timestamp, windows, pane));
    }

    private List<WindowedValue<KV<K, Iterable<V>>>> getWindowedValues() {
      return windowedValues;
    }

    @Override
    public <AdditionalOutputT> void outputWindowedValue(
        final TupleTag<AdditionalOutputT> tag,
        final AdditionalOutputT output,
        final Instant timestamp,
        final Collection<? extends BoundedWindow> windows,
        final PaneInfo pane) {
      throw new UnsupportedOperationException(
          "Tagged outputs are not allowed in GroupAlsoByWindow.");
    }
  }

  private static class UpdateStateByKeyFunction<K, InputT, W extends BoundedWindow>
      extends AbstractFunction1<
          Iterator<
              Tuple3<
                  /*K*/ ByteArray,
                  Seq</*WV<I>*/ byte[]>,
                  Option<Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>>>,
          Iterator<
              Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>>>
      implements Serializable {

    private class UpdateStateByKeyOutputIterator
        extends AbstractIterator<
            Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>> {

      private final Iterator<
              Tuple3<ByteArray, Seq<byte[]>, Option<Tuple2<StateAndTimers, List<byte[]>>>>>
          input;
      private final SystemReduceFn<K, InputT, Iterable<InputT>, Iterable<InputT>, W> reduceFn;
      private final CounterCell droppedDueToLateness;

      private SparkStateInternals<K> processPreviousState(
          final Option<Tuple2<StateAndTimers, List<byte[]>>> prevStateAndTimersOpt,
          final K key,
          final SparkTimerInternals timerInternals) {

        final SparkStateInternals<K> stateInternals;

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
          final SystemReduceFn<K, InputT, Iterable<InputT>, Iterable<InputT>, W> reduceFn,
          final CounterCell droppedDueToLateness) {
        this.input = input;
        this.reduceFn = reduceFn;
        this.droppedDueToLateness = droppedDueToLateness;
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
      protected Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>
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

          final K key = CoderHelpers.fromByteArray(encodedKey.getValue(), keyCoder);

          final Map<Integer, GlobalWatermarkHolder.SparkWatermarks> watermarks =
              GlobalWatermarkHolder.get(getBatchDuration(options));

          final SparkTimerInternals timerInternals =
              SparkTimerInternals.forStreamFromSources(sourceIds, watermarks);

          final SparkStateInternals<K> stateInternals =
              processPreviousState(prevStateAndTimersOpt, key, timerInternals);

          final ExecutableTriggerStateMachine triggerStateMachine =
              ExecutableTriggerStateMachine.create(
                  TriggerStateMachines.stateMachineForTrigger(
                      TriggerTranslation.toProto(windowingStrategy.getTrigger())));

          final OutputWindowedValueHolder<K, InputT> outputHolder =
              new OutputWindowedValueHolder<>();

          final ReduceFnRunner<K, InputT, Iterable<InputT>, W> reduceFnRunner =
              new ReduceFnRunner<>(
                  key,
                  windowingStrategy,
                  triggerStateMachine,
                  stateInternals,
                  timerInternals,
                  outputHolder,
                  new UnsupportedSideInputReader("GroupAlsoByWindow"),
                  reduceFn,
                  options.get());

          if (!encodedElements.isEmpty()) {
            // new input for key.
            try {
              final Iterable<WindowedValue<InputT>> elements =
                  FluentIterable.from(JavaConversions.asJavaIterable(encodedElements))
                      .transform(bytes -> CoderHelpers.fromByteArray(bytes, wvCoder));

              LOG.trace("{}: input elements: {}", logPrefix, elements);

              // Incoming expired windows are filtered based on
              // timerInternals.currentInputWatermarkTime() and the configured allowed
              // lateness. Note that this is done prior to calling
              // timerInternals.advanceWatermark so essentially the inputWatermark is
              // the highWatermark of the previous batch and the lowWatermark of the
              // current batch.
              // The highWatermark of the current batch will only affect filtering
              // as of the next batch.
              final Iterable<WindowedValue<InputT>> nonExpiredElements =
                  Lists.newArrayList(
                      LateDataUtils.dropExpiredWindows(
                          key, elements, timerInternals, windowingStrategy, droppedDueToLateness));

              LOG.trace("{}: non expired input elements: {}", logPrefix, nonExpiredElements);

              reduceFnRunner.processElements(nonExpiredElements);
            } catch (final Exception e) {
              throw new RuntimeException("Failed to process element with ReduceFnRunner", e);
            }
          } else if (stateInternals.getState().isEmpty()) {
            // no input and no state -> GC evict now.
            continue;
          }
          try {
            // advance the watermark to HWM to fire by timers.
            LOG.debug("{}: timerInternals before advance are {}", logPrefix, timerInternals);

            // store the highWatermark as the new inputWatermark to calculate triggers
            timerInternals.advanceWatermark();

            final Collection<TimerInternals.TimerData> timersEligibleForProcessing =
                filterTimersEligibleForProcessing(
                    timerInternals.getTimers(), timerInternals.currentInputWatermarkTime());

            LOG.debug(
                "{}: timers eligible for processing are {}",
                logPrefix,
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
            reduceFnRunner.onTimers(timersEligibleForProcessing);
          } catch (final Exception e) {
            throw new RuntimeException("Failed to process ReduceFnRunner onTimer.", e);
          }
          // this is mostly symbolic since actual persist is done by emitting output.
          reduceFnRunner.persist();
          // obtain output, if fired.
          final List<WindowedValue<KV<K, Iterable<InputT>>>> outputs =
              outputHolder.getWindowedValues();

          if (!outputs.isEmpty() || !stateInternals.getState().isEmpty()) {

            TimerUtils.dropExpiredTimers(timerInternals, windowingStrategy);

            // empty outputs are filtered later using DStream filtering
            final StateAndTimers updated =
                StateAndTimers.of(
                    stateInternals.getState(),
                    SparkTimerInternals.serializeTimers(
                        timerInternals.getTimers(), timerDataCoder));

            if (LOG.isTraceEnabled()) {
              // Not something we want to happen in production, but is very helpful when debugging.
              LOG.trace("{}: output elements are {}", logPrefix, Joiner.on(", ").join(outputs));
            }
            // persist Spark's state by outputting.
            final List<byte[]> serOutput = CoderHelpers.toByteArrays(outputs, wvKvIterCoder);
            return new Tuple2<>(encodedKey, new Tuple2<>(updated, serOutput));
          }
          // an empty state with no output, can be evicted completely - do nothing.
        }
        return endOfData();
      }
    }

    private final FullWindowedValueCoder<InputT> wvCoder;
    private final Coder<K> keyCoder;
    private final List<Integer> sourceIds;
    private final TimerInternals.TimerDataCoderV2 timerDataCoder;
    private final WindowingStrategy<?, W> windowingStrategy;
    private final SerializablePipelineOptions options;
    private final String logPrefix;
    private final Coder<WindowedValue<KV<K, Iterable<InputT>>>> wvKvIterCoder;

    UpdateStateByKeyFunction(
        final List<Integer> sourceIds,
        final WindowingStrategy<?, W> windowingStrategy,
        final FullWindowedValueCoder<InputT> wvCoder,
        final Coder<K> keyCoder,
        final SerializablePipelineOptions options,
        final String logPrefix) {
      this.wvCoder = wvCoder;
      this.keyCoder = keyCoder;
      this.sourceIds = sourceIds;
      this.timerDataCoder = timerDataCoderOf(windowingStrategy);
      this.windowingStrategy = windowingStrategy;
      this.options = options;
      this.logPrefix = logPrefix;
      this.wvKvIterCoder =
          windowedValueKeyValueCoderOf(
              keyCoder,
              wvCoder.getValueCoder(),
              ((FullWindowedValueCoder<InputT>) wvCoder).getWindowCoder());
    }

    @Override
    public Iterator<
            Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>>
        apply(
            final Iterator<
                    Tuple3<
                        /*K*/ ByteArray,
                        Seq</*WV<I>*/ byte[]>,
                        Option<Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>>>
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

      final SystemReduceFn<K, InputT, Iterable<InputT>, Iterable<InputT>, W> reduceFn =
          SystemReduceFn.buffering(wvCoder.getValueCoder());

      final MetricsContainerImpl cellProvider = new MetricsContainerImpl("cellProvider");

      final CounterCell droppedDueToClosedWindow =
          cellProvider.getCounter(
              MetricName.named(
                  SparkGroupAlsoByWindowViaWindowSet.class,
                  GroupAlsoByWindowsAggregators.DROPPED_DUE_TO_CLOSED_WINDOW_COUNTER));

      final CounterCell droppedDueToLateness =
          cellProvider.getCounter(
              MetricName.named(
                  SparkGroupAlsoByWindowViaWindowSet.class,
                  GroupAlsoByWindowsAggregators.DROPPED_DUE_TO_LATENESS_COUNTER));

      // log if there's something to log.
      final long lateDropped = droppedDueToLateness.getCumulative();
      if (lateDropped > 0) {
        LOG.info("Dropped {} elements due to lateness.", lateDropped);
        droppedDueToLateness.inc(-droppedDueToLateness.getCumulative());
      }
      final long closedWindowDropped = droppedDueToClosedWindow.getCumulative();
      if (closedWindowDropped > 0) {
        LOG.info("Dropped {} elements due to closed window.", closedWindowDropped);
        droppedDueToClosedWindow.inc(-droppedDueToClosedWindow.getCumulative());
      }

      return scala.collection.JavaConversions.asScalaIterator(
          new UpdateStateByKeyOutputIterator(input, reduceFn, droppedDueToLateness));
    }
  }

  private static <K, InputT>
      FullWindowedValueCoder<KV<K, Iterable<InputT>>> windowedValueKeyValueCoderOf(
          final Coder<K> keyCoder,
          final Coder<InputT> iCoder,
          final Coder<? extends BoundedWindow> wCoder) {
    return FullWindowedValueCoder.of(KvCoder.of(keyCoder, IterableCoder.of(iCoder)), wCoder);
  }

  private static <W extends BoundedWindow> TimerInternals.TimerDataCoderV2 timerDataCoderOf(
      final WindowingStrategy<?, W> windowingStrategy) {
    return TimerInternals.TimerDataCoderV2.of(windowingStrategy.getWindowFn().windowCoder());
  }

  private static void checkpointIfNeeded(
      final DStream<Tuple2<ByteArray, Tuple2<StateAndTimers, List<byte[]>>>> firedStream,
      final SerializablePipelineOptions options) {

    final Long checkpointDurationMillis = getBatchDuration(options);

    if (checkpointDurationMillis > 0) {
      firedStream.checkpoint(new Duration(checkpointDurationMillis));
    }
  }

  private static Long getBatchDuration(final SerializablePipelineOptions options) {
    return options.get().as(SparkPipelineOptions.class).getCheckpointDurationMillis();
  }

  private static <K, InputT> JavaDStream<WindowedValue<KV<K, Iterable<InputT>>>> stripStateValues(
      final DStream<Tuple2<ByteArray, Tuple2<StateAndTimers, List<byte[]>>>> firedStream,
      final Coder<K> keyCoder,
      final FullWindowedValueCoder<InputT> wvCoder) {

    return JavaPairDStream.fromPairDStream(
            firedStream,
            JavaSparkContext$.MODULE$.fakeClassTag(),
            JavaSparkContext$.MODULE$.fakeClassTag())
        .filter(
            // filter output if defined.
            t2 -> !t2._2()._2().isEmpty())
        .flatMap(
            new FlatMapFunction<
                Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>,
                WindowedValue<KV<K, Iterable<InputT>>>>() {

              private final FullWindowedValueCoder<KV<K, Iterable<InputT>>>
                  windowedValueKeyValueCoder =
                      windowedValueKeyValueCoderOf(
                          keyCoder, wvCoder.getValueCoder(), wvCoder.getWindowCoder());

              @Override
              public java.util.Iterator<WindowedValue<KV<K, Iterable<InputT>>>> call(
                  final Tuple2<
                          /*K*/ ByteArray,
                          Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>
                      t2)
                  throws Exception {
                // drop the state since it is already persisted at this point.
                // return in serialized form.
                return CoderHelpers.fromByteArrays(t2._2()._2(), windowedValueKeyValueCoder)
                    .iterator();
              }
            });
  }

  private static <K, InputT> PairDStreamFunctions<ByteArray, byte[]> buildPairDStream(
      final JavaDStream<WindowedValue<KV<K, InputT>>> inputDStream,
      final Coder<K> keyCoder,
      final Coder<WindowedValue<InputT>> wvCoder) {

    // we have to switch to Scala API to avoid Optional in the Java API, see: SPARK-4819.
    // we also have a broader API for Scala (access to the actual key and entire iterator).
    // we use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle and be in serialized form
    // for checkpointing.
    // for readability, we add comments with actual type next to byte[].
    // to shorten line length, we use:
    // ---- WV: WindowedValue
    // ---- Iterable: Itr
    // ---- AccumT: A
    // ---- InputT: I
    final DStream<Tuple2<ByteArray, byte[]>> tupleDStream =
        inputDStream
            .map(new ReifyTimestampsAndWindowsFunction<>())
            .mapToPair(TranslationUtils.toPairFunction())
            .mapToPair(CoderHelpers.toByteFunction(keyCoder, wvCoder))
            .dstream();

    return DStream.toPairDStreamFunctions(
        tupleDStream,
        JavaSparkContext$.MODULE$.fakeClassTag(),
        JavaSparkContext$.MODULE$.fakeClassTag(),
        null);
  }

  public static <K, InputT, W extends BoundedWindow>
      JavaDStream<WindowedValue<KV<K, Iterable<InputT>>>> groupByKeyAndWindow(
          final JavaDStream<WindowedValue<KV<K, InputT>>> inputDStream,
          final Coder<K> keyCoder,
          final Coder<WindowedValue<InputT>> wvCoder,
          final WindowingStrategy<?, W> windowingStrategy,
          final SerializablePipelineOptions options,
          final List<Integer> sourceIds,
          final String transformFullName) {

    final PairDStreamFunctions<ByteArray, byte[]> pairDStream =
        buildPairDStream(inputDStream, keyCoder, wvCoder);

    // use updateStateByKey to scan through the state and update elements and timers.
    final UpdateStateByKeyFunction<K, InputT, W> updateFunc =
        new UpdateStateByKeyFunction<>(
            sourceIds,
            windowingStrategy,
            (FullWindowedValueCoder<InputT>) wvCoder,
            keyCoder,
            options,
            transformFullName);

    final DStream<
            Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>>
        firedStream =
            pairDStream.updateStateByKey(
                updateFunc,
                pairDStream.defaultPartitioner(pairDStream.defaultPartitioner$default$1()),
                true,
                JavaSparkContext$.MODULE$.fakeClassTag());

    checkpointIfNeeded(firedStream, options);

    // filter state-only output (nothing to fire) and remove the state from the output.
    return stripStateValues(firedStream, keyCoder, (FullWindowedValueCoder<InputT>) wvCoder);
  }
}
