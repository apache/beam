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

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
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
import org.apache.beam.runners.core.construction.TriggerTranslation;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.translation.TranslationUtils;
import org.apache.beam.runners.spark.translation.WindowingHelpers;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.dstream.PairDStreamFunctions;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

/**
 * An implementation of {@link GroupAlsoByWindow}
 * logic for grouping by windows and controlling trigger firings and pane accumulation.
 *
 * <p>This implementation is a composite of Spark transformations revolving around state management
 * using Spark's
 * {@link PairDStreamFunctions#updateStateByKey(Function1, Partitioner, boolean, ClassTag)}
 * to update state with new data and timers.
 *
 * <p>Using updateStateByKey allows to scan through the entire state visiting not just the
 * updated state (new values for key) but also check if timers are ready to fire.
 * Since updateStateByKey bounds the types of state and output to be the same,
 * a (state, output) tuple is used, filtering the state (and output if no firing)
 * in the following steps.
 */
public class SparkGroupAlsoByWindowViaWindowSet implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(
      SparkGroupAlsoByWindowViaWindowSet.class);

  /**
   * A helper class that is essentially a {@link Serializable} {@link AbstractFunction1}.
   */
  private abstract static class SerializableFunction1<T1, T2>
      extends AbstractFunction1<T1, T2> implements Serializable {
  }

  public static <K, InputT, W extends BoundedWindow>
      JavaDStream<WindowedValue<KV<K, Iterable<InputT>>>> groupAlsoByWindow(
      final JavaDStream<WindowedValue<KV<K, Iterable<WindowedValue<InputT>>>>> inputDStream,
      final Coder<K> keyCoder,
      final Coder<WindowedValue<InputT>> wvCoder,
      final WindowingStrategy<?, W> windowingStrategy,
      final SerializablePipelineOptions options,
      final List<Integer> sourceIds,
      final String transformFullName) {

    final long batchDurationMillis =
        options.get().as(SparkPipelineOptions.class).getBatchIntervalMillis();
    final IterableCoder<WindowedValue<InputT>> itrWvCoder = IterableCoder.of(wvCoder);
    final Coder<InputT> iCoder = ((FullWindowedValueCoder<InputT>) wvCoder).getValueCoder();
    final Coder<? extends BoundedWindow> wCoder =
        ((FullWindowedValueCoder<InputT>) wvCoder).getWindowCoder();
    final Coder<WindowedValue<KV<K, Iterable<InputT>>>> wvKvIterCoder =
        FullWindowedValueCoder.of(KvCoder.of(keyCoder, IterableCoder.of(iCoder)), wCoder);
    final TimerInternals.TimerDataCoder timerDataCoder =
        TimerInternals.TimerDataCoder.of(windowingStrategy.getWindowFn().windowCoder());

    long checkpointDurationMillis =
        options.get().as(SparkPipelineOptions.class)
            .getCheckpointDurationMillis();

    // we have to switch to Scala API to avoid Optional in the Java API, see: SPARK-4819.
    // we also have a broader API for Scala (access to the actual key and entire iterator).
    // we use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle and be in serialized form
    // for checkpointing.
    // for readability, we add comments with actual type next to byte[].
    // to shorten line length, we use:
    //---- WV: WindowedValue
    //---- Iterable: Itr
    //---- AccumT: A
    //---- InputT: I
    DStream<Tuple2</*K*/ ByteArray, /*Itr<WV<I>>*/ byte[]>> pairDStream =
        inputDStream
            .transformToPair(
                new org.apache.spark.api.java.function.Function2<
                    JavaRDD<WindowedValue<KV<K, Iterable<WindowedValue<InputT>>>>>,
                    Time, JavaPairRDD<ByteArray, byte[]>>() {
                  // we use mapPartitions with the RDD API because its the only available API
                  // that allows to preserve partitioning.
                  @Override
                  public JavaPairRDD<ByteArray, byte[]> call(
                      JavaRDD<WindowedValue<KV<K, Iterable<WindowedValue<InputT>>>>> rdd,
                      final Time time)
                      throws Exception {
                    return rdd.mapPartitions(
                        TranslationUtils.functionToFlatMapFunction(
                            WindowingHelpers
                                .<KV<K, Iterable<WindowedValue<InputT>>>>unwindowFunction()),
                        true)
                              .mapPartitionsToPair(
                                  TranslationUtils
                                      .<K, Iterable<WindowedValue<InputT>>>toPairFlatMapFunction(),
                                  true)
                              .mapValues(new Function<Iterable<WindowedValue<InputT>>, KV<Long,
                                  Iterable<WindowedValue<InputT>>>>() {

                                @Override
                                public KV<Long, Iterable<WindowedValue<InputT>>> call
                                    (Iterable<WindowedValue<InputT>> values)
                                    throws Exception {
                                  // add the batch timestamp for visibility (e.g., debugging)
                                  return KV.of(time.milliseconds(), values);
                                }
                              })
                              // move to bytes representation and use coders for deserialization
                              // because of checkpointing.
                              .mapPartitionsToPair(
                                  TranslationUtils.pairFunctionToPairFlatMapFunction(
                                      CoderHelpers.toByteFunction(keyCoder,
                                                                  KvCoder.of(VarLongCoder.of(),
                                                                             itrWvCoder))),
                                  true);
                  }
                })
            .dstream();

    PairDStreamFunctions<ByteArray, byte[]> pairDStreamFunctions =
        DStream.toPairDStreamFunctions(
        pairDStream,
        JavaSparkContext$.MODULE$.<ByteArray>fakeClassTag(),
        JavaSparkContext$.MODULE$.<byte[]>fakeClassTag(),
        null);
    int defaultNumPartitions = pairDStreamFunctions.defaultPartitioner$default$1();
    Partitioner partitioner = pairDStreamFunctions.defaultPartitioner(defaultNumPartitions);

    // use updateStateByKey to scan through the state and update elements and timers.
    DStream<Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>>
        firedStream = pairDStreamFunctions.updateStateByKey(
            new SerializableFunction1<
                scala.collection.Iterator<Tuple3</*K*/ ByteArray, Seq</*Itr<WV<I>>*/ byte[]>,
                    Option<Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>>>,
                scala.collection.Iterator<Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers,
                    /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>>>() {

      @Override
      public scala.collection.Iterator<Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers,
          /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>> apply(
              final scala.collection.Iterator<Tuple3</*K*/ ByteArray, Seq</*Itr<WV<I>>*/ byte[]>,
              Option<Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>>> iter) {
        //--- ACTUAL STATEFUL OPERATION:
        //
        // Input Iterator: the partition (~bundle) of a cogrouping of the input
        // and the previous state (if exists).
        //
        // Output Iterator: the output key, and the updated state.
        //
        // possible input scenarios for (K, Seq, Option<S>):
        // (1) Option<S>.isEmpty: new data with no previous state.
        // (2) Seq.isEmpty: no new data, but evaluating previous state (timer-like behaviour).
        // (3) Seq.nonEmpty && Option<S>.isDefined: new data with previous state.

        final SystemReduceFn<K, InputT, Iterable<InputT>, Iterable<InputT>, W> reduceFn =
            SystemReduceFn.buffering(
                ((FullWindowedValueCoder<InputT>) wvCoder).getValueCoder());
        final OutputWindowedValueHolder<K, InputT> outputHolder =
            new OutputWindowedValueHolder<>();
        // use in memory Aggregators since Spark Accumulators are not resilient
        // in stateful operators, once done with this partition.
        final MetricsContainerImpl cellProvider = new MetricsContainerImpl("cellProvider");
        final CounterCell droppedDueToClosedWindow = cellProvider.getCounter(
            MetricName.named(SparkGroupAlsoByWindowViaWindowSet.class,
            GroupAlsoByWindowsAggregators.DROPPED_DUE_TO_CLOSED_WINDOW_COUNTER));
        final CounterCell droppedDueToLateness = cellProvider.getCounter(
            MetricName.named(SparkGroupAlsoByWindowViaWindowSet.class,
                GroupAlsoByWindowsAggregators.DROPPED_DUE_TO_LATENESS_COUNTER));

        AbstractIterator<
            Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers, /*WV<KV<K, KV<Long(Time),Itr<I>>>>*/
                List<byte[]>>>>
                outIter = new AbstractIterator<Tuple2</*K*/ ByteArray,
                    Tuple2<StateAndTimers, /*WV<KV<K, KV<Long(Time),Itr<I>>>>*/ List<byte[]>>>>() {
                  @Override
                  protected Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers,
                      /*WV<KV<K, Itr<I>>>*/ List<byte[]>>> computeNext() {
                    // input iterator is a Spark partition (~bundle), containing keys and their
                    // (possibly) previous-state and (possibly) new data.
                    while (iter.hasNext()) {
                      // for each element in the partition:
                      Tuple3<ByteArray, Seq<byte[]>,
                          Option<Tuple2<StateAndTimers, List<byte[]>>>> next = iter.next();
                      ByteArray encodedKey = next._1();
                      K key = CoderHelpers.fromByteArray(encodedKey.getValue(), keyCoder);

                      Seq<byte[]> seq = next._2();

                      Option<Tuple2<StateAndTimers,
                          List<byte[]>>> prevStateAndTimersOpt = next._3();

                      SparkStateInternals<K> stateInternals;
                      Map<Integer, GlobalWatermarkHolder.SparkWatermarks> watermarks =
                          GlobalWatermarkHolder.get(batchDurationMillis);
                      SparkTimerInternals timerInternals = SparkTimerInternals.forStreamFromSources(
                          sourceIds, watermarks);

                      // get state(internals) per key.
                      if (prevStateAndTimersOpt.isEmpty()) {
                        // no previous state.
                        stateInternals = SparkStateInternals.forKey(key);
                      } else {
                        // with pre-existing state.
                        StateAndTimers prevStateAndTimers = prevStateAndTimersOpt.get()._1();
                        stateInternals = SparkStateInternals.forKeyAndState(key,
                            prevStateAndTimers.getState());
                        Collection<byte[]> serTimers = prevStateAndTimers.getTimers();
                        timerInternals.addTimers(
                            SparkTimerInternals.deserializeTimers(serTimers, timerDataCoder));
                      }

                      ReduceFnRunner<K, InputT, Iterable<InputT>, W> reduceFnRunner =
                          new ReduceFnRunner<>(
                              key,
                              windowingStrategy,
                              ExecutableTriggerStateMachine.create(
                                  TriggerStateMachines.stateMachineForTrigger(
                                      TriggerTranslation.toProto(windowingStrategy.getTrigger()))),
                              stateInternals,
                              timerInternals,
                              outputHolder,
                              new UnsupportedSideInputReader("GroupAlsoByWindow"),
                              reduceFn,
                              options.get());

                      outputHolder.clear(); // clear before potential use.

                      if (!seq.isEmpty()) {
                        // new input for key.
                        try {
                          final KV<Long, Iterable<WindowedValue<InputT>>> keyedElements =
                              CoderHelpers.fromByteArray(seq.head(),
                                                         KvCoder.of(VarLongCoder.of(), itrWvCoder));

                          final Long rddTimestamp = keyedElements.getKey();

                          LOG.debug(
                              transformFullName
                                  + ": processing RDD with timestamp: {}, watermarks: {}",
                              rddTimestamp,
                              watermarks);

                          final Iterable<WindowedValue<InputT>> elements = keyedElements.getValue();

                          LOG.trace(transformFullName + ": input elements: {}", elements);

                          /*
                          Incoming expired windows are filtered based on
                          timerInternals.currentInputWatermarkTime() and the configured allowed
                          lateness. Note that this is done prior to calling
                          timerInternals.advanceWatermark so essentially the inputWatermark is
                          the highWatermark of the previous batch and the lowWatermark of the
                          current batch.
                          The highWatermark of the current batch will only affect filtering
                          as of the next batch.
                           */
                          final Iterable<WindowedValue<InputT>> nonExpiredElements =
                              Lists.newArrayList(LateDataUtils
                                                     .dropExpiredWindows(
                                                         key,
                                                         elements,
                                                         timerInternals,
                                                         windowingStrategy,
                                                         droppedDueToLateness));

                          LOG.trace(transformFullName + ": non expired input elements: {}",
                                    elements);

                          reduceFnRunner.processElements(nonExpiredElements);
                        } catch (Exception e) {
                          throw new RuntimeException(
                              "Failed to process element with ReduceFnRunner", e);
                        }
                      } else if (stateInternals.getState().isEmpty()) {
                        // no input and no state -> GC evict now.
                        continue;
                      }
                      try {
                        // advance the watermark to HWM to fire by timers.
                        LOG.debug(transformFullName + ": timerInternals before advance are {}",
                                  timerInternals.toString());

                        // store the highWatermark as the new inputWatermark to calculate triggers
                        timerInternals.advanceWatermark();

                        LOG.debug(transformFullName + ": timerInternals after advance are {}",
                                  timerInternals.toString());

                        // call on timers that are ready.
                        final Collection<TimerInternals.TimerData> readyToProcess =
                            timerInternals.getTimersReadyToProcess();

                        LOG.debug(transformFullName + ": ready timers are {}", readyToProcess);

                        /*
                        Note that at this point, the watermark has already advanced since
                        timerInternals.advanceWatermark() has been called and the highWatermark
                        is now stored as the new inputWatermark, according to which triggers are
                        calculated.
                         */
                        reduceFnRunner.onTimers(readyToProcess);
                      } catch (Exception e) {
                        throw new RuntimeException(
                            "Failed to process ReduceFnRunner onTimer.", e);
                      }
                      // this is mostly symbolic since actual persist is done by emitting output.
                      reduceFnRunner.persist();
                      // obtain output, if fired.
                      List<WindowedValue<KV<K, Iterable<InputT>>>> outputs = outputHolder.get();

                      if (!outputs.isEmpty() || !stateInternals.getState().isEmpty()) {
                        // empty outputs are filtered later using DStream filtering
                        StateAndTimers updated = new StateAndTimers(stateInternals.getState(),
                            SparkTimerInternals.serializeTimers(
                                timerInternals.getTimers(), timerDataCoder));

                        /*
                        Not something we want to happen in production, but is very helpful
                        when debugging - TRACE.
                         */
                        LOG.trace(transformFullName + ": output elements are {}",
                                  Joiner.on(", ").join(outputs));

                        // persist Spark's state by outputting.
                        List<byte[]> serOutput = CoderHelpers.toByteArrays(outputs, wvKvIterCoder);
                        return new Tuple2<>(encodedKey, new Tuple2<>(updated, serOutput));
                      }
                      // an empty state with no output, can be evicted completely - do nothing.
                    }
                    return endOfData();
                  }
        };

        // log if there's something to log.
        long lateDropped = droppedDueToLateness.getCumulative();
        if (lateDropped > 0) {
          LOG.info(String.format("Dropped %d elements due to lateness.", lateDropped));
          droppedDueToLateness.inc(-droppedDueToLateness.getCumulative());
        }
        long closedWindowDropped = droppedDueToClosedWindow.getCumulative();
        if (closedWindowDropped > 0) {
          LOG.info(String.format("Dropped %d elements due to closed window.", closedWindowDropped));
          droppedDueToClosedWindow.inc(-droppedDueToClosedWindow.getCumulative());
        }

        return scala.collection.JavaConversions.asScalaIterator(outIter);
      }
    }, partitioner, true,
        JavaSparkContext$.MODULE$.<Tuple2<StateAndTimers, List<byte[]>>>fakeClassTag());

    if (checkpointDurationMillis > 0) {
      firedStream.checkpoint(new Duration(checkpointDurationMillis));
    }

    // go back to Java now.
    JavaPairDStream</*K*/ ByteArray, Tuple2<StateAndTimers, /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>
        javaFiredStream = JavaPairDStream.fromPairDStream(
            firedStream,
            JavaSparkContext$.MODULE$.<ByteArray>fakeClassTag(),
            JavaSparkContext$.MODULE$.<Tuple2<StateAndTimers, List<byte[]>>>fakeClassTag());

    // filter state-only output (nothing to fire) and remove the state from the output.
    return javaFiredStream.filter(
        new Function<Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers,
            /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>, Boolean>() {
              @Override
              public Boolean call(
                  Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers,
                  /*WV<KV<K, Itr<I>>>*/ List<byte[]>>> t2) throws Exception {
                // filter output if defined.
                return !t2._2()._2().isEmpty();
              }
        })
        .flatMap(
            new FlatMapFunction<Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers,
                /*WV<KV<K, Itr<I>>>*/ List<byte[]>>>,
                WindowedValue<KV<K, Iterable<InputT>>>>() {
              @Override
              public Iterable<WindowedValue<KV<K, Iterable<InputT>>>> call(
                  Tuple2</*K*/ ByteArray, Tuple2<StateAndTimers,
                  /*WV<KV<K, Itr<I>>>*/ List<byte[]>>> t2) throws Exception {
                // drop the state since it is already persisted at this point.
                // return in serialized form.
                return CoderHelpers.fromByteArrays(t2._2()._2(), wvKvIterCoder);
              }
        });
  }

  private static class StateAndTimers implements Serializable {
    //Serializable state for internals (namespace to state tag to coded value).
    private final Table<String, String, byte[]> state;
    private final Collection<byte[]> serTimers;

    private StateAndTimers(
        Table<String, String, byte[]> state, Collection<byte[]> timers) {
      this.state = state;
      this.serTimers = timers;
    }

    public Table<String, String, byte[]> getState() {
      return state;
    }

    public Collection<byte[]> getTimers() {
      return serTimers;
    }
  }

  private static class OutputWindowedValueHolder<K, V>
      implements OutputWindowedValue<KV<K, Iterable<V>>> {
    private List<WindowedValue<KV<K, Iterable<V>>>> windowedValues = new ArrayList<>();

    @Override
    public void outputWindowedValue(
        KV<K, Iterable<V>> output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      windowedValues.add(WindowedValue.of(output, timestamp, windows, pane));
    }

    private List<WindowedValue<KV<K, Iterable<V>>>> get() {
      return windowedValues;
    }

    private void clear() {
      windowedValues.clear();
    }

    @Override
    public <AdditionalOutputT> void outputWindowedValue(
        TupleTag<AdditionalOutputT> tag,
        AdditionalOutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      throw new UnsupportedOperationException(
          "Tagged outputs are not allowed in GroupAlsoByWindow.");
    }
  }
}
