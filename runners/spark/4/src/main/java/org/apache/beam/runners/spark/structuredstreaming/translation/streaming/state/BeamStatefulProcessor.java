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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.TimerInternals.TimerDataCoderV2;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.stateful.SparkStateInternals;
import org.apache.beam.runners.spark.stateful.SparkStateInternals.StateCells;
import org.apache.beam.runners.spark.stateful.SparkTimerInternals;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.DoFnRunnerFactory;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.DoFnRunnerFactory.DoFnRunnerWithTeardown;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.StatefulTaskRunner;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValueMultiReceiver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.ExpiredTimerInfo;
import org.apache.spark.sql.streaming.MapState;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StatefulProcessor;
import org.apache.spark.sql.streaming.StatefulProcessorHandle;
import org.apache.spark.sql.streaming.TTLConfig;
import org.apache.spark.sql.streaming.TimeMode;
import org.apache.spark.sql.streaming.TimerValues;
import org.apache.spark.sql.streaming.ValueState;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import scala.collection.Iterator;

/** Runs a stateful {@link DoFn} within Spark 4 Structured Streaming {@code transformWithState}. */
public class BeamStatefulProcessor<K, V, OutputT>
    extends StatefulProcessor<K, WindowedValue<KV<K, V>>, WindowedValue<OutputT>> {

  private static final String BEAM_STATE = "beamState";
  private static final String BEAM_TIMERS = "beamTimers";

  private final DoFn<KV<K, V>, OutputT> doFn;
  private final DoFnRunnerFactory<KV<K, V>, OutputT> runnerFactory;
  private final Coder<KV<K, V>> inputCoder;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Supplier<PipelineOptions> optionsSupplier;
  private final MetricsAccumulator metrics;
  private final TimerDataCoderV2 timerDataCoder;
  private final StatefulTaskRunner<KV<K, V>, OutputT> taskRunner = new StatefulTaskRunner<>();

  private transient @Nullable MapState<String, byte[]> beamState;
  private transient @Nullable ValueState<byte[]> beamTimers;
  private transient @Nullable List<WindowedValue<OutputT>> currentOutputs;
  private transient boolean needsBundleStart;

  public BeamStatefulProcessor(
      DoFn<KV<K, V>, OutputT> doFn,
      DoFnRunnerFactory<KV<K, V>, OutputT> runnerFactory,
      Coder<KV<K, V>> inputCoder,
      WindowingStrategy<?, ?> windowingStrategy,
      Supplier<PipelineOptions> optionsSupplier,
      MetricsAccumulator metrics) {
    this.doFn = doFn;
    this.runnerFactory = runnerFactory;
    this.inputCoder = inputCoder;
    this.windowingStrategy = windowingStrategy;
    this.optionsSupplier = optionsSupplier;
    this.metrics = metrics;
    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> windowCoder =
        (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder();
    this.timerDataCoder = TimerDataCoderV2.of(windowCoder);
  }

  @Override
  public void init(OutputMode outputMode, TimeMode timeMode) {
    beamState =
        getHandle().getMapState(BEAM_STATE, Encoders.STRING(), Encoders.BINARY(), TTLConfig.NONE());
    beamTimers = getHandle().getValueState(BEAM_TIMERS, Encoders.BINARY(), TTLConfig.NONE());
  }

  @Override
  public Iterator<WindowedValue<OutputT>> handleInputRows(
      K key, Iterator<WindowedValue<KV<K, V>>> rows, TimerValues timerValues) {
    List<WindowedValue<OutputT>> outputs = new ArrayList<>();
    SparkTimerInternals timerInternals =
        SparkTimerInternals.forWatermark(new Instant(timerValues.getCurrentWatermarkInMs()));
    restoreTimers(timerInternals);

    DoFnRunner<KV<K, V>, OutputT> runner = keyRunner(key, timerInternals, outputs);
    while (rows.hasNext()) {
      runner.processElement(rows.next());
    }
    runner.finishBundle();
    needsBundleStart = true;

    persistTimers(timerInternals);
    reconcileWakeupTimer(timerInternals, null);
    return ScalaInterop.scalaIterator(outputs);
  }

  @Override
  public Iterator<WindowedValue<OutputT>> handleExpiredTimer(
      K key, TimerValues timerValues, ExpiredTimerInfo expiredTimerInfo) {
    List<WindowedValue<OutputT>> outputs = new ArrayList<>();
    Instant watermark = new Instant(timerValues.getCurrentWatermarkInMs());
    SparkTimerInternals timerInternals = SparkTimerInternals.forWatermark(watermark);
    restoreTimers(timerInternals);

    DoFnRunner<KV<K, V>, OutputT> runner = keyRunner(key, timerInternals, outputs);
    fireDueTimers(key, watermark, timerInternals, runner);
    runner.finishBundle();
    needsBundleStart = true;

    persistTimers(timerInternals);
    reconcileWakeupTimer(timerInternals, expiredTimerInfo.getExpiryTimeInMs());
    return ScalaInterop.scalaIterator(outputs);
  }

  private DoFnRunnerWithTeardown<KV<K, V>, OutputT> baseRunner() {
    return taskRunner.getOrCreate(
        ctx ->
            runnerFactory.create(
                optionsSupplier.get(),
                metrics,
                new WindowedValueMultiReceiver() {
                  @Override
                  public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
                    @SuppressWarnings("unchecked")
                    WindowedValue<OutputT> out = (WindowedValue<OutputT>) output;
                    checkStateNotNull(currentOutputs, "currentOutputs not initialized").add(out);
                  }
                },
                ctx));
  }

  private DoFnRunner<KV<K, V>, OutputT> keyRunner(
      K key, SparkTimerInternals timerInternals, List<WindowedValue<OutputT>> outputs) {
    DoFnRunnerWithTeardown<KV<K, V>, OutputT> base = baseRunner();
    MapState<String, byte[]> state = checkStateNotNull(beamState);
    SparkStateInternals<K> stateInternals =
        SparkStateInternals.forKey(key, new MapStateAdapter(state));
    taskRunner.stepContext().set(stateInternals, timerInternals);
    this.currentOutputs = outputs;

    if (needsBundleStart) {
      needsBundleStart = false;
      base.startBundle();
    }

    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> windowCoder =
        (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder();
    StatefulDoFnRunner.CleanupTimer<KV<K, V>> cleanupTimer =
        new StatefulDoFnRunner.TimeInternalsCleanupTimer<>(timerInternals, windowingStrategy);
    StatefulDoFnRunner.StateCleaner<BoundedWindow> stateCleaner =
        new StatefulDoFnRunner.StateInternalsStateCleaner<>(doFn, stateInternals, windowCoder);

    return DoFnRunners.defaultStatefulDoFnRunner(
        doFn,
        inputCoder,
        base,
        taskRunner.stepContext(),
        windowingStrategy,
        cleanupTimer,
        stateCleaner);
  }

  private void restoreTimers(SparkTimerInternals timerInternals) {
    ValueState<byte[]> timersState = checkStateNotNull(beamTimers);
    if (timersState.exists()) {
      byte[] timerBytes = timersState.get();
      if (timerBytes != null) {
        List<TimerData> timers =
            CoderHelpers.fromByteArray(timerBytes, ListCoder.of(timerDataCoder));
        timerInternals.addTimers(timers.iterator());
      }
    }
  }

  private void persistTimers(SparkTimerInternals timerInternals) {
    ValueState<byte[]> timersState = checkStateNotNull(beamTimers);
    Collection<TimerData> timers = timerInternals.getTimers();
    if (timers.isEmpty()) {
      timersState.clear();
    } else {
      timersState.update(
          CoderHelpers.toByteArray(new ArrayList<>(timers), ListCoder.of(timerDataCoder)));
    }
  }

  private void fireDueTimers(
      K key,
      Instant watermark,
      SparkTimerInternals timerInternals,
      DoFnRunner<KV<K, V>, OutputT> runner) {
    while (true) {
      TimerData next =
          timerInternals.getTimers().stream()
              .filter(
                  t ->
                      t.getDomain().equals(TimeDomain.EVENT_TIME)
                          && watermark.isAfter(t.getTimestamp()))
              .min(Comparator.comparing(TimerData::getTimestamp))
              .orElse(null);
      if (next == null) {
        break;
      }
      timerInternals.deleteTimer(next);
      StatefulTaskRunner.fireTimer(runner, key, next);
    }
  }

  private void reconcileWakeupTimer(
      SparkTimerInternals timerInternals, @Nullable Long firedExpiryMs) {
    Long nextWakeupMs = null;
    TimerData earliest =
        timerInternals.getTimers().stream()
            .filter(t -> t.getDomain().equals(TimeDomain.EVENT_TIME))
            .min(Comparator.comparing(TimerData::getTimestamp))
            .orElse(null);
    if (earliest != null) {
      nextWakeupMs = earliest.getTimestamp().getMillis() + 1;
    }

    StatefulProcessorHandle handle = getHandle();
    Iterator<Object> it = handle.listTimers();
    Set<Long> registered = new HashSet<>();
    while (it.hasNext()) {
      registered.add(((Number) it.next()).longValue());
    }

    for (Long expiry : registered) {
      if (expiry.equals(firedExpiryMs)) {
        continue;
      }
      if (nextWakeupMs != null && expiry.equals(nextWakeupMs)) {
        continue;
      }
      handle.deleteTimer(expiry);
    }

    if (nextWakeupMs != null && !registered.contains(nextWakeupMs)) {
      handle.registerTimer(nextWakeupMs);
    }
  }

  private static class MapStateAdapter implements StateCells {
    private final MapState<String, byte[]> mapState;

    MapStateAdapter(MapState<String, byte[]> mapState) {
      this.mapState = mapState;
    }

    @Override
    public byte @Nullable [] get(String namespace, String stateId) {
      return mapState.getValue(namespace + "+" + stateId);
    }

    @Override
    public void put(String namespace, String stateId, byte[] value) {
      mapState.updateValue(namespace + "+" + stateId, value);
    }

    @Override
    public void remove(String namespace, String stateId) {
      mapState.removeKey(namespace + "+" + stateId);
    }
  }
}
