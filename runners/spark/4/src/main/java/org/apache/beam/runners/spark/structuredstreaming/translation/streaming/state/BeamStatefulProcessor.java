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
import java.util.stream.Collectors;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.TimerInternals.TimerDataCoderV2;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.stateful.SparkStateInternals;
import org.apache.beam.runners.spark.stateful.SparkStateInternals.StateCells;
import org.apache.beam.runners.spark.stateful.SparkTimerInternals;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.state.TimeDomain;
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

/** Base stateful processor bridging Beam state and timers over Spark transformWithState. */
public abstract class BeamStatefulProcessor<K, V, OutputT>
    extends StatefulProcessor<byte[], WindowedValue<KV<K, V>>, WindowedValue<OutputT>> {

  private static final String BEAM_STATE = "beamState";
  private static final String BEAM_TIMERS = "beamTimers";
  private static final String BEAM_WATERMARK = "beamWatermark";

  protected final KvCoder<K, V> inputCoder;
  protected final WindowingStrategy<?, ?> windowingStrategy;
  protected final Coder<BoundedWindow> windowCoder;
  private final TimerDataCoderV2 timerDataCoder;

  private transient @Nullable MapState<String, byte[]> beamState;
  private transient @Nullable ValueState<byte[]> beamTimers;
  private transient @Nullable ValueState<Long> beamWatermark;
  private transient @Nullable List<WindowedValue<OutputT>> currentOutputs;

  private transient @Nullable WindowedValueMultiReceiver outputReceiver;

  protected WindowedValueMultiReceiver outputReceiver() {
    if (outputReceiver == null) {
      outputReceiver =
          new WindowedValueMultiReceiver() {
            @Override
            public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
              @SuppressWarnings("unchecked")
              WindowedValue<OutputT> out = (WindowedValue<OutputT>) output;
              checkStateNotNull(currentOutputs, "currentOutputs not initialized").add(out);
            }
          };
    }
    return outputReceiver;
  }

  public BeamStatefulProcessor(
      KvCoder<K, V> inputCoder, WindowingStrategy<?, ?> windowingStrategy) {
    this.inputCoder = inputCoder;
    this.windowingStrategy = windowingStrategy;
    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> windowCoder =
        (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder();
    this.windowCoder = windowCoder;
    this.timerDataCoder = TimerDataCoderV2.of(windowCoder);
  }

  @Override
  public void init(OutputMode outputMode, TimeMode timeMode) {
    beamState =
        getHandle().getMapState(BEAM_STATE, Encoders.STRING(), Encoders.BINARY(), TTLConfig.NONE());
    beamTimers = getHandle().getValueState(BEAM_TIMERS, Encoders.BINARY(), TTLConfig.NONE());
    beamWatermark = getHandle().getValueState(BEAM_WATERMARK, Encoders.LONG(), TTLConfig.NONE());
  }

  private void storeWatermark(SparkTimerInternals timerInternals) {
    ValueState<Long> watermarkState = checkStateNotNull(beamWatermark);
    // A key without timers and state keeps no row, its next visit is a first visit.
    if (timerInternals.getTimers().isEmpty() && !checkStateNotNull(beamState).exists()) {
      if (watermarkState.exists()) {
        watermarkState.clear();
      }
    } else {
      watermarkState.update(timerInternals.currentInputWatermarkTime().getMillis());
    }
  }

  private SparkTimerInternals timerInternals(TimerValues timerValues) {
    ValueState<Long> watermarkState = checkStateNotNull(beamWatermark);
    // Late data is filtered against the watermark of the key's previous visit.
    // A key seen for the first time, cleared after its last window expired or idle for several
    // batches filters against an older watermark.
    Instant low =
        watermarkState.exists()
            ? new Instant(watermarkState.get())
            : BoundedWindow.TIMESTAMP_MIN_VALUE;
    Instant high = new Instant(timerValues.getCurrentWatermarkInMs());
    SparkTimerInternals timerInternals = SparkTimerInternals.forWatermarks(low, high);
    restoreTimers(timerInternals);
    return timerInternals;
  }

  @Override
  public Iterator<WindowedValue<OutputT>> handleInputRows(
      byte[] keyBytes, Iterator<WindowedValue<KV<K, V>>> rows, TimerValues timerValues) {
    K key = CoderHelpers.fromByteArray(keyBytes, inputCoder.getKeyCoder());
    List<WindowedValue<OutputT>> outputs = new ArrayList<>();
    this.currentOutputs = outputs;
    SparkTimerInternals timerInternals = timerInternals(timerValues);
    SparkStateInternals<K> state = stateInternals(key);

    processElements(key, rows, timerInternals, state);
    timerInternals.advanceWatermark();
    processTimers(key, timerInternals, state);

    persistTimers(timerInternals);
    reconcileWakeupTimer(timerInternals, null);
    storeWatermark(timerInternals);
    return ScalaInterop.scalaIterator(outputs);
  }

  @Override
  public Iterator<WindowedValue<OutputT>> handleExpiredTimer(
      byte[] keyBytes, TimerValues timerValues, ExpiredTimerInfo expiredTimerInfo) {
    K key = CoderHelpers.fromByteArray(keyBytes, inputCoder.getKeyCoder());
    List<WindowedValue<OutputT>> outputs = new ArrayList<>();
    this.currentOutputs = outputs;
    SparkTimerInternals timerInternals = timerInternals(timerValues);
    SparkStateInternals<K> state = stateInternals(key);
    timerInternals.advanceWatermark();

    processTimers(key, timerInternals, state);

    persistTimers(timerInternals);
    reconcileWakeupTimer(timerInternals, expiredTimerInfo.getExpiryTimeInMs());
    storeWatermark(timerInternals);
    return ScalaInterop.scalaIterator(outputs);
  }

  private SparkStateInternals<K> stateInternals(K key) {
    return SparkStateInternals.forKey(
        key, new MapStateAdapter(checkStateNotNull(beamState, "beamState not initialized")));
  }

  protected List<TimerData> dueEventTimeTimers(SparkTimerInternals timerInternals) {
    Instant watermark = timerInternals.currentInputWatermarkTime();
    return timerInternals.getTimers().stream()
        .filter(
            t -> t.getDomain().equals(TimeDomain.EVENT_TIME) && watermark.isAfter(t.getTimestamp()))
        .sorted(Comparator.comparing(TimerData::getTimestamp))
        .collect(Collectors.toList());
  }

  protected abstract void processElements(
      K key,
      Iterator<WindowedValue<KV<K, V>>> rows,
      SparkTimerInternals timers,
      SparkStateInternals<K> state);

  protected abstract void processTimers(
      K key, SparkTimerInternals timers, SparkStateInternals<K> state);

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
      if (timersState.exists()) {
        timersState.clear();
      }
    } else {
      timersState.update(
          CoderHelpers.toByteArray(new ArrayList<>(timers), ListCoder.of(timerDataCoder)));
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
      return mapState.getValue(key(namespace, stateId));
    }

    @Override
    public void put(String namespace, String stateId, byte[] value) {
      mapState.updateValue(key(namespace, stateId), value);
    }

    @Override
    public void remove(String namespace, String stateId) {
      mapState.removeKey(key(namespace, stateId));
    }

    private static String key(String namespace, String stateId) {
      return namespace.length() + ":" + namespace + stateId;
    }
  }
}
