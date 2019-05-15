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
package org.apache.beam.runners.jet.processors;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.LateDataUtils;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.TriggerTranslation;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.runners.jet.Utils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * Jet {@link com.hazelcast.jet.core.Processor} implementation for Beam's GroupByKeyOnly +
 * GroupAlsoByWindow primitives.
 *
 * @param <K> key type of {@link KV} values from the output of this primitive
 * @param <V> type of elements being windowed
 */
public class WindowGroupP<K, V> extends AbstractProcessor {

  private static final byte[] EMPTY_BYTES = new byte[0];
  private final SerializablePipelineOptions pipelineOptions;
  private final Coder<V> inputValueValueCoder;
  private final Coder outputCoder;
  private final WindowingStrategy<V, BoundedWindow> windowingStrategy;
  private final Map<K, KeyManager> keyManagers = new HashMap<>();
  private final AppendableTraverser<byte[]> appendableTraverser =
      new AppendableTraverser<>(128); // todo: right capacity?
  private final FlatMapper<byte[], byte[]> flatMapper;

  @SuppressWarnings({"FieldCanBeLocal", "unused"})
  private final String ownerId; // do not remove, useful for debugging

  private Instant latestWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private WindowGroupP(
      SerializablePipelineOptions pipelineOptions,
      Coder inputCoder,
      Coder inputValueCoder,
      Coder outputCoder,
      WindowingStrategy<V, BoundedWindow> windowingStrategy,
      String ownerId) {
    this.pipelineOptions = pipelineOptions;
    this.inputValueValueCoder = ((KvCoder<K, V>) inputValueCoder).getValueCoder();
    this.outputCoder = outputCoder;
    this.windowingStrategy = windowingStrategy;
    this.ownerId = ownerId;

    this.flatMapper =
        flatMapper(
            item -> {
              if (item.length > 0) {
                WindowedValue<KV<K, V>> windowedValue = Utils.decodeWindowedValue(item, inputCoder);
                KV<K, V> kv = windowedValue.getValue();
                K key = kv.getKey();
                V value = kv.getValue();
                WindowedValue<V> updatedWindowedValue =
                    WindowedValue.of(
                        value,
                        windowedValue.getTimestamp(),
                        windowedValue.getWindows(),
                        windowedValue.getPane());
                KeyManager keyManager =
                    keyManagers.computeIfAbsent(key, k -> new KeyManager(k, latestWatermark));
                keyManager.processElement(updatedWindowedValue);
              }
              return appendableTraverser;
            });
  }

  @SuppressWarnings("unchecked")
  public static SupplierEx<Processor> supplier(
      SerializablePipelineOptions pipelineOptions,
      Coder inputValueCoder,
      Coder inputCoder,
      Coder outputCoder,
      WindowingStrategy windowingStrategy,
      String ownerId) {
    return () ->
        new WindowGroupP<>(
            pipelineOptions, inputCoder, inputValueCoder, outputCoder, windowingStrategy, ownerId);
  }

  @Override
  protected boolean tryProcess(int ordinal, @Nonnull Object item) {
    return flatMapper.tryProcess((byte[]) item);
  }

  @Override
  public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
    advanceWatermark(watermark.timestamp());
    return flatMapper.tryProcess(EMPTY_BYTES);
  }

  @Override
  public boolean complete() {
    advanceWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
    return flatMapper.tryProcess(EMPTY_BYTES);
  }

  private void advanceWatermark(long millis) {
    this.latestWatermark = new Instant(millis);
    this.keyManagers.values().forEach(m -> m.advanceWatermark(latestWatermark));
  }

  private static class InMemoryStateInternalsImpl extends InMemoryStateInternals {

    InMemoryStateInternalsImpl(@Nullable Object key) {
      super(key);
    }

    Instant earliestWatermarkHold() {
      Instant minimum = null;
      for (State storage : inMemoryState.values()) {
        if (storage instanceof WatermarkHoldState) {
          Instant hold = ((WatermarkHoldState) storage).read();
          if (minimum == null || (hold != null && hold.isBefore(minimum))) {
            minimum = hold;
          }
        }
      }
      return minimum;
    }
  }

  /** Helper class that holds all the per key based data structures needed. */
  private class KeyManager {

    private final InMemoryTimerInternals timerInternals;
    private final InMemoryStateInternalsImpl stateInternals;
    private final ReduceFnRunner<K, V, Iterable<V>, BoundedWindow> reduceFnRunner;

    KeyManager(K key, Instant currentWaterMark) {
      this.timerInternals = new InMemoryTimerInternals();
      this.stateInternals = new InMemoryStateInternalsImpl(key);
      this.reduceFnRunner =
          new ReduceFnRunner<>(
              key,
              windowingStrategy,
              ExecutableTriggerStateMachine.create(
                  TriggerStateMachines.stateMachineForTrigger(
                      TriggerTranslation.toProto(windowingStrategy.getTrigger()))),
              stateInternals,
              timerInternals,
              new OutputWindowedValue<KV<K, Iterable<V>>>() {
                @Override
                public void outputWindowedValue(
                    KV<K, Iterable<V>> output,
                    Instant timestamp,
                    Collection<? extends BoundedWindow> windows,
                    PaneInfo pane) {
                  WindowedValue<KV<K, Iterable<V>>> windowedValue =
                      WindowedValue.of(output, timestamp, windows, pane);
                  byte[] encodedValue = Utils.encodeWindowedValue(windowedValue, outputCoder);
                  //noinspection ResultOfMethodCallIgnored
                  appendableTraverser.append(encodedValue);
                }

                @Override
                public <AdditionalOutputT> void outputWindowedValue(
                    TupleTag<AdditionalOutputT> tag,
                    AdditionalOutputT output,
                    Instant timestamp,
                    Collection<? extends BoundedWindow> windows,
                    PaneInfo pane) {
                  throw new UnsupportedOperationException("Grouping should not use side outputs");
                }
              },
              NullSideInputReader.empty(),
              SystemReduceFn.buffering(inputValueValueCoder),
              pipelineOptions.get());
      advanceWatermark(currentWaterMark);
    }

    void advanceWatermark(Instant watermark) {
      try {
        timerInternals.advanceProcessingTime(Instant.now());
        advanceInputWatermark(watermark);
        Instant hold = stateInternals.earliestWatermarkHold();
        if (hold == null) {
          WindowTracing.trace(
              "TestInMemoryTimerInternals.advanceInputWatermark: no holds, "
                  + "so output watermark = input watermark");
          hold = timerInternals.currentInputWatermarkTime();
        }
        advanceOutputWatermark(hold);
        reduceFnRunner.persist();
      } catch (Exception e) {
        throw ExceptionUtil.rethrow(e);
      }
    }

    private void advanceInputWatermark(Instant watermark) throws Exception {
      timerInternals.advanceInputWatermark(watermark);
      while (true) {
        TimerInternals.TimerData timer;
        List<TimerInternals.TimerData> timers = new ArrayList<>();
        while ((timer = timerInternals.removeNextEventTimer()) != null) {
          timers.add(timer);
        }
        if (timers.isEmpty()) {
          break;
        }
        reduceFnRunner.onTimers(timers);
      }
    }

    private void advanceOutputWatermark(Instant watermark) {
      Objects.requireNonNull(watermark);
      timerInternals.advanceOutputWatermark(watermark);
    }

    void processElement(WindowedValue<V> windowedValue) {
      Collection<? extends BoundedWindow> windows = dropLateWindows(windowedValue.getWindows());
      if (!windows.isEmpty()) {
        try {
          reduceFnRunner.processElements(
              Collections.singletonList(
                  windowedValue)); // todo: try to process more than one element at a time...
          reduceFnRunner.persist();
        } catch (Exception e) {
          throw ExceptionUtil.rethrow(e);
        }
      }
    }

    private Collection<? extends BoundedWindow> dropLateWindows(
        Collection<? extends BoundedWindow> windows) {
      List<BoundedWindow> filteredWindows =
          new ArrayList<>(
              windows
                  .size()); // todo: reduce garbage, most of the time it will be one window only and
      // there won't be expired windows
      for (BoundedWindow window : windows) {
        if (!isExpiredWindow(window)) {
          filteredWindows.add(window);
        }
      }
      return filteredWindows;
    }

    private boolean isExpiredWindow(BoundedWindow window) {
      Instant inputWM = timerInternals.currentInputWatermarkTime();
      return LateDataUtils.garbageCollectionTime(window, windowingStrategy).isBefore(inputWM);
    }
  }
}
