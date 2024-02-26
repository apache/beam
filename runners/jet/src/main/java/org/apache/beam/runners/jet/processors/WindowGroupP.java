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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.LateDataUtils;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
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
import org.apache.beam.sdk.util.construction.TriggerTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Jet {@link com.hazelcast.jet.core.Processor} implementation for Beam's GroupByKeyOnly +
 * GroupAlsoByWindow primitives.
 *
 * @param <K> key type of {@link KV} values from the output of this primitive
 * @param <V> type of elements being windowed
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WindowGroupP<K, V> extends AbstractProcessor {

  private static final int PROCESSING_TIME_MIN_INCREMENT = 100;

  private static final Object COMPLETE_MARKER = new Object();
  private static final Object TRY_PROCESS_MARKER = new Object();

  private final SerializablePipelineOptions pipelineOptions;
  private final Coder<V> inputValueValueCoder;
  private final Coder outputCoder;
  private final WindowingStrategy<V, BoundedWindow> windowingStrategy;
  private final Map<Utils.ByteArrayKey, KeyManager> keyManagers = new HashMap<>();
  private final AppendableTraverser<Object> appendableTraverser =
      new AppendableTraverser<>(128); // todo: right capacity?
  private final FlatMapper<Object, Object> flatMapper;

  @SuppressWarnings({"FieldCanBeLocal", "unused"})
  private final String ownerId; // do not remove, useful for debugging

  private Instant latestWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private long lastProcessingTime = System.currentTimeMillis();

  private WindowGroupP(
      SerializablePipelineOptions pipelineOptions,
      WindowedValue.WindowedValueCoder<KV<K, V>> inputCoder,
      Coder outputCoder,
      WindowingStrategy<V, BoundedWindow> windowingStrategy,
      String ownerId) {
    this.pipelineOptions = pipelineOptions;
    KvCoder<K, V> inputValueCoder = (KvCoder<K, V>) inputCoder.getValueCoder();
    this.inputValueValueCoder = inputValueCoder.getValueCoder();
    this.outputCoder = outputCoder;
    this.windowingStrategy = windowingStrategy;
    this.ownerId = ownerId;

    this.flatMapper =
        flatMapper(
            item -> {
              if (COMPLETE_MARKER == item) {
                long millis = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
                advanceWatermark(millis);
              } else if (TRY_PROCESS_MARKER == item) {
                Instant now = Instant.now();
                if (now.getMillis() - lastProcessingTime > PROCESSING_TIME_MIN_INCREMENT) {
                  lastProcessingTime = now.getMillis();
                  advanceProcessingTime(now);
                }
              } else if (item instanceof Watermark) {
                advanceWatermark(((Watermark) item).timestamp());
                appendableTraverser.append(item);
              } else {
                WindowedValue<KV<K, V>> windowedValue =
                    Utils.decodeWindowedValue((byte[]) item, inputCoder);
                KV<K, V> kv = windowedValue.getValue();
                K key = kv.getKey();
                V value = kv.getValue();
                Utils.ByteArrayKey keyBytes =
                    new Utils.ByteArrayKey(Utils.encode(key, inputValueCoder.getKeyCoder()));
                WindowedValue<V> updatedWindowedValue =
                    WindowedValue.of(
                        value,
                        windowedValue.getTimestamp(),
                        windowedValue.getWindows(),
                        windowedValue.getPane());
                keyManagers
                    .computeIfAbsent(keyBytes, x -> new KeyManager(key))
                    .processElement(updatedWindowedValue);
              }
              return appendableTraverser;
            });
  }

  @SuppressWarnings("unchecked")
  public static <K, V> SupplierEx<Processor> supplier(
      SerializablePipelineOptions pipelineOptions,
      WindowedValue.WindowedValueCoder<KV<K, V>> inputCoder,
      Coder outputCoder,
      WindowingStrategy windowingStrategy,
      String ownerId) {
    return () ->
        new WindowGroupP<>(pipelineOptions, inputCoder, outputCoder, windowingStrategy, ownerId);
  }

  @Override
  public boolean tryProcess() {
    return flatMapper.tryProcess(TRY_PROCESS_MARKER);
  }

  @Override
  protected boolean tryProcess(int ordinal, @Nonnull Object item) {
    return flatMapper.tryProcess(item);
  }

  @Override
  public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
    return flatMapper.tryProcess(watermark);
  }

  @Override
  public boolean complete() {
    return flatMapper.tryProcess(COMPLETE_MARKER);
  }

  private void advanceWatermark(long millis) {
    this.latestWatermark = new Instant(millis);
    Instant now = Instant.now();
    for (KeyManager m : keyManagers.values()) {
      m.advanceWatermark(latestWatermark, now);
    }
  }

  private void advanceProcessingTime(Instant now) {
    for (KeyManager m : keyManagers.values()) {
      m.advanceProcessingTime(now);
    }
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

  private class KeyManager {

    private final InMemoryTimerInternals timerInternals;
    private final InMemoryStateInternalsImpl stateInternals;
    private final ReduceFnRunner<K, V, Iterable<V>, BoundedWindow> reduceFnRunner;

    KeyManager(K key) {
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
                  byte[] encodedValue = Utils.encode(windowedValue, outputCoder);
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
      advanceWatermark(latestWatermark, Instant.now());
    }

    void advanceWatermark(Instant watermark, Instant now) {
      try {
        timerInternals.advanceProcessingTime(now);
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

    void advanceProcessingTime(Instant now) {
      try {
        timerInternals.advanceProcessingTime(now);
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

    public void processElement(WindowedValue<V> windowedValue) {
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
      boolean hasExpired = false;
      for (Iterator<? extends BoundedWindow> iterator = windows.iterator();
          !hasExpired && iterator.hasNext(); ) {
        if (isExpiredWindow(iterator.next())) {
          hasExpired = true;
        }
      }
      if (!hasExpired) {
        return windows;
      }
      // if there are expired items, return a filtered collection
      return windows.stream()
          .filter(window -> !isExpiredWindow(window))
          .collect(Collectors.toList());
    }

    private boolean isExpiredWindow(BoundedWindow window) {
      Instant inputWM = timerInternals.currentInputWatermarkTime();
      return LateDataUtils.garbageCollectionTime(window, windowingStrategy).isBefore(inputWM);
    }
  }
}
