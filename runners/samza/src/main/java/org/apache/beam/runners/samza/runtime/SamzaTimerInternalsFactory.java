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
package org.apache.beam.runners.samza.runtime;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.SamzaRunner;
import org.apache.beam.runners.samza.state.SamzaSetState;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.operators.Scheduler;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TimerInternalsFactory} that creates Samza {@link TimerInternals}. This class keeps track
 * of the {@link org.apache.beam.runners.core.TimerInternals.TimerData} added to the sorted timer
 * set, and removes the ready timers when the watermark is advanced.
 */
public class SamzaTimerInternalsFactory<K> implements TimerInternalsFactory<K> {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaTimerInternalsFactory.class);

  private final NavigableSet<KeyedTimerData<K>> eventTimeTimers;
  private final Coder<K> keyCoder;
  private final Scheduler<KeyedTimerData<K>> timerRegistry;
  private final int timerBufferSize;
  private final SamzaTimerState state;
  private final IsBounded isBounded;

  private Instant inputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private Instant outputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private SamzaTimerInternalsFactory(
      Coder<K> keyCoder,
      Scheduler<KeyedTimerData<K>> timerRegistry,
      int timerBufferSize,
      String timerStateId,
      SamzaStoreStateInternals.Factory<?> nonKeyedStateInternalsFactory,
      Coder<BoundedWindow> windowCoder,
      IsBounded isBounded) {
    this.keyCoder = keyCoder;
    this.timerRegistry = timerRegistry;
    this.timerBufferSize = timerBufferSize;
    this.eventTimeTimers = new TreeSet<>();
    this.state = new SamzaTimerState(timerStateId, nonKeyedStateInternalsFactory, windowCoder);
    this.isBounded = isBounded;
  }

  static <K> SamzaTimerInternalsFactory<K> createTimerInternalFactory(
      Coder<K> keyCoder,
      Scheduler<KeyedTimerData<K>> timerRegistry,
      String timerStateId,
      SamzaStoreStateInternals.Factory<?> nonKeyedStateInternalsFactory,
      WindowingStrategy<?, BoundedWindow> windowingStrategy,
      IsBounded isBounded,
      SamzaPipelineOptions pipelineOptions) {

    final Coder<BoundedWindow> windowCoder = windowingStrategy.getWindowFn().windowCoder();

    return new SamzaTimerInternalsFactory<>(
        keyCoder,
        timerRegistry,
        pipelineOptions.getTimerBufferSize(),
        timerStateId,
        nonKeyedStateInternalsFactory,
        windowCoder,
        isBounded);
  }

  @Override
  public TimerInternals timerInternalsForKey(@Nullable K key) {
    final byte[] keyBytes;

    if (keyCoder == null) {
      if (key != null) {
        throw new IllegalArgumentException(
            String.format("Received non-null key for unkeyed timer factory. Key: %s", key));
      }
      keyBytes = null;
    } else {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        keyCoder.encode(key, baos);
      } catch (IOException e) {
        throw new RuntimeException("Could not encode key: " + key, e);
      }
      keyBytes = baos.toByteArray();
    }

    return new SamzaTimerInternals(keyBytes, key);
  }

  public void setInputWatermark(Instant watermark) {
    if (watermark.isBefore(inputWatermark)) {
      throw new IllegalArgumentException("New input watermark is before current watermark");
    }
    LOG.debug("Advancing input watermark from {} to {}.", inputWatermark, watermark);
    inputWatermark = watermark;
  }

  public void setOutputWatermark(Instant watermark) {
    if (watermark.isAfter(inputWatermark)) {
      LOG.debug("Clipping new output watermark from {} to {}.", watermark, inputWatermark);
      watermark = inputWatermark;
    }

    if (watermark.isBefore(outputWatermark)) {
      throw new IllegalArgumentException("New output watermark is before current watermark");
    }
    LOG.debug("Advancing output watermark from {} to {}.", outputWatermark, watermark);
    outputWatermark = watermark;
  }

  public Collection<KeyedTimerData<K>> removeReadyTimers() {
    final Collection<KeyedTimerData<K>> readyTimers = new ArrayList<>();

    while (!eventTimeTimers.isEmpty()
        && eventTimeTimers.first().getTimerData().getTimestamp().isBefore(inputWatermark)) {
      final KeyedTimerData<K> keyedTimerData = eventTimeTimers.pollFirst();
      readyTimers.add(keyedTimerData);
      state.deletePersisted(keyedTimerData);

      // if all the buffered timers are processed, load the next batch from state
      if (eventTimeTimers.isEmpty()) {
        state.loadEventTimeTimers();
      }
    }

    return readyTimers;
  }

  public void removeProcessingTimer(KeyedTimerData<K> keyedTimerData) {
    state.deletePersisted(keyedTimerData);
  }

  public Instant getInputWatermark() {
    return inputWatermark;
  }

  public Instant getOutputWatermark() {
    return outputWatermark;
  }

  private class SamzaTimerInternals implements TimerInternals {
    private final byte[] keyBytes;
    private final K key;

    public SamzaTimerInternals(byte[] keyBytes, K key) {
      this.keyBytes = keyBytes;
      this.key = key;
    }

    @Override
    public void setTimer(
        StateNamespace namespace, String timerId, Instant target, TimeDomain timeDomain) {
      setTimer(TimerData.of(timerId, namespace, target, timeDomain));
    }

    @Override
    public void setTimer(TimerData timerData) {
      if (isBounded == IsBounded.UNBOUNDED
          && timerData.getTimestamp().getMillis()
              >= GlobalWindow.INSTANCE.maxTimestamp().getMillis()) {
        // No need to register a timer of max timestamp if the input is unbounded
        return;
      }

      final KeyedTimerData<K> keyedTimerData = new KeyedTimerData<>(keyBytes, key, timerData);

      // persist it first
      state.persist(keyedTimerData);

      switch (timerData.getDomain()) {
        case EVENT_TIME:
          eventTimeTimers.add(keyedTimerData);
          while (eventTimeTimers.size() > timerBufferSize) {
            eventTimeTimers.pollLast();
          }
          break;

        case PROCESSING_TIME:
          timerRegistry.schedule(keyedTimerData, timerData.getTimestamp().getMillis());
          break;

        default:
          throw new UnsupportedOperationException(
              String.format(
                  "%s currently only supports even time or processing time", SamzaRunner.class));
      }
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
      deleteTimer(TimerData.of(timerId, namespace, Instant.now(), timeDomain));
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId) {
      deleteTimer(TimerData.of(timerId, namespace, Instant.now(), TimeDomain.EVENT_TIME));
    }

    @Override
    public void deleteTimer(TimerData timerData) {
      final KeyedTimerData<K> keyedTimerData = new KeyedTimerData<>(keyBytes, key, timerData);

      state.deletePersisted(keyedTimerData);

      switch (timerData.getDomain()) {
        case EVENT_TIME:
          eventTimeTimers.remove(keyedTimerData);
          break;

        case PROCESSING_TIME:
          timerRegistry.delete(keyedTimerData);
          break;

        default:
          throw new UnsupportedOperationException(
              String.format("%s currently only supports event time", SamzaRunner.class));
      }
    }

    @Override
    public Instant currentProcessingTime() {
      return new Instant();
    }

    @Override
    public Instant currentSynchronizedProcessingTime() {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support synchronized processing time", SamzaRunner.class));
    }

    @Override
    public Instant currentInputWatermarkTime() {
      return inputWatermark;
    }

    @Override
    public Instant currentOutputWatermarkTime() {
      return outputWatermark;
    }
  }

  private class SamzaTimerState {
    private final SamzaSetState<KeyedTimerData<K>> eventTimerTimerState;
    private final SamzaSetState<KeyedTimerData<K>> processingTimerTimerState;

    SamzaTimerState(
        String timerStateId,
        SamzaStoreStateInternals.Factory<?> nonKeyedStateInternalsFactory,
        Coder<BoundedWindow> windowCoder) {

      this.eventTimerTimerState =
          (SamzaSetState<KeyedTimerData<K>>)
              nonKeyedStateInternalsFactory
                  .stateInternalsForKey(null)
                  .state(
                      StateNamespaces.global(),
                      StateTags.set(
                          timerStateId + "-et",
                          new KeyedTimerData.KeyedTimerDataCoder<>(keyCoder, windowCoder)));

      this.processingTimerTimerState =
          (SamzaSetState<KeyedTimerData<K>>)
              nonKeyedStateInternalsFactory
                  .stateInternalsForKey(null)
                  .state(
                      StateNamespaces.global(),
                      StateTags.set(
                          timerStateId + "-pt",
                          new KeyedTimerData.KeyedTimerDataCoder<>(keyCoder, windowCoder)));

      restore();
    }

    void persist(KeyedTimerData<K> keyedTimerData) {
      switch (keyedTimerData.getTimerData().getDomain()) {
        case EVENT_TIME:
          if (!eventTimeTimers.contains(keyedTimerData)) {
            eventTimerTimerState.add(keyedTimerData);
          }
          break;

        case PROCESSING_TIME:
          processingTimerTimerState.add(keyedTimerData);
          break;

        default:
          throw new UnsupportedOperationException(
              String.format("%s currently only supports event time", SamzaRunner.class));
      }
    }

    void deletePersisted(KeyedTimerData<K> keyedTimerData) {
      switch (keyedTimerData.getTimerData().getDomain()) {
        case EVENT_TIME:
          eventTimerTimerState.remove(keyedTimerData);
          break;

        case PROCESSING_TIME:
          processingTimerTimerState.remove(keyedTimerData);
          break;

        default:
          throw new UnsupportedOperationException(
              String.format("%s currently only supports event time", SamzaRunner.class));
      }
    }

    private void loadEventTimeTimers() {
      if (!eventTimerTimerState.isEmpty().read()) {
        final Iterator<KeyedTimerData<K>> iter = eventTimerTimerState.readIterator().read();
        int i = 0;
        for (; i < timerBufferSize && iter.hasNext(); i++) {
          eventTimeTimers.add(iter.next());
        }

        LOG.info("Loaded {} event time timers in memory", i);

        // manually close the iterator here
        final SamzaStoreStateInternals.KeyValueIteratorState iteratorState =
            (SamzaStoreStateInternals.KeyValueIteratorState) eventTimerTimerState;

        iteratorState.closeIterators();
      }
    }

    private void loadProcessingTimeTimers() {
      if (!processingTimerTimerState.isEmpty().read()) {
        final Iterator<KeyedTimerData<K>> iter = processingTimerTimerState.readIterator().read();
        // since the iterator will reach to the end, it will be closed automatically
        int count = 0;
        while (iter.hasNext()) {
          final KeyedTimerData<K> keyedTimerData = iter.next();
          timerRegistry.schedule(
              keyedTimerData, keyedTimerData.getTimerData().getTimestamp().getMillis());
          ++count;
        }

        LOG.info("Loaded {} processing time timers in memory", count);
      }
    }

    private void restore() {
      loadEventTimeTimers();
      loadProcessingTimeTimers();
    }
  }
}
