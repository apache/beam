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

import com.google.auto.value.AutoValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.SamzaRunner;
import org.apache.beam.runners.samza.state.SamzaMapState;
import org.apache.beam.runners.samza.state.SamzaSetState;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.operators.Scheduler;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TimerInternalsFactory} that creates Samza {@link TimerInternals}. This class keeps track
 * of the {@link org.apache.beam.runners.core.TimerInternals.TimerData} added to the sorted timer
 * set, and removes the ready timers when the watermark is advanced.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SamzaTimerInternalsFactory<K> implements TimerInternalsFactory<K> {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaTimerInternalsFactory.class);
  private final NavigableSet<KeyedTimerData<K>> eventTimeBuffer;
  private final Coder<K> keyCoder;
  // Buffer size maintained to represent the timers scheduled in the timerRegistry
  private final NavigableSet<KeyedTimerData<K>> processTimeBuffer;
  private final Scheduler<KeyedTimerData<K>> timerRegistry;
  private final SamzaTimerState state;
  private final IsBounded isBounded;

  private Instant inputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private Instant outputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  // Size of each event timer is around 200B, by default with buffer size 50k, the default size is
  // 10M
  private final int maxEventTimerBufferSize;
  // Flag variable to indicate whether events timers are present in state
  private boolean eventsTimersInState = true;

  // Size of each processing timer
  private final long maxProcessTimerBufferSize;
  // Flag variable to indicate whether process timers are present in state
  private boolean processTimersInState = true;

  // The maximum number of ready timers to process at once per watermark.
  private final long maxReadyTimersToProcessOnce;

  private SamzaTimerInternalsFactory(
      Coder<K> keyCoder,
      Scheduler<KeyedTimerData<K>> timerRegistry,
      String timerStateId,
      SamzaStoreStateInternals.Factory<?> nonKeyedStateInternalsFactory,
      Coder<BoundedWindow> windowCoder,
      IsBounded isBounded,
      SamzaPipelineOptions pipelineOptions) {
    this.keyCoder = keyCoder;
    this.processTimeBuffer = new TreeSet<>();
    this.eventTimeBuffer = new TreeSet<>();
    this.maxEventTimerBufferSize =
        pipelineOptions.getEventTimerBufferSize(); // must be placed before state initialization
    this.maxProcessTimerBufferSize = pipelineOptions.getProcessingTimerBufferSize();
    this.maxReadyTimersToProcessOnce = pipelineOptions.getMaxReadyTimersToProcessOnce();
    this.timerRegistry = timerRegistry;
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
        timerStateId,
        nonKeyedStateInternalsFactory,
        windowCoder,
        isBounded,
        pipelineOptions);
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

  /**
   * The method is called when watermark comes. It compares timers in memory buffer with watermark
   * to prepare ready timers. When memory buffer is empty, it asks store to reload timers into
   * buffer. note that the number of timers returned may be larger than memory buffer size.
   *
   * @return a collection of ready timers to be fired
   */
  public Collection<KeyedTimerData<K>> removeReadyTimers() {
    final Collection<KeyedTimerData<K>> readyTimers = new ArrayList<>();

    while (!eventTimeBuffer.isEmpty()
        && !eventTimeBuffer.first().getTimerData().getTimestamp().isAfter(inputWatermark)
        && readyTimers.size() < maxReadyTimersToProcessOnce) {

      final KeyedTimerData<K> keyedTimerData = eventTimeBuffer.pollFirst();
      readyTimers.add(keyedTimerData);
      state.deletePersisted(keyedTimerData);

      if (eventTimeBuffer.isEmpty()) {
        state.reloadEventTimeTimers();
      }
    }
    LOG.debug("Removed {} ready timers", readyTimers.size());

    if (readyTimers.size() >= maxReadyTimersToProcessOnce
        && !eventTimeBuffer.isEmpty()
        && eventTimeBuffer.first().getTimerData().getTimestamp().isBefore(inputWatermark)) {
      LOG.warn(
          "Loaded {} expired timers, the remaining will be processed at next watermark.",
          maxReadyTimersToProcessOnce);
    }
    return readyTimers;
  }

  public void removeProcessingTimer(KeyedTimerData<K> keyedTimerData) {
    state.deletePersisted(keyedTimerData);
    processTimeBuffer.remove(keyedTimerData);
    // Reload timers for process time if buffer is empty
    if (processTimeBuffer.isEmpty()) {
      state.reloadProcessingTimeTimers();
    }
  }

  public Instant getInputWatermark() {
    return inputWatermark;
  }

  public Instant getOutputWatermark() {
    return outputWatermark;
  }

  // for unit test only
  NavigableSet<KeyedTimerData<K>> getEventTimeBuffer() {
    return eventTimeBuffer;
  }

  Set<KeyedTimerData<K>> getProcessTimeBuffer() {
    return processTimeBuffer;
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
        StateNamespace namespace,
        String timerId,
        String timerFamilyId,
        Instant target,
        Instant outputTimestamp,
        TimeDomain timeDomain) {
      setTimer(
          TimerData.of(timerId, timerFamilyId, namespace, target, outputTimestamp, timeDomain));
    }

    @Override
    public void setTimer(TimerData timerData) {
      if (isBounded == IsBounded.UNBOUNDED
          && timerData.getTimestamp().getMillis()
              > GlobalWindow.INSTANCE.maxTimestamp().getMillis()) {
        // No need to register a timer greater than maxTimestamp if the input is unbounded.
        // 1. It will ignore timers with (maxTimestamp + 1) created by stateful ParDo with global
        // window.
        // 2. It will register timers with maxTimestamp so that global window can be closed
        // correctly when max watermark comes.
        return;
      }

      final KeyedTimerData<K> keyedTimerData = new KeyedTimerData<>(keyBytes, key, timerData);

      // if the buffers contain the keyedTimerData then it is already in state
      if (eventTimeBuffer.contains(keyedTimerData) || processTimeBuffer.contains(keyedTimerData)) {
        return;
      }

      final Long lastTimestamp = state.get(keyedTimerData);
      final Long newTimestamp = timerData.getTimestamp().getMillis();

      if (newTimestamp.equals(lastTimestamp)) {
        return;
      }

      if (lastTimestamp != null) {
        deleteTimer(
            timerData.getNamespace(),
            timerData.getTimerId(),
            timerData.getTimerFamilyId(),
            new Instant(lastTimestamp),
            new Instant(lastTimestamp),
            timerData.getDomain());
      }

      // persist it first
      state.persist(keyedTimerData);

      // add timer data to buffer where applicable
      switch (timerData.getDomain()) {
        case EVENT_TIME:
          /*
           * To determine if the upcoming KeyedTimerData could be added to the Buffer while
           * guaranteeing the Buffer's timestamps are all <= than those in State Store to preserve
           * timestamp eviction priority:
           *
           * <p>If eventTimeBuffer size < maxEventTimerBufferSize and there are no more potential earlier timers
           * in state, or newTimestamp < maxEventTimeInBuffer then it indicates that there are entries greater than
           * newTimestamp, so it is safe to add it to the buffer
           *
           * <p>In case that the Buffer is full, we remove the largest timer from memory according
           * to {@link KeyedTimerData.compareTo()}
           */
          if ((maxEventTimerBufferSize > eventTimeBuffer.size() && !eventsTimersInState) ||
              newTimestamp < eventTimeBuffer.last().getTimerData().getTimestamp().getMillis()) {
            eventTimeBuffer.add(keyedTimerData);
            if (eventTimeBuffer.size() > maxEventTimerBufferSize) {
              eventTimeBuffer.pollLast();
            }
          } else {
            eventsTimersInState = true;
          }
          break;

        case PROCESSING_TIME:
          // The timer is added to the buffer if the new timestamp will be triggered earlier than
          // the oldest time in the current buffer.
          // Any timers removed from the buffer will also be deleted from the scheduler.
          if ((maxProcessTimerBufferSize > processTimeBuffer.size() && !processTimersInState) ||
              newTimestamp < processTimeBuffer.last().getTimerData().getTimestamp().getMillis()) {
            processTimeBuffer.add(keyedTimerData);
            timerRegistry.schedule(keyedTimerData, newTimestamp);
            if (processTimeBuffer.size() > maxProcessTimerBufferSize) {
              KeyedTimerData oldKeyedTimerData = processTimeBuffer.pollLast();
              timerRegistry.delete(oldKeyedTimerData);
            }
          } else {
            processTimersInState = true;
          }

          break;

        default:
          throw new UnsupportedOperationException(
              String.format(
                  "%s currently only supports even time or processing time", SamzaRunner.class));
      }
    }

    /** @deprecated use {@link #deleteTimer(StateNamespace, String, String, TimeDomain)}. */
    @Override
    @Deprecated
    public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
      deleteTimer(namespace, timerId, timerFamilyId, TimeDomain.EVENT_TIME);
    }

    /** @deprecated use {@link #deleteTimer(StateNamespace, String, String, TimeDomain)}. */
    @Override
    @Deprecated
    public void deleteTimer(TimerData timerData) {
      deleteTimer(
          timerData.getNamespace(),
          timerData.getTimerId(),
          timerData.getTimerFamilyId(),
          timerData.getDomain());
    }

    @Override
    public void deleteTimer(
        StateNamespace namespace, String timerId, String timerFamilyId, TimeDomain timeDomain) {
      final TimerKey<K> timerKey = TimerKey.of(key, namespace, timerId, timerFamilyId);
      final Long lastTimestamp = state.get(timerKey, timeDomain);

      if (lastTimestamp == null) {
        return;
      }

      final Instant timestamp = Instant.ofEpochMilli(lastTimestamp);
      deleteTimer(namespace, timerId, timerFamilyId, timestamp, timestamp, timeDomain);
    }

    private void deleteTimer(
        StateNamespace namespace,
        String timerId,
        String timerFamilyId,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain timeDomain) {
      final TimerData timerData =
          TimerData.of(timerId, timerFamilyId, namespace, timestamp, outputTimestamp, timeDomain);
      final KeyedTimerData<K> keyedTimerData = new KeyedTimerData<>(keyBytes, key, timerData);

      state.deletePersisted(keyedTimerData);

      // delete from buffers
      switch (timerData.getDomain()) {
        case EVENT_TIME:
          eventTimeBuffer.remove(keyedTimerData);
          break;

        case PROCESSING_TIME:
          processTimeBuffer.remove(keyedTimerData);
          timerRegistry.delete(keyedTimerData);
          break;

        default:
          throw new UnsupportedOperationException(
              String.format(
                  "%s currently only supports event time or processing time but get %s",
                  SamzaRunner.class, timerData.getDomain()));
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
    private final SamzaMapState<TimerKey<K>, Long> eventTimeTimerState;
    private final SamzaSetState<KeyedTimerData<K>> timestampSortedEventTimeTimerState;
    private final SamzaMapState<TimerKey<K>, Long> processingTimeTimerState;
    private final SamzaSetState<KeyedTimerData<K>> timestampSortedProcessTimeTimerState;

    SamzaTimerState(
        String timerStateId,
        SamzaStoreStateInternals.Factory<?> nonKeyedStateInternalsFactory,
        Coder<BoundedWindow> windowCoder) {

      this.eventTimeTimerState =
          (SamzaMapState<TimerKey<K>, Long>)
              nonKeyedStateInternalsFactory
                  .stateInternalsForKey(null)
                  .state(
                      StateNamespaces.global(),
                      StateTags.map(
                          timerStateId + "-et",
                          new TimerKeyCoder<>(keyCoder, windowCoder),
                          VarLongCoder.of()));

      this.timestampSortedEventTimeTimerState =
          (SamzaSetState<KeyedTimerData<K>>)
              nonKeyedStateInternalsFactory
                  .stateInternalsForKey(null)
                  .state(
                      StateNamespaces.global(),
                      StateTags.set(
                          timerStateId + "-ts",
                          new KeyedTimerData.KeyedTimerDataCoder<>(keyCoder, windowCoder)));

      this.processingTimeTimerState =
          (SamzaMapState<TimerKey<K>, Long>)
              nonKeyedStateInternalsFactory
                  .stateInternalsForKey(null)
                  .state(
                      StateNamespaces.global(),
                      StateTags.map(
                          timerStateId + "-pt",
                          new TimerKeyCoder<>(keyCoder, windowCoder),
                          VarLongCoder.of()));

      this.timestampSortedProcessTimeTimerState =
          (SamzaSetState<KeyedTimerData<K>>)
              nonKeyedStateInternalsFactory
                  .stateInternalsForKey(null)
                  .state(
                      StateNamespaces.global(),
                      StateTags.set(
                          timerStateId + "-process-timers-sorted",
                          new KeyedTimerData.KeyedTimerDataCoder<>(keyCoder, windowCoder)));

      init();
    }

    Long get(KeyedTimerData<K> keyedTimerData) {
      return get(TimerKey.of(keyedTimerData), keyedTimerData.getTimerData().getDomain());
    }

    Long get(TimerKey<K> key, TimeDomain domain) {
      switch (domain) {
        case EVENT_TIME:
          return eventTimeTimerState.get(key).read();

        case PROCESSING_TIME:
          return processingTimeTimerState.get(key).read();

        default:
          throw new UnsupportedOperationException(
              String.format(
                  "%s currently only supports event time or processing time but get %s",
                  SamzaRunner.class, domain));
      }
    }

    void persist(KeyedTimerData<K> keyedTimerData) {
      final TimerKey<K> timerKey = TimerKey.of(keyedTimerData);
      switch (keyedTimerData.getTimerData().getDomain()) {
        case EVENT_TIME:
          final Long timestamp = eventTimeTimerState.get(timerKey).read();

          // Remove migrated state if already present
          if (timestamp != null) {
            final KeyedTimerData<K> keyedTimerDataInStore =
                TimerKey.toKeyedTimerData(timerKey, timestamp, TimeDomain.EVENT_TIME, keyCoder);
            timestampSortedEventTimeTimerState.remove(keyedTimerDataInStore);
          }
          eventTimeTimerState.put(
              timerKey, keyedTimerData.getTimerData().getTimestamp().getMillis());

          timestampSortedEventTimeTimerState.add(keyedTimerData);

          break;

        case PROCESSING_TIME:
          final Long processTimestamp = processingTimeTimerState.get(timerKey).read();

          // Remove migrated state if already present
          if (processTimestamp != null) {
            final KeyedTimerData<K> keyedTimerDataInStore =
                TimerKey.toKeyedTimerData(
                    timerKey, processTimestamp, TimeDomain.PROCESSING_TIME, keyCoder);
            timestampSortedProcessTimeTimerState.remove(keyedTimerDataInStore);
          }

          processingTimeTimerState.put(
              timerKey, keyedTimerData.getTimerData().getTimestamp().getMillis());

          timestampSortedProcessTimeTimerState.add(keyedTimerData);

          break;

        default:
          throw new UnsupportedOperationException(
              String.format(
                  "%s currently only supports event time or processing time but get %s",
                  SamzaRunner.class, keyedTimerData.getTimerData().getDomain()));
      }
    }

    void deletePersisted(KeyedTimerData<K> keyedTimerData) {
      final TimerKey<K> timerKey = TimerKey.of(keyedTimerData);
      switch (keyedTimerData.getTimerData().getDomain()) {
        case EVENT_TIME:
          eventTimeTimerState.remove(timerKey);
          timestampSortedEventTimeTimerState.remove(keyedTimerData);
          break;

        case PROCESSING_TIME:
          processingTimeTimerState.remove(timerKey);
          timestampSortedProcessTimeTimerState.remove(keyedTimerData);
          break;

        default:
          throw new UnsupportedOperationException(
              String.format(
                  "%s currently only supports event time or processing time but get %s",
                  SamzaRunner.class, keyedTimerData.getTimerData().getDomain()));
      }
    }

    /**
     * Reload event time timers from state to memory buffer. Buffer size is bound by
     * maxEventTimerBufferSize
     */
    private void reloadEventTimeTimers() {
      final Iterator<KeyedTimerData<K>> iter =
          timestampSortedEventTimeTimerState.readIterator().read();

      while (iter.hasNext() && eventTimeBuffer.size() < maxEventTimerBufferSize) {
        final KeyedTimerData<K> keyedTimerData = iter.next();
        eventTimeBuffer.add(keyedTimerData);
      }

      timestampSortedEventTimeTimerState.closeIterators();
      LOG.info("Loaded {} event time timers in memory", eventTimeBuffer.size());

      if (eventTimeBuffer.size() < maxEventTimerBufferSize) {
        LOG.debug(
            "Event time timers in State is empty, filled {} timers out of {} buffer capacity",
            eventTimeBuffer.size(),
            maxEventTimerBufferSize);
        // Reset the flag variable to indicate there are no more KeyedTimerData in State
        eventsTimersInState = false;
      }
    }

    private void reloadProcessingTimeTimers() {
      final Iterator<KeyedTimerData<K>> iter =
          timestampSortedProcessTimeTimerState.readIterator().read();

      while (iter.hasNext() && processTimeBuffer.size() < maxProcessTimerBufferSize) {
        final KeyedTimerData keyedTimerData = iter.next();
        processTimeBuffer.add(keyedTimerData);
        timerRegistry.schedule(
            keyedTimerData, keyedTimerData.getTimerData().getTimestamp().getMillis());
      }
      timestampSortedProcessTimeTimerState.closeIterators();
      LOG.info("Loaded {} processing time timers in memory", processTimeBuffer.size());

      if (processTimeBuffer.size() < maxProcessTimerBufferSize) {
        LOG.debug(
            "Process time timers in State is empty, filled {} timers out of {} buffer capacity",
            processTimeBuffer.size(),
            maxProcessTimerBufferSize);
        // Reset the flag variable to indicate there are no more KeyedTimerData in State
        processTimersInState = false;
      }
    }

    /** Restore timer state from RocksDB. */
    private void init() {
      migrateToKeyedTimerState(
          eventTimeTimerState, timestampSortedEventTimeTimerState, TimeDomain.EVENT_TIME);
      migrateToKeyedTimerState(
          processingTimeTimerState,
          timestampSortedProcessTimeTimerState,
          TimeDomain.PROCESSING_TIME);

      reloadEventTimeTimers();
      reloadProcessingTimeTimers();
    }
  }

  /**
   * This is needed for migration of existing jobs. Give events in timerState, construct
   * keyedTimerState preparing for memory reloading.
   *
   * <p>TODO (dchen): To be removed after all jobs have migrated to use KeyedTimerData
   */
  private void migrateToKeyedTimerState(
      SamzaMapState<TimerKey<K>, Long> timerState,
      SamzaSetState<KeyedTimerData<K>> keyedTimerState,
      TimeDomain timeDomain) {
    final Iterator<Map.Entry<TimerKey<K>, Long>> timersIter = timerState.readIterator().read();
    // use hasNext to check empty, because this is relatively cheap compared with Iterators.size()
    if (timersIter.hasNext()) {
      final Iterator keyedTimerIter = keyedTimerState.readIterator().read();

      if (!keyedTimerIter.hasNext()) {
        // Migrate from timerState to keyedTimerState
        while (timersIter.hasNext()) {
          final Map.Entry<TimerKey<K>, Long> entry = timersIter.next();
          final KeyedTimerData<K> keyedTimerData =
              TimerKey.toKeyedTimerData(entry.getKey(), entry.getValue(), timeDomain, keyCoder);
          keyedTimerState.add(keyedTimerData);
        }
      }
      keyedTimerState.closeIterators();
    }
    timerState.closeIterators();
  }

  @AutoValue
  abstract static class TimerKey<K> {
    abstract @Nullable K getKey();

    abstract StateNamespace getStateNamespace();

    abstract String getTimerId();

    abstract String getTimerFamilyId();

    static <K> Builder<K> builder() {
      return new AutoValue_SamzaTimerInternalsFactory_TimerKey.Builder<>();
    }

    static <K> TimerKey<K> of(KeyedTimerData<K> keyedTimerData) {
      final TimerInternals.TimerData timerData = keyedTimerData.getTimerData();
      return of(
          keyedTimerData.getKey(),
          timerData.getNamespace(),
          timerData.getTimerId(),
          timerData.getTimerFamilyId());
    }

    static <K> TimerKey<K> of(
        K key, StateNamespace namespace, String timerId, String timerFamilyId) {
      return TimerKey.<K>builder()
          .setKey(key)
          .setStateNamespace(namespace)
          .setTimerId(timerId)
          .setTimerFamilyId(timerFamilyId)
          .build();
    }

    static <K> KeyedTimerData<K> toKeyedTimerData(
        TimerKey<K> timerKey, long timestamp, TimeDomain domain, Coder<K> keyCoder) {
      byte[] keyBytes = null;
      if (keyCoder != null && timerKey.getKey() != null) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          keyCoder.encode(timerKey.getKey(), baos);
        } catch (IOException e) {
          throw new RuntimeException("Could not encode key: " + timerKey.getKey(), e);
        }
        keyBytes = baos.toByteArray();
      }

      return new KeyedTimerData<>(
          keyBytes,
          timerKey.getKey(),
          TimerInternals.TimerData.of(
              timerKey.getTimerId(),
              timerKey.getTimerFamilyId(),
              timerKey.getStateNamespace(),
              new Instant(timestamp),
              new Instant(timestamp),
              domain));
    }

    @AutoValue.Builder
    abstract static class Builder<K> {
      abstract Builder<K> setKey(K key);

      abstract Builder<K> setStateNamespace(StateNamespace stateNamespace);

      abstract Builder<K> setTimerId(String timerId);

      abstract Builder<K> setTimerFamilyId(String timerFamilyId);

      abstract TimerKey<K> build();
    }
  }

  /** Coder for {@link TimerKey}. */
  public static class TimerKeyCoder<K> extends StructuredCoder<TimerKey<K>> {
    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

    private final Coder<K> keyCoder;
    private final Coder<? extends BoundedWindow> windowCoder;

    TimerKeyCoder(Coder<K> keyCoder, Coder<? extends BoundedWindow> windowCoder) {
      this.keyCoder = keyCoder;
      this.windowCoder = windowCoder;
    }

    @Override
    public void encode(TimerKey<K> value, OutputStream outStream)
        throws CoderException, IOException {

      // encode the timestamp first
      STRING_CODER.encode(value.getTimerId(), outStream);
      STRING_CODER.encode(value.getStateNamespace().stringKey(), outStream);

      if (keyCoder != null) {
        keyCoder.encode(value.getKey(), outStream);
      }

      STRING_CODER.encode(value.getTimerFamilyId(), outStream);
    }

    @Override
    public TimerKey<K> decode(InputStream inStream) throws CoderException, IOException {
      // decode the timestamp first
      final String timerId = STRING_CODER.decode(inStream);
      // The namespace needs two-phase deserialization:
      // first from bytes into a string, then from string to namespace object using windowCoder.
      final StateNamespace namespace =
          StateNamespaces.fromString(STRING_CODER.decode(inStream), windowCoder);
      K key = null;
      if (keyCoder != null) {
        key = keyCoder.decode(inStream);
      }

      // check if the stream has more available bytes. This is to ensure backward compatibility with
      // old rocksdb state which does not encode timer family data
      final String timerFamilyId = inStream.available() > 0 ? STRING_CODER.decode(inStream) : "";

      return TimerKey.<K>builder()
          .setTimerId(timerId)
          .setStateNamespace(namespace)
          .setKey(key)
          .setTimerFamilyId(timerFamilyId)
          .build();
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(keyCoder, windowCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }
}
