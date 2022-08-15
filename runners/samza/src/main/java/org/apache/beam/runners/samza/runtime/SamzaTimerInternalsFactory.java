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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
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
  private final Scheduler<KeyedTimerData<K>> timerRegistry;
  private final SamzaTimerState state;
  private final IsBounded isBounded;

  private Instant inputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private Instant outputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  // Size of each event timer is around 200B, by default with buffer size 50k, the default size is
  // 10M
  private final int maxEventTimerBufferSize;
  // Max event time stored in eventTimerBuffer
  // If it is set to long.MAX_VALUE, it indicates the State does not contain any KeyedTimerData
  private long maxEventTimeInBuffer;

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
    this.timerRegistry = timerRegistry;
    this.eventTimeBuffer = new TreeSet<>();
    this.maxEventTimerBufferSize =
        pipelineOptions.getEventTimerBufferSize(); // must be placed before state initialization
    this.maxEventTimeInBuffer = Long.MAX_VALUE;
    this.maxReadyTimersToProcessOnce = pipelineOptions.getMaxReadyTimersToProcessOnce();
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

    if (readyTimers.size() == maxReadyTimersToProcessOnce
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
      if (eventTimeBuffer.contains(keyedTimerData)) {
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

      // TO-DO: apply the same memory optimization over processing timers
      switch (timerData.getDomain()) {
        case EVENT_TIME:
          /*
           * To determine if the upcoming KeyedTimerData could be added to the Buffer while
           * guaranteeing the Buffer's timestamps are all <= than those in State Store to preserve
           * timestamp eviction priority:
           *
           * <p>1) If maxEventTimeInBuffer == long.MAX_VALUE, it indicates that the State is empty,
           * therefore all the Event times greater or lesser than newTimestamp are in the buffer;
           *
           * <p>2) If newTimestamp < maxEventTimeInBuffer, it indicates that there are entries
           * greater than newTimestamp, so it is safe to add it to the buffer
           *
           * <p>In case that the Buffer is full, we remove the largest timer from memory according
           * to {@link KeyedTimerData.compareTo()}
           */
          if (newTimestamp < maxEventTimeInBuffer) {
            eventTimeBuffer.add(keyedTimerData);
            if (eventTimeBuffer.size() > maxEventTimerBufferSize) {
              eventTimeBuffer.pollLast();
              maxEventTimeInBuffer =
                  eventTimeBuffer.last().getTimerData().getTimestamp().getMillis();
            }
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

      switch (timerData.getDomain()) {
        case EVENT_TIME:
          eventTimeBuffer.remove(keyedTimerData);
          break;

        case PROCESSING_TIME:
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
          processingTimeTimerState.put(
              timerKey, keyedTimerData.getTimerData().getTimestamp().getMillis());
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
        maxEventTimeInBuffer = keyedTimerData.getTimerData().getTimestamp().getMillis();
      }

      timestampSortedEventTimeTimerState.closeIterators();
      LOG.info("Loaded {} event time timers in memory", eventTimeBuffer.size());

      if (eventTimeBuffer.size() < maxEventTimerBufferSize) {
        LOG.debug(
            "Event time timers in State is empty, filled {} timers out of {} buffer capacity",
            eventTimeBuffer.size(),
            maxEventTimeInBuffer);
        // Reset the flag variable to indicate there are no more KeyedTimerData in State
        maxEventTimeInBuffer = Long.MAX_VALUE;
      }
    }

    private void loadProcessingTimeTimers() {
      final Iterator<Map.Entry<TimerKey<K>, Long>> iter =
          processingTimeTimerState.readIterator().read();
      // since the iterator will reach to the end, it will be closed automatically
      int count = 0;
      while (iter.hasNext()) {
        final Map.Entry<TimerKey<K>, Long> entry = iter.next();
        final KeyedTimerData keyedTimerData =
            TimerKey.toKeyedTimerData(
                entry.getKey(), entry.getValue(), TimeDomain.PROCESSING_TIME, keyCoder);

        timerRegistry.schedule(
            keyedTimerData, keyedTimerData.getTimerData().getTimestamp().getMillis());
        ++count;
      }
      processingTimeTimerState.closeIterators();

      LOG.info("Loaded {} processing time timers in memory", count);
    }

    /**
     * Restore timer state from RocksDB. This is needed for migration of existing jobs. Give events
     * in eventTimeTimerState, construct timestampSortedEventTimeTimerState preparing for memory
     * reloading. TO-DO: processing time timers are still loaded into memory in one shot; will apply
     * the same optimization mechanism as event time timer
     */
    private void init() {
      final Iterator<Map.Entry<TimerKey<K>, Long>> eventTimersIter =
          eventTimeTimerState.readIterator().read();
      // use hasNext to check empty, because this is relatively cheap compared with Iterators.size()
      if (eventTimersIter.hasNext()) {
        final Iterator sortedEventTimerIter =
            timestampSortedEventTimeTimerState.readIterator().read();

        if (!sortedEventTimerIter.hasNext()) {
          // inline the migration code
          while (eventTimersIter.hasNext()) {
            final Map.Entry<TimerKey<K>, Long> entry = eventTimersIter.next();
            final KeyedTimerData<K> keyedTimerData =
                TimerKey.toKeyedTimerData(
                    entry.getKey(), entry.getValue(), TimeDomain.EVENT_TIME, keyCoder);
            timestampSortedEventTimeTimerState.add(keyedTimerData);
          }
        }
        timestampSortedEventTimeTimerState.closeIterators();
      }
      eventTimeTimerState.closeIterators();

      reloadEventTimeTimers();
      loadProcessingTimeTimers();
    }
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
