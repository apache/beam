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
public class SamzaTimerInternalsFactory<K> implements TimerInternalsFactory<K> {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaTimerInternalsFactory.class);
  private final NavigableSet<KeyedTimerData<K>> eventTimeBuffer;
  private final Coder<K> keyCoder;
  private final Scheduler<KeyedTimerData<K>> timerRegistry;
  private final SamzaTimerState state;
  private final IsBounded isBounded;

  private Instant inputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private Instant outputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  // size of each event timer is around 200B, so keep the memory boundary for timers as 1M
  private Integer eventTimerBufferSize;
  private Long maxEventTimeInBuffer;

  private SamzaTimerInternalsFactory(
      Coder<K> keyCoder,
      Scheduler<KeyedTimerData<K>> timerRegistry,
      String timerStateId,
      SamzaStateInternals.Factory<?> nonKeyedStateInternalsFactory,
      Coder<BoundedWindow> windowCoder,
      IsBounded isBounded,
      SamzaPipelineOptions pipelineOptions) {
    this.keyCoder = keyCoder;
    this.timerRegistry = timerRegistry;
    this.eventTimeBuffer = new TreeSet<>();
    this.eventTimerBufferSize =
        pipelineOptions.getEventTimerBufferSize(); // must be placed before state initialization
    this.maxEventTimeInBuffer = Long.MAX_VALUE;
    this.state = new SamzaTimerState(timerStateId, nonKeyedStateInternalsFactory, windowCoder);
    this.isBounded = isBounded;
  }

  static <K> SamzaTimerInternalsFactory<K> createTimerInternalFactory(
      Coder<K> keyCoder,
      Scheduler<KeyedTimerData<K>> timerRegistry,
      String timerStateId,
      SamzaStateInternals.Factory<?> nonKeyedStateInternalsFactory,
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
        && eventTimeBuffer.first().getTimerData().getTimestamp().isBefore(inputWatermark)) {
      final KeyedTimerData<K> keyedTimerData = eventTimeBuffer.pollFirst();
      readyTimers.add(keyedTimerData);
      state.deletePersisted(keyedTimerData);

      if (eventTimeBuffer.isEmpty()) {
        state.reloadEventTimeTimers();
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

      if (!newTimestamp.equals(lastTimestamp)) {
        if (lastTimestamp != null) {
          final TimerData lastTimerData =
              TimerData.of(
                  timerData.getTimerId(),
                  timerData.getNamespace(),
                  new Instant(lastTimestamp),
                  new Instant(lastTimestamp),
                  timerData.getDomain());
          deleteTimer(lastTimerData, false);
        }

        // persist it first
        state.persist(keyedTimerData);

        // TO-DO: apply the same memory optimization over processing timers
        switch (timerData.getDomain()) {
          case EVENT_TIME:
            /**
             * 4 conditions (combination of two if-else conditions): 1) incoming timer is before or
             * after maxEventTimeInBuffer; 2) eventTimerBufferSize is full or not. If incoming timer
             * is after maxEventTimeInBuffer, we do not add it into memory. Otherwise, its timestamp
             * may be larger than the one's in store, so the timer order cannot be guaranteed. In
             * contrast, incoming timer is before maxEventTimeInBuffer, then we add it into memory.
             * In case that memory buffer is full, we remove the last one in memory and update the
             * timestamp accordingly
             */
            if (newTimestamp < maxEventTimeInBuffer) {
              eventTimeBuffer.add(keyedTimerData);
              if (eventTimeBuffer.size() > eventTimerBufferSize) {
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
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
      Instant now = Instant.now();
      deleteTimer(TimerData.of(timerId, namespace, now, now, timeDomain));
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
      Instant now = Instant.now();
      deleteTimer(TimerData.of(timerId, namespace, now, now, TimeDomain.EVENT_TIME));
    }

    @Override
    public void deleteTimer(TimerData timerData) {
      deleteTimer(timerData, true);
    }

    private void deleteTimer(TimerData timerData, boolean updateState) {
      final KeyedTimerData<K> keyedTimerData = new KeyedTimerData<>(keyBytes, key, timerData);
      if (updateState) {
        state.deletePersisted(keyedTimerData);
      }

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
                  "%s currently only supports event time or processing time", SamzaRunner.class));
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
        SamzaStateInternals.Factory<?> nonKeyedStateInternalsFactory,
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
      final TimerKey<K> timerKey = TimerKey.of(keyedTimerData);
      switch (keyedTimerData.getTimerData().getDomain()) {
        case EVENT_TIME:
          return eventTimeTimerState.get(timerKey).read();

        case PROCESSING_TIME:
          return processingTimeTimerState.get(timerKey).read();

        default:
          throw new UnsupportedOperationException(
              String.format("%s currently only supports event time", SamzaRunner.class));
      }
    }

    void persist(KeyedTimerData<K> keyedTimerData) {
      final TimerKey<K> timerKey = TimerKey.of(keyedTimerData);
      switch (keyedTimerData.getTimerData().getDomain()) {
        case EVENT_TIME:
          final Long timestamp = eventTimeTimerState.get(timerKey).read();

          if (timestamp != null) {
            final KeyedTimerData keyedTimerDataInStore =
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
                  "%s currently only supports event time or processing time", SamzaRunner.class));
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
                  "%s currently only supports event time or processing time", SamzaRunner.class));
      }
    }

    /**
     * Reload event time timers from state to memory buffer. Buffer size is bound by
     * eventTimerBufferSize
     */
    private void reloadEventTimeTimers() {
      final Iterator<KeyedTimerData<K>> iter =
          timestampSortedEventTimeTimerState.readIterator().read();

      while (iter.hasNext() && eventTimeBuffer.size() < eventTimerBufferSize) {
        final KeyedTimerData<K> keyedTimerData = iter.next();
        eventTimeBuffer.add(keyedTimerData);
        maxEventTimeInBuffer = keyedTimerData.getTimerData().getTimestamp().getMillis();
      }

      ((SamzaStateInternals.KeyValueIteratorState) timestampSortedEventTimeTimerState)
          .closeIterators();
      LOG.info("Loaded {} event time timers in memory", eventTimeBuffer.size());
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
      ((SamzaStateInternals.KeyValueIteratorState) processingTimeTimerState).closeIterators();

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
            final KeyedTimerData keyedTimerData =
                TimerKey.toKeyedTimerData(
                    entry.getKey(), entry.getValue(), TimeDomain.EVENT_TIME, keyCoder);
            timestampSortedEventTimeTimerState.add(keyedTimerData);
          }
        }
        ((SamzaStateInternals.KeyValueIteratorState) timestampSortedEventTimeTimerState)
            .closeIterators();
      }
      ((SamzaStateInternals.KeyValueIteratorState) eventTimeTimerState).closeIterators();

      reloadEventTimeTimers();
      loadProcessingTimeTimers();
    }
  }

  private static class TimerKey<K> {
    private final K key;
    private final StateNamespace stateNamespace;
    private final String timerId;

    static <K> TimerKey<K> of(KeyedTimerData<K> keyedTimerData) {
      final TimerInternals.TimerData timerData = keyedTimerData.getTimerData();
      return new TimerKey<>(
          keyedTimerData.getKey(), timerData.getNamespace(), timerData.getTimerId());
    }

    static <K> KeyedTimerData<K> toKeyedTimerData(
        TimerKey<K> timerKey, long timestamp, TimeDomain domain, Coder<K> keyCoder) {
      byte[] keyBytes = null;
      if (keyCoder != null && timerKey.key != null) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          keyCoder.encode(timerKey.key, baos);
        } catch (IOException e) {
          throw new RuntimeException("Could not encode key: " + timerKey.key, e);
        }
        keyBytes = baos.toByteArray();
      }

      return new KeyedTimerData<K>(
          keyBytes,
          timerKey.key,
          TimerInternals.TimerData.of(
              timerKey.timerId,
              timerKey.stateNamespace,
              new Instant(timestamp),
              new Instant(timestamp),
              domain));
    }

    private TimerKey(K key, StateNamespace stateNamespace, String timerId) {
      this.key = key;
      this.stateNamespace = stateNamespace;
      this.timerId = timerId;
    }

    public K getKey() {
      return key;
    }

    public StateNamespace getStateNamespace() {
      return stateNamespace;
    }

    public String getTimerId() {
      return timerId;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TimerKey<?> timerKey = (TimerKey<?>) o;

      if (key != null ? !key.equals(timerKey.key) : timerKey.key != null) {
        return false;
      }
      if (!stateNamespace.equals(timerKey.stateNamespace)) {
        return false;
      }

      return timerId.equals(timerKey.timerId);
    }

    @Override
    public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + stateNamespace.hashCode();
      result = 31 * result + timerId.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "TimerKey{"
          + "key="
          + key
          + ", stateNamespace="
          + stateNamespace
          + ", timerId='"
          + timerId
          + '\''
          + '}';
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
      STRING_CODER.encode(value.timerId, outStream);
      STRING_CODER.encode(value.stateNamespace.stringKey(), outStream);

      if (keyCoder != null) {
        keyCoder.encode(value.key, outStream);
      }
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

      return new TimerKey<>(key, namespace, timerId);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(keyCoder, windowCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }
}
