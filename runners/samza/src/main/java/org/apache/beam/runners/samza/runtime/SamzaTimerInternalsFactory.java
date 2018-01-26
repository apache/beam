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
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.runners.samza.SamzaRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TimerInternalsFactory} that creates Samza {@link TimerInternals}.
 * This class keeps track of the {@link org.apache.beam.runners.core.TimerInternals.TimerData}
 * added to the sorted timer set, and removes the ready timers when the watermark is advanced.
 */
public class SamzaTimerInternalsFactory<K> implements TimerInternalsFactory<K> {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaTimerInternalsFactory.class);

  private final Map<TimerKey, KeyedTimerData<K>> timerMap = new HashMap<>();
  private final NavigableSet<KeyedTimerData<K>> eventTimeTimers = new TreeSet<>();

  private final Coder<K> keyCoder;

  // TODO: use BoundedWindow.TIMESTAMP_MIN_VALUE when KafkaIO emits watermarks in bounds.
  private Instant inputWatermark = new Instant(Long.MIN_VALUE);
  private Instant outputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  public SamzaTimerInternalsFactory(Coder<K> keyCoder) {
    this.keyCoder = keyCoder;
  }

  @Override
  public TimerInternals timerInternalsForKey(K key) {
    final byte[] keyBytes;

    if (keyCoder == null) {
      if (key != null) {
        throw new IllegalArgumentException(
            String.format("Received non-null key for unkeyed timer factory. Key: %s",
                key));
      }
      keyBytes = null;
    } else {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        keyCoder.encode(key, baos, Coder.Context.OUTER);
      } catch (IOException e) {
        throw new RuntimeException("Could not encode key: " + key);
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
      final TimerInternals.TimerData timerData = keyedTimerData.getTimerData();

      final TimerKey<K> timerKey = new TimerKey<>(
          keyedTimerData.getKey(),
          timerData.getNamespace(),
          timerData.getTimerId());

      timerMap.remove(timerKey);

      readyTimers.add(keyedTimerData);
    }

    return readyTimers;
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
    public void setTimer(StateNamespace namespace,
                         String timerId,
                         Instant target,
                         TimeDomain timeDomain) {
      setTimer(TimerData.of(timerId, namespace, target, timeDomain));
    }

    @Override
    public void setTimer(TimerData timerData) {
      if (!timerData.getDomain().equals(TimeDomain.EVENT_TIME)) {
        throw new UnsupportedOperationException(
            String.format("%s currently only supports even time", SamzaRunner.class));
      }

      final TimerKey<K> timerKey = new TimerKey<>(
          key,
          timerData.getNamespace(),
          timerData.getTimerId());

      final KeyedTimerData<K> oldTimer = timerMap.get(timerKey);
      if (oldTimer != null) {
        if (!oldTimer.getTimerData().getDomain().equals(timerData.getDomain())) {
          throw new IllegalArgumentException(
              String.format("Attempt to set %s for time domain %s, "
                  + "but it is already set for time domain %s",
                  timerData.getTimerId(),
                  timerData.getDomain(),
                  oldTimer.getTimerData().getDomain()));
        }
        deleteTimer(oldTimer.getTimerData());
      }

      final KeyedTimerData<K> keyedTimerData = new KeyedTimerData<>(keyBytes, key, timerData);
      eventTimeTimers.add(keyedTimerData);
      timerMap.put(timerKey, keyedTimerData);
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
      deleteTimer(TimerData.of(timerId, namespace, null, timeDomain));
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId) {
      deleteTimer(TimerData.of(timerId, namespace, null, TimeDomain.EVENT_TIME));
      // TODO: add support for removing other time domains.
    }

    @Override
    public void deleteTimer(TimerData timerData) {
      if (!timerData.getDomain().equals(TimeDomain.EVENT_TIME)) {
        throw new UnsupportedOperationException(
            String.format("%s currently only supports event time", SamzaRunner.class));
      }

      final TimerKey<K> timerKey = new TimerKey<>(
          key,
          timerData.getNamespace(),
          timerData.getTimerId());

      final KeyedTimerData<K> keyedTimerData = timerMap.remove(timerKey);
      if (keyedTimerData != null) {
        eventTimeTimers.remove(keyedTimerData);
      }
    }

    @Override
    public Instant currentProcessingTime() {
      throw new UnsupportedOperationException(
          String.format("%s does not currently support processing time", SamzaRunner.class));
    }

    @Override
    public Instant currentSynchronizedProcessingTime() {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support synchronized processing time",
              SamzaRunner.class));
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

  private static class TimerKey<K> {
    private final K key;
    private final StateNamespace stateNamespace;
    private final String timerId;

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
    public boolean equals(Object o) {
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
          + "key=" + key
          + ", stateNamespace=" + stateNamespace
          + ", timerId='" + timerId + '\''
          + '}';
    }
  }
}
