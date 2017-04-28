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
package org.apache.beam.runners.apex.translation.operators;

import com.datatorrent.netlet.util.Slice;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.TimeDomain;
import org.joda.time.Instant;

/**
 * An implementation of Beam's {@link TimerInternals}.
 *
 * <p>Assumes that the current key is set prior to accessing the state.<br>
 * This implementation stores timer data in heap memory and is serialized
 * during checkpointing, it will only work with a small number of timers.
 * @param <K>
 */
@DefaultSerializer(JavaSerializer.class)
class ApexTimerInternals<K> implements TimerInternals, Serializable {

  private Map<Slice, Set<Slice>> activeTimers = new HashMap<>();
  private TimerDataCoder timerDataCoder;
  private transient K currentKey;
  private transient Instant currentInputWatermark;
  private transient Coder<K> keyCoder;

  public ApexTimerInternals(TimerDataCoder timerDataCoder) {
    this.timerDataCoder = timerDataCoder;
  }

  public void setContext(K key, Coder<K> keyCoder, Instant inputWatermark) {
    this.currentKey = key;
    this.keyCoder = keyCoder;
    this.currentInputWatermark = inputWatermark;
  }

  @Override
  public void setTimer(StateNamespace namespace, String timerId, Instant target,
      TimeDomain timeDomain) {
    TimerData timerData = TimerData.of(timerId, namespace, target, timeDomain);
    registerActiveTimer(currentKey, timerData);
  }

  @Override
  public void setTimer(TimerData timerData) {
    registerActiveTimer(currentKey, timerData);
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteTimer(TimerData timerKey) {
    unregisterActiveTimer(currentKey, timerKey);
  }

  @Override
  public Instant currentProcessingTime() {
    return Instant.now();
  }

  @Override
  public Instant currentSynchronizedProcessingTime() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return new Instant(currentInputWatermark);
  }

  @Override
  public Instant currentOutputWatermarkTime() {
    return null;
  }

  /**
   * Returns the list of timers that are ready to fire. These are the timers
   * that are registered to be triggered at a time before the current watermark.
   * We keep these timers in a Set, so that they are deduplicated, as the same
   * timer can be registered multiple times.
   */
  public Multimap<Slice, TimerInternals.TimerData> getTimersReadyToProcess(
      long currentWatermark) {

    // we keep the timers to return in a different list and launch them later
    // because we cannot prevent a trigger from registering another timer,
    // which would lead to concurrent modification exception.
    Multimap<Slice, TimerInternals.TimerData> toFire = HashMultimap.create();

    Iterator<Map.Entry<Slice, Set<Slice>>> it =
        activeTimers.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Slice, Set<Slice>> keyWithTimers = it.next();

      Iterator<Slice> timerIt = keyWithTimers.getValue().iterator();
      while (timerIt.hasNext()) {
        try {
          TimerData timerData = CoderUtils.decodeFromByteArray(timerDataCoder,
              timerIt.next().buffer);
          if (timerData.getTimestamp().isBefore(currentWatermark)) {
            toFire.put(keyWithTimers.getKey(), timerData);
            timerIt.remove();
          }
        } catch (CoderException e) {
          throw new RuntimeException(e);
        }
      }

      if (keyWithTimers.getValue().isEmpty()) {
        it.remove();
      }
    }
    return toFire;
  }

  private void registerActiveTimer(K key, TimerData timer) {
    final Slice keyBytes;
    try {
      keyBytes = new Slice(CoderUtils.encodeToByteArray(keyCoder, key));
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
    Set<Slice> timersForKey = activeTimers.get(keyBytes);
    if (timersForKey == null) {
      timersForKey = new HashSet<>();
    }

    try {
      Slice timerBytes = new Slice(CoderUtils.encodeToByteArray(timerDataCoder, timer));
      timersForKey.add(timerBytes);
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }

    activeTimers.put(keyBytes, timersForKey);
  }

  private void unregisterActiveTimer(K key, TimerData timer) {
    final Slice keyBytes;
    try {
      keyBytes = new Slice(CoderUtils.encodeToByteArray(keyCoder, key));
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }

    Set<Slice> timersForKey = activeTimers.get(keyBytes);
    if (timersForKey != null) {
      try {
        Slice timerBytes = new Slice(CoderUtils.encodeToByteArray(timerDataCoder, timer));
        timersForKey.add(timerBytes);
        timersForKey.remove(timerBytes);
      } catch (CoderException e) {
        throw new RuntimeException(e);
      }

      if (timersForKey.isEmpty()) {
        activeTimers.remove(keyBytes);
      } else {
        activeTimers.put(keyBytes, timersForKey);
      }
    }
  }

}
