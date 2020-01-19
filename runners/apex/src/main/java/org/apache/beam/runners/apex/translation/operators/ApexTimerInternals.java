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
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ComparisonChain;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.joda.time.Instant;

/**
 * An implementation of Beam's {@link TimerInternals}.
 *
 * <p>Assumes that the current key is set prior to accessing the state.<br>
 * This implementation stores timer data in heap memory and is serialized during checkpointing, it
 * will only work with a small number of timers.
 */
@DefaultSerializer(JavaSerializer.class)
class ApexTimerInternals<K> implements TimerInternals, Serializable {

  private final TimerSet eventTimeTimeTimers;
  private final TimerSet processingTimeTimers;

  private transient K currentKey;
  private transient Instant currentInputWatermark;
  private transient Instant currentOutputWatermark;
  private transient Coder<K> keyCoder;

  public ApexTimerInternals(TimerDataCoderV2 timerDataCoder) {
    this.eventTimeTimeTimers = new TimerSet(timerDataCoder);
    this.processingTimeTimers = new TimerSet(timerDataCoder);
  }

  public void setContext(
      K key, Coder<K> keyCoder, Instant inputWatermark, Instant outputWatermark) {
    this.currentKey = key;
    this.keyCoder = keyCoder;
    this.currentInputWatermark = inputWatermark;
    this.currentOutputWatermark = outputWatermark;
  }

  @VisibleForTesting
  protected TimerSet getTimerSet(TimeDomain domain) {
    return (domain == TimeDomain.EVENT_TIME) ? eventTimeTimeTimers : processingTimeTimers;
  }

  @Override
  public void setTimer(
      StateNamespace namespace,
      String timerId,
      String timerFamilyId,
      Instant target,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    TimerData timerData =
        TimerData.of(timerId, timerFamilyId, namespace, target, outputTimestamp, timeDomain);
    setTimer(timerData);
  }

  @Override
  public void setTimer(TimerData timerData) {
    getTimerSet(timerData.getDomain()).addTimer(getKeyBytes(this.currentKey), timerData);
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
    getTimerSet(timeDomain).deleteTimer(getKeyBytes(this.currentKey), namespace, timerId);
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
    this.eventTimeTimeTimers.deleteTimer(getKeyBytes(this.currentKey), namespace, timerId);
    this.processingTimeTimers.deleteTimer(getKeyBytes(this.currentKey), namespace, timerId);
  }

  @Override
  public void deleteTimer(TimerData timerKey) {
    getTimerSet(timerKey.getDomain()).deleteTimer(getKeyBytes(this.currentKey), timerKey);
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
    return currentInputWatermark;
  }

  @Override
  public Instant currentOutputWatermarkTime() {
    return currentOutputWatermark;
  }

  public interface TimerProcessor<K> {
    void fireTimer(K key, Collection<TimerData> timerData);
  }

  /**
   * Fire the timers that are ready. These are the timers that are registered to be triggered at a
   * time before the current time. Timer processing may register new timers, which can cause the
   * returned timestamp to be before the the current time. The caller may repeat the call until such
   * backdated timers are cleared.
   *
   * @return minimum timestamp of registered timers.
   */
  public long fireReadyTimers(
      long currentTime, TimerProcessor<K> timerProcessor, TimeDomain timeDomain) {
    TimerSet timers = getTimerSet(timeDomain);

    // move minTimestamp first,
    // timer additions that result from firing may modify it
    timers.minTimestamp = currentTime;

    // we keep the timers to return in a different list and launch them later
    // because we cannot prevent a trigger from registering another timer,
    // which would lead to concurrent modification exception.
    Multimap<Slice, TimerInternals.TimerData> toFire = HashMultimap.create();

    Iterator<Map.Entry<Slice, Set<Slice>>> it = timers.activeTimers.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Slice, Set<Slice>> keyWithTimers = it.next();

      Iterator<Slice> timerIt = keyWithTimers.getValue().iterator();
      while (timerIt.hasNext()) {
        try {
          TimerData timerData =
              CoderUtils.decodeFromByteArray(timers.timerDataCoder, timerIt.next().buffer);
          if (timerData.getTimestamp().isBefore(currentTime)) {
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

    // fire ready timers
    if (!toFire.isEmpty()) {
      for (Slice keyBytes : toFire.keySet()) {
        try {
          K key = CoderUtils.decodeFromByteArray(keyCoder, keyBytes.buffer);
          timerProcessor.fireTimer(key, toFire.get(keyBytes));
        } catch (CoderException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return timers.minTimestamp;
  }

  private Slice getKeyBytes(K key) {
    try {
      return new Slice(CoderUtils.encodeToByteArray(keyCoder, key));
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
  }

  protected static class TimerSet implements Serializable {
    private final Map<Slice, Set<Slice>> activeTimers = new HashMap<>();
    private final TimerDataCoderV2 timerDataCoder;
    private long minTimestamp = Long.MAX_VALUE;

    protected TimerSet(TimerDataCoderV2 timerDataCoder) {
      this.timerDataCoder = timerDataCoder;
    }

    public void addTimer(Slice keyBytes, TimerData timer) {
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
      this.minTimestamp = Math.min(minTimestamp, timer.getTimestamp().getMillis());
    }

    public void deleteTimer(Slice keyBytes, StateNamespace namespace, String timerId) {
      Set<Slice> timersForKey = activeTimers.get(keyBytes);
      if (timersForKey == null) {
        return;
      }

      Iterator<Slice> timerIt = timersForKey.iterator();
      while (timerIt.hasNext()) {
        try {
          TimerData timerData =
              CoderUtils.decodeFromByteArray(timerDataCoder, timerIt.next().buffer);
          ComparisonChain chain = ComparisonChain.start().compare(timerData.getTimerId(), timerId);
          if (chain.result() == 0 && !timerData.getNamespace().equals(namespace)) {
            // Obtaining the stringKey may be expensive; only do so if required
            chain = chain.compare(timerData.getNamespace().stringKey(), namespace.stringKey());
          }
          if (chain.result() == 0) {
            timerIt.remove();
          }
        } catch (CoderException e) {
          throw new RuntimeException(e);
        }
      }

      if (timersForKey.isEmpty()) {
        activeTimers.remove(keyBytes);
      }
    }

    public void deleteTimer(Slice keyBytes, TimerData timerKey) {
      Set<Slice> timersForKey = activeTimers.get(keyBytes);
      if (timersForKey != null) {
        try {
          Slice timerBytes = new Slice(CoderUtils.encodeToByteArray(timerDataCoder, timerKey));
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

    @VisibleForTesting
    protected Map<Slice, Set<Slice>> getMap() {
      return activeTimers;
    }
  }
}
