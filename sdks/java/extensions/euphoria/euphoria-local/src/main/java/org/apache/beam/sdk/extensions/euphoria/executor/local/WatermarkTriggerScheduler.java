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
package org.apache.beam.sdk.extensions.euphoria.executor.local;

import com.google.common.collect.Iterables;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Trigger scheduler based on watermarks. Uses event-time instead of real wall-clock time. */
public class WatermarkTriggerScheduler<W extends Window, K> implements TriggerScheduler<W, K> {

  // how delayed is the watermark time after the event time (how long are
  // we going to accept latecomers)
  private final long watermarkDuration;
  // following fields are accessed from owner thread only
  private final SortedMap<Long, List<Pair<KeyedWindow<W, K>, Triggerable<W, K>>>> scheduledEvents =
      new TreeMap<>();
  private final Map<KeyedWindow<W, K>, Set<Long>> eventStampsForWindow = new HashMap<>();
  // read-only accessed from multiple threads
  private volatile long currentWatermark;

  /**
   * Create the triggering with specified duration in ms.
   *
   * @param duration duration of the watermark in ms.
   */
  public WatermarkTriggerScheduler(long duration) {
    this.watermarkDuration = duration;
    this.currentWatermark = 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void updateStamp(long stamp) {
    if (currentWatermark < stamp) {
      final long triggeringStamp = stamp - watermarkDuration;

      // fire all triggers that passed watermark duration

      SortedMap<Long, List<Pair<KeyedWindow<W, K>, Triggerable<W, K>>>> headMap;
      headMap = scheduledEvents.headMap(triggeringStamp);

      headMap
          .entrySet()
          .stream()
          .flatMap(e -> e.getValue().stream().map(p -> Pair.of(e.getKey(), p)))
          // need to collect to list to prevent ConcurrentModificationException
          .collect(Collectors.toList())
          .forEach(
              p -> {
                // move the clock to the triggering time
                currentWatermark = p.getFirst();
                KeyedWindow<W, K> w = p.getSecond().getFirst();
                Triggerable<W, K> purged = purge(w, currentWatermark);
                purged.fire(currentWatermark, w);
              });

      // and set the final watermark
      currentWatermark = stamp;
    }
  }

  @Override
  public long getCurrentTimestamp() {
    return currentWatermark;
  }

  @Override
  public boolean scheduleAt(long stamp, KeyedWindow<W, K> window, Triggerable<W, K> trigger) {
    if (stamp <= currentWatermark - watermarkDuration) {
      return false;
    }
    purge(window, stamp);
    add(stamp, trigger, window);
    return true;
  }

  @Override
  public void cancelAll() {
    scheduledEvents.clear();
    eventStampsForWindow.clear();
  }

  @Override
  public void cancel(long timestamp, KeyedWindow<W, K> window) {
    purge(window, timestamp);
  }

  @Override
  public void close() {
    cancelAll();
  }

  /** Purge the scheduled event from all indices and return it (if exists). */
  private Triggerable purge(KeyedWindow<W, K> w, long stamp) {
    Set<Long> scheduled = eventStampsForWindow.get(w);
    if (scheduled == null || !scheduled.remove(stamp)) {
      return null;
    }

    List<Pair<KeyedWindow<W, K>, Triggerable<W, K>>> eventsForWindow;
    eventsForWindow = scheduledEvents.get(stamp);

    List<Pair<KeyedWindow<W, K>, Triggerable<W, K>>> filtered =
        eventsForWindow.stream().filter(p -> w.equals(p.getFirst())).collect(Collectors.toList());

    Pair<KeyedWindow<W, K>, Triggerable<W, K>> event = Iterables.getOnlyElement(filtered);

    eventsForWindow.remove(event);

    if (eventsForWindow.isEmpty()) {
      scheduledEvents.remove(stamp);
    }

    if (scheduled.isEmpty()) {
      eventStampsForWindow.remove(w);
    }

    return event.getSecond();
  }

  private void add(long stamp, Triggerable<W, K> trigger, KeyedWindow<W, K> w) {
    List<Pair<KeyedWindow<W, K>, Triggerable<W, K>>> scheduledForTime = scheduledEvents.get(stamp);
    if (scheduledForTime == null) {
      scheduledEvents.put(stamp, scheduledForTime = new ArrayList<>());
    }
    scheduledForTime.add(Pair.of(w, trigger));
    Set<Long> stamps = eventStampsForWindow.get(w);
    if (stamps == null) {
      eventStampsForWindow.put(w, stamps = new HashSet<>());
    }
    stamps.add(stamp);
  }

  public long getWatermarkDuration() {
    return watermarkDuration;
  }
}
