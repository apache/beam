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

package org.apache.beam.runners.spark.util;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.apache.spark.streaming.api.java.JavaStreamingListenerBatchCompleted;
import org.joda.time.Instant;


/**
 * A {@link Broadcast} variable to hold the global watermarks for a micro-batch.
 *
 * <p>For each source, holds a queue for the watermarks of each micro-batch that was read,
 * and advances the watermarks according to the queue (first-in-first-out).
 */
public class GlobalWatermarkHolder {
  // the broadcast is broadcasted to the workers.
  private static volatile Broadcast<Map<Integer, SparkWatermarks>> broadcast = null;
  // this should only live in the driver so transient.
  private static final transient Map<Integer, Queue<SparkWatermarks>> sourceTimes = new HashMap<>();

  public static void add(int sourceId, SparkWatermarks sparkWatermarks) {
    Queue<SparkWatermarks> timesQueue = sourceTimes.get(sourceId);
    if (timesQueue == null) {
      timesQueue = new ConcurrentLinkedQueue<>();
    }
    timesQueue.offer(sparkWatermarks);
    sourceTimes.put(sourceId, timesQueue);
  }

  @VisibleForTesting
  public static void addAll(Map<Integer, Queue<SparkWatermarks>> sourceTimes) {
    for (Map.Entry<Integer, Queue<SparkWatermarks>> en: sourceTimes.entrySet()) {
      int sourceId = en.getKey();
      Queue<SparkWatermarks> timesQueue = en.getValue();
      while (!timesQueue.isEmpty()) {
        add(sourceId, timesQueue.poll());
      }
    }
  }

  /**
   * Returns the {@link Broadcast} containing the {@link SparkWatermarks} mapped
   * to their sources.
   */
  public static Broadcast<Map<Integer, SparkWatermarks>> get() {
    return broadcast;
  }

  /**
   * Advances the watermarks to the next-in-line watermarks.
   * SparkWatermarks are monotonically increasing.
   */
  public static void advance(JavaSparkContext jsc) {
    synchronized (GlobalWatermarkHolder.class){
      if (sourceTimes.isEmpty()) {
        return;
      }

      // update all sources' watermarks into the new broadcast.
      Map<Integer, SparkWatermarks> newBroadcast = new HashMap<>();

      for (Map.Entry<Integer, Queue<SparkWatermarks>> en: sourceTimes.entrySet()) {
        if (en.getValue().isEmpty()) {
          continue;
        }
        Integer sourceId = en.getKey();
        Queue<SparkWatermarks> timesQueue = en.getValue();

        // current state, if exists.
        Instant currentLowWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
        Instant currentHighWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
        Instant currentSynchronizedProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
        if (broadcast != null && broadcast.getValue().containsKey(sourceId)) {
          SparkWatermarks currentTimes = broadcast.getValue().get(sourceId);
          currentLowWatermark = currentTimes.getLowWatermark();
          currentHighWatermark = currentTimes.getHighWatermark();
          currentSynchronizedProcessingTime = currentTimes.getSynchronizedProcessingTime();
        }

        SparkWatermarks next = timesQueue.poll();
        // advance watermarks monotonically.
        Instant nextLowWatermark = next.getLowWatermark().isAfter(currentLowWatermark)
            ? next.getLowWatermark() : currentLowWatermark;
        Instant nextHighWatermark = next.getHighWatermark().isAfter(currentHighWatermark)
            ? next.getHighWatermark() : currentHighWatermark;
        Instant nextSynchronizedProcessingTime = next.getSynchronizedProcessingTime();
        checkState(!nextLowWatermark.isAfter(nextHighWatermark),
            String.format(
                "Low watermark %s cannot be later then high watermark %s",
                nextLowWatermark, nextHighWatermark));
        checkState(nextSynchronizedProcessingTime.isAfter(currentSynchronizedProcessingTime),
            "Synchronized processing time must advance.");
        newBroadcast.put(
            sourceId,
            new SparkWatermarks(
                nextLowWatermark, nextHighWatermark, nextSynchronizedProcessingTime));
      }

      // update the watermarks broadcast only if something has changed.
      if (!newBroadcast.isEmpty()) {
        if (broadcast != null) {
          // for now this is blocking, we could make this asynchronous
          // but it could slow down WM propagation.
          broadcast.destroy();
        }
        broadcast = jsc.broadcast(newBroadcast);
      }
    }
  }

  @VisibleForTesting
  public static synchronized void clear() {
    sourceTimes.clear();
    broadcast = null;
  }

  /**
   * A {@link SparkWatermarks} holds the watermarks and batch time
   * relevant to a micro-batch input from a specific source.
   */
  public static class SparkWatermarks implements Serializable {
    private final Instant lowWatermark;
    private final Instant highWatermark;
    private final Instant synchronizedProcessingTime;

    @VisibleForTesting
    public SparkWatermarks(
        Instant lowWatermark,
        Instant highWatermark,
        Instant synchronizedProcessingTime) {
      this.lowWatermark = lowWatermark;
      this.highWatermark = highWatermark;
      this.synchronizedProcessingTime = synchronizedProcessingTime;
    }

    public Instant getLowWatermark() {
      return lowWatermark;
    }

    public Instant getHighWatermark() {
      return highWatermark;
    }

    public Instant getSynchronizedProcessingTime() {
      return synchronizedProcessingTime;
    }

    @Override
    public String toString() {
      return "SparkWatermarks{"
          + "lowWatermark=" + lowWatermark
          + ", highWatermark=" + highWatermark
          + ", synchronizedProcessingTime=" + synchronizedProcessingTime + '}';
    }
  }

  /** Advance the WMs onBatchCompleted event. */
  public static class WatermarksListener extends JavaStreamingListener {
    private final JavaStreamingContext jssc;

    public WatermarksListener(JavaStreamingContext jssc) {
      this.jssc = jssc;
    }

    @Override
    public void onBatchCompleted(JavaStreamingListenerBatchCompleted batchCompleted) {
      GlobalWatermarkHolder.advance(jssc.sparkContext());
    }
  }
}
