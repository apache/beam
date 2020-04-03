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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.spark.SparkEnv;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockResult;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaBatchInfo;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.apache.spark.streaming.api.java.JavaStreamingListenerBatchCompleted;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

/**
 * A store to hold the global watermarks for a micro-batch.
 *
 * <p>For each source, holds a queue for the watermarks of each micro-batch that was read, and
 * advances the watermarks according to the queue (first-in-first-out).
 */
public class GlobalWatermarkHolder {

  private static final Logger LOG = LoggerFactory.getLogger(GlobalWatermarkHolder.class);

  private static final Map<Integer, Queue<SparkWatermarks>> sourceTimes = new HashMap<>();
  private static final BlockId WATERMARKS_BLOCK_ID = BlockId.apply("broadcast_0WATERMARKS");
  private static final ClassTag<Map> WATERMARKS_TAG =
      scala.reflect.ClassManifestFactory.fromClass(Map.class);

  // a local copy of the watermarks is stored on the driver node so that it can be
  // accessed in test mode instead of fetching blocks remotely
  private static volatile Map<Integer, SparkWatermarks> driverNodeWatermarks = null;

  private static volatile LoadingCache<String, Map<Integer, SparkWatermarks>> watermarkCache = null;
  private static volatile long lastWatermarkedBatchTime = 0;

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
    for (Map.Entry<Integer, Queue<SparkWatermarks>> en : sourceTimes.entrySet()) {
      int sourceId = en.getKey();
      Queue<SparkWatermarks> timesQueue = en.getValue();
      while (!timesQueue.isEmpty()) {
        add(sourceId, timesQueue.poll());
      }
    }
  }

  public static long getLastWatermarkedBatchTime() {
    return lastWatermarkedBatchTime;
  }

  /**
   * Returns the {@link Broadcast} containing the {@link SparkWatermarks} mapped to their sources.
   */
  public static Map<Integer, SparkWatermarks> get(Long cacheInterval) {
    if (canBypassRemoteWatermarkFetching()) {
      /*
      driverNodeWatermarks != null =>
      => advance() was called
      => WatermarkAdvancingStreamingListener#onBatchCompleted() was called
      => we are currently running on the driver node
      => we can get the watermarks from the driver local copy instead of fetching their block
      remotely using block manger
      /------------------------------------------------------------------------------------------/
      In test mode, the system is running inside a single JVM, and thus both driver and executors
      "canBypassWatermarkBlockFetching" by using the static driverNodeWatermarks copy.
      This allows tests to avoid the asynchronous nature of using the BlockManager directly.
      */
      return getLocalWatermarkCopy();
    } else {
      if (watermarkCache == null) {
        watermarkCache = createWatermarkCache(cacheInterval);
      }
      try {
        return watermarkCache.get("SINGLETON");
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static boolean canBypassRemoteWatermarkFetching() {
    return driverNodeWatermarks != null;
  }

  private static synchronized LoadingCache<String, Map<Integer, SparkWatermarks>>
      createWatermarkCache(final Long batchDuration) {
    return CacheBuilder.newBuilder()
        // expire watermarks every half batch duration to ensure they update in every batch.
        .expireAfterWrite(batchDuration / 2, TimeUnit.MILLISECONDS)
        .build(new WatermarksLoader());
  }

  /**
   * Advances the watermarks to the next-in-line watermarks. SparkWatermarks are monotonically
   * increasing.
   */
  private static void advance(final String batchId) {
    synchronized (GlobalWatermarkHolder.class) {
      final BlockManager blockManager = SparkEnv.get().blockManager();
      final Map<Integer, SparkWatermarks> newWatermarks = computeNewWatermarks(blockManager);

      if (!newWatermarks.isEmpty()) {
        writeRemoteWatermarkBlock(newWatermarks, blockManager);
        writeLocalWatermarkCopy(newWatermarks);
      } else {
        LOG.info("No new watermarks could be computed upon completion of batch: {}", batchId);
      }
    }
  }

  private static void writeLocalWatermarkCopy(Map<Integer, SparkWatermarks> newWatermarks) {
    driverNodeWatermarks = newWatermarks;
  }

  private static Map<Integer, SparkWatermarks> getLocalWatermarkCopy() {
    return driverNodeWatermarks;
  }

  /** See {@link GlobalWatermarkHolder#advance(String)}. */
  public static void advance() {
    advance("N/A");
  }

  /**
   * Computes the next watermark values per source id.
   *
   * @return The new watermarks values or null if no source has reported its progress.
   */
  private static Map<Integer, SparkWatermarks> computeNewWatermarks(BlockManager blockManager) {

    if (sourceTimes.isEmpty()) {
      return new HashMap<>();
    }

    // update all sources' watermarks into the new broadcast.
    final Map<Integer, SparkWatermarks> newValues = new HashMap<>();

    for (final Map.Entry<Integer, Queue<SparkWatermarks>> watermarkInfo : sourceTimes.entrySet()) {

      if (watermarkInfo.getValue().isEmpty()) {
        continue;
      }

      final Integer sourceId = watermarkInfo.getKey();

      // current state, if exists.
      Instant currentLowWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
      Instant currentHighWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
      Instant currentSynchronizedProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

      final Map<Integer, SparkWatermarks> currentWatermarks = initWatermarks(blockManager);

      if (currentWatermarks.containsKey(sourceId)) {
        final SparkWatermarks currentTimes = currentWatermarks.get(sourceId);
        currentLowWatermark = currentTimes.getLowWatermark();
        currentHighWatermark = currentTimes.getHighWatermark();
        currentSynchronizedProcessingTime = currentTimes.getSynchronizedProcessingTime();
      }

      final Queue<SparkWatermarks> timesQueue = watermarkInfo.getValue();
      final SparkWatermarks next = timesQueue.poll();

      // advance watermarks monotonically.

      final Instant nextLowWatermark =
          next.getLowWatermark().isAfter(currentLowWatermark)
              ? next.getLowWatermark()
              : currentLowWatermark;

      final Instant nextHighWatermark =
          next.getHighWatermark().isAfter(currentHighWatermark)
              ? next.getHighWatermark()
              : currentHighWatermark;

      final Instant nextSynchronizedProcessingTime = next.getSynchronizedProcessingTime();

      checkState(
          !nextLowWatermark.isAfter(nextHighWatermark),
          String.format(
              "Low watermark %s cannot be later then high watermark %s",
              nextLowWatermark, nextHighWatermark));

      checkState(
          nextSynchronizedProcessingTime.isAfter(currentSynchronizedProcessingTime),
          "Synchronized processing time must advance.");

      newValues.put(
          sourceId,
          new SparkWatermarks(nextLowWatermark, nextHighWatermark, nextSynchronizedProcessingTime));
    }

    return newValues;
  }

  private static void writeRemoteWatermarkBlock(
      final Map<Integer, SparkWatermarks> newWatermarks, final BlockManager blockManager) {
    blockManager.removeBlock(WATERMARKS_BLOCK_ID, true);
    // if an executor tries to fetch the watermark block here, it will fail to do so since
    // the watermark block has just been removed, but the new copy has not been put yet.
    blockManager.putSingle(
        WATERMARKS_BLOCK_ID, newWatermarks, StorageLevel.MEMORY_ONLY(), true, WATERMARKS_TAG);
    // if an executor tries to fetch the watermark block here, it still may fail to do so since
    // the put operation might not have been executed yet
    // see also https://issues.apache.org/jira/browse/BEAM-2789
    LOG.info("Put new watermark block: {}", newWatermarks);
  }

  private static Map<Integer, SparkWatermarks> initWatermarks(final BlockManager blockManager) {

    final Map<Integer, SparkWatermarks> watermarks = fetchSparkWatermarks(blockManager);

    if (watermarks == null) {
      final HashMap<Integer, SparkWatermarks> empty = Maps.newHashMap();
      blockManager.putSingle(
          WATERMARKS_BLOCK_ID, empty, StorageLevel.MEMORY_ONLY(), true, WATERMARKS_TAG);
      return empty;
    } else {
      return watermarks;
    }
  }

  private static Map<Integer, SparkWatermarks> fetchSparkWatermarks(BlockManager blockManager) {
    final Option<BlockResult> blockResultOption =
        blockManager.get(WATERMARKS_BLOCK_ID, WATERMARKS_TAG);
    if (blockResultOption.isDefined()) {
      Iterator<Object> data = blockResultOption.get().data();
      Map<Integer, SparkWatermarks> next = (Map<Integer, SparkWatermarks>) data.next();
      // Spark 2 only triggers completion at the end of the iterator.
      while (data.hasNext()) {
        // NO-OP
      }
      return next;
    } else {
      return null;
    }
  }

  private static class WatermarksLoader extends CacheLoader<String, Map<Integer, SparkWatermarks>> {

    @Override
    public Map<Integer, SparkWatermarks> load(@Nonnull String key) throws Exception {
      final BlockManager blockManager = SparkEnv.get().blockManager();
      final Map<Integer, SparkWatermarks> watermarks = fetchSparkWatermarks(blockManager);
      return watermarks != null ? watermarks : Maps.newHashMap();
    }
  }

  @VisibleForTesting
  public static synchronized void clear() {
    sourceTimes.clear();
    lastWatermarkedBatchTime = 0;
    writeLocalWatermarkCopy(null);
    final SparkEnv sparkEnv = SparkEnv.get();
    if (sparkEnv != null) {
      final BlockManager blockManager = sparkEnv.blockManager();
      blockManager.removeBlock(WATERMARKS_BLOCK_ID, true);
    }
  }

  /**
   * A {@link SparkWatermarks} holds the watermarks and batch time relevant to a micro-batch input
   * from a specific source.
   */
  public static class SparkWatermarks implements Serializable {
    private final Instant lowWatermark;
    private final Instant highWatermark;
    private final Instant synchronizedProcessingTime;

    @VisibleForTesting
    public SparkWatermarks(
        Instant lowWatermark, Instant highWatermark, Instant synchronizedProcessingTime) {
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
          + "lowWatermark="
          + lowWatermark
          + ", highWatermark="
          + highWatermark
          + ", synchronizedProcessingTime="
          + synchronizedProcessingTime
          + '}';
    }
  }

  /** Advance the WMs onBatchCompleted event. */
  public static class WatermarkAdvancingStreamingListener extends JavaStreamingListener {
    private static final Logger LOG =
        LoggerFactory.getLogger(WatermarkAdvancingStreamingListener.class);

    private long timeOf(JavaBatchInfo info) {
      return info.batchTime().milliseconds();
    }

    private long laterOf(long t1, long t2) {
      return Math.max(t1, t2);
    }

    @Override
    public void onBatchCompleted(JavaStreamingListenerBatchCompleted batchCompleted) {

      final long currentBatchTime = timeOf(batchCompleted.batchInfo());

      GlobalWatermarkHolder.advance(Long.toString(currentBatchTime));

      // make sure to update the last watermarked batch time AFTER the watermarks have already
      // been updated (i.e., after the call to GlobalWatermarkHolder.advance(...))
      // in addition, the watermark's block in the BlockManager is updated in an asynchronous manner
      lastWatermarkedBatchTime = laterOf(lastWatermarkedBatchTime, currentBatchTime);

      LOG.info(
          "Batch with timestamp: {} has completed, watermarks have been updated.",
          lastWatermarkedBatchTime);
    }
  }
}
