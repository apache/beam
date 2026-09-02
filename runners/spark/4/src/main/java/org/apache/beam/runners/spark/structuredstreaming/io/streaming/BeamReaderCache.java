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
package org.apache.beam.runners.spark.structuredstreaming.io.streaming;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalListener;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor side cache of live Beam {@link UnboundedReader}s, keyed by (checkpoint location, source
 * id, split id).
 *
 * <p>A Spark micro-batch creates a fresh {@link BeamPartitionReader} every batch, but a Beam
 * unbounded reader is expensive to create and holds the read position. Keeping the reader alive
 * between micro-batches lets the next batch continue where the previous one stopped, mirroring
 * {@code org.apache.beam.runners.spark.io.MicrobatchSource} in the legacy runner.
 *
 * <p><b>Durable recovery.</b> The {@code CheckpointMark} of every split is remembered in executor
 * memory after each micro-batch, and {@link BeamPartitionReader} additionally persists it under the
 * checkpoint location, see {@link BeamCheckpointFiles}. When a reader has to be created and no mark
 * is in memory, for example after an executor or driver restart or after the cache entry expired,
 * the caller supplied fallback restores the newest durable mark at or before the epoch the batch
 * starts at. Two caveats remain. The source is consumed with at least once semantics, a mark is
 * written when a batch finished reading rather than transactionally with Spark's commit, so a crash
 * between the two replays the last micro-batch. And persisting a mark is best effort, an IO failure
 * only degrades recovery to an older mark or to a fresh start, it never fails the batch.
 */
public final class BeamReaderCache {

  private static final Logger LOG = LoggerFactory.getLogger(BeamReaderCache.class);

  /** Readers idle for longer than this are closed, releasing the underlying source connections. */
  private static final long READER_CACHE_INTERVAL_MILLIS = 10 * 60 * 1000L;

  private static final RemovalListener<String, CachedReader<?>> CLOSE_ON_REMOVAL =
      notification -> {
        CachedReader<?> reader = notification.getValue();
        String key = String.valueOf(notification.getKey());
        if (reader != null) {
          LOG.info("Evicting cached Beam reader {}.", key);
          try {
            reader.close();
          } catch (IOException e) {
            LOG.warn("Failed to close evicted Beam reader {}.", key, e);
          }
        }
      };

  private static final Cache<String, CachedReader<?>> READERS =
      CacheBuilder.newBuilder()
          .expireAfterAccess(READER_CACHE_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
          .removalListener(CLOSE_ON_REMOVAL)
          .build();

  /** Last known checkpoint mark per key, used when a reader has to be recreated. */
  private static final ConcurrentMap<String, CheckpointMark> MARKS = new ConcurrentHashMap<>();

  private BeamReaderCache() {}

  /** Builds the cache key of one split of one source of one streaming query. */
  public static String key(String checkpointLocation, String sourceId, int splitId) {
    return checkpointLocation + '|' + sourceId + '|' + splitId;
  }

  /**
   * Returns the cached reader for {@code key}, creating it from the last cached checkpoint mark if
   * there is none.
   */
  public static <T, CheckpointMarkT extends CheckpointMark> CachedReader<T> getOrCreate(
      String key, UnboundedSource<T, CheckpointMarkT> source, PipelineOptions options) {
    return getOrCreate(key, source, options, () -> null);
  }

  /**
   * Returns the cached reader for {@code key}, creating it from the last cached checkpoint mark if
   * there is none. The {@code durableMarkFallback} is consulted only when no mark is in memory
   * either, it typically restores a mark persisted by {@link BeamCheckpointFiles} and may return
   * {@code null} for a fresh start.
   */
  @SuppressWarnings({"unchecked", "nullness"}) // the mark type always matches the source
  public static <T, CheckpointMarkT extends CheckpointMark> CachedReader<T> getOrCreate(
      String key,
      UnboundedSource<T, CheckpointMarkT> source,
      PipelineOptions options,
      Supplier<@Nullable CheckpointMark> durableMarkFallback) {
    try {
      return (CachedReader<T>)
          READERS.get(
              key,
              () -> {
                CheckpointMarkT mark = (CheckpointMarkT) MARKS.get(key);
                if (mark == null) {
                  mark = (CheckpointMarkT) durableMarkFallback.get();
                }
                LOG.info(
                    "No cached Beam reader for {}, creating one at checkpoint mark {}.", key, mark);
                return new CachedReader<>(source.createReader(options, mark));
              });
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get or create Beam unbounded reader " + key, e);
    }
  }

  /** Remembers the checkpoint mark of {@code key} so a recreated reader can resume from it. */
  public static void rememberCheckpointMark(String key, @Nullable CheckpointMark mark) {
    if (mark != null) {
      MARKS.put(key, mark);
    }
  }

  /** Closes and forgets every cached reader, intended for tests and for query shutdown. */
  @VisibleForTesting
  public static void invalidateAll() {
    READERS.invalidateAll();
    READERS.cleanUp();
    MARKS.clear();
  }

  /** A cached {@link UnboundedReader} that remembers whether it has been started already. */
  public static final class CachedReader<T> implements Closeable {
    private final UnboundedReader<T> reader;
    private boolean started;

    CachedReader(UnboundedReader<T> reader) {
      this.reader = reader;
    }

    /** The wrapped Beam reader. */
    public UnboundedReader<T> reader() {
      return reader;
    }

    /**
     * Starts the reader on first use and advances it afterwards, returning {@code true} if an
     * element is available.
     */
    public synchronized boolean startOrAdvance() throws IOException {
      if (!started) {
        started = true;
        return reader.start();
      }
      return reader.advance();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
