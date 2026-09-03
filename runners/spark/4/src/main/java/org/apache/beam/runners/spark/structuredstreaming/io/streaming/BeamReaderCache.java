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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor side cache of live Beam {@link UnboundedReader}s keyed by checkpoint location and split.
 *
 * <p>An entry records the epoch its reader is positioned at and the mark taken there. A batch
 * starting at that epoch reuses the reader and finalizes the pending mark, the start epoch of a
 * batch is always committed by Spark. Any other start epoch, or a reader that moved without
 * completing its batch, closes the entry without finalizing and restores the reader from the
 * durable mark at the start epoch.
 *
 * <p>A reader idle for longer than its idle timeout is closed without finalizing its mark, the
 * source redelivers. The timeout must exceed the longest gap between two micro-batches of one
 * split. Speculative execution can leave a losing attempt's mark finalized on another executor,
 * this source is not safe under {@code spark.speculation} with sources whose reads are not
 * deterministic.
 */
public final class BeamReaderCache {

  private static final Logger LOG = LoggerFactory.getLogger(BeamReaderCache.class);

  private static final ConcurrentMap<String, CachedReader<?>> READERS = new ConcurrentHashMap<>();

  /** One monitor per key, acquire serializes per split, not across splits. */
  private static final ConcurrentMap<String, Object> LOCKS = new ConcurrentHashMap<>();

  private BeamReaderCache() {}

  public static String key(String checkpointLocation, int splitId) {
    return checkpointLocation + '|' + splitId;
  }

  /** Supplies the durable coded mark at the start epoch of a batch, null if there is none. */
  @FunctionalInterface
  interface MarkRestorer {
    byte @Nullable [] restore() throws IOException;
  }

  /**
   * Returns the reader for {@code key} positioned at {@code startEpoch}, reusing the cached one if
   * it is there, restoring from the durable mark otherwise. A zero length durable mark means a
   * fresh start.
   *
   * @throws IllegalStateException if {@code startEpoch > 0} and no durable mark exists
   */
  public static <T> CachedReader<T> acquire(
      String key,
      long startEpoch,
      UnboundedSource<T, ?> source,
      PipelineOptions options,
      long idleTimeoutMillis,
      MarkRestorer restorer)
      throws IOException {
    closeIdle();
    synchronized (lock(key)) {
      CachedReader<?> existing = READERS.get(key);
      if (existing != null) {
        if (existing.beginBatch(startEpoch)) {
          existing.finalizePendingMark(key);
          @SuppressWarnings("unchecked") // one source per key, its element type never changes
          CachedReader<T> reused = (CachedReader<T>) existing;
          return reused;
        }
        LOG.info(
            "Cached Beam reader {} is at epoch {}, batch starts at {}, restoring from the durable"
                + " mark.",
            key,
            existing.positionEpoch(),
            startEpoch);
        invalidate(key);
      }
      byte[] codedMark = restorer.restore();
      if (codedMark == null && startEpoch > 0) {
        throw new IllegalStateException(
            "No durable checkpoint mark for Beam reader " + key + " at epoch " + startEpoch);
      }
      if (codedMark != null && codedMark.length == 0) {
        codedMark = null;
      }
      LOG.info(
          "Creating Beam reader {} at epoch {} ({} mark).",
          key,
          startEpoch,
          codedMark == null ? "no" : "restored");
      CachedReader<T> created =
          new CachedReader<>(
              createReader(source, options, codedMark), startEpoch, codedMark, idleTimeoutMillis);
      created.beginBatch(startEpoch);
      READERS.put(key, created);
      return created;
    }
  }

  private static <T, MarkT extends CheckpointMark> UnboundedReader<T> createReader(
      UnboundedSource<T, MarkT> source, PipelineOptions options, byte @Nullable [] codedMark)
      throws IOException {
    MarkT mark =
        codedMark == null
            ? null
            : CoderHelpers.fromByteArray(codedMark, source.getCheckpointMarkCoder());
    return source.createReader(options, mark);
  }

  /** Closes and forgets the reader of {@code key}, nothing is finalized. */
  public static void invalidate(String key) {
    synchronized (lock(key)) {
      CachedReader<?> removed = READERS.remove(key);
      if (removed != null) {
        close(key, removed);
      }
    }
  }

  /** Closes and forgets every cached reader. */
  public static void invalidateAll() {
    for (String key : READERS.keySet()) {
      invalidate(key);
    }
  }

  private static void closeIdle() {
    long now = System.currentTimeMillis();
    for (Map.Entry<String, CachedReader<?>> entry : READERS.entrySet()) {
      if (entry.getValue().isIdleSince(now)) {
        LOG.info("Closing idle Beam reader {}.", entry.getKey());
        invalidate(entry.getKey());
      }
    }
  }

  private static void close(String key, CachedReader<?> reader) {
    try {
      reader.close();
    } catch (IOException | RuntimeException e) {
      LOG.warn("Failed to close Beam reader {}.", key, e);
    }
  }

  private static Object lock(String key) {
    return LOCKS.computeIfAbsent(key, k -> new Object());
  }

  /** A live reader with the epoch it is positioned at and the coded mark taken there. */
  public static final class CachedReader<T> implements Closeable {
    private final UnboundedReader<T> reader;
    private final long idleTimeoutMillis;
    private boolean started;
    private boolean inBatch;
    private boolean moved;
    private long positionEpoch;
    private byte @Nullable [] positionMark;
    private @Nullable CheckpointMark pendingMark;
    private long lastUsedMillis;

    CachedReader(
        UnboundedReader<T> reader,
        long positionEpoch,
        byte @Nullable [] positionMark,
        long idleTimeoutMillis) {
      this.reader = reader;
      this.positionEpoch = positionEpoch;
      this.positionMark = positionMark;
      this.idleTimeoutMillis = idleTimeoutMillis;
      this.lastUsedMillis = System.currentTimeMillis();
    }

    public UnboundedReader<T> reader() {
      return reader;
    }

    public synchronized boolean startOrAdvance() throws IOException {
      moved = true;
      if (!started) {
        started = true;
        return reader.start();
      }
      return reader.advance();
    }

    /**
     * Whether {@link #startOrAdvance()} was called at least once, only then may a mark be taken.
     */
    public synchronized boolean started() {
      return started;
    }

    public synchronized long positionEpoch() {
      return positionEpoch;
    }

    /** The coded mark of the current position, null for a fresh start. */
    synchronized byte @Nullable [] positionMark() {
      return positionMark;
    }

    /**
     * Claims the reader for a batch starting at {@code epoch}, false if it cannot continue there.
     */
    synchronized boolean beginBatch(long epoch) {
      if (positionEpoch != epoch || moved) {
        return false;
      }
      inBatch = true;
      lastUsedMillis = System.currentTimeMillis();
      return true;
    }

    /** Records a completed batch, the reader is positioned at {@code endEpoch} from now on. */
    synchronized void endBatch(long endEpoch, @Nullable CheckpointMark mark, byte[] codedMark) {
      positionEpoch = endEpoch;
      positionMark = codedMark;
      pendingMark = mark;
      moved = false;
      inBatch = false;
      lastUsedMillis = System.currentTimeMillis();
    }

    synchronized boolean isIdleSince(long nowMillis) {
      return !inBatch && nowMillis - lastUsedMillis > idleTimeoutMillis;
    }

    /** Finalizes the pending mark if any, a failure is logged. */
    synchronized void finalizePendingMark(String key) {
      CheckpointMark mark = pendingMark;
      pendingMark = null;
      if (mark == null) {
        return;
      }
      LOG.debug("Finalizing checkpoint mark of Beam reader {} at epoch {}.", key, positionEpoch);
      try {
        mark.finalizeCheckpoint();
      } catch (IOException | RuntimeException e) {
        LOG.warn(
            "Failed to finalize checkpoint mark of Beam reader {} at epoch {}.",
            key,
            positionEpoch,
            e);
      }
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
