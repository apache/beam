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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Weigher;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Ints;

/** A {@link ShuffleBatchReader} that caches batches as they're read. */
public class CachingShuffleBatchReader implements ShuffleBatchReader {
  private final ShuffleBatchReader reader;
  @VisibleForTesting final LoadingCache<BatchRange, Batch> cache;

  /**
   * Limit the size of the cache to 1GiB of batches.
   *
   * <p>If this increases beyond Integer.MAX_VALUE then {@link BatchWeigher} must be updated.
   * Because a batch may be larger than 1GiB, the actual in-memory batch size may exceed this value.
   */
  private static final int MAXIMUM_WEIGHT = 1024 * 1024 * 1024;

  // Ensure that batches in the cache are expired quickly
  // for improved GC performance.
  private static final Duration EXPIRE_AFTER = Duration.ofMillis(250);

  /**
   * Creates the caching reader.
   *
   * @param shuffleReader wrapped reader.
   * @param maximumWeightBytes maximum bytes for the cache.
   * @param expireAfterAccess cache items may be evicted after the elapsed duration.
   */
  public CachingShuffleBatchReader(
      ShuffleBatchReader shuffleReader, long maximumWeightBytes, Duration expireAfterAccess) {
    this.reader = shuffleReader;
    this.cache =
        CacheBuilder.newBuilder()
            .maximumWeight(maximumWeightBytes)
            .weigher(new BatchWeigher())
            .expireAfterAccess(expireAfterAccess)
            .<BatchRange, Batch>build(
                new CacheLoader<BatchRange, Batch>() {
                  @Override
                  public Batch load(BatchRange batchRange) throws Exception {
                    final Batch batch =
                        reader.read(batchRange.startPosition, batchRange.endPosition);
                    return batch;
                  }
                });
  }

  /**
   * Creates the caching reader with a maximum size of {@link MAXIMUM_WEIGHT} and an element expiry
   * duration of {@link EXPIRE_AFTER}.
   *
   * @param shuffleReader wrapped reader.
   */
  public CachingShuffleBatchReader(ShuffleBatchReader shuffleReader) {
    this(shuffleReader, MAXIMUM_WEIGHT, EXPIRE_AFTER);
  }

  /**
   * Creates the caching reader with an element expiry duration of {@link EXPIRE_AFTER}.
   *
   * @param shuffleReader wrapped reader.
   * @param maximumWeightBytes maximum bytes for the cache.
   */
  public CachingShuffleBatchReader(ShuffleBatchReader shuffleReader, long maximumWeightBytes) {
    this(shuffleReader, maximumWeightBytes, EXPIRE_AFTER);
  }

  @Override
  public Batch read(@Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition)
      throws IOException {
    final BatchRange range = new BatchRange(startPosition, endPosition);
    try {
      return cache.get(range);
    } catch (RuntimeException | ExecutionException e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw new RuntimeException("unexpected", e);
    }
  }

  /** The key for the entries stored in the batch cache. */
  static final class BatchRange {
    @Nullable protected final ShufflePosition startPosition;
    @Nullable protected final ShufflePosition endPosition;

    public BatchRange(
        @Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition) {
      this.startPosition = startPosition;
      this.endPosition = endPosition;
    }

    @Override
    public boolean equals(Object o) {
      return o == this
          || (o instanceof BatchRange
              && Objects.equal(((BatchRange) o).startPosition, startPosition)
              && Objects.equal(((BatchRange) o).endPosition, endPosition));
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(startPosition, endPosition);
    }
  }

  /**
   * Returns the weight of a Batch, in bytes, within the range [0, Integer.MAX_VALUE].
   *
   * <p>The cache holds {@link MAX_WEIGHT} bytes. If {@link MAX_WEIGHT} is increased beyond
   * Integer.MAX_VALUE bytes, a new weighing heuristic will be required to avoid under representing
   * the number of bytes in memory.
   */
  static final class BatchWeigher implements Weigher<BatchRange, Batch> {
    @Override
    public int weigh(BatchRange key, Batch value) {
      return Ints.saturatedCast(value.bytes);
    }
  }
}
