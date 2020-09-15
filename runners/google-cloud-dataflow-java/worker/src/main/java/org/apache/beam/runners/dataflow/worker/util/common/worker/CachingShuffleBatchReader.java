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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link ShuffleBatchReader} that caches batches as they're read. */
public class CachingShuffleBatchReader implements ShuffleBatchReader {
  private final ShuffleBatchReader reader;
  @VisibleForTesting final LoadingCache<BatchRange, Batch> cache;

  /** Limit the size of the cache to 1000 batches. */
  private static final int MAXIMUM_BATCHES = 1000;

  // Ensure that batches in the cache are expired quickly
  // for improved GC performance.
  private static final long EXPIRE_AFTER_MS = 250;

  public CachingShuffleBatchReader(
      ShuffleBatchReader shuffleReader, int maximumBatches, long expireAfterAccessMillis) {
    this.reader = shuffleReader;
    this.cache =
        CacheBuilder.newBuilder()
            .maximumSize(maximumBatches)
            .expireAfterAccess(expireAfterAccessMillis, TimeUnit.MILLISECONDS)
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

  public CachingShuffleBatchReader(ShuffleBatchReader shuffleReader) {
    this(shuffleReader, MAXIMUM_BATCHES, EXPIRE_AFTER_MS);
  }

  public CachingShuffleBatchReader(ShuffleBatchReader shuffleReader, int maximumBatches) {
    this(shuffleReader, maximumBatches, EXPIRE_AFTER_MS);
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
    protected final @Nullable ShufflePosition startPosition;
    protected final @Nullable ShufflePosition endPosition;

    public BatchRange(
        @Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition) {
      this.startPosition = startPosition;
      this.endPosition = endPosition;
    }

    @Override
    public boolean equals(@Nullable Object o) {
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
}
