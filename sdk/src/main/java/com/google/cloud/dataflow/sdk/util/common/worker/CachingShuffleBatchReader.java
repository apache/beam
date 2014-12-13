/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.HashMap;

import javax.annotation.Nullable;

/** A {@link ShuffleBatchReader} that caches batches as they're read. */
public final class CachingShuffleBatchReader implements ShuffleBatchReader {
  private final ShuffleBatchReader reader;

  // The cache itself is implemented as a HashMap of RangeReadReference values,
  // keyed by the start and end positions describing the range of a particular
  // request (represented by BatchRange).
  //
  // The first reader for a particular range builds an AsyncReadResult for the
  // result, inserts it into the cache, drops the lock, and then completes the
  // read; subsequent readers simply wait for the AsyncReadResult to complete.
  //
  // Note that overlapping ranges are considered distinct; cached entries for
  // one range are not used for any other range, even if doing so would avoid a
  // fetch.
  //
  // So this is not a particularly sophisticated algorithm: a smarter cache
  // would be able to use subranges of previous requests to satisfy new
  // requests.  But in this particular case, we expect that the simple algorithm
  // will work well.  For a given shuffle source, the splits read by various
  // iterators over that source starting from a particular position (which is
  // how this class is used in practice) should turn out to be constant, if the
  // result returned by the service for a particular [start, end) range are
  // consistent.  So we're not expecting to see overlapping ranges of entries
  // within a cache.
  //
  // It's also been shown -- by implementing it -- that the more thorough
  // algorithm is relatively complex, with numerous edge cases requiring very
  // careful thought to get right.  It's doable, but non-trivial and hard to
  // understand and maintain; without a compelling justification, it's better to
  // stick with the simpler implementation.
  //
  // @VisibleForTesting
  final HashMap<BatchRange, RangeReadReference> cache = new HashMap<>();

  // The queue of references which have been collected by the garbage collector.
  // This queue should only be used with references of class RangeReadReference.
  private final ReferenceQueue<AsyncReadResult> refQueue = new ReferenceQueue<>();

  /**
   * Constructs a new {@link CachingShuffleBatchReader}.
   *
   * @param reader supplies the downstream {@link ShuffleBatchReader}
   * this {@code CachingShuffleBatchReader} will use to issue reads
   */
  public CachingShuffleBatchReader(ShuffleBatchReader reader) {
    this.reader = checkNotNull(reader);
  }

  @Override
  public Batch read(
      @Nullable ShufflePosition startPosition,
      @Nullable ShufflePosition endPosition) throws IOException {

    @Nullable AsyncReadResult waitResult = null;
    @Nullable AsyncReadResult runResult = null;
    final BatchRange batchRange = new BatchRange(startPosition, endPosition);

    synchronized (cache) {
      // Remove any GCd entries.
      for (Reference<? extends AsyncReadResult> ref = refQueue.poll();
           ref != null;
           ref = refQueue.poll()) {
        RangeReadReference rangeReadRef = (RangeReadReference) ref;
        cache.remove(rangeReadRef.getBatchRange());
      }

      // Find the range reference; note that one might not be in the map, or it
      // might contain a null if its target has been GCd.
      @Nullable RangeReadReference rangeReadRef = cache.get(batchRange);

      // Get a strong reference to the existing AsyncReadResult for the range, if possible.
      if (rangeReadRef != null) {
        waitResult = rangeReadRef.get();
      }

      // Create a new AsyncReadResult if one is needed.
      if (waitResult == null) {
        runResult = new AsyncReadResult();
        waitResult = runResult;
        rangeReadRef = null;  // Replace the previous RangeReadReference.
      }

      // Insert a new RangeReadReference into the map if we don't have a usable
      // one (either we weren't able to find one in the map, or we did but it
      // was already cleared by the GC).
      if (rangeReadRef == null) {
        cache.put(batchRange,
            new RangeReadReference(batchRange, runResult, refQueue));
      }
    }  // Drop the cache lock.

    if (runResult != null) {
      // This thread created the AsyncReadResult, and is responsible for
      // actually performing the read.
      try {
        Batch result = reader.read(startPosition, endPosition);
        runResult.setResult(result);
      } catch (RuntimeException | IOException e) {
        runResult.setException(e);
        synchronized (cache) {
          // No reason to continue to cache the fact that there was a problem.
          // Note that since this thread holds a strong reference to the
          // AsyncReadResult, it won't be GCd, so the soft reference held by the
          // cache is guaranteed to still be present.
          cache.remove(batchRange);
        }
      }
    }

    return waitResult.getResult();
  }

  /** The key for the entries stored in the batch cache. */
  // @VisibleForTesting
  static final class BatchRange {
    @Nullable private final ShufflePosition startPosition;
    @Nullable private final ShufflePosition endPosition;

    public BatchRange(@Nullable ShufflePosition startPosition,
                      @Nullable ShufflePosition endPosition) {
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

  /** Holds an asynchronously batch read result. */
  private static final class AsyncReadResult {
    @Nullable private Batch batch = null;
    @Nullable private Throwable thrown = null;

    public synchronized void setResult(Batch b) {
      batch = b;
      notifyAll();
    }

    public synchronized void setException(Throwable t) {
      thrown = t;
      notifyAll();
    }

    public synchronized Batch getResult() throws IOException {
      while (batch == null && thrown == null) {
        try {
          wait();
        } catch (InterruptedException e) {
          throw new RuntimeException("interrupted", e);
        }
      }
      if (thrown != null) {
        // N.B. setException can only be called with a RuntimeException or an
        // IOException, so propagateIfPossible should always do the throw.
        Throwables.propagateIfPossible(thrown, IOException.class);
        throw new RuntimeException("unexpected", thrown);
      }
      return batch;
    }
  }

  /**
   * Maintains a soft reference to an AsyncReadResult.
   *
   * <p>This class extends {@link SoftReference} so that when the garbage
   * collector collects a batch and adds its reference to the cache's reference
   * queue, that reference can be cast back to {@code RangeReadReference},
   * allowing us to identify the reference's position in the cache (and to
   * therefore remove it).
   */
  // @VisibleForTesting
  static final class RangeReadReference extends SoftReference<AsyncReadResult> {
    private final BatchRange range;

    public RangeReadReference(
        BatchRange range, AsyncReadResult result,
        ReferenceQueue<? super AsyncReadResult> refQueue) {
      super(result, refQueue);
      this.range = checkNotNull(range);
    }

    public BatchRange getBatchRange() {
      return range;
    }
  }
}
