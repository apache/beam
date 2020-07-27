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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Arrays;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.common.Reiterable;
import org.apache.beam.sdk.util.common.Reiterator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An iterator through KeyGroupedShuffleEntries.
 *
 * <p>Much like a {@link NativeReader.NativeReaderIterator}, but without {@code start()}, and not
 * used via the interface of that class, hence doesn't inherit it.
 */
public abstract class GroupingShuffleEntryIterator {
  private static final Logger LOG = LoggerFactory.getLogger(GroupingShuffleEntryIterator.class);

  /** The iterator through the underlying shuffle records. */
  private Reiterator<ShuffleEntry> shuffleIterator;

  private final GroupingShuffleRangeTracker rangeTracker;

  /**
   * The key of the most recent KeyGroupedShuffleEntries returned by {@link #advance}, if any.
   *
   * <p>If currentKeyBytes is non-null, then it's the key for the last entry returned by {@link
   * #advance}, and all incoming entries with that key should be skipped over by this iterator
   * (since this iterator is iterating over keys, not the individual values associated with a given
   * key).
   *
   * <p>If currentKeyBytes is null, and shuffleIterator.hasNext(), then the key of
   * shuffleIterator.next() is the key of the next KeyGroupedShuffleEntries to return via {@link
   * #advance}/{@link #getCurrent}.
   */
  private byte @Nullable [] currentKeyBytes = null;

  private ShufflePosition lastGroupStart;

  /**
   * The size of the shuffle entries read so far for the current key (if currentKeyBytes is
   * non-null), or the previous key (if currentKeyBytes is null).
   */
  private long totalByteSizeOfEntriesForCurrentKey = 0L;

  private KeyGroupedShuffleEntries current = null;
  private boolean isFinished = false;

  /**
   * Constructs a GroupingShuffleEntryIterator, given a Reiterator over ungrouped ShuffleEntries,
   * assuming the ungrouped ShuffleEntries for a given key are consecutive. The counter given as
   * argument, if non-null, will be updated with the byte size of the entries read.
   */
  public GroupingShuffleEntryIterator(
      ShuffleEntryReader entryReader, ShufflePosition startPosition, ShufflePosition stopPosition) {
    this.rangeTracker = new GroupingShuffleRangeTracker(startPosition, stopPosition);
    this.shuffleIterator =
        new ProgressTrackingReiterator<>(
            entryReader.read(startPosition, stopPosition),
            new ProgressTrackerGroup<ShuffleEntry>() {
              @Override
              protected void report(ShuffleEntry entry) {
                notifyElementRead(entry.length());
              }
            }.start());
  }

  /**
   * Notifies observers about a new ShuffleEntry (key and value, not key and iterable of values)
   * read.
   */
  protected abstract void notifyElementRead(long byteSize);

  /** Notifies observers about the bytes read that are to be committed to the counters. */
  protected abstract void commitBytesRead(long bytesRead);

  public boolean advance() {
    if (isFinished) {
      return false;
    }
    Reiterator<ShuffleEntry> atCurrentEntry;
    ShuffleEntry entry;
    // Skip entries with the same key - we're an iterator over keys, and advancement through
    // values of the same key happens through ValuesIterable instead.
    while (true) {
      if (!shuffleIterator.hasNext()) {
        return markFinishedAndCommitBytesCounter();
      }
      // Save an iterator to the current entry, in case it's in a different key
      // than the previous entry.
      // If it's in the same key, we just skip it; if it's a new key, we'll need to
      // start ValuesIterable below from this entry.
      atCurrentEntry = shuffleIterator.copy();
      entry = shuffleIterator.next();
      if (!Arrays.equals(entry.getKey(), currentKeyBytes)) {
        break;
      }
      // Note: we can get here only if the ValuesIterable of the preceding key has NOT been
      // read fully. If it had, then the "fast forward" code path in ValuesIterable
      // would have triggered instead, and would prevent this path by setting currentKeyBytes
      // to null, and we'd have exited the loop via the inequality check above.
      totalByteSizeOfEntriesForCurrentKey += entry.length();
    }

    // Now "atCurrentEntry" points to "entry", and "entry" is the first entry in the next key.
    ShufflePosition groupStart = entry.getPosition();
    boolean isAtSplitPoint = (groupStart == null) || !groupStart.equals(lastGroupStart);
    if (!rangeTracker.tryReturnRecordAt(isAtSplitPoint, groupStart)) {
      return markFinishedAndCommitBytesCounter();
    }
    lastGroupStart = groupStart;

    commitBytesRead(totalByteSizeOfEntriesForCurrentKey);
    totalByteSizeOfEntriesForCurrentKey = entry.length();

    currentKeyBytes = entry.getKey();
    current =
        new KeyGroupedShuffleEntries(
            entry.getPosition(),
            currentKeyBytes,
            new ValuesIterable(this, currentKeyBytes, atCurrentEntry));

    return true;
  }

  private boolean markFinishedAndCommitBytesCounter() {
    commitBytesRead(totalByteSizeOfEntriesForCurrentKey);

    current = null;
    isFinished = true;
    return false;
  }

  public KeyGroupedShuffleEntries getCurrent() {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }

  public boolean finished() {
    return isFinished;
  }

  public boolean trySplitAtPosition(ShufflePosition newStopPosition) {
    if (rangeTracker.trySplitAtPosition(newStopPosition)) {
      LOG.info(
          "Split GroupingShuffleReader at {}, now {}", newStopPosition.toString(), rangeTracker);
      return true;
    } else {
      LOG.debug(
          "Will not split GroupingShuffleReader {} at {}",
          rangeTracker,
          newStopPosition.toString());
      return false;
    }
  }

  public GroupingShuffleRangeTracker getRangeTracker() {
    return rangeTracker;
  }

  private static class ValuesIterable
      extends ElementByteSizeObservableIterable<ShuffleEntry, ValuesIterator>
      implements Reiterable<ShuffleEntry> {
    private final GroupingShuffleEntryIterator parent;
    private final byte[] currentKeyBytes;
    private Reiterator<ShuffleEntry> baseValuesIterator;

    public ValuesIterable(
        GroupingShuffleEntryIterator parent,
        byte[] keyBytes,
        Reiterator<ShuffleEntry> baseValuesIterator) {
      this.parent = parent;
      this.currentKeyBytes = keyBytes;
      this.baseValuesIterator = baseValuesIterator;
    }

    @Override
    public ValuesIterator createIterator() {
      return new ValuesIterator(parent, baseValuesIterator.copy(), currentKeyBytes);
    }
  }

  /**
   * Provides the {@link Reiterator} used to iterate through the shuffle entries of a
   * KeyGroupedShuffleEntries.
   */
  private static class ValuesIterator extends ElementByteSizeObservableIterator<ShuffleEntry>
      implements Reiterator<ShuffleEntry> {
    private final GroupingShuffleEntryIterator parent;
    private final Reiterator<ShuffleEntry> valuesIterator;
    private final ProgressTracker<ShuffleEntry> tracker;
    private final byte[] expectedKeyBytes;

    private Boolean cachedHasNext;
    private long byteSizeRead = 0L;
    private ShuffleEntry current;

    public ValuesIterator(
        GroupingShuffleEntryIterator parent,
        Reiterator<ShuffleEntry> valuesIterator,
        byte[] expectedKeyBytes) {
      this.parent = parent;
      this.valuesIterator = valuesIterator;
      this.expectedKeyBytes = checkNotNull(expectedKeyBytes);
      // N.B. The ProgressTrackerGroup captures the reference to the original
      // ValuesIterator for a given values iteration, which happens to be
      // exactly what we want, since this is also the ValuesIterator whose
      // base Observable has the references to all of the Observers watching
      // the iteration.  Copied ValuesIterator instances do *not* have these
      // Observers, but that's fine, since the derived ProgressTracker
      // instances reference the ProgressTrackerGroup, which references the
      // original ValuesIterator, which does have them.
      this.tracker =
          new ProgressTrackerGroup<ShuffleEntry>() {
            @Override
            protected void report(ShuffleEntry entry) {
              ValuesIterator.this.notifyValueReturned(entry.length());
            }
          }.start();
    }

    private ValuesIterator(
        GroupingShuffleEntryIterator parent, ValuesIterator it, long byteSizeRead) {
      this.parent = parent;
      this.valuesIterator = it.valuesIterator.copy();
      this.tracker = it.tracker.copy();
      this.expectedKeyBytes = it.expectedKeyBytes;
      this.cachedHasNext = it.cachedHasNext;
      this.byteSizeRead = byteSizeRead;
      this.current = it.current;
    }

    @Override
    public boolean hasNext() {
      if (cachedHasNext != null) {
        return cachedHasNext;
      }
      cachedHasNext = advance();
      return cachedHasNext;
    }

    private boolean advance() {
      // Save a copy of the iterator pointing at the next entry, to use below in case we're right
      // before a key boundary (or end of stream).
      Reiterator<ShuffleEntry> possibleStartOfNextKey = valuesIterator.copy();
      if (valuesIterator.hasNext()) {
        ShuffleEntry entry = valuesIterator.next();
        if (Arrays.equals(entry.getKey(), expectedKeyBytes)) {
          byteSizeRead += entry.length();
          tracker.saw(entry);
          current = entry;
          return true;
        }
      }

      // Fast-forwarding:
      //
      // This ValuesIterator is the iterator for the Iterable<V> accompanying the current K
      // for the parent (which is an Iterable<KV<K, Iterable<V>>>).
      // When ValuesIterator was created, the parent iterator was seeked to the beginning of
      // this Iterable<V>.
      //
      // Now that we've reached the end, we can fast-forward the parent iterator to our iterator
      // which points to the next key (or end of stream). Otherwise, parent's advance() would have
      // to read through all entries of K1 again just to skip them.
      //
      // "expectedKeyBytes" is initialized from parent.currentKeyBytes by reference,
      // and the byte array is not mutated, so a reference equality check is sufficient
      // to guard against two adversary situations:
      // 1) when the parent GroupingShuffleEntryIterator has been advanced to the next key
      //    between calls to hasNext/next on the current ValuesIterator.
      // 2) when another ValuesIterator produced from the same parent at a different key
      //    has fast-forwarded it to the successor of that key.
      //    (note that if this "antagonist" ValuesIterator was at the same key, then this
      //    codepath may trigger more than once, hence the body of the if statement should
      //    be idempotent)
      if (expectedKeyBytes == parent.currentKeyBytes) {
        // Fast-forward the parent's shuffle iterator.
        parent.shuffleIterator = possibleStartOfNextKey;
        // The parent is also tracking the total byte size read. Since we have been tracking this
        // amount in byteSizeRead, we can just set it to the right value.
        parent.totalByteSizeOfEntriesForCurrentKey = byteSizeRead;
        // We need to do both of these things for the fast-forward to be correct.
      }

      return false;
    }

    @Override
    public ShuffleEntry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      checkState(current != null, "current entry should be non-null");
      ShuffleEntry toReturn = current;
      cachedHasNext = null;
      current = null;
      return toReturn;
    }

    @Override
    public ValuesIterator copy() {
      return new ValuesIterator(parent, this, byteSizeRead);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
