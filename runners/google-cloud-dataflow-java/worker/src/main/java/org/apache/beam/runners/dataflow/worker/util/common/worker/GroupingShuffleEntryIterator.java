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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.NoSuchElementException;
import java.util.Observer;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.common.Reiterable;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An iterator through KeyGroupedShuffleEntries.
 *
 * <p>Much like a {@link NativeReader.NativeReaderIterator}, but without {@code start()}, and not
 * used via the interface of that class, hence doesn't inherit it.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class GroupingShuffleEntryIterator {
  private static final Logger LOG = LoggerFactory.getLogger(GroupingShuffleEntryIterator.class);

  /**
   * The iterator through the underlying shuffle records. Typed as the concrete final {@link
   * ProgressTrackingReiterator} so hot-path calls stay monomorphic {@code invokevirtual}.
   */
  private ProgressTrackingReiterator<ShuffleEntry> shuffleIterator;

  /**
   * One-entry lookahead populated when a {@link ValuesIterator} reads the first entry of the next
   * key and fast-forwards this iterator directly past the current key. Already accounted for by the
   * underlying {@link ProgressTrackerGroup}.
   */
  private @Nullable ShuffleEntry nextKeyEntry = null;

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
  private @Nullable ByteString currentKeyBytes = null;

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
    ShuffleEntry entry = nextKeyEntry;
    if (entry != null) {
      // Consumed from ValuesIterator fast-forward; already known to have a different key than
      // currentKeyBytes and already reported to the underlying ProgressTrackerGroup.
      nextKeyEntry = null;
    } else {
      // Skip entries with the same key - we're an iterator over keys, and advancement through
      // values of the same key happens through ValuesIterable instead.
      while (true) {
        if (!shuffleIterator.hasNext()) {
          return markFinishedAndCommitBytesCounter();
        }
        entry = shuffleIterator.next();
        if (!entry.getKey().equals(currentKeyBytes)) {
          break;
        }
        // Note: we can get here only if the ValuesIterable of the preceding key has NOT been
        // read fully. If it had, then the "fast forward" code path in ValuesIterator
        // would have triggered instead, updating nextKeyEntry / shuffleIterator to the next key
        // or EOF.
        totalByteSizeOfEntriesForCurrentKey += entry.length();
      }
    }

    ShufflePosition groupStart = entry.getPosition();
    boolean isAtSplitPoint = (groupStart == null) || !groupStart.equals(lastGroupStart);
    if (!rangeTracker.tryReturnRecordAt(isAtSplitPoint, groupStart)) {
      return markFinishedAndCommitBytesCounter();
    }
    lastGroupStart = groupStart;

    commitBytesRead(totalByteSizeOfEntriesForCurrentKey);
    totalByteSizeOfEntriesForCurrentKey = entry.length();

    currentKeyBytes = entry.getKey();
    // "entry" is the first entry of the new key, already popped from shuffleIterator. Copy the
    // iterator positioned immediately after "entry" only once after the split check succeeds.
    current =
        new KeyGroupedShuffleEntries(
            entry.getPosition(),
            currentKeyBytes,
            new ValuesIterable(this, currentKeyBytes, entry, shuffleIterator.copy()));

    return true;
  }

  private boolean markFinishedAndCommitBytesCounter() {
    commitBytesRead(totalByteSizeOfEntriesForCurrentKey);

    current = null;
    currentKeyBytes = null;
    nextKeyEntry = null;
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

  private static final class ValuesIterable
      extends ElementByteSizeObservableIterable<ShuffleEntry, ValuesIterator>
      implements Reiterable<ShuffleEntry> {
    private final GroupingShuffleEntryIterator parent;
    private final ByteString currentKeyBytes;
    private final ShuffleEntry firstEntry;
    private final ProgressTrackingReiterator<ShuffleEntry> baseValuesIterator;

    public ValuesIterable(
        GroupingShuffleEntryIterator parent,
        ByteString keyBytes,
        ShuffleEntry firstEntry,
        ProgressTrackingReiterator<ShuffleEntry> baseValuesIterator) {
      this.parent = parent;
      this.currentKeyBytes = keyBytes;
      this.firstEntry = checkNotNull(firstEntry);
      this.baseValuesIterator = checkNotNull(baseValuesIterator);
    }

    @Override
    public ValuesIterator createIterator() {
      return new ValuesIterator(parent, firstEntry, baseValuesIterator.copy(), currentKeyBytes);
    }
  }

  /**
   * Provides the {@link Reiterator} used to iterate through the shuffle entries of a
   * KeyGroupedShuffleEntries.
   */
  private static final class ValuesIterator extends ElementByteSizeObservableIterator<ShuffleEntry>
      implements Reiterator<ShuffleEntry> {
    private static final byte HAS_NEXT_UNKNOWN = 0;
    private static final byte HAS_NEXT_TRUE = 1;
    private static final byte HAS_NEXT_FALSE = -1;

    private final GroupingShuffleEntryIterator parent;
    private @Nullable ShuffleEntry pendingFirstEntry;
    private @Nullable ProgressTrackingReiterator<ShuffleEntry> valuesIterator;
    private final ProgressTracker<ShuffleEntry> tracker;
    private final ByteString expectedKeyBytes;

    // Tracks whether any java.util.Observer is attached so the common zero-observer path skips
    // Observable's synchronized setChanged()/notifyObservers() and Long autoboxing.
    private volatile boolean hasObservers;

    private byte cachedHasNext = HAS_NEXT_UNKNOWN;
    private long byteSizeRead = 0L;
    private ShuffleEntry current;

    public ValuesIterator(
        GroupingShuffleEntryIterator parent,
        ShuffleEntry firstEntry,
        ProgressTrackingReiterator<ShuffleEntry> valuesIterator,
        ByteString expectedKeyBytes) {
      this.parent = parent;
      this.pendingFirstEntry = checkNotNull(firstEntry);
      this.valuesIterator = checkNotNull(valuesIterator);
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
              if (ValuesIterator.this.hasObservers) {
                ValuesIterator.this.notifyValueReturned(entry.length());
              }
            }
          }.start();
    }

    private ValuesIterator(
        GroupingShuffleEntryIterator parent, ValuesIterator it, long byteSizeRead) {
      this.parent = parent;
      this.pendingFirstEntry = it.pendingFirstEntry;
      this.valuesIterator = it.valuesIterator != null ? it.valuesIterator.copy() : null;
      this.tracker = it.tracker.copy();
      this.expectedKeyBytes = it.expectedKeyBytes;
      this.cachedHasNext = it.cachedHasNext;
      this.byteSizeRead = byteSizeRead;
      this.current = it.current;
    }

    @Override
    @SuppressWarnings("deprecation")
    public synchronized void addObserver(Observer o) {
      super.addObserver(o);
      hasObservers = countObservers() > 0;
    }

    @Override
    @SuppressWarnings("deprecation")
    public synchronized void deleteObserver(Observer o) {
      super.deleteObserver(o);
      hasObservers = countObservers() > 0;
    }

    @Override
    public synchronized void deleteObservers() {
      super.deleteObservers();
      hasObservers = false;
    }

    @Override
    public boolean hasNext() {
      if (cachedHasNext != HAS_NEXT_UNKNOWN) {
        return cachedHasNext == HAS_NEXT_TRUE;
      }
      boolean hasMore = advance();
      cachedHasNext = hasMore ? HAS_NEXT_TRUE : HAS_NEXT_FALSE;
      return hasMore;
    }

    @SuppressWarnings("ReferenceEquality")
    private boolean advance() {
      ShuffleEntry first = pendingFirstEntry;
      if (first != null) {
        // First entry of the key was already validated by GroupingShuffleEntryIterator.advance().
        pendingFirstEntry = null;
        byteSizeRead += first.length();
        tracker.saw(first);
        current = first;
        return true;
      }

      ShuffleEntry nextKeyEntry = null;
      ProgressTrackingReiterator<ShuffleEntry> iter = valuesIterator;
      if (iter != null && iter.hasNext()) {
        ShuffleEntry entry = iter.next();
        if (entry.getKey().equals(expectedKeyBytes)) {
          byteSizeRead += entry.length();
          tracker.saw(entry);
          current = entry;
          return true;
        }
        nextKeyEntry = entry;
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
      //    between calls to hasNext/next on the current ValuesIterator (or finished).
      // 2) when another ValuesIterator produced from the same parent at a different key
      //    has fast-forwarded it to the successor of that key.
      //    (note that if this "antagonist" ValuesIterator was at the same key, then this
      //    codepath may trigger more than once, hence the body of the if statement should
      //    be idempotent)
      if (expectedKeyBytes == parent.currentKeyBytes && iter != null) {
        // Donate iter directly to the parent without an extra copy(): hasNext() immediately caches
        // HAS_NEXT_FALSE after this returns false, so this ValuesIterator will never advance iter
        // again (and copy() preserves the exhausted state with valuesIterator == null).
        parent.nextKeyEntry = nextKeyEntry;
        parent.shuffleIterator = iter;
        // The parent is also tracking the total byte size read. Since we have been tracking this
        // amount in byteSizeRead, we can just set it to the right value.
        parent.totalByteSizeOfEntriesForCurrentKey = byteSizeRead;
        // We need to do both of these things for the fast-forward to be correct.
      }
      valuesIterator = null;

      return false;
    }

    @Override
    public ShuffleEntry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      checkState(current != null, "current entry should be non-null");
      ShuffleEntry toReturn = current;
      cachedHasNext = HAS_NEXT_UNKNOWN;
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
