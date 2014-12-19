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

import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObservableIterable;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObservableIterator;
import com.google.cloud.dataflow.sdk.util.common.PeekingReiterator;
import com.google.cloud.dataflow.sdk.util.common.Reiterable;
import com.google.cloud.dataflow.sdk.util.common.Reiterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * An iterator through KeyGroupedShuffleEntries.
 */
public abstract class GroupingShuffleEntryIterator
    implements Iterator<KeyGroupedShuffleEntries> {
  /** The iterator through the underlying shuffle records. */
  private PeekingReiterator<ShuffleEntry> shuffleIterator;

  /**
   * The key of the most recent KeyGroupedShuffleEntries returned by
   * {@link #next}, if any.
   *
   * <p>If currentKeyBytes is non-null, then it's the key for the last entry
   * returned by {@link #next}, and all incoming entries with that key should
   * be skipped over by this iterator (since this iterator is iterating over
   * keys, not the individual values associated with a given key).
   *
   * <p>If currentKeyBytes is null, and shuffleIterator.hasNext(), then the
   * key of shuffleIterator.next() is the key of the next
   * KeyGroupedShuffleEntries to return from {@link #next}.
   */
  @Nullable private byte[] currentKeyBytes = null;

  /**
   * Constructs a GroupingShuffleEntryIterator, given a Reiterator
   * over ungrouped ShuffleEntries, assuming the ungrouped
   * ShuffleEntries for a given key are consecutive.
   */
  public GroupingShuffleEntryIterator(
      Reiterator<ShuffleEntry> shuffleIterator) {
    this.shuffleIterator =
        new PeekingReiterator<>(
            new ProgressTrackingReiterator<>(
                shuffleIterator,
                new ProgressTrackerGroup<ShuffleEntry>() {
                  @Override
                  protected void report(ShuffleEntry entry) {
                    notifyElementRead(entry.length());
                  }
                }.start()));
  }

  /** Notifies observers about a new element read. */
  protected abstract void notifyElementRead(long byteSize);

  @Override
  public boolean hasNext() {
    advanceIteratorToNextKey();
    return shuffleIterator.hasNext();
  }

  @Override
  public KeyGroupedShuffleEntries next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    ShuffleEntry entry = shuffleIterator.peek();
    currentKeyBytes = entry.getKey();
    return new KeyGroupedShuffleEntries(
        entry.getPosition(),
        currentKeyBytes,
        new ValuesIterable(new ValuesIterator(currentKeyBytes)));
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private void advanceIteratorToNextKey() {
    if (currentKeyBytes == null) {
      return;
    }
    while (shuffleIterator.hasNext()) {
      ShuffleEntry entry = shuffleIterator.peek();
      if (!Arrays.equals(entry.getKey(), currentKeyBytes)) {
        break;
      }
      shuffleIterator.next();
    }
    currentKeyBytes = null;
  }

  private static class ValuesIterable
      extends ElementByteSizeObservableIterable<ShuffleEntry, ValuesIterator>
      implements Reiterable<ShuffleEntry> {
    private final ValuesIterator base;

    public ValuesIterable(ValuesIterator base) {
      this.base = checkNotNull(base);
    }

    @Override
    public ValuesIterator createIterator() {
      return base.copy();
    }
  }

  /**
   * Provides the {@link Reiterator} used to iterate through the
   * shuffle entries of a KeyGroupedShuffleEntries.
   */
  private class ValuesIterator
      extends ElementByteSizeObservableIterator<ShuffleEntry>
      implements Reiterator<ShuffleEntry> {
    // N.B. This class is *not* static; it maintains a reference to its
    // enclosing KeyGroupedShuffleEntriesIterator instance so that it can update
    // that instance's shuffleIterator as an optimization.

    private final byte[] valueKeyBytes;
    private final PeekingReiterator<ShuffleEntry> valueShuffleIterator;
    private final ProgressTracker<ShuffleEntry> tracker;
    private boolean nextKnownValid = false;

    public ValuesIterator(byte[] valueKeyBytes) {
      this.valueKeyBytes = checkNotNull(valueKeyBytes);
      this.valueShuffleIterator = shuffleIterator.copy();
      // N.B. The ProgressTrackerGroup captures the reference to the original
      // ValuesIterator for a given values iteration.  Which happens to be
      // exactly what we want, since this is also the ValuesIterator whose
      // base Observable has the references to all of the Observers watching
      // the iteration.  Copied ValuesIterator instances do *not* have these
      // Observers, but that's fine, since the derived ProgressTracker
      // instances reference the ProgressTrackerGroup which references the
      // original ValuesIterator which does have them.
      this.tracker = new ProgressTrackerGroup<ShuffleEntry>() {
        @Override
        protected void report(ShuffleEntry entry) {
          notifyValueReturned(entry.length());
        }
      }.start();
    }

    private ValuesIterator(ValuesIterator it) {
      this.valueKeyBytes = it.valueKeyBytes;
      this.valueShuffleIterator = it.valueShuffleIterator.copy();
      this.tracker = it.tracker.copy();
      this.nextKnownValid = it.nextKnownValid;
    }

    @Override
    public boolean hasNext() {
      if (nextKnownValid) {
        return true;
      }
      if (!valueShuffleIterator.hasNext()) {
        return false;
      }
      ShuffleEntry entry = valueShuffleIterator.peek();
      nextKnownValid = Arrays.equals(entry.getKey(), valueKeyBytes);

      // Opportunistically update the parent KeyGroupedShuffleEntriesIterator,
      // potentially allowing it to skip a large number of key/value pairs
      // with this key.
      if (!nextKnownValid && valueKeyBytes == currentKeyBytes) {
        shuffleIterator = valueShuffleIterator.copy();
        currentKeyBytes = null;
      }

      return nextKnownValid;
    }

    @Override
    public ShuffleEntry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      ShuffleEntry entry = valueShuffleIterator.next();
      nextKnownValid = false;
      tracker.saw(entry);
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ValuesIterator copy() {
      return new ValuesIterator(this);
    }
  }
}
