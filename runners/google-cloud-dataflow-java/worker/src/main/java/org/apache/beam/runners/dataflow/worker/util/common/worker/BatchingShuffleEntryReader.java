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

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.util.common.Reiterator;
import org.checkerframework.checker.nullness.qual.Nullable;

/** BatchingShuffleEntryReader provides a mechanism for reading entries from a shuffle dataset. */
@NotThreadSafe
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class BatchingShuffleEntryReader implements ShuffleEntryReader {
  private final ShuffleBatchReader batchReader;

  /**
   * Constructs a {@link BatchingShuffleEntryReader}.
   *
   * @param batchReader supplies the underlying {@link ShuffleBatchReader} to read batches of
   *     entries from
   */
  public BatchingShuffleEntryReader(ShuffleBatchReader batchReader) {
    this.batchReader = checkNotNull(batchReader);
  }

  @Override
  public Reiterator<ShuffleEntry> read(
      @Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition) {
    return new ShuffleReadIterator(batchReader, startPosition, endPosition);
  }

  /** ShuffleReadIterator iterates over a (potentially huge) sequence of shuffle entries. */
  private static final class ShuffleReadIterator implements Reiterator<ShuffleEntry> {
    private final ShuffleBatchReader batchReader;
    // Shuffle service returns entries in pages. If the response contains a
    // non-null nextStartPosition, we have to ask for more pages. The response
    // with null nextStartPosition signifies the end of stream.
    private final @Nullable ShufflePosition endPosition;
    private @Nullable ShufflePosition nextStartPosition;

    /** Random-access list view of the most recently read batch, its cached size, and index. */
    private @Nullable List<ShuffleEntry> currentEntries;

    private int currentSize;
    private int nextIndex;

    ShuffleReadIterator(
        ShuffleBatchReader batchReader,
        @Nullable ShufflePosition startPosition,
        @Nullable ShufflePosition endPosition) {
      this.batchReader = batchReader;
      this.nextStartPosition = startPosition;
      this.endPosition = endPosition;
    }

    private ShuffleReadIterator(ShuffleReadIterator it) {
      this.batchReader = it.batchReader;
      this.endPosition = it.endPosition;
      this.nextStartPosition = it.nextStartPosition;
      this.currentEntries = it.currentEntries;
      this.currentSize = it.currentSize;
      this.nextIndex = it.nextIndex;
    }

    @Override
    public boolean hasNext() {
      if (nextIndex < currentSize) {
        return true;
      }
      return fillAndHasNext();
    }

    private boolean fillAndHasNext() {
      fillEntriesIfNeeded();
      // TODO: Report API errors to the caller using checked
      // exceptions.
      return nextIndex < currentSize;
    }

    @Override
    public ShuffleEntry next() throws NoSuchElementException {
      if (nextIndex < currentSize) {
        return currentEntries.get(nextIndex++);
      }
      return fillAndNext();
    }

    private ShuffleEntry fillAndNext() {
      fillEntriesIfNeeded();
      if (nextIndex >= currentSize) {
        throw new NoSuchElementException();
      }
      return currentEntries.get(nextIndex++);
    }

    @Override
    public void remove() throws UnsupportedOperationException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ShuffleReadIterator copy() {
      return new ShuffleReadIterator(this);
    }

    private void fillEntriesIfNeeded() {
      if (currentEntries != null && nextStartPosition == null) {
        // End of stream.
        checkState(nextIndex >= currentSize);
        return;
      }

      do {
        fillEntries();
      } while (nextIndex >= currentSize && nextStartPosition != null);
    }

    private void fillEntries() {
      try {
        ShuffleBatchReader.Batch batch = batchReader.read(nextStartPosition, endPosition);
        List<ShuffleEntry> batchEntries = checkNotNull(batch.entries);
        List<ShuffleEntry> entriesView =
            batchEntries instanceof RandomAccess ? batchEntries : new ArrayList<>(batchEntries);
        int entriesSize = entriesView.size();
        nextStartPosition = batch.nextStartPosition;
        currentEntries = entriesView;
        currentSize = entriesSize;
        nextIndex = 0;
      } catch (RuntimeException | OutOfMemoryError e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }

  @Override
  public void close() {}
}
