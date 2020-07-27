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

import java.util.ListIterator;
import java.util.NoSuchElementException;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.util.common.Reiterator;
import org.checkerframework.checker.nullness.qual.Nullable;

/** BatchingShuffleEntryReader provides a mechanism for reading entries from a shuffle dataset. */
@NotThreadSafe
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
    return new ShuffleReadIterator(startPosition, endPosition);
  }

  /** ShuffleReadIterator iterates over a (potentially huge) sequence of shuffle entries. */
  private final class ShuffleReadIterator implements Reiterator<ShuffleEntry> {
    // Shuffle service returns entries in pages. If the response contains a
    // non-null nextStartPosition, we have to ask for more pages. The response
    // with null nextStartPosition signifies the end of stream.
    private final @Nullable ShufflePosition endPosition;
    private @Nullable ShufflePosition nextStartPosition;

    /** The most recently read batch. */
    ShuffleBatchReader.@Nullable Batch currentBatch;
    /** An iterator over the most recently read batch. */
    private @Nullable ListIterator<ShuffleEntry> entries;

    ShuffleReadIterator(
        @Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition) {
      this.nextStartPosition = startPosition;
      this.endPosition = endPosition;
    }

    private ShuffleReadIterator(ShuffleReadIterator it) {
      this.endPosition = it.endPosition;
      this.nextStartPosition = it.nextStartPosition;
      this.currentBatch = it.currentBatch;
      // The idea here: if the iterator being copied was in the middle of a
      // batch (the typical case), create a new iteration state at the same
      // point in the same batch.
      this.entries =
          (it.entries == null
              ? null
              : it.currentBatch.entries.listIterator(it.entries.nextIndex()));
    }

    @Override
    public boolean hasNext() {
      fillEntriesIfNeeded();
      // TODO: Report API errors to the caller using checked
      // exceptions.
      return entries.hasNext();
    }

    @Override
    public ShuffleEntry next() throws NoSuchElementException {
      fillEntriesIfNeeded();
      return entries.next();
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
      if (entries != null && entries.hasNext()) {
        // Has more records in the current page, or error.
        return;
      }

      if (entries != null && nextStartPosition == null) {
        // End of stream.
        checkState(!entries.hasNext());
        return;
      }

      do {
        fillEntries();
      } while (!entries.hasNext() && nextStartPosition != null);
    }

    private void fillEntries() {
      try {
        ShuffleBatchReader.Batch batch = batchReader.read(nextStartPosition, endPosition);
        nextStartPosition = batch.nextStartPosition;
        entries = batch.entries.listIterator();
        currentBatch = batch;
      } catch (RuntimeException e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }

      checkState(entries != null);
    }
  }

  @Override
  public void close() {}
}
