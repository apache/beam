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

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.api.client.util.Preconditions.checkNotNull;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudPositionToSourcePosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudProgressToSourceProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.sourceProgressToCloudProgress;
import static com.google.cloud.dataflow.sdk.util.TimeUtil.toCloudDuration;

import com.google.api.client.util.Preconditions;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.Reiterable;
import com.google.cloud.dataflow.sdk.util.common.Reiterator;
import com.google.cloud.dataflow.sdk.util.common.worker.BatchingShuffleEntryReader;
import com.google.cloud.dataflow.sdk.util.common.worker.GroupingShuffleEntryIterator;
import com.google.cloud.dataflow.sdk.util.common.worker.KeyGroupedShuffleEntries;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntryReader;
import com.google.cloud.dataflow.sdk.util.common.worker.Source;
import com.google.cloud.dataflow.sdk.values.KV;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A source that reads from a shuffled dataset and yields key-grouped data.
 *
 * @param <K> the type of the keys read from the shuffle
 * @param <V> the type of the values read from the shuffle
 */
public class GroupingShuffleSource<K, V>
    extends Source<WindowedValue<KV<K, Reiterable<V>>>> {
  private static final Logger LOG =
      LoggerFactory.getLogger(GroupingShuffleSource.class);

  final byte[] shuffleReaderConfig;
  final String startShufflePosition;
  final String stopShufflePosition;
  final BatchModeExecutionContext executionContext;

  Coder<K> keyCoder;
  Coder<V> valueCoder;

  public GroupingShuffleSource(PipelineOptions options,
                               byte[] shuffleReaderConfig,
                               String startShufflePosition,
                               String stopShufflePosition,
                               Coder<WindowedValue<KV<K, Iterable<V>>>> coder,
                               BatchModeExecutionContext executionContext)
      throws Exception {
    this.shuffleReaderConfig = shuffleReaderConfig;
    this.startShufflePosition = startShufflePosition;
    this.stopShufflePosition = stopShufflePosition;
    this.executionContext = executionContext;
    initCoder(coder);
  }

  @Override
  public SourceIterator<WindowedValue<KV<K, Reiterable<V>>>> iterator()
      throws IOException {
    Preconditions.checkArgument(shuffleReaderConfig != null);
    return iterator(new BatchingShuffleEntryReader(
        new ChunkingShuffleBatchReader(new ApplianceShuffleReader(
            shuffleReaderConfig))));
  }

  private void initCoder(Coder<WindowedValue<KV<K, Iterable<V>>>> coder) throws Exception {
    if (!(coder instanceof WindowedValueCoder)) {
      throw new Exception(
          "unexpected kind of coder for WindowedValue: " + coder);
    }
    Coder<KV<K, Iterable<V>>> elemCoder =
        ((WindowedValueCoder<KV<K, Iterable<V>>>) coder).getValueCoder();
    if (!(elemCoder instanceof KvCoder)) {
      throw new Exception(
          "unexpected kind of coder for elements read from " +
          "a key-grouping shuffle: " + elemCoder);
    }
    KvCoder<K, Iterable<V>> kvCoder = (KvCoder<K, Iterable<V>>) elemCoder;
    this.keyCoder = kvCoder.getKeyCoder();
    Coder<Iterable<V>> kvValueCoder = kvCoder.getValueCoder();
    if (!(kvValueCoder instanceof IterableCoder)) {
      throw new Exception(
          "unexpected kind of coder for values of KVs read from " +
          "a key-grouping shuffle");
    }
    IterableCoder<V> iterCoder = (IterableCoder<V>) kvValueCoder;
    this.valueCoder = iterCoder.getElemCoder();
  }

  final SourceIterator<WindowedValue<KV<K, Reiterable<V>>>> iterator(ShuffleEntryReader reader)
      throws IOException {
    return new GroupingShuffleSourceIterator(reader);
  }

  /**
   * A SourceIterator that reads from a ShuffleEntryReader and groups
   * all the values with the same key.
   *
   * <p>A key limitation of this implementation is that all iterator accesses
   * must by externally synchronized (the iterator objects are not individually
   * thread-safe, and the iterators derived from a single original iterator
   * access shared state which is not thread-safe).
   *
   * <p>To access the current position, the iterator must advance
   * on-demand and cache the next batch of key grouped shuffle
   * entries. The iterator does not advance a second time in @next()
   * to avoid asking the underlying iterator to advance to the next
   * key before the caller/user iterates over the values corresponding
   * to the current key -- which would introduce a performance
   * penalty.
   */
  private final class GroupingShuffleSourceIterator
      extends AbstractSourceIterator<WindowedValue<KV<K, Reiterable<V>>>> {
    // N.B. This class is *not* static; it uses the keyCoder, valueCoder, and
    // executionContext from its enclosing GroupingShuffleSource.

    /** The iterator over shuffle entries, grouped by common key. */
    private final Iterator<KeyGroupedShuffleEntries> groups;

    /** The stop position. No records with a position at or after
     * @stopPosition will be returned.  Initialized
     * to @AbstractShuffleSource.stopShufflePosition but can be
     * dynamically updated via @updateStopPosition() (note that such
     * updates can only decrease @stopPosition).
     *
     * <p> The granularity of the stop position is such that it can
     * only refer to records at the boundary of a key.
     */
    private ByteArrayShufflePosition stopPosition = null;

    /** The next group to be consumed, if available. */
    private KeyGroupedShuffleEntries nextGroup = null;

    public GroupingShuffleSourceIterator(ShuffleEntryReader reader) {
      stopPosition = ByteArrayShufflePosition.fromBase64(stopShufflePosition);
      this.groups =
          new GroupingShuffleEntryIterator(reader.read(
              ByteArrayShufflePosition.fromBase64(startShufflePosition),
              stopPosition)) {
          @Override
          protected void notifyElementRead(long byteSize) {
            GroupingShuffleSource.this.notifyElementRead(byteSize);
          }
        };
    }

    private void advanceIfNecessary() {
      if (nextGroup == null && groups.hasNext()) {
        nextGroup = groups.next();
      }
    }

    @Override
    public boolean hasNext() throws IOException {
      return hasNextInternal();
    }

    /**
     * Returns false if the next group does not exist (i.e., no more
     * records available) or the group is beyond @stopPosition.
     */
    private boolean hasNextInternal() {
      advanceIfNecessary();
      if (nextGroup == null) {
        return false;
      }
      ByteArrayShufflePosition current =
          ByteArrayShufflePosition.of(nextGroup.position);
      return stopPosition == null || current.compareTo(stopPosition) < 0;
    }

    @Override
    public WindowedValue<KV<K, Reiterable<V>>> next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      KeyGroupedShuffleEntries group = nextGroup;
      nextGroup = null;

      K key = CoderUtils.decodeFromByteArray(keyCoder, group.key);
      if (executionContext != null) {
        executionContext.setKey(key);
      }

      return WindowedValue.valueInEmptyWindows(
          KV.<K, Reiterable<V>>of(key, new ValuesIterable(group.values)));
    }

    /**
     * Returns the position before the next {@code KV<K, Reiterable<V>>} to be returned by the
     * {@link GroupingShuffleSourceIterator}. Returns null if the
     * {@link GroupingShuffleSourceIterator} is finished.
     */
    @Override
    public Progress getProgress() {
      com.google.api.services.dataflow.model.Position currentPosition =
          new com.google.api.services.dataflow.model.Position();
      ApproximateProgress progress = new ApproximateProgress();
      if (hasNextInternal()) {
        ByteArrayShufflePosition current =
            ByteArrayShufflePosition.of(nextGroup.position);
        currentPosition.setShufflePosition(current.encodeBase64());
      } else {
        if (stopPosition != null) {
          currentPosition.setShufflePosition(stopPosition.encodeBase64());
        } else {
          // The original stop position described the end of the
          // shuffle-position-space (or infinity) and all records have
          // been consumed.
          progress.setPercentComplete((float) 1.0);
          progress.setRemainingTime(toCloudDuration(Duration.ZERO));
          return cloudProgressToSourceProgress(progress);
        }
      }

      progress.setPosition(currentPosition);
      return cloudProgressToSourceProgress(progress);
    }

    /**
     * Updates the stop position of the shuffle source to the position proposed. Ignores the
     * proposed stop position if it is smaller than or equal to the position before the next
     * {@code KV<K, Reiterable<V>>} to be returned by the {@link GroupingShuffleSourceIterator}.
     */
    @Override
    public Position updateStopPosition(Progress proposedStopPosition) {
      checkNotNull(proposedStopPosition);
      com.google.api.services.dataflow.model.Position stopCloudPosition =
          sourceProgressToCloudProgress(proposedStopPosition).getPosition();
      if (stopCloudPosition == null) {
        LOG.warn(
            "A stop position other than a Position is not supported now.");
        return null;
      }

      if (stopCloudPosition.getShufflePosition() == null) {
        LOG.warn(
            "A stop position other than shuffle position is not supported in "
            + "a grouping shuffle source: " + stopCloudPosition.toString());
        return null;
      }
      ByteArrayShufflePosition newStopPosition =
          ByteArrayShufflePosition.fromBase64(stopCloudPosition.getShufflePosition());

      if (!hasNextInternal()) {
        LOG.warn("Cannot update stop position to "
            + stopCloudPosition.getShufflePosition()
            + " since all input was consumed.");
        return null;
      }
      ByteArrayShufflePosition current =
          ByteArrayShufflePosition.of(nextGroup.position);
      if (newStopPosition.compareTo(current) <= 0) {
        LOG.warn("Proposed stop position: "
            + stopCloudPosition.getShufflePosition() + " <= current position: "
            + current.encodeBase64());
        return null;
      }

      if (this.stopPosition != null
          && newStopPosition.compareTo(this.stopPosition) >= 0) {
        LOG.warn("Proposed stop position: "
            + stopCloudPosition.getShufflePosition()
            + " >= current stop position: "
            + this.stopPosition.encodeBase64());
        return null;
      }

      this.stopPosition = newStopPosition;
      LOG.info("Updated the stop position to "
          + stopCloudPosition.getShufflePosition());

      return cloudPositionToSourcePosition(stopCloudPosition);
    }

    /**
     * Provides the {@link Reiterable} used to iterate through the values part
     * of a {@code KV<K, Reiterable<V>>} entry produced by a
     * {@link GroupingShuffleSource}.
     */
    private final class ValuesIterable implements Reiterable<V> {
      // N.B. This class is *not* static; it uses the valueCoder from
      // its enclosing GroupingShuffleSource.

      private final Reiterable<ShuffleEntry> base;

      public ValuesIterable(Reiterable<ShuffleEntry> base) {
        this.base = checkNotNull(base);
      }

      @Override
      public ValuesIterator iterator() {
        return new ValuesIterator(base.iterator());
      }
    }

    /**
     * Provides the {@link Reiterator} used to iterate through the values part
     * of a {@code KV<K, Reiterable<V>>} entry produced by a
     * {@link GroupingShuffleSource}.
     */
    private final class ValuesIterator implements Reiterator<V> {
      // N.B. This class is *not* static; it uses the valueCoder from
      // its enclosing GroupingShuffleSource.

      private final Reiterator<ShuffleEntry> base;

      public ValuesIterator(Reiterator<ShuffleEntry> base) {
        this.base = checkNotNull(base);
      }

      @Override
      public boolean hasNext() {
        return base.hasNext();
      }

      @Override
      public V next() {
        ShuffleEntry entry = base.next();
        try {
          return CoderUtils.decodeFromByteArray(valueCoder, entry.getValue());
        } catch (IOException exn) {
          throw new RuntimeException(exn);
        }
      }

      @Override
      public void remove() {
        base.remove();
      }

      @Override
      public ValuesIterator copy() {
        return new ValuesIterator(base.copy());
      }
    }
  }
}
