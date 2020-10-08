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
package org.apache.beam.runners.dataflow.worker;

import static com.google.api.client.util.Preconditions.checkNotNull;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.cloudPositionToReaderPosition;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.cloudProgressToReaderProgress;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.splitRequestToApproximateSplitRequest;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import com.google.api.services.dataflow.model.ApproximateSplitRequest;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.worker.ExperimentContext.Experiment;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ByteArrayShufflePosition;
import org.apache.beam.runners.dataflow.worker.util.common.worker.GroupingShuffleEntryIterator;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntryReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleReadCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleReadCounterFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.common.Reiterable;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A source that reads from a shuffled dataset and yields key-grouped data.
 *
 * @param <K> the type of the keys read from the shuffle
 * @param <V> the type of the values read from the shuffle
 */
public class GroupingShuffleReader<K, V> extends NativeReader<WindowedValue<KV<K, Reiterable<V>>>> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupingShuffleReader.class);

  private final PipelineOptions options;
  final byte[] shuffleReaderConfig;
  final @Nullable String startShufflePosition;
  final @Nullable String stopShufflePosition;
  final BatchModeExecutionContext executionContext;
  private final DataflowOperationContext operationContext;
  private final ShuffleReadCounterFactory shuffleReadCounterFactory;

  // Counts how many bytes were from by a given operation from a given shuffle session.
  @Nullable Counter<Long, Long> perOperationPerDatasetBytesCounter;
  Coder<K> keyCoder;
  Coder<?> valueCoder;
  @Nullable Coder<?> secondaryKeyCoder;

  public GroupingShuffleReader(
      PipelineOptions options,
      byte[] shuffleReaderConfig,
      @Nullable String startShufflePosition,
      @Nullable String stopShufflePosition,
      Coder<WindowedValue<KV<K, Iterable<V>>>> coder,
      BatchModeExecutionContext executionContext,
      DataflowOperationContext operationContext,
      ShuffleReadCounterFactory shuffleReadCounterFactory,
      boolean valuesAreSorted)
      throws Exception {
    this.options = options;
    this.shuffleReaderConfig = shuffleReaderConfig;
    this.startShufflePosition = startShufflePosition;
    this.stopShufflePosition = stopShufflePosition;
    this.executionContext = executionContext;
    this.operationContext = operationContext;
    this.shuffleReadCounterFactory = shuffleReadCounterFactory;
    initCoder(coder, valuesAreSorted);
    // We cannot initialize perOperationPerDatasetBytesCounter here, as it
    // depends on shuffleReaderConfig, which isn't populated yet.
  }

  private synchronized void initCounter(String datasetId) {
    if (perOperationPerDatasetBytesCounter == null) {
      perOperationPerDatasetBytesCounter =
          operationContext
              .counterFactory()
              .longSum(
                  CounterName.named(
                      String.format(
                          "dax-shuffle-%s-wf-%s-read-bytes",
                          datasetId, operationContext.nameContext().systemName())));
    }
  }

  @Override
  public GroupingShuffleReaderIterator<K, V> iterator() throws IOException {
    ApplianceShuffleEntryReader entryReader =
        new ApplianceShuffleEntryReader(
            shuffleReaderConfig, executionContext, operationContext, true);

    initCounter(entryReader.getDatasetId());

    return iterator(entryReader);
  }

  private void initCoder(Coder<WindowedValue<KV<K, Iterable<V>>>> coder, boolean valuesAreSorted)
      throws Exception {
    if (!(coder instanceof WindowedValueCoder)) {
      throw new Exception("unexpected kind of coder for WindowedValue: " + coder);
    }
    Coder<KV<K, Iterable<V>>> elemCoder =
        ((WindowedValueCoder<KV<K, Iterable<V>>>) coder).getValueCoder();
    if (!(elemCoder instanceof KvCoder)) {
      throw new Exception(
          "unexpected kind of coder for elements read from "
              + "a key-grouping shuffle: "
              + elemCoder);
    }

    @SuppressWarnings("unchecked")
    KvCoder<K, Iterable<V>> kvCoder = (KvCoder<K, Iterable<V>>) elemCoder;
    this.keyCoder = kvCoder.getKeyCoder();
    Coder<Iterable<V>> kvValueCoder = kvCoder.getValueCoder();
    if (!(kvValueCoder instanceof IterableCoder)) {
      throw new Exception(
          "unexpected kind of coder for values of KVs read from " + "a key-grouping shuffle");
    }
    IterableCoder<V> iterCoder = (IterableCoder<V>) kvValueCoder;
    if (valuesAreSorted) {
      checkState(
          iterCoder.getElemCoder() instanceof KvCoder,
          "unexpected kind of coder for elements read from a "
              + "key-grouping value sorting shuffle: %s",
          iterCoder.getElemCoder());
      @SuppressWarnings("rawtypes")
      KvCoder<?, ?> valueKvCoder = (KvCoder) iterCoder.getElemCoder();
      this.secondaryKeyCoder = valueKvCoder.getKeyCoder();
      this.valueCoder = valueKvCoder.getValueCoder();
    } else {
      this.valueCoder = iterCoder.getElemCoder();
    }
  }

  /**
   * Creates an iterator on top of the given entry reader.
   *
   * <p>Takes "ownership" of the reader: closes the reader once the iterator is closed.
   */
  final GroupingShuffleReaderIterator<K, V> iterator(ShuffleEntryReader reader) {
    return new GroupingShuffleReaderIterator<K, V>(this, reader);
  }

  /**
   * A ReaderIterator that reads from a ShuffleEntryReader and groups all the values with the same
   * key.
   *
   * <p>A key limitation of this implementation is that all iterator accesses must by externally
   * synchronized (the iterator objects are not individually thread-safe, and the iterators derived
   * from a single original iterator access shared state that is not thread-safe).
   *
   * <p>To access the current position, the iterator must advance on-demand and cache the next batch
   * of key grouped shuffle entries. The iterator does not advance a second time in @next() to avoid
   * asking the underlying iterator to advance to the next key before the caller/user iterates over
   * the values corresponding to the current key, which would introduce a performance penalty.
   */
  @VisibleForTesting
  static final class GroupingShuffleReaderIterator<K, V>
      extends NativeReaderIterator<WindowedValue<KV<K, Reiterable<V>>>> {
    // The enclosing GroupingShuffleReader.
    private final GroupingShuffleReader<K, V> parentReader;
    private final ShuffleEntryReader entryReader;

    /** The iterator over shuffle entries, grouped by common key. */
    private final GroupingShuffleEntryIterator groups;

    private final AtomicLong currentGroupSize = new AtomicLong(0L);

    private final ShuffleReadCounter shuffleReadCounter;

    private ExecutionStateTracker tracker;
    private ExecutionState readState;
    private WindowedValue<KV<K, Reiterable<V>>> current;

    /**
     * If the task is cancelled for any reason, signal that the iterator and any underlying
     * ValuesIterable should abort. This is typically signalled via Thread.interrupted(), but user
     * code (and other libraries, including IO ones) may improperly handle that signal. We use this
     * as a fail-safe to ensure that no further records are processed, especially when there may be
     * hot keys with many values.
     */
    private final AtomicBoolean aborted = new AtomicBoolean(false);

    public GroupingShuffleReaderIterator(
        final GroupingShuffleReader<K, V> parentReader, ShuffleEntryReader entryReader) {
      this.parentReader = parentReader;
      this.entryReader = entryReader;
      this.readState =
          parentReader.operationContext.newExecutionState(ExecutionStateTracker.PROCESS_STATE_NAME);
      this.tracker = parentReader.executionContext.getExecutionStateTracker();
      String baseShuffleOriginalStepName =
          parentReader.operationContext.nameContext().originalName();

      // Note: Some usage of the GroupingShuffleReader may not set the
      // shuffleReadCounterFactory.
      ExperimentContext ec = ExperimentContext.parseFrom(parentReader.options);
      if (parentReader.shuffleReadCounterFactory == null) {
        this.shuffleReadCounter = null;
      } else {
        this.shuffleReadCounter =
            parentReader.shuffleReadCounterFactory.create(
                baseShuffleOriginalStepName,
                ec.isEnabled(Experiment.IntertransformIO),
                parentReader.perOperationPerDatasetBytesCounter);
      }

      try (Closeable read = tracker.enterState(readState)) {
        this.groups =
            new GroupingShuffleEntryIterator(
                entryReader,
                parentReader.startShufflePosition == null
                    ? null
                    : ByteArrayShufflePosition.fromBase64(parentReader.startShufflePosition),
                parentReader.stopShufflePosition == null
                    ? null
                    : ByteArrayShufflePosition.fromBase64(parentReader.stopShufflePosition)) {
              @Override
              protected void notifyElementRead(long byteSize) {
                // We accumulate the sum of bytes read in a local variable. This sum will be counted
                // when the values are actually read by the consumer of the shuffle reader.
                currentGroupSize.addAndGet(byteSize);
                parentReader.notifyElementRead(byteSize);
              }

              @Override
              protected void commitBytesRead(long bytes) {
                if (shuffleReadCounter != null) {
                  shuffleReadCounter.addBytesRead(bytes);
                }
              }
            };
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      try (Closeable read = tracker.enterState(readState)) {
        if (!groups.advance()) {
          current = null;
          return false;
        }
      }

      K key = CoderUtils.decodeFromByteArray(parentReader.keyCoder, groups.getCurrent().key);
      parentReader.executionContext.setKey(key);
      current =
          new ValueInEmptyWindows<>(
              KV.<K, Reiterable<V>>of(
                  key, new ValuesIterable(groups.getCurrent().values, aborted)));
      return true;
    }

    @Override
    public WindowedValue<KV<K, Reiterable<V>>> getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    /**
     * Returns the position before the next {@code KV<K, Reiterable<V>>} to be returned by the
     * {@link GroupingShuffleReaderIterator}. Returns null if the {@link
     * GroupingShuffleReaderIterator} is finished.
     */
    @Override
    public Progress getProgress() {
      com.google.api.services.dataflow.model.Position position =
          new com.google.api.services.dataflow.model.Position();
      ApproximateReportedProgress progress = new ApproximateReportedProgress();
      ByteArrayShufflePosition groupStart =
          (ByteArrayShufflePosition) groups.getRangeTracker().getLastGroupStart();
      if (groupStart != null) {
        position.setShufflePosition(groupStart.encodeBase64());
        progress.setPosition(position);
      }
      com.google.api.services.dataflow.model.ReportedParallelism consumedParallelism =
          new com.google.api.services.dataflow.model.ReportedParallelism();
      consumedParallelism.setValue((double) groups.getRangeTracker().getSplitPointsProcessed());
      progress.setConsumedParallelism(consumedParallelism);
      return cloudProgressToReaderProgress(progress);
    }

    /**
     * Updates the stop position of the shuffle source to the position proposed. Ignores the
     * proposed stop position if it is smaller than or equal to the position before the next {@code
     * KV<K, Reiterable<V>>} to be returned by the {@link GroupingShuffleReaderIterator}.
     */
    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      checkNotNull(splitRequest);
      ApproximateSplitRequest splitProgress = splitRequestToApproximateSplitRequest(splitRequest);
      com.google.api.services.dataflow.model.Position splitPosition = splitProgress.getPosition();
      if (splitPosition == null) {
        LOG.warn(
            "GroupingShuffleReader only supports split at a Position. Requested: {}", splitRequest);
        return null;
      }
      String splitShufflePosition = splitPosition.getShufflePosition();
      if (splitShufflePosition == null) {
        LOG.warn(
            "GroupingShuffleReader only supports split at a shuffle position. Requested: {}",
            splitPosition);
        return null;
      }
      ByteArrayShufflePosition newStopPosition =
          ByteArrayShufflePosition.fromBase64(splitShufflePosition);
      if (groups.trySplitAtPosition(newStopPosition)) {
        return new DynamicSplitResultWithPosition(cloudPositionToReaderPosition(splitPosition));
      } else {
        return null;
      }
    }

    @Override
    public void close() throws IOException {
      entryReader.close();
    }

    @Override
    public void asyncAbort() {
      aborted.set(true);
    }

    /**
     * Provides the {@link Reiterable} used to iterate through the values part of a {@code KV<K,
     * Reiterable<V>>} entry produced by a {@link GroupingShuffleReader}.
     */
    private final class ValuesIterable extends ElementByteSizeObservableIterable<V, ValuesIterator>
        implements Reiterable<V> {
      // N.B. This class is *not* static; it uses the valueCoder from
      // its enclosing GroupingShuffleReader.

      private final Reiterable<ShuffleEntry> base;
      private final AtomicBoolean aborted;

      public ValuesIterable(Reiterable<ShuffleEntry> base, AtomicBoolean aborted) {
        this.base = checkNotNull(base);
        this.aborted = aborted;
      }

      @Override
      public ValuesIterator iterator() {
        return new ValuesIterator(base.iterator(), aborted);
      }

      @Override
      protected ValuesIterator createIterator() {
        return iterator();
      }
    }

    /**
     * Provides the {@link Reiterator} used to iterate through the values part of a {@code KV<K,
     * Reiterable<V>>} entry produced by a {@link GroupingShuffleReader}.
     */
    private final class ValuesIterator extends ElementByteSizeObservableIterator<V>
        implements Reiterator<V> {
      // N.B. This class is *not* static; it uses the valueCoder from
      // its enclosing GroupingShuffleReader.

      private final Reiterator<ShuffleEntry> base;

      private final AtomicBoolean aborted;

      private boolean atFirstValue = true;

      public ValuesIterator(Reiterator<ShuffleEntry> base, AtomicBoolean aborted) {
        this.base = checkNotNull(base);
        this.aborted = aborted;
      }

      private ValuesIterator(
          Reiterator<ShuffleEntry> base, AtomicBoolean aborted, boolean atFirstValue) {
        this.base = checkNotNull(base);
        this.aborted = aborted;
        this.atFirstValue = atFirstValue;
      }

      @Override
      public boolean hasNext() {
        try (Closeable read = tracker.enterState(readState)) {
          return base.hasNext();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public V next() {
        // Given that the underlying ReadOperation already checks the abort status after every
        // record it advances over (i.e., for every distinct key), we skip the check when at
        // the first value as that is redundant. Signal by thread interruption may be better, but
        // it may also have unintended side-effects.
        if (!atFirstValue && aborted.get()) {
          throw new RuntimeException(new InterruptedException("Worker was asked to abort"));
        }
        atFirstValue = false;
        try (Closeable read = tracker.enterState(readState)) {
          ShuffleEntry entry = base.next();
          checkNotNull(entry);

          // The shuffle entries are handed over to the consumer of this iterator. Therefore, we can
          // notify the bytes that have been read so far.
          notifyValueReturned(currentGroupSize.getAndSet(0L));
          try {
            if (parentReader.secondaryKeyCoder != null) {
              ByteArrayInputStream bais = new ByteArrayInputStream(entry.getSecondaryKey());
              @SuppressWarnings("unchecked")
              V value =
                  (V)
                      KV.of(
                          // We ignore decoding the timestamp.
                          parentReader.secondaryKeyCoder.decode(bais),
                          CoderUtils.decodeFromByteArray(
                              parentReader.valueCoder, entry.getValue()));
              return value;
            } else {
              @SuppressWarnings("unchecked")
              V value =
                  (V) CoderUtils.decodeFromByteArray(parentReader.valueCoder, entry.getValue());
              return value;
            }
          } catch (IOException exn) {
            throw new RuntimeException(exn);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void remove() {
        base.remove();
      }

      @Override
      public ValuesIterator copy() {
        return new ValuesIterator(base.copy(), aborted, atFirstValue);
      }
    }
  }
}
