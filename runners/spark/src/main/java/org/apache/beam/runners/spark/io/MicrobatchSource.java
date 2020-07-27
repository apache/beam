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
package org.apache.beam.runners.spark.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.RemovalListener;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Source} that accommodates Spark's micro-batch oriented nature and wraps an {@link
 * UnboundedSource}.
 */
public class MicrobatchSource<T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
    extends Source<T> {

  private static final Logger LOG = LoggerFactory.getLogger(MicrobatchSource.class);
  private static volatile Cache<MicrobatchSource<?, ?>, Source.Reader<?>> readerCache;

  private final UnboundedSource<T, CheckpointMarkT> source;
  private final Duration maxReadTime;
  private final int numInitialSplits;
  private final long maxNumRecords;
  private final int sourceId;
  private final double readerCacheInterval;

  // each split of the underlying UnboundedSource is associated with a (consistent) id
  // to match it's corresponding CheckpointMark state.
  private final int splitId;

  MicrobatchSource(
      final UnboundedSource<T, CheckpointMarkT> source,
      final Duration maxReadTime,
      final int numInitialSplits,
      final long maxNumRecords,
      final int splitId,
      final int sourceId,
      final double readerCacheInterval) {
    this.source = source;
    this.maxReadTime = maxReadTime;
    this.numInitialSplits = numInitialSplits;
    this.maxNumRecords = maxNumRecords;
    this.splitId = splitId;
    this.sourceId = sourceId;
    this.readerCacheInterval = readerCacheInterval;
  }

  private static synchronized void initReaderCache(final long readerCacheInterval) {
    if (readerCache == null) {
      LOG.info("Creating reader cache. Cache interval = {} ms.", readerCacheInterval);
      readerCache =
          CacheBuilder.newBuilder()
              .expireAfterAccess(readerCacheInterval, TimeUnit.MILLISECONDS)
              .removalListener(new ReaderCacheRemovalListener())
              .build();
    }
  }

  /**
   * Divide the given number of records into {@code numSplits} approximately equal parts that sum to
   * {@code numRecords}.
   */
  private static long[] splitNumRecords(final long numRecords, final int numSplits) {
    final long[] splitNumRecords = new long[numSplits];
    for (int i = 0; i < numSplits; i++) {
      splitNumRecords[i] = numRecords / numSplits;
    }
    for (int i = 0; i < numRecords % numSplits; i++) {
      splitNumRecords[i] = splitNumRecords[i] + 1;
    }
    return splitNumRecords;
  }

  List<? extends Source<T>> split(final PipelineOptions options) throws Exception {
    final List<MicrobatchSource<T, CheckpointMarkT>> result = new ArrayList<>();
    final List<? extends UnboundedSource<T, CheckpointMarkT>> splits =
        source.split(numInitialSplits, options);
    final int numSplits = splits.size();
    final long[] numRecords = splitNumRecords(maxNumRecords, numSplits);
    for (int i = 0; i < numSplits; i++) {
      // splits must be stable, and cannot change during consecutive executions
      // for example: Kafka should not add partitions if more then one topic is read.
      result.add(
          new MicrobatchSource<>(
              splits.get(i), maxReadTime, 1, numRecords[i], i, sourceId, readerCacheInterval));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public Source.Reader<T> getOrCreateReader(
      final PipelineOptions options, final CheckpointMarkT checkpointMark) throws IOException {
    try {
      initReaderCache((long) readerCacheInterval);
      return (Source.Reader<T>) readerCache.get(this, new ReaderLoader(options, checkpointMark));
    } catch (final ExecutionException e) {
      throw new RuntimeException("Failed to get or create reader", e);
    }
  }

  @Override
  public void validate() {
    source.validate();
  }

  @Override
  public Coder<T> getOutputCoder() {
    return source.getOutputCoder();
  }

  public Coder<CheckpointMarkT> getCheckpointMarkCoder() {
    return source.getCheckpointMarkCoder();
  }

  public String getId() {
    return sourceId + "_" + splitId;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MicrobatchSource)) {
      return false;
    }
    MicrobatchSource<?, ?> that = (MicrobatchSource<?, ?>) o;
    if (sourceId != that.sourceId) {
      return false;
    }
    return splitId == that.splitId;
  }

  @Override
  public int hashCode() {
    int result = sourceId;
    result = 31 * result + splitId;
    return result;
  }

  /**
   * Mostly based on {@link org.apache.beam.sdk.io.BoundedReadFromUnboundedSource}'s <code>
   * UnboundedToBoundedSourceAdapter</code>, with some adjustments for Spark specifics.
   *
   * <p>This Reader reads until one of the following thresholds has been reached:
   *
   * <ol>
   *   <li>max records (per batch)
   *   <li>max read duration (per batch)
   * </ol>
   */
  public class Reader extends Source.Reader<T> {
    private long recordsRead = 0L;
    private Instant readEndTime;
    private final FluentBackoff backoffFactory;
    private final UnboundedSource.UnboundedReader<T> unboundedReader;
    private boolean started;

    private Reader(final UnboundedSource.UnboundedReader<T> unboundedReader) {
      this.unboundedReader = unboundedReader;
      backoffFactory =
          FluentBackoff.DEFAULT
              .withInitialBackoff(Duration.millis(10))
              .withMaxBackoff(maxReadTime.minus(1))
              .withMaxCumulativeBackoff(maxReadTime.minus(1));
    }

    private boolean startIfNeeded() throws IOException {
      return !started && ((started = true) && unboundedReader.start());
    }

    private void prepareForNewBatchReading() {
      readEndTime = Instant.now().plus(maxReadTime);
      recordsRead = 0L;
    }

    @Override
    public boolean start() throws IOException {
      LOG.debug(
          "MicrobatchReader-{}: Starting a microbatch read from an unbounded source with a max "
              + "read time of {} millis, and max number of records {}.",
          splitId,
          maxReadTime,
          maxNumRecords);

      prepareForNewBatchReading();

      // either start a new read, or continue an existing one
      return startIfNeeded() || advanceWithBackoff();
    }

    @Override
    public boolean advance() throws IOException {
      if (recordsRead >= maxNumRecords) {
        finalizeCheckpoint();
        return false;
      } else {
        return advanceWithBackoff();
      }
    }

    private boolean advanceWithBackoff() throws IOException {
      // Try reading from the source with exponential backoff
      final BackOff backoff = backoffFactory.backoff();
      long nextSleep = backoff.nextBackOffMillis();
      while (nextSleep != BackOff.STOP) {
        if (readEndTime != null && Instant.now().isAfter(readEndTime)) {
          finalizeCheckpoint();
          return false;
        }
        if (unboundedReader.advance()) {
          recordsRead++;
          return true;
        }
        Uninterruptibles.sleepUninterruptibly(nextSleep, TimeUnit.MILLISECONDS);
        nextSleep = backoff.nextBackOffMillis();
      }
      finalizeCheckpoint();
      return false;
    }

    private void finalizeCheckpoint() throws IOException {
      unboundedReader.getCheckpointMark().finalizeCheckpoint();
      LOG.debug(
          "MicrobatchReader-{}: finalized CheckpointMark successfully after "
              + "reading {} records.",
          splitId,
          recordsRead);
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return unboundedReader.getCurrent();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return unboundedReader.getCurrentTimestamp();
    }

    @Override
    public void close() throws IOException {
      unboundedReader.close();
    }

    @Override
    public Source<T> getCurrentSource() {
      return MicrobatchSource.this;
    }

    @SuppressWarnings("unchecked")
    public CheckpointMarkT getCheckpointMark() {
      return (CheckpointMarkT) unboundedReader.getCheckpointMark();
    }

    public Instant getWatermark() {
      return unboundedReader.getWatermark();
    }
  }

  /** {@link Callable} which creates a {@link Reader}. */
  private class ReaderLoader implements Callable<Source.Reader<T>> {
    private final PipelineOptions options;
    private final CheckpointMarkT checkpointMark;

    ReaderLoader(final PipelineOptions options, final CheckpointMarkT checkpointMark) {
      this.options = options;
      this.checkpointMark = checkpointMark;
    }

    @Override
    public Reader call() throws Exception {
      LOG.info(
          "No cached reader found for split: ["
              + source
              + "]. Creating new reader at checkpoint mark "
              + checkpointMark);
      return new Reader(source.createReader(options, checkpointMark));
    }
  }

  /** Listener to be called when a reader is removed from {@link MicrobatchSource#readerCache}. */
  private static class ReaderCacheRemovalListener
      implements RemovalListener<MicrobatchSource<?, ?>, Source.Reader<?>> {

    @Override
    public void onRemoval(
        final RemovalNotification<MicrobatchSource<?, ?>, Source.Reader<?>> notification) {
      try {
        notification.getValue().close();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @VisibleForTesting
  public static void clearCache() {
    synchronized (MicrobatchSource.class) {
      readerCache.invalidateAll();
    }
  }
}
