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

import java.io.Closeable;
import java.io.IOException;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A read operation.
 *
 * <p>Its start() method iterates through all elements of the source and emits them on its output.
 */
public class ReadOperation extends Operation {
  private static final Logger LOG = LoggerFactory.getLogger(ReadOperation.class);

  // This is the rate at which the local, threadsafe progress variable is updated from the iterator,
  // not the rate of reporting.
  public static final long DEFAULT_PROGRESS_UPDATE_PERIOD_MS = 100;

  /** The Reader this operation reads from. */
  public final NativeReader<?> reader;

  /** The total byte counter for all data read by this operation. */
  final Counter<Long, ?> byteCount;

  /**
   * The Reader's iterator this operation reads from, created by start().
   *
   * <p>Guarded by {@link Operation#initializationStateLock}.
   */
  volatile SynchronizedReaderIterator readerIterator = null;

  /**
   * A cache of {@link #readerIterator}'s progress updated inside the read loop at a bounded rate.
   *
   * <p>Necessary so that ReadOperation.getProgress() can return immediately, rather than
   * potentially wait for a read to complete (which can take an unbounded time, delay a worker
   * progress update, and cause lease expiration and all sorts of trouble).
   */
  private AtomicReference<NativeReader.Progress> progress = new AtomicReference<>();

  /**
   * If the task is cancelled for any reason, signal that the read loop should abort. This is
   * typically signalled via Thread.interrupted(), but user code may improperly swallow that signal.
   * We use this as a fail-safe to ensure that no further records are processed.
   */
  private final AtomicBoolean abortRead = new AtomicBoolean(false);

  /**
   * On every iteration of the read loop, "progress" is fetched from {@link #readerIterator} if
   * requested.
   */
  private long progressUpdatePeriodMs = DEFAULT_PROGRESS_UPDATE_PERIOD_MS;

  private final ScheduledExecutorService scheduler;

  protected ReadOperation(
      NativeReader<?> reader,
      OutputReceiver[] receivers,
      OperationContext context,
      CounterName bytesCounterName) {
    this(
        reader, receivers, Executors.newSingleThreadScheduledExecutor(), context, bytesCounterName);
  }

  protected ReadOperation(
      NativeReader<?> reader,
      OutputReceiver[] receivers,
      ScheduledExecutorService scheduler,
      OperationContext context,
      CounterName bytesCounterName) {
    super(receivers, context);
    this.reader = reader;
    this.byteCount = context.counterFactory().longSum(bytesCounterName);
    reader.addObserver(new ReaderObserver());
    this.scheduler = scheduler;
  }

  public static ReadOperation create(
      NativeReader<?> reader, OutputReceiver[] receivers, OperationContext context) {
    return new ReadOperation(reader, receivers, context, bytesCounterName(context));
  }

  @VisibleForTesting
  public static ReadOperation forTest(
      NativeReader<?> reader, OutputReceiver outputReceiver, OperationContext context) {
    return create(reader, new OutputReceiver[] {outputReceiver}, context);
  }

  static ReadOperation forTest(
      NativeReader<?> reader,
      OutputReceiver outputReceiver,
      ScheduledExecutorService scheduler,
      OperationContext context) {
    return new ReadOperation(
        reader,
        new OutputReceiver[] {outputReceiver},
        scheduler,
        context,
        bytesCounterName(context));
  }

  public static final long DONT_UPDATE_PERIODICALLY = -1;
  public static final long UPDATE_ON_EACH_ITERATION = 0;

  /**
   * Controls the frequency at which progress is updated. The given value must be positive or one of
   * the special values of DONT_UPDATE_PERIODICALLY or UPDATE_ON_EACH_ITERATION.
   */
  public void setProgressUpdatePeriodMs(long millis) {
    assert millis > 0 || millis == DONT_UPDATE_PERIODICALLY || millis == UPDATE_ON_EACH_ITERATION;
    progressUpdatePeriodMs = millis;
  }

  protected static CounterName bytesCounterName(OperationContext context) {
    return CounterName.named(String.format("%s-ByteCount", context.nameContext().systemName()));
  }

  public NativeReader<?> getReader() {
    return reader;
  }

  @Override
  public void start() throws Exception {
    try (Closeable scope = context.enterStart()) {
      super.start();
      runReadLoop();
    }
  }

  @Override
  public boolean supportsRestart() {
    return reader.supportsRestart();
  }

  protected void runReadLoop() throws Exception {
    try (Closeable scope = context.enterProcess()) {
      Receiver receiver = receivers[0];
      if (receiver == null) {
        // No consumer of this data; don't do anything.
        return;
      }

      // Call reader.iterator() outside the lock, because it can take an
      // unbounded amount of time.
      NativeReader.NativeReaderIterator<?> iterator = reader.iterator();
      synchronized (initializationStateLock) {
        readerIterator = new SynchronizedReaderIterator<>(iterator, progress);
      }

      Runnable setProgressFromIterator =
          new Runnable() {
            @Override
            public void run() {
              readerIterator.setProgressFromIterator();
            }
          };
      try (AutoCloseable updater =
          schedulePeriodicActivity(scheduler, setProgressFromIterator, progressUpdatePeriodMs)) {
        // Force a progress update at the beginning.
        readerIterator.setProgressFromIterator();
        for (boolean more = readerIterator.start(); more; more = readerIterator.advance()) {
          if (abortRead.get()) {
            throw new InterruptedException("Read loop was aborted.");
          }
          if (progressUpdatePeriodMs == UPDATE_ON_EACH_ITERATION) {
            readerIterator.setProgressFromIterator();
          }
          receiver.process(readerIterator.getCurrent());
        }
        // Force a progress update at the end.
        readerIterator.setProgressFromIterator();
      } finally {
        scheduler.shutdown();
        scheduler.awaitTermination(1, TimeUnit.MINUTES);
        if (!scheduler.isTerminated()) {
          LOG.error(
              "Failed to terminate periodic progress reporting in 1 minute. "
                  + "Waiting for it to terminate indefinitely...");
          scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
          LOG.info("Periodic progress reporting terminated.");
        }
      }
    }
  }

  private static AutoCloseable schedulePeriodicActivity(
      ScheduledExecutorService scheduler, Runnable runnable, long periodMs) {
    if (periodMs <= 0) {
      return null;
    }
    final ScheduledFuture<?> future =
        scheduler.scheduleAtFixedRate(runnable, periodMs, periodMs, TimeUnit.MILLISECONDS);
    return new AutoCloseable() {
      @Override
      public void close() throws Exception {
        future.cancel(true /* mayInterruptIfRunning */);
        try {
          future.get();
        } catch (CancellationException ignored) {
          // Expected;
        }
      }
    };
  }

  @Override
  public void finish() throws Exception {
    try (Closeable scope = context.enterFinish()) {
      // Mark operation finished before closing the reader, so that anybody who checks if
      // it's finished (e.g. requestDynamicSplit) won't use a closed reader.
      super.finish();
      readerIterator.close();
    }
  }

  @Override
  public void abort() throws Exception {
    if (readerIterator != null) {
      try (Closeable scope = context.enterAbort()) {
        // Mark operation finished before aborting the reader, so that anybody who checks if
        // it's finished (e.g. requestDynamicSplit) won't use an aborted reader.
        super.abort();
        readerIterator.abort();
      }
    }
  }

  /**
   * Marks the read loop as aborted. start() will throw an {@code InterruptedException} as soon as
   * the current record finishes processing. This method is thread-safe (unlike {@code abort()}).
   */
  public void abortReadLoop() {
    abortRead.set(true);
  }

  /**
   * Returns a (possibly slightly stale) value of the progress of the task. Guaranteed to not block
   * indefinitely. Needs to be thread-safe for sources which support dynamic work rebalancing.
   *
   * @return the task progress, or {@code null} if the source iterator has not been initialized
   */
  public NativeReader.Progress getProgress() {
    return progress.get();
  }

  /**
   * Relays the checkpoint request to {@code ReaderIterator}. This method is called concurrently to
   * the readLoop.
   */
  public NativeReader.@Nullable DynamicSplitResult requestCheckpoint() {
    synchronized (initializationStateLock) {
      if (isFinished() || isAborted()) {
        LOG.info(
            "Iterator is in the {} state; returning null stop position.",
            isFinished() ? "Finished" : "Aborted");
        return null;
      }
      if (readerIterator == null) {
        LOG.info("Iterator has not been initialized, refusing to checkpoint");
        return null;
      }
      return readerIterator.requestCheckpoint();
    }
  }

  /**
   * Relays the split request to {@code ReaderIterator}. This method is called concurrently to the
   * readLoop.
   */
  public NativeReader.@Nullable DynamicSplitResult requestDynamicSplit(
      NativeReader.DynamicSplitRequest splitRequest) {
    synchronized (initializationStateLock) {
      if (isFinished() || isAborted()) {
        LOG.info(
            "Iterator is in the {} state; returning null stop position.",
            isFinished() ? "Finished" : "Aborted");
        return null;
      }
      if (readerIterator == null) {
        LOG.info("Iterator has not been initialized, refusing to split at {}", splitRequest);
        return null;
      }
      return readerIterator.requestDynamicSplit(splitRequest);
    }
  }

  /**
   * This is an observer on the instance of the source. Whenever source reads an element, update()
   * gets called with the byte size of the element, which gets added up into the ReadOperation's
   * byte counter.
   */
  private class ReaderObserver implements Observer {
    @Override
    public void update(Observable obs, Object obj) {
      Preconditions.checkArgument(obs == reader, "unexpected observable");
      Preconditions.checkArgument(obj instanceof Long, "unexpected parameter object");
      byteCount.addValue((Long) obj);
    }
  }

  /**
   * A thread safe wrapper over a {@link NativeReader.NativeReaderIterator}. Sources do not have to
   * be thread safe unless they support liquid sharding, in which case they should support
   * concurrent calls to {@code requestDynamicSplit} and {@code getProgress}. All other methods are
   * therefore synchronised. Method {@code requestDynamicSplit} can be called concurrently to
   * support liquid sharding (it internally will call {@code getProgress}).
   */
  private static class SynchronizedReaderIterator<T> extends NativeReader.NativeReaderIterator<T> {

    /** The Reader's iterator this operation reads from, created by start(). */
    private final NativeReader.NativeReaderIterator<T> readerIterator;

    /** Pointers to ReadOperation fields. */
    private final AtomicReference<NativeReader.Progress> progress;

    public SynchronizedReaderIterator(
        NativeReader.NativeReaderIterator<T> readerIterator,
        AtomicReference<NativeReader.Progress> progress) {
      this.readerIterator = readerIterator;
      this.progress = progress;
    }

    /** Synchronized methods that are called from the main reading loop and the update thread. */
    @Override
    public synchronized boolean start() throws IOException {
      return readerIterator.start();
    }

    @Override
    public synchronized void close() throws IOException {
      readerIterator.close();
    }

    @Override
    public synchronized void abort() throws IOException {
      readerIterator.abort();
    }

    public synchronized void setProgressFromIterator() {
      setProgressFromIteratorConcurrent();
    }

    /**
     * Non-synchronized method that is called from requestDynamicSplit and has to be non-blocking.
     */
    private void setProgressFromIteratorConcurrent() {
      try {
        progress.set(readerIterator.getProgress());
      } catch (UnsupportedOperationException ignored) {
        // Ignore: same semantics as null.
      } catch (Exception e) {
        // This is not a normal situation, but should not kill the task.
        LOG.warn("Progress estimation failed", e);
      }
    }

    @Override
    public synchronized T getCurrent() {
      return readerIterator.getCurrent();
    }

    @Override
    public synchronized boolean advance() throws IOException {
      return readerIterator.advance();
    }

    /** Methods called from requestCheckpoint. These can be executed concurrently to others. */
    @Override
    public NativeReader.DynamicSplitResult requestCheckpoint() {
      NativeReader.DynamicSplitResult result = readerIterator.requestCheckpoint();
      if (result != null) {
        // After a successful split, the stop position changed and progress has to be recomputed.
        setProgressFromIteratorConcurrent();
      }
      return result;
    }

    /** Methods called from requestDynamicSplit. These can be executed concurrently to others. */
    @Override
    public NativeReader.DynamicSplitResult requestDynamicSplit(
        NativeReader.DynamicSplitRequest splitRequest) {
      NativeReader.DynamicSplitResult result = readerIterator.requestDynamicSplit(splitRequest);
      if (result != null) {
        // After a successful split, the stop position changed and progress has to be recomputed.
        setProgressFromIteratorConcurrent();
      }
      return result;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("reader", reader)
        .add("readerIterator", readerIterator)
        .add("byteCount", byteCount)
        .toString();
  }
}
