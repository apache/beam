/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
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

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A read operation.
 *
 * <p>Its start() method iterates through all elements of the source
 * and emits them on its output.
 */
public class ReadOperation extends Operation {
  private static final Logger LOG = LoggerFactory.getLogger(ReadOperation.class);

  // This is the rate at which the local, threadsafe progress variable is updated from the iterator,
  // not the rate of reporting.
  public static final long DEFAULT_PROGRESS_UPDATE_PERIOD_MS = 100;

  /**
   * For the reader parallelism counters, large enough values should be sufficient, and there
   * are issues with arbitrarily large values.
   *
   * Specifically, When reporting parallelism as a part of a sum, we want to cap it at a value that
   * won't impose an artifical constraint on the services view of available parallelism, but small
   * enough that that adding and subtracting this value for every bundle will not overwhelm values
   * as small as 1.0.
   */
  @VisibleForTesting
  public static final double LARGE_PARALLELISM_BOUND = 1e7;

  /** The Reader this operation reads from. */
  public final NativeReader<?> reader;

  /** The total byte counter for all data read by this operation. */
  final Counter<Long> byteCount;

  /** The counter for estimating total parallelism in this task. */
  private final Counter<Double> totalParallelismCounter;

  /** The counter for estimating remaining parallelism in this task. */
  private final Counter<Double> remainingParallelismCounter;

  /**
   * The Reader's iterator this operation reads from, created by start().
   *
   * Guarded by {@link Operation#initializationStateLock}.
   */
  volatile NativeReader.NativeReaderIterator<?> readerIterator = null;

  /**
   * A cache of {@link #readerIterator}'s progress updated inside the read loop
   * at a bounded rate.
   *
   * <p>Necessary so that ReadOperation.getProgress() can return immediately, rather than
   * potentially wait for a read to complete (which can take an unbounded time, delay a worker
   * progress update, and cause lease expiration and all sorts of trouble).
   */
  private AtomicReference<NativeReader.Progress> progress = new AtomicReference<>();

  /**
   * On every iteration of the read loop, "progress" is fetched from
   * {@link #readerIterator} if requested.
   */
  private long progressUpdatePeriodMs = DEFAULT_PROGRESS_UPDATE_PERIOD_MS;

  /**
   * Signals whether the next iteration of the read loop should update the progress.
   *
   * <p>Set to true every progressUpdatePeriodMs.
   */
  private AtomicBoolean isProgressUpdateRequested = new AtomicBoolean(true);


  public ReadOperation(
      String operationName,
      NativeReader<?> reader,
      OutputReceiver[] receivers,
      String counterPrefix,
      String systemStageName,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler) {
    super(operationName, receivers, counterPrefix, addCounterMutator,
          stateSampler, reader.getStateSamplerStateKind());
    this.reader = reader;
    this.byteCount = addCounterMutator.addCounter(
        Counter.longs(bytesCounterName(counterPrefix, operationName), SUM));
    reader.addObserver(new ReaderObserver());
    reader.setStateSamplerAndOperationName(stateSampler, operationName);
    this.totalParallelismCounter = addCounterMutator.addCounter(
        Counter.doubles(totalParallelismCounterName(systemStageName), SUM));
    // Set only when a task is started or split.
    totalParallelismCounter.resetToValue(boundParallelism(reader.getTotalParallelism()));
    this.remainingParallelismCounter = addCounterMutator.addCounter(
        Counter.doubles(remainingParallelismCounterName(systemStageName), SUM));
  }

  static ReadOperation forTest(
      NativeReader<?> reader,
      OutputReceiver outputReceiver,
      String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler) {
    return new ReadOperation("ReadOperation", reader, new OutputReceiver[]{outputReceiver},
        counterPrefix, "systemStageName", addCounterMutator, stateSampler);
  }

  public static final long DONT_UPDATE_PERIODICALLY = -1;
  public static final long UPDATE_ON_EACH_ITERATION = 0;

  /**
   * Controls the frequency at which progress is updated.  The given value must
   * be positive or one of the special values of DONT_UPDATE_PERIODICALLY or
   * UPDATE_ON_EACH_ITERATION.
   */
  public void setProgressUpdatePeriodMs(long millis) {
    assert millis > 0 || millis == DONT_UPDATE_PERIODICALLY || millis == UPDATE_ON_EACH_ITERATION;
    progressUpdatePeriodMs = millis;
  }

  protected String bytesCounterName(String counterPrefix, String operationName) {
    return operationName + "-ByteCount";
  }

  protected String totalParallelismCounterName(String systemStageName) {
    return "dataflow_total_parallelism-" + systemStageName;
  }

  protected String remainingParallelismCounterName(String systemStageName) {
    return "dataflow_remaining_parallelism-" + systemStageName;
  }

  public NativeReader<?> getReader() {
    return reader;
  }

  @Override
  public void start() throws Exception {
    try (StateSampler.ScopedState start = stateSampler.scopedState(startState)) {
      assert start != null;
      super.start();
      runReadLoop();
    }
  }

  @Override
  public boolean supportsRestart() {
    return reader.supportsRestart();
  }

  protected void runReadLoop() throws Exception {
    Receiver receiver = receivers[0];
    if (receiver == null) {
      // No consumer of this data; don't do anything.
      return;
    }

    try (StateSampler.ScopedState process = stateSampler.scopedState(processState)) {
      assert process != null;
      {
        // Call reader.iterator() outside the lock, because it can take an
        // unbounded amount of time.
        NativeReader.NativeReaderIterator<?> iterator = reader.iterator();
        synchronized (initializationStateLock) {
          readerIterator = iterator;
        }
      }

      // TODO: Consider using the ExecutorService from PipelineOptions instead.
      Thread updateRequester = null;
      if (progressUpdatePeriodMs > 0) {
        updateRequester = new Thread() {
          @Override
          public void run() {
            while (true) {
              isProgressUpdateRequested.set(true);
              try {
                Thread.sleep(progressUpdatePeriodMs);
              } catch (InterruptedException e) {
                break;
              }
            }
          }
        };
        updateRequester.start();
      }

      try {
        // Force a progress update at the beginning and at the end.
        setProgressFromIterator();
        for (boolean more = readerIterator.start(); more; more = readerIterator.advance()) {
          if (isProgressUpdateRequested.getAndSet(false) ||
              progressUpdatePeriodMs == UPDATE_ON_EACH_ITERATION) {
            setProgressFromIterator();
          }
          receiver.process(readerIterator.getCurrent());
        }
        setProgressFromIterator();
      } finally {
        if (updateRequester != null) {
          updateRequester.interrupt();
          updateRequester.join();
        }
      }
    }
  }

  @Override
  public void finish() throws Exception {
    // Mark operation finished before closing the reader, so that anybody who checks if
    // it's finished (e.g. requestDynamicSplit) won't use a closed reader.
    super.finish();
    readerIterator.close();
  }

  private void setProgressFromIterator() {
    try {
      progress.set(readerIterator.getProgress());
      remainingParallelismCounter.resetToValue(
          boundParallelism(readerIterator.getRemainingParallelism()));
    } catch (UnsupportedOperationException e) {
      // Ignore: same semantics as null.
    } catch (Exception e) {
      // This is not a normal situation, but should not kill the task.
      LOG.warn("Progress estimation failed", e);
    }
  }

  /**
   * Returns a (possibly slightly stale) value of the progress of the task.
   * Guaranteed to not block indefinitely. Needs to be thread-safe for sources
   * which support dynamic work rebalancing.
   *
   * @return the task progress, or {@code null} if the source iterator has not
   * been initialized
   */
  public NativeReader.Progress getProgress() {
    return progress.get();
  }

  /**
   * Relays the split request to {@code ReaderIterator}.
   */
  public NativeReader.DynamicSplitResult requestDynamicSplit(
      NativeReader.DynamicSplitRequest splitRequest) {
    synchronized (initializationStateLock) {
      if (isFinished()) {
        LOG.warn("Iterator is in the Finished state, returning null stop position.");
        return null;
      }
      if (readerIterator == null) {
        LOG.warn("Iterator has not been initialized, refusing to split at {}",
            splitRequest);
        return null;
      }
      NativeReader.DynamicSplitResult result = readerIterator.requestDynamicSplit(splitRequest);
      if (result != null) {
        // After a successful split, the stop position changed and progress has to be recomputed.
        setProgressFromIterator();
        totalParallelismCounter.resetToValue(boundParallelism(reader.getTotalParallelism()));
      }
      return result;
    }
  }

  /**
   * This is an observer on the instance of the source. Whenever source reads
   * an element, update() gets called with the byte size of the element, which
   * gets added up into the ReadOperation's byte counter.
   *
   * <p>Note that when the reader is a {@link GroupingShuffleReader}, update()
   * is called for each underlying {@link ShuffleEntry} being read, with the
   * byte size of the {@code ShuffleEntry} - it is not called for each grouped
   * shuffle element (i.e. key and iterable of values).
   */
  private class ReaderObserver implements Observer {
    @Override
    public void update(Observable obs, Object obj) {
      Preconditions.checkArgument(obs == reader, "unexpected observable");
      Preconditions.checkArgument(obj instanceof Long, "unexpected parameter object");
      byteCount.addValue((long) obj);
    }
  }

  /**
   * JSON doesn't correctly handle non-finite values, and we want to bound how large each
   * term in the total sum is.  See {@link #LARGE_PARALLELISM_BOUND}.
   *
   * <p>TODO: Remove this hack once we move to gRPC or report this value in a more structured
   * format.
   */
  private static double boundParallelism(double x) {
    if (Double.isNaN(x) || x < 1) {
      if (x < 1) {
        LOG.warn("Invalid parallelism value: " + x);
      }
      // Irrational; sums won't come out to an integral value. This is to better avoid
      // accidental coincidences which would imply the remaining parallelism is zero when it's not.
      // Also, negative so that it's recognized as "invalid."
      return -LARGE_PARALLELISM_BOUND * Math.sqrt(2);
    } else if (x > LARGE_PARALLELISM_BOUND) {
      return LARGE_PARALLELISM_BOUND;
    } else {
      return x;
    }
  }
}
