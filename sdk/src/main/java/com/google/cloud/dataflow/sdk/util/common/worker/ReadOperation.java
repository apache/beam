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
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A read operation.
 * <p>
 * Its start() method iterates through all elements of the source
 * and emits them on its output.
 */
public class ReadOperation extends Operation {
  private static final Logger LOG = LoggerFactory.getLogger(ReadOperation.class);
  private static final long DEFAULT_PROGRESS_UPDATE_PERIOD_MS = TimeUnit.SECONDS.toMillis(1);

  /** The Reader this operation reads from. */
  public final Reader<?> reader;

  /** The total byte counter for all data read by this operation. */
  final Counter<Long> byteCount;

  /**
   * The Reader's iterator this operation reads from, created by start().
   * Guarded by sourceIteratorLock.
   */
  volatile Reader.ReaderIterator<?> readerIterator = null;

  /**
   * A cache of sourceIterator.getProgress() updated inside the read loop at a bounded rate.
   * <p>
   * Necessary so that ReadOperation.getProgress() can return immediately, rather than potentially
   * wait for a read to complete (which can take an unbounded time, delay a worker progress update,
   * and cause lease expiration and all sorts of trouble).
   */
  private AtomicReference<Reader.Progress> progress = new AtomicReference<>();

  /**
   * On every iteration of the read loop, "progress" is fetched from sourceIterator if requested.
   */
  private long progressUpdatePeriodMs = DEFAULT_PROGRESS_UPDATE_PERIOD_MS;

  /**
   * Signals whether the next iteration of the read loop should update the progress.
   * Set to true every progressUpdatePeriodMs.
   */
  private AtomicBoolean isProgressUpdateRequested = new AtomicBoolean(true);


  public ReadOperation(String operationName, Reader<?> reader, OutputReceiver[] receivers,
      String counterPrefix, CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler) {
    super(operationName, receivers, counterPrefix, addCounterMutator, stateSampler);
    this.reader = reader;
    this.byteCount = addCounterMutator.addCounter(
        Counter.longs(bytesCounterName(counterPrefix, operationName), SUM));
    reader.addObserver(new ReaderObserver());
    reader.setStateSamplerAndOperationName(stateSampler, operationName);
  }

  /** Invoked by tests. */
  ReadOperation(Reader<?> reader, OutputReceiver outputReceiver, String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) {
    this("ReadOperation", reader, new OutputReceiver[] {outputReceiver}, counterPrefix,
        addCounterMutator, stateSampler);
  }

  /**
   * Controls the frequency at which progress is updated. A value of zero means
   * "update progress on each iteration". A value of less than zero means never
   * update progress. Ignored after starting.
   */
  public void setProgressUpdatePeriodMs(long millis) {
    progressUpdatePeriodMs = millis;
  }

  protected String bytesCounterName(String counterPrefix, String operationName) {
    return operationName + "-ByteCount";
  }

  public Reader<?> getReader() {
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
      synchronized (initializationStateLock) {
        readerIterator = reader.iterator();
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
        while (true) {
          Object value;
          if (!readerIterator.hasNext()) {
            break;
          }
          value = readerIterator.next();

          if (isProgressUpdateRequested.getAndSet(false) || progressUpdatePeriodMs == 0) {
            setProgressFromIterator();
          }
          receiver.process(value);
        }
        setProgressFromIterator();
      } finally {
        readerIterator.close();
        if (progressUpdatePeriodMs != 0) {
          updateRequester.interrupt();
          updateRequester.join();
        }
      }
    }
  }

  private void setProgressFromIterator() {
    try {
      progress.set(readerIterator.getProgress());
    } catch (UnsupportedOperationException e) {
      // Ignore: same semantics as null.
    } catch (Exception e) {
      // This is not a normal situation, but should not kill the task.
      LOG.warn("Progress estimation failed", e);
    }
  }

  /**
   * Returns a (possibly slightly stale) value of the progress of the task.
   * Guaranteed to not block indefinitely.
   *
   * @return the task progress, or {@code null} if the source iterator has not
   * been initialized
   */
  public Reader.Progress getProgress() {
    return progress.get();
  }

  /**
   * Relays the split request to {@code ReaderIterator}.
   */
  public Reader.DynamicSplitResult requestDynamicSplit(Reader.DynamicSplitRequest splitRequest) {
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
      return readerIterator.requestDynamicSplit(splitRequest);
    }
  }

  /**
   * This is an observer on the instance of the source. Whenever source reads
   * an element, update() gets called with the byte size of the element, which
   * gets added up into the ReadOperation's byte counter.
   */
  private class ReaderObserver implements Observer {
    @Override
    public void update(Observable obs, Object obj) {
      Preconditions.checkArgument(obs == reader, "unexpected observable");
      Preconditions.checkArgument(obj instanceof Long, "unexpected parameter object");
      byteCount.addValue((long) obj);
    }
  }
}
