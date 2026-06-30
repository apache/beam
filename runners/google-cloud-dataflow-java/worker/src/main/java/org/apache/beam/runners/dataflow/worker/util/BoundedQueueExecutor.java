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
package org.apache.beam.runners.dataflow.worker.util;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.dataflow.worker.streaming.BoundedQueueExecutorWorkHandle;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.Work.KeyGroup;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Monitor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Monitor.Guard;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An executor for executing work on windmill items. */
public class BoundedQueueExecutor {

  private final ThreadPoolExecutor executor;
  private final long maximumBytesOutstanding;

  // Used to guard elementsOutstanding and bytesOutstanding.
  private final Monitor monitor;

  private static class Budget {

    final int elements;
    final long bytes;

    Budget(int elements, long bytes) {
      this.elements = elements;
      this.bytes = bytes;
    }
  }

  private final ConcurrentLinkedQueue<Budget> decrementQueue = new ConcurrentLinkedQueue<>();
  private final Object decrementQueueDrainLock = new Object();
  private final AtomicBoolean isDecrementBatchPending = new AtomicBoolean(false);
  private int elementsOutstanding = 0;
  private long bytesOutstanding = 0;

  @GuardedBy("this")
  private int maximumElementsOutstanding;

  @GuardedBy("this")
  private int activeCount;

  @GuardedBy("this")
  private int maximumPoolSize;

  @GuardedBy("this")
  private long startTimeMaxActiveThreadsUsed;

  @GuardedBy("this")
  private long totalTimeMaxActiveThreadsUsed;

  // If set the keyGroupWorkQueue is used by the underlying executor.
  private final @Nullable KeyGroupWorkQueue keyGroupWorkQueue;

  public BoundedQueueExecutor(
      int initialMaximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      int maximumElementsOutstanding,
      long maximumBytesOutstanding,
      ThreadFactory threadFactory,
      boolean useFairMonitor,
      boolean useKeyGroupWorkQueue) {
    this.keyGroupWorkQueue = useKeyGroupWorkQueue ? new KeyGroupWorkQueue(useFairMonitor) : null;
    this.maximumPoolSize = initialMaximumPoolSize;
    monitor = new Monitor(useFairMonitor);
    executor =
        new ThreadPoolExecutor(
            initialMaximumPoolSize,
            initialMaximumPoolSize,
            keepAliveTime,
            unit,
            keyGroupWorkQueue != null ? keyGroupWorkQueue : new LinkedBlockingQueue<>(),
            threadFactory) {
          @Override
          protected void beforeExecute(Thread t, Runnable r) {
            super.beforeExecute(t, r);
            synchronized (BoundedQueueExecutor.this) {
              if (++activeCount >= maximumPoolSize && startTimeMaxActiveThreadsUsed == 0) {
                startTimeMaxActiveThreadsUsed = System.currentTimeMillis();
              }
            }
          }

          @Override
          protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            synchronized (BoundedQueueExecutor.this) {
              if (--activeCount < maximumPoolSize && startTimeMaxActiveThreadsUsed > 0) {
                totalTimeMaxActiveThreadsUsed +=
                    (System.currentTimeMillis() - startTimeMaxActiveThreadsUsed);
                startTimeMaxActiveThreadsUsed = 0;
              }
            }
          }
        };
    executor.allowCoreThreadTimeOut(true);
    this.maximumElementsOutstanding = maximumElementsOutstanding;
    this.maximumBytesOutstanding = maximumBytesOutstanding;
  }

  // Before adding a Work to the queue, check that there are enough bytes of space or no other
  // outstanding elements of work.
  public void execute(ExecutableWork work, long workBytes) {
    monitor.enterWhenUninterruptibly(
        new Guard(monitor) {
          @Override
          public boolean isSatisfied() {
            return elementsOutstanding == 0
                || (bytesAvailable() >= workBytes
                    && elementsOutstanding < maximumElementsOutstanding());
          }
        });
    executeMonitorHeld(work, workBytes);
  }

  // Forcibly add ExecutableWork to the queue, ignoring the limits.
  public void forceExecute(ExecutableWork work, long workBytes) {
    monitor.enter();
    executeMonitorHeld(work, workBytes);
  }

  /** Forcibly execute a Runnable callback with 0 bytes of size. */
  public void forceExecute(Runnable runnable) {
    monitor.enter();
    executeMonitorHeld(runnable);
  }

  // Set the maximum/core pool size of the executor.
  public synchronized void setMaximumPoolSize(int maximumPoolSize, int maximumElementsOutstanding) {
    // For ThreadPoolExecutor, the maximum pool size should always greater than or equal to core
    // pool size.
    if (maximumPoolSize > executor.getCorePoolSize()) {
      executor.setMaximumPoolSize(maximumPoolSize);
      executor.setCorePoolSize(maximumPoolSize);
    } else {
      executor.setCorePoolSize(maximumPoolSize);
      executor.setMaximumPoolSize(maximumPoolSize);
    }
    this.maximumPoolSize = maximumPoolSize;
    this.maximumElementsOutstanding = maximumElementsOutstanding;
  }

  public void shutdown() throws InterruptedException {
    executor.shutdown();
    if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
      throw new RuntimeException("Work executor did not terminate within 5 minutes");
    }
  }

  public boolean executorQueueIsEmpty() {
    return executor.getQueue().isEmpty();
  }

  public synchronized long allThreadsActiveTime() {
    return totalTimeMaxActiveThreadsUsed;
  }

  public synchronized int activeCount() {
    return activeCount;
  }

  public long bytesOutstanding() {
    monitor.enter();
    try {
      return bytesOutstanding;
    } finally {
      monitor.leave();
    }
  }

  public int elementsOutstanding() {
    monitor.enter();
    try {
      return elementsOutstanding;
    } finally {
      monitor.leave();
    }
  }

  public long maximumBytesOutstanding() {
    return maximumBytesOutstanding;
  }

  public synchronized int maximumElementsOutstanding() {
    return maximumElementsOutstanding;
  }

  public synchronized int getMaximumPoolSize() {
    return maximumPoolSize;
  }

  public String summaryHtml() {
    monitor.enter();
    try {
      StringBuilder builder = new StringBuilder();
      builder.append("Worker Threads: ");
      builder.append(executor.getPoolSize());
      builder.append("/");
      builder.append(executor.getMaximumPoolSize());
      builder.append("<br>/n");

      builder.append("Active Threads: ");
      builder.append(executor.getActiveCount());
      builder.append("<br>/n");

      builder.append("Work Queue Size: ");
      builder.append(elementsOutstanding);
      builder.append("/");
      builder.append(maximumElementsOutstanding());
      builder.append("<br>/n");

      builder.append("Work Queue Bytes: ");
      builder.append(bytesOutstanding);
      builder.append("/");
      builder.append(maximumBytesOutstanding);
      builder.append("<br>/n");

      return builder.toString();
    } finally {
      monitor.leave();
    }
  }

  /**
   * A handle to use when requesting pulling more work from @BoundedQueueExecutor
   * via @BoundedQueueExecutor.pollWork. A single handle aggregates all budgets from work pulled for
   * inline execution and releases the budget after the multi work bundle is complete.
   */
  final class BoundedQueueExecutorWorkHandleImpl
      implements BoundedQueueExecutorWorkHandle, AutoCloseable {

    @GuardedBy("this")
    private int elements;

    @GuardedBy("this")
    private long bytes;

    @GuardedBy("this")
    private boolean closed = false;

    private BoundedQueueExecutorWorkHandleImpl(int elements, long bytes) {
      checkArgument(elements >= 0 && bytes >= 0);
      this.elements = elements;
      this.bytes = bytes;
    }

    /**
     * Merges the budget from another handle into this handle.
     *
     * <p>This transfers the budget (elements and bytes) from the {@code other} handle to this
     * handle, and marks the {@code other} handle as closed to prevent it from releasing the budget
     * again if it is closed.
     */
    public void merge(BoundedQueueExecutorWorkHandleImpl other) {
      checkArgumentNotNull(other);
      synchronized (this) {
        Preconditions.checkState(!closed, "Cannot merge into a closed handle");
        synchronized (other) {
          Preconditions.checkState(!other.closed, "Cannot merge a closed handle");
          this.elements += other.elements;
          this.bytes += other.bytes;
          other.closed = true;
          other.elements = 0;
          other.bytes = 0;
        }
      }
    }

    public synchronized boolean isClosed() {
      return closed;
    }

    @VisibleForTesting
    synchronized int elements() {
      return elements;
    }

    @VisibleForTesting
    synchronized long bytes() {
      return bytes;
    }

    @Override
    public synchronized void close() {
      if (closed) return;
      closed = true;
      decrementCounters(this.elements, this.bytes);
    }
  }

  static final class QueuedWork implements Runnable {

    private final ExecutableWork work;
    private final BoundedQueueExecutorWorkHandleImpl handle;

    public QueuedWork(ExecutableWork work, BoundedQueueExecutorWorkHandleImpl handle) {
      this.work = work;
      this.handle = handle;
    }

    public ExecutableWork getWork() {
      return work;
    }

    public BoundedQueueExecutorWorkHandleImpl getHandle() {
      return handle;
    }

    @Override
    public void run() {
      checkArgument(!handle.isClosed());
      try (handle) {
        work.run(handle);
      }
    }
  }

  private void executeMonitorHeld(ExecutableWork work, long workBytes) {
    ++elementsOutstanding;
    bytesOutstanding += workBytes;
    monitor.leave();
    BoundedQueueExecutorWorkHandleImpl handle =
        new BoundedQueueExecutorWorkHandleImpl(1, workBytes);
    try {
      executor.execute(new QueuedWork(work, handle));
    } catch (Throwable t) {
      handle.close();
      throw ExceptionUtils.safeWrapThrowableAsException(t);
    }
  }

  private void executeMonitorHeld(Runnable work) {
    ++elementsOutstanding;
    monitor.leave();

    try {
      executor.execute(
          () -> {
            try {
              work.run();
            } finally {
              decrementCounters(1, 0L);
            }
          });
    } catch (Throwable t) {
      decrementCounters(1, 0L);
      throw ExceptionUtils.safeWrapThrowableAsException(t);
    }
  }

  @VisibleForTesting
  BoundedQueueExecutorWorkHandleImpl createBudgetHandle(int elements, long bytes) {
    return new BoundedQueueExecutorWorkHandleImpl(elements, bytes);
  }

  public @Nullable ExecutableWork pollWork(
      String computationId, Work.KeyGroup keyGroup, BoundedQueueExecutorWorkHandle handle) {
    checkArgument(handle instanceof BoundedQueueExecutorWorkHandleImpl);
    checkArgument(computationId != null && keyGroup != null && !keyGroup.equals(KeyGroup.DEFAULT));
    BoundedQueueExecutorWorkHandleImpl internalHandle = (BoundedQueueExecutorWorkHandleImpl) handle;
    if (keyGroupWorkQueue == null) {
      return null;
    }
    while (true) {
      @Nullable QueuedWork queuedWork = keyGroupWorkQueue.pollWork(computationId, keyGroup);
      if (queuedWork == null) {
        return null;
      }
      if (queuedWork.getWork().work().isFailed()) {
        queuedWork.getHandle().close();
      } else {
        internalHandle.merge(queuedWork.getHandle());
        return queuedWork.getWork();
      }
    }
  }

  private void decrementCounters(int elements, long bytes) {
    // All threads queue decrements and one thread grabs the monitor and updates
    // counters. We do this to reduce contention on monitor which is locked by
    // GetWork thread
    decrementQueue.add(new Budget(elements, bytes));
    boolean submittedToExistingBatch = isDecrementBatchPending.getAndSet(true);
    if (submittedToExistingBatch) {
      // There is already a thread about to drain the decrement queue
      // Current thread does not need to drain.
      return;
    }
    synchronized (decrementQueueDrainLock) {
      // By setting false here, we may allow another decrement to claim submission of the next batch
      // and start waiting on the decrementQueueDrainLock.
      //
      // However this prevents races that would leave decrements in the queue and unclaimed and we
      // are ensured there is at most one additional thread blocked. This helps prevent the executor
      // from creating threads over the limit if many were contending on the lock while their
      // decrements were already applied.
      isDecrementBatchPending.set(false);
      long bytesToDecrement = 0;
      int elementsToDecrement = 0;
      while (true) {
        Budget pollResult = decrementQueue.poll();
        if (pollResult == null) {
          break;
        }
        bytesToDecrement += pollResult.bytes;
        elementsToDecrement += pollResult.elements;
      }
      if (elementsToDecrement == 0) {
        return;
      }

      monitor.enter();
      elementsOutstanding -= elementsToDecrement;
      bytesOutstanding -= bytesToDecrement;
      monitor.leave();
    }
  }

  private long bytesAvailable() {
    return maximumBytesOutstanding - bytesOutstanding;
  }
}
