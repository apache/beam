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
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.dataflow.worker.streaming.BoundedQueueExecutorWorkHandle;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.FailedWorkHandler;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Monitor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Monitor.Guard;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An executor for executing work on windmill items.
 *
 * <p>When multi-key bundles are enabled, batchable work is also indexed in {@link
 * KeyGroupWorkIndex} so running bundles can steal it via {@link #pollWork}. Stolen work leaves a
 * no-op stale shell in the executor queue, which is discarded when dequeued or purged in bulk if
 * the queue grows too large.
 */
public class BoundedQueueExecutor {

  private final ThreadPoolExecutor executor;
  private final long maximumBytesOutstanding;

  // Used to guard writes to elementsOutstanding and bytesOutstanding.
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
  private final AtomicInteger elementsOutstanding = new AtomicInteger();
  private long bytesOutstanding = 0;

  private volatile int maximumElementsOutstanding;

  @GuardedBy("this")
  private int activeCount;

  @GuardedBy("this")
  private int maximumPoolSize;

  @GuardedBy("this")
  private long startTimeMaxActiveThreadsUsed;

  @GuardedBy("this")
  private long totalTimeMaxActiveThreadsUsed;

  private final LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
  private final @Nullable KeyGroupWorkIndex keyGroupWorkIndex;

  private final AtomicBoolean purgingStaleShells = new AtomicBoolean(false);

  public BoundedQueueExecutor(
      int initialMaximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      int maximumElementsOutstanding,
      long maximumBytesOutstanding,
      ThreadFactory threadFactory,
      boolean useFairMonitor,
      boolean multiKeyBundleEnabled) {
    this.keyGroupWorkIndex = multiKeyBundleEnabled ? new KeyGroupWorkIndex() : null;
    this.maximumPoolSize = initialMaximumPoolSize;
    monitor = new Monitor(useFairMonitor);
    executor =
        new ThreadPoolExecutor(
            initialMaximumPoolSize,
            initialMaximumPoolSize,
            keepAliveTime,
            unit,
            workQueue,
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
            int outstanding = elementsOutstanding.get();
            return outstanding == 0
                || (bytesAvailable() >= workBytes && outstanding < maximumElementsOutstanding());
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

  @VisibleForTesting
  public boolean executorQueueIsEmptyForTesting() {
    return workQueue.stream().allMatch(BoundedQueueExecutor::isStaleShell);
  }

  /** Returns the raw number of entries in the executor queue, including stale shells. */
  @VisibleForTesting
  int rawQueueSizeForTesting() {
    return workQueue.size();
  }

  @VisibleForTesting
  boolean keyGroupWorkIndexIsEmpty() {
    @Nullable KeyGroupWorkIndex index = keyGroupWorkIndex;
    return index == null || index.isEmpty();
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
    return elementsOutstanding.get();
  }

  public long maximumBytesOutstanding() {
    return maximumBytesOutstanding;
  }

  public int maximumElementsOutstanding() {
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
      builder.append(elementsOutstanding.get());
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
    private final List<Work> workBatch;

    @GuardedBy("this")
    private long bytes;

    @GuardedBy("this")
    private boolean closed = false;

    private BoundedQueueExecutorWorkHandleImpl(Work work, long bytes) {
      checkArgument(bytes >= 0);
      this.workBatch = new ArrayList<>();
      this.workBatch.add(checkArgumentNotNull(work));
      this.bytes = bytes;
    }

    /**
     * Merges the budget from another handle into this handle.
     *
     * <p>This transfers the budget (workBatch and bytes) from the {@code other} handle to this
     * handle, and marks the {@code other} handle as closed to prevent it from releasing the budget
     * again if it is closed.
     */
    public void merge(BoundedQueueExecutorWorkHandleImpl other) {
      checkArgumentNotNull(other);
      synchronized (this) {
        Preconditions.checkState(!closed, "Cannot merge into a closed handle");
        synchronized (other) {
          Preconditions.checkState(!other.closed, "Cannot merge a closed handle");
          this.workBatch.addAll(other.workBatch);
          this.bytes += other.bytes;
          other.closed = true;
          other.workBatch.clear();
          other.bytes = 0;
        }
      }
    }

    public synchronized boolean isClosed() {
      return closed;
    }

    @Override
    /*
     * Returns an unmodifiable view over the underlying list.
     * It is unsafe to use the returned list with concurrent calls to mutating methods
     * like merge/close
     */
    public synchronized List<Work> getWorkBatch() {
      return Collections.unmodifiableList(workBatch);
    }

    @VisibleForTesting
    synchronized long bytes() {
      return bytes;
    }

    @Override
    public synchronized void close() {
      if (closed) return;
      closed = true;
      decrementCounters(this.workBatch.size(), this.bytes);
    }
  }

  /**
   * A {@link Runnable} for an {@link ExecutableWork} that can be claimed either by an executor
   * thread in {@link #run} or by a multi-key bundle in {@link #pollWork}.
   *
   * <p>Thread safety: {@link #state} is {@code volatile} and transitions at most once from {@link
   * #QUEUED} to {@link #STARTED} or {@link #STOLEN} via {@link #STATE}{@code .compareAndSet}.
   * Non-final fields {@link #work} and {@link #handle} are written before {@code state} is
   * initialized in the constructor, and are subsequently read and cleared only by the single thread
   * that wins that {@code compareAndSet}. All other accesses read only {@code final} fields or
   * {@code volatile state}.
   */
  static final class QueuedWork implements Runnable {

    private static final int QUEUED = 0;
    private static final int STARTED = 1; // Claimed by an executor thread in run().
    private static final int STOLEN = 2; // Claimed by a multi-key bundle in pollWork().

    private static final AtomicIntegerFieldUpdater<QueuedWork> STATE =
        AtomicIntegerFieldUpdater.newUpdater(QueuedWork.class, "state");

    // Non-null iff the work is eligible to be stolen by a multi-key bundle.
    private final @Nullable KeyGroupWorkIndex keyGroupWorkIndex;
    private final KeyGroupWorkIndex.@Nullable QueueKey queueKey;
    // Cleared upon claim so stale shells and claimed index entries do not retain work references.
    private @Nullable ExecutableWork work;
    private @Nullable BoundedQueueExecutorWorkHandleImpl handle;
    private volatile int state = QUEUED;

    QueuedWork(
        ExecutableWork work,
        BoundedQueueExecutorWorkHandleImpl handle,
        @Nullable KeyGroupWorkIndex keyGroupWorkIndex) {
      this.work = work;
      this.handle = handle;
      Work w = work.work();
      if (keyGroupWorkIndex != null
          && !w.getKeyGroup().equals(Work.KeyGroup.DEFAULT)
          && !w.isMultiKeyBatchingDisabled()) {
        this.keyGroupWorkIndex = keyGroupWorkIndex;
        this.queueKey = new KeyGroupWorkIndex.QueueKey(w.getComputationId(), w.getKeyGroup());
      } else {
        this.keyGroupWorkIndex = null;
        this.queueKey = null;
      }
    }

    public ExecutableWork getWork() {
      return checkStateNotNull(work, "QueuedWork was already claimed.");
    }

    public BoundedQueueExecutorWorkHandleImpl getHandle() {
      return checkStateNotNull(handle, "QueuedWork was already claimed.");
    }

    boolean isStealable() {
      return keyGroupWorkIndex != null;
    }

    KeyGroupWorkIndex.QueueKey queueKey() {
      return checkStateNotNull(queueKey);
    }

    boolean isClaimed() {
      return state != QUEUED;
    }

    private boolean isStolen() {
      return state == STOLEN;
    }

    boolean tryClaimForExecution() {
      return STATE.compareAndSet(this, QUEUED, STARTED);
    }

    boolean tryClaimForSteal() {
      return STATE.compareAndSet(this, QUEUED, STOLEN);
    }

    private void clearClaimedWork() {
      work = null;
      handle = null;
    }

    @Override
    public void run() {
      if (!tryClaimForExecution()) {
        return; // Already stolen by a multi-key bundle.
      }
      ExecutableWork workToRun = getWork();
      BoundedQueueExecutorWorkHandleImpl workHandle = getHandle();
      clearClaimedWork();
      @Nullable KeyGroupWorkIndex index = keyGroupWorkIndex;
      if (index != null) {
        index.drainClaimed(this);
      }
      checkArgument(!workHandle.isClosed());
      try (workHandle) {
        workToRun.run(workHandle);
      }
    }
  }

  private void executeMonitorHeld(ExecutableWork work, long workBytes) {
    elementsOutstanding.incrementAndGet();
    bytesOutstanding += workBytes;
    monitor.leave();
    BoundedQueueExecutorWorkHandleImpl handle =
        new BoundedQueueExecutorWorkHandleImpl(work.work(), workBytes);
    QueuedWork queuedWork = new QueuedWork(work, handle, keyGroupWorkIndex);
    try {
      executor.execute(queuedWork);
    } catch (Throwable t) {
      handle.close();
      throw ExceptionUtils.safeWrapThrowableAsException(t);
    }
    if (queuedWork.isStealable()) {
      // Index after queuing so work rejected after shutdown cannot be stolen.
      checkStateNotNull(keyGroupWorkIndex).add(queuedWork);
    }
  }

  private void executeMonitorHeld(Runnable work) {
    elementsOutstanding.incrementAndGet();
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
  BoundedQueueExecutorWorkHandleImpl createBudgetHandle(Work work, long bytes) {
    return new BoundedQueueExecutorWorkHandleImpl(work, bytes);
  }

  public @Nullable ExecutableWork pollWork(
      String computationId,
      Work.KeyGroup keyGroup,
      BoundedQueueExecutorWorkHandle handle,
      FailedWorkHandler onFailedWorkHandler) {
    checkArgument(
        computationId != null && keyGroup != null && !keyGroup.equals(Work.KeyGroup.DEFAULT));
    checkArgument(handle instanceof BoundedQueueExecutorWorkHandleImpl);
    checkStateNotNull(onFailedWorkHandler);
    BoundedQueueExecutorWorkHandleImpl internalHandle = (BoundedQueueExecutorWorkHandleImpl) handle;
    @Nullable KeyGroupWorkIndex index = keyGroupWorkIndex;
    if (index == null) {
      return null;
    }
    while (true) {
      @Nullable QueuedWork stolen = index.pollAndClaim(computationId, keyGroup);
      if (stolen == null) {
        return null;
      }
      ExecutableWork stolenWork = stolen.getWork();
      BoundedQueueExecutorWorkHandleImpl stolenHandle = stolen.getHandle();
      stolen.clearClaimedWork();
      onWorkStolen();
      Work work = stolenWork.work();
      if (work.isFailed()) {
        stolenHandle.close();
        onFailedWorkHandler.onFailedWork(work);
        continue;
      }
      internalHandle.merge(stolenHandle);
      return stolenWork;
    }
  }

  /**
   * Purges stale shells in bulk if they accumulate while all executor threads are busy running
   * multi-key bundles.
   */
  private void onWorkStolen() {
    if (workQueue.size() <= (long) elementsOutstanding.get() + maximumElementsOutstanding
        || !purgingStaleShells.compareAndSet(false, true)) {
      return;
    }
    try {
      if (!executor.isShutdown()) {
        workQueue.removeIf(BoundedQueueExecutor::isStaleShell);
      }
    } finally {
      purgingStaleShells.set(false);
    }
  }

  private static boolean isStaleShell(Runnable runnable) {
    return runnable instanceof QueuedWork && ((QueuedWork) runnable).isStolen();
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
      elementsOutstanding.addAndGet(-elementsToDecrement);
      bytesOutstanding -= bytesToDecrement;
      monitor.leave();
    }
  }

  private long bytesAvailable() {
    return maximumBytesOutstanding - bytesOutstanding;
  }
}
