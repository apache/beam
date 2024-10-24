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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Monitor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Monitor.Guard;

/** An executor for executing work on windmill items. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BoundedQueueExecutor {
  private final ThreadPoolExecutor executor;
  private final long maximumBytesOutstanding;

  // Used to guard elementsOutstanding and bytesOutstanding.
  private final Monitor monitor = new Monitor();
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

  public BoundedQueueExecutor(
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      int maximumElementsOutstanding,
      long maximumBytesOutstanding,
      ThreadFactory threadFactory) {
    this.maximumPoolSize = maximumPoolSize;
    executor =
        new ThreadPoolExecutor(
            maximumPoolSize,
            maximumPoolSize,
            keepAliveTime,
            unit,
            new LinkedBlockingQueue<>(),
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
  public void execute(Runnable work, long workBytes) {
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

  // Forcibly add something to the queue, ignoring the length limit.
  public void forceExecute(Runnable work, long workBytes) {
    monitor.enter();
    executeMonitorHeld(work, workBytes);
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

  private void executeMonitorHeld(Runnable work, long workBytes) {
    bytesOutstanding += workBytes;
    ++elementsOutstanding;
    monitor.leave();

    try {
      executor.execute(
          () -> {
            try {
              work.run();
            } finally {
              decrementCounters(workBytes);
            }
          });
    } catch (RuntimeException e) {
      // If the execute() call threw an exception, decrement counters here.
      decrementCounters(workBytes);
      throw e;
    }
  }

  private void decrementCounters(long workBytes) {
    monitor.enter();
    --elementsOutstanding;
    bytesOutstanding -= workBytes;
    monitor.leave();
  }

  private long bytesAvailable() {
    return maximumBytesOutstanding - bytesOutstanding;
  }
}
