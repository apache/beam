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
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Executor that blocks on execute() if its queue is full. */
public class BoundedQueueExecutor extends ThreadPoolExecutor {
  private static class ReducableSemaphore extends Semaphore {
    ReducableSemaphore(int permits) {
      super(permits);
    }

    @Override
    public void reducePermits(int permits) {
      super.reducePermits(permits);
    }
  }

  private ReducableSemaphore semaphore;

  public BoundedQueueExecutor(
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      int maximumQueueSize,
      ThreadFactory threadFactory) {
    super(
        maximumPoolSize,
        maximumPoolSize,
        keepAliveTime,
        unit,
        new LinkedBlockingQueue<Runnable>(),
        threadFactory);
    this.semaphore = new ReducableSemaphore(maximumQueueSize);
    allowCoreThreadTimeOut(true);
  }

  // Before adding a Runnable to the queue, acquire the semaphore.
  @Override
  public void execute(Runnable r) {
    semaphore.acquireUninterruptibly();
    super.execute(r);
  }

  // Forcibly add something to the queue, ignoring the length limit.
  public void forceExecute(Runnable r) {
    semaphore.reducePermits(1);
    super.execute(r);
  }

  // Release the semaphore after taking a Runnable off the queue.
  @Override
  public void beforeExecute(Thread t, Runnable r) {
    semaphore.release();
  }
}
