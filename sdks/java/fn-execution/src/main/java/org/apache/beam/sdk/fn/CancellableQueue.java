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
package org.apache.beam.sdk.fn;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.ThreadSafe;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A simplified {@link ThreadSafe} blocking queue that can be cancelled freeing any blocked {@link
 * Thread}s and preventing future {@link Thread}s from blocking.
 *
 * <p>The queue is able to be reset and re-used.
 */
@ThreadSafe
public class CancellableQueue<T extends @NonNull Object> {

  private final int capacity;
  private final @Nullable Object[] elements;
  private final Lock lock;
  private final Condition notFull;
  private final Condition notEmpty;
  int addIndex;
  int takeIndex;
  int count;
  @Nullable Exception cancellationException;

  /** Creates a {@link ThreadSafe} blocking queue with a maximum capacity. */
  public CancellableQueue(int capacity) {
    this.capacity = capacity;
    this.elements = new Object[capacity];
    this.lock = new ReentrantLock();
    this.notFull = lock.newCondition();
    this.notEmpty = lock.newCondition();
  }

  /**
   * Adds an element to this queue. Will block until the queue is not full or is cancelled.
   *
   * @throws InterruptedException if this thread was interrupted waiting to put the element. The
   *     caller must invoke {@link #cancel} if the interrupt is unrecoverable.
   * @throws Exception if the queue is cancelled.
   */
  public void put(T t) throws Exception, InterruptedException {
    try {
      lock.lockInterruptibly();
      while (count >= capacity && cancellationException == null) {
        notFull.await();
      }
      if (cancellationException != null) {
        throw cancellationException;
      }
      elements[addIndex] = t;
      addIndex = (addIndex + 1) % elements.length;
      count += 1;
      notEmpty.signal();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Takes an element from this queue. Will block until the queue is not full or is cancelled.
   *
   * @throws InterruptedException if this thread was interrupted waiting for an element. The caller
   *     must invoke {@link #cancel} if the interrupt is unrecoverable.
   * @throws Exception if the queue is cancelled.
   */
  @SuppressWarnings({"cast"})
  public T take() throws Exception, InterruptedException {
    T rval;
    try {
      lock.lockInterruptibly();
      while (count == 0 && cancellationException == null) {
        notEmpty.await();
      }
      if (cancellationException != null) {
        throw cancellationException;
      }

      rval = (T) elements[takeIndex];
      elements[takeIndex] = null;
      takeIndex = (takeIndex + 1) % elements.length;
      count -= 1;
      notFull.signal();
    } finally {
      lock.unlock();
    }
    return rval;
  }

  /**
   * Causes any pending and future {@link #put} and {@link #take} invocations to throw an exception.
   *
   * <p>The first call to {@link #cancel} sets the exception that will be thrown. Resetting the
   * queue clears the exception.
   */
  public void cancel(Exception exception) {
    checkNotNull(exception);
    lock.lock();
    try {
      if (cancellationException == null) {
        cancellationException = exception;
        clearElementsLocked();
      }
      notEmpty.signalAll();
      notFull.signalAll();
    } finally {
      lock.unlock();
    }
  }

  private void clearElementsLocked() {
    for (int i = takeIndex; count > 0; i = (i + 1) % elements.length) {
      elements[i] = null;
      --count;
    }
    addIndex = 0;
    takeIndex = 0;
  }

  /** Enables the queue to be re-used after it has been cancelled. */
  public void reset() {
    lock.lock();
    try {
      cancellationException = null;
      clearElementsLocked();
    } finally {
      lock.unlock();
    }
  }
}
