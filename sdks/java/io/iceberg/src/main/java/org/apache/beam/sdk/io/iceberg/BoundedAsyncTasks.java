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
package org.apache.beam.sdk.io.iceberg;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Runs tasks on a fixed thread pool while bounding how many are in flight. Results are handed to
 * {@code onDone} on the caller's thread (from {@link #submit} and {@link #awaitAll}), so a DoFn can
 * emit them without a thread-safe output.
 */
class BoundedAsyncTasks<T> {
  private final ExecutorService executor;
  private final int maxInFlight;
  private final Deque<Future<T>> active = new ArrayDeque<>();

  BoundedAsyncTasks(int threads, int maxInFlight) {
    this.executor = Executors.newFixedThreadPool(threads);
    this.maxInFlight = maxInFlight;
  }

  /**
   * Submits a task, first delivering any finished results. Blocks while {@code maxInFlight} tasks
   * are outstanding. If a task failed, its exception is rethrown and every other outstanding task
   * is cancelled.
   */
  void submit(Callable<T> task, Consumer<T> onDone) throws Exception {
    try {
      drainFinished(onDone);
      while (active.size() >= maxInFlight) {
        Future<T> oldest = active.removeFirst();
        onDone.accept(oldest.get()); // blocks until the oldest task completes
      }
    } catch (Exception e) {
      cancelAll();
      throw e;
    }
    active.add(executor.submit(task));
  }

  /** Delivers every outstanding result in submission order. The queue is empty afterwards. */
  void awaitAll(Consumer<T> onDone) throws Exception {
    try {
      while (!active.isEmpty()) {
        Future<T> oldest = active.removeFirst();
        onDone.accept(oldest.get());
      }
    } finally {
      cancelAll();
    }
  }

  /** Cancels and forgets every outstanding task. Results already delivered are unaffected. */
  void cancelAll() {
    for (Future<T> future : active) {
      future.cancel(true);
    }
    active.clear();
  }

  void shutdown() {
    cancelAll();
    executor.shutdownNow();
  }

  private void drainFinished(Consumer<T> onDone) throws Exception {
    Iterator<Future<T>> iterator = active.iterator();
    while (iterator.hasNext()) {
      Future<T> future = iterator.next();
      if (future.isDone()) {
        iterator.remove();
        onDone.accept(future.get());
      }
    }
  }
}
