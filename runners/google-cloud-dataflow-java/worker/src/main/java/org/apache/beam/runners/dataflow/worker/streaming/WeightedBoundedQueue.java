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
package org.apache.beam.runners.dataflow.worker.streaming;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Queue bounded by a {@link WeightedSemaphore}. */
public final class WeightedBoundedQueue<V> {

  private final LinkedBlockingQueue<V> queue;
  private final WeightedSemaphore<V> weightedSemaphore;

  private WeightedBoundedQueue(
      LinkedBlockingQueue<V> linkedBlockingQueue, WeightedSemaphore<V> weightedSemaphore) {
    this.queue = linkedBlockingQueue;
    this.weightedSemaphore = weightedSemaphore;
  }

  public static <V> WeightedBoundedQueue<V> create(WeightedSemaphore<V> weightedSemaphore) {
    return new WeightedBoundedQueue<>(new LinkedBlockingQueue<>(), weightedSemaphore);
  }

  /**
   * Adds the value to the queue, blocking if this would cause the overall weight to exceed the
   * limit.
   */
  public void put(V value) {
    weightedSemaphore.acquireUninterruptibly(value);
    queue.add(value);
  }

  /** Returns and removes the next value, or null if there is no such value. */
  public @Nullable V poll() {
    @Nullable V result = queue.poll();
    if (result != null) {
      weightedSemaphore.release(result);
    }
    return result;
  }

  /**
   * Retrieves and removes the head of this queue, waiting up to the specified wait time if
   * necessary for an element to become available.
   *
   * @param timeout how long to wait before giving up, in units of {@code unit}
   * @param unit a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
   * @return the head of this queue, or {@code null} if the specified waiting time elapses before an
   *     element is available
   * @throws InterruptedException if interrupted while waiting
   */
  public @Nullable V poll(long timeout, TimeUnit unit) throws InterruptedException {
    @Nullable V result = queue.poll(timeout, unit);
    if (result != null) {
      weightedSemaphore.release(result);
    }
    return result;
  }

  /** Returns and removes the next value, or blocks until one is available. */
  public V take() throws InterruptedException {
    V result = queue.take();
    weightedSemaphore.release(result);
    return result;
  }

  @VisibleForTesting
  int size() {
    return queue.size();
  }
}
