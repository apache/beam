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

import java.util.concurrent.Semaphore;
import java.util.function.Function;

public final class WeightedSemaphore<V> {
  private final int maxWeight;
  private final Semaphore limit;
  private final Function<V, Integer> weigher;

  private WeightedSemaphore(int maxWeight, Semaphore limit, Function<V, Integer> weigher) {
    this.maxWeight = maxWeight;
    this.limit = limit;
    this.weigher = weigher;
  }

  public static <V> WeightedSemaphore<V> create(int maxWeight, Function<V, Integer> weigherFn) {
    return new WeightedSemaphore<>(maxWeight, new Semaphore(maxWeight, true), weigherFn);
  }

  public void acquireUninterruptibly(V value) {
    limit.acquireUninterruptibly(weigher.apply(value));
  }

  public void release(V value) {
    limit.release(weigher.apply(value));
  }

  public int currentWeight() {
    return maxWeight - limit.availablePermits();
  }
}
