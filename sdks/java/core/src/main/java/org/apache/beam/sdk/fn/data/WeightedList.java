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
package org.apache.beam.sdk.fn.data;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Facade for a {@link List<T>} that keeps track of weight, for cache limit reasons. */
public class WeightedList<T> {

  /** Original list that is being wrapped. */
  private final List<T> backing;

  /** Weight of all the elements being tracked. */
  private final AtomicLong weight;

  public WeightedList(List<T> backing, long weight) {
    this.backing = backing;
    this.weight = new AtomicLong(weight);
  }

  public List<T> getBacking() {
    return this.backing;
  }

  public int size() {
    return this.backing.size();
  }

  public boolean isEmpty() {
    return this.backing.isEmpty();
  }

  public long getWeight() {
    return weight.longValue();
  }

  public void add(T element, long weight) {
    this.backing.add(element);
    accumulateWeight(weight);
  }

  public void addAll(WeightedList<T> values) {
    this.addAll(values.getBacking(), values.getWeight());
  }

  public void addAll(List<T> values, long weight) {
    this.backing.addAll(values);
    accumulateWeight(weight);
  }

  public void accumulateWeight(long weight) {
    this.weight.accumulateAndGet(
        weight,
        (first, second) -> {
          try {
            return Math.addExact(first, second);
          } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
          }
        });
  }
}
