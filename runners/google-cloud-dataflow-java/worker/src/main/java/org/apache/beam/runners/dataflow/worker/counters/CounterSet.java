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
package org.apache.beam.runners.dataflow.worker.counters;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.dataflow.worker.counters.Counter.AtomicCounterValue;
import org.apache.beam.runners.dataflow.worker.counters.Counter.CounterUpdateExtractor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A CounterSet is a {@link CounterFactory} that can be used for creating counters. It also retains
 * all of the created counters and allows retrieving them later..
 *
 * <p>Thread-safe.
 */
public class CounterSet extends CounterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CounterSet.class);

  /**
   * Registered counters.
   *
   * <p>Uses a ConcurrentHashMap explicitly because we rely on the stronger iteration guarantees.
   */
  private final ConcurrentHashMap<CounterName, Counter<?, ?>> counters = new ConcurrentHashMap<>();

  @Override
  protected <InputT, AccumT> Counter<InputT, AccumT> createCounter(
      CounterName name, AtomicCounterValue<InputT, AccumT> counterValue) {
    Counter<InputT, AccumT> counter = super.createCounter(name, counterValue);
    Counter<?, ?> oldCounter = counters.putIfAbsent(name, counter);
    if (oldCounter != null) {
      checkArgument(
          oldCounter.equals(counter),
          "Counter %s duplicates incompatible counter %s in %s",
          counter,
          oldCounter,
          this);

      @SuppressWarnings("unchecked")
      Counter<InputT, AccumT> compatibleCounter = (Counter<InputT, AccumT>) oldCounter;
      return compatibleCounter;
    }
    return counter;
  }

  public Counter<?, ?> getExistingCounter(CounterName name) {
    return counters.get(name);
  }

  /**
   * Returns the Counter with the given name in this CounterSet; returns null if no such Counter
   * exists.
   */
  @Deprecated
  public Counter<?, ?> getExistingCounter(String name) {
    CounterName counterName = CounterName.named(name);
    return getExistingCounter(counterName);
  }

  public long size() {
    return counters.size();
  }

  /**
   * Extract an update. If there is a exception from the extractor, return null and logs the error.
   */
  private static <UpdateT> UpdateT extractUpdate(
      Counter<?, ?> counter, boolean delta, CounterUpdateExtractor<UpdateT> extractor) {
    try {
      return counter.extractUpdate(delta, extractor);
    } catch (IllegalArgumentException e) {
      LOG.warn("Error extracting counter update from counter {}: ", counter, e);
      return null;
    }
  }

  /**
   * Exracts counter updates. When both dirtyOnly and delta flags are set, only modified counter
   * updates are returned and the counters are marked 'committed'.
   */
  private <UpdateT> List<UpdateT> extractUpdatesImpl(
      boolean delta, boolean dirtyOnly, CounterUpdateExtractor<UpdateT> extractors) {
    // We don't allow removing counters, only adding counters. If a counter is added, it may or may
    // not show up in this iteration (counters.values()). But, ConcurrentHashMap guarantees that the
    // elements in the iterator will reflect the state "at or after the creation of the iterator".
    List<UpdateT> updates = new ArrayList<>(counters.size());
    for (Counter<?, ?> counter : counters.values()) {
      // commit the counter for detla updates when 'dirtyOnly' is set. This could
      // apply to cumulative counters too in future, not just for deltas.
      boolean shouldCommit = delta && dirtyOnly && counter.isDirty();

      if (shouldCommit) {
        counter.committing();
      }
      if (!dirtyOnly || shouldCommit) {
        UpdateT update = extractUpdate(counter, delta, extractors);
        if (update != null) {
          updates.add(update);
        }
      }
      if (shouldCommit) {
        counter.committed();
      }
    }
    return Collections.unmodifiableList(updates);
  }

  /**
   * Return a list of all the updates. This synchronizes with the addition/retrieval of counters so
   * it should be thread safe.
   */
  public <UpdateT> List<UpdateT> extractUpdates(
      boolean delta, CounterUpdateExtractor<UpdateT> extractors) {
    return extractUpdatesImpl(delta, false, extractors);
  }

  /**
   * Returns a list of delta updates of dirty counters. This does not includes counter updates that
   * haven't been modified since last extraction.
   */
  public <UpdateT> List<UpdateT> extractModifiedDeltaUpdates(
      CounterUpdateExtractor<UpdateT> extractors) {
    return extractUpdatesImpl(true, true, extractors);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("counters", counters.values()).toString();
  }
}
