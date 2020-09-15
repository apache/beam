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
package org.apache.beam.runners.core.metrics;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A map from {@code K} to {@code T} that supports getting or creating values associated with a key
 * in a thread-safe manner.
 */
public class MetricsMap<K, T> implements Serializable {

  /** Interface for creating instances to populate the {@link MetricsMap}. */
  public interface Factory<K, T> extends Serializable {
    /**
     * Create an instance of {@code T} to use with the given {@code key}.
     *
     * <p>It must be safe to call this from multiple threads.
     */
    T createInstance(K key);
  }

  private final Factory<K, T> factory;
  private final ConcurrentMap<K, T> metrics = new ConcurrentHashMap<>();

  public MetricsMap(Factory<K, T> factory) {
    this.factory = factory;
  }

  /** Get or create the value associated with the given key. */
  public T get(K key) {
    T metric = metrics.get(key);
    if (metric == null) {
      metric = factory.createInstance(key);
      metric = MoreObjects.firstNonNull(metrics.putIfAbsent(key, metric), metric);
    }
    return metric;
  }

  /** Get the value associated with the given key, if it exists. */
  public @Nullable T tryGet(K key) {
    return metrics.get(key);
  }

  /** Return an iterable over the entries in the current {@link MetricsMap}. */
  public Iterable<Map.Entry<K, T>> entries() {
    return Iterables.unmodifiableIterable(metrics.entrySet());
  }

  /** Return an iterable over the values in the current {@link MetricsMap}. */
  public Iterable<T> values() {
    return Iterables.unmodifiableIterable(metrics.values());
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof MetricsMap) {
      MetricsMap<?, ?> metricsMap = (MetricsMap<?, ?>) object;
      return Objects.equals(metrics, metricsMap.metrics);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return metrics.hashCode();
  }
}
