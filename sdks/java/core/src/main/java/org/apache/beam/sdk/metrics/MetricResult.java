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
package org.apache.beam.sdk.metrics;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.google.auto.value.AutoValue;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The results of a single current metric.
 *
 * <p>TODO(BEAM-6265): Decouple wire formats from internal formats, remove usage of MetricName.
 */
@JsonFilter("committedMetrics")
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class MetricResult<T> {
  /** Return the name of the metric. */
  public MetricName getName() {
    return getKey().metricName();
  }

  public abstract MetricKey getKey();

  /**
   * Return the value of this metric across all successfully completed parts of the pipeline.
   *
   * <p>Not all runners will support committed metrics. If they are not supported, the runner will
   * throw an {@link UnsupportedOperationException}.
   */
  public T getCommitted() {
    T committed = getCommittedOrNull();
    if (committed == null) {
      throw new UnsupportedOperationException(
          "This runner does not currently support committed"
              + " metrics results. Please use 'attempted' instead.");
    }
    return committed;
  }

  public boolean hasCommitted() {
    return getCommittedOrNull() != null;
  }

  /** Return the value of this metric across all attempts of executing all parts of the pipeline. */
  public abstract @Nullable T getCommittedOrNull();

  /** Return the value of this metric across all attempts of executing all parts of the pipeline. */
  public abstract T getAttempted();

  public <V> MetricResult<V> transform(Function<T, V> fn) {
    T committed = getCommittedOrNull();
    return create(
        getKey(), committed == null ? null : fn.apply(committed), fn.apply(getAttempted()));
  }

  public MetricResult<T> addAttempted(T update, BiFunction<T, T, T> combine) {
    return create(getKey(), getCommitted(), combine.apply(getAttempted(), update));
  }

  public MetricResult<T> addCommitted(T update, BiFunction<T, T, T> combine) {
    T committed = getCommittedOrNull();
    return create(
        getKey(), committed == null ? update : combine.apply(committed, update), getAttempted());
  }

  public static <T> MetricResult<T> attempted(MetricKey key, T attempted) {
    return new AutoValue_MetricResult<>(key, null, attempted);
  }

  public static <T> MetricResult<T> create(MetricKey key, Boolean isCommittedSupported, T value) {
    if (isCommittedSupported) {
      return create(key, value, value);
    }
    return attempted(key, value);
  }

  public static <T> MetricResult<T> create(MetricKey key, @Nullable T committed, T attempted) {
    return new AutoValue_MetricResult<T>(key, committed, attempted);
  }
}
