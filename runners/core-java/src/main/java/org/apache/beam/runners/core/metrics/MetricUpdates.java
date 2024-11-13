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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/** Representation of multiple metric updates. */
@AutoValue
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public abstract class MetricUpdates {
  public static final MetricUpdates EMPTY =
      MetricUpdates.create(
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList());

  /**
   * Representation of a single metric update.
   *
   * @param <T> The type of value representing the update.
   */
  @AutoValue
  public abstract static class MetricUpdate<T> implements Serializable {

    /** The key being updated. */
    public abstract MetricKey getKey();
    /** The value of the update. */
    public abstract T getUpdate();

    public static <T> MetricUpdate<T> create(MetricKey key, T update) {
      return new AutoValue_MetricUpdates_MetricUpdate(key, update);
    }
  }

  /** All the counter updates. */
  public abstract Iterable<MetricUpdate<Long>> counterUpdates();

  /** All the distribution updates. */
  public abstract Iterable<MetricUpdate<DistributionData>> distributionUpdates();

  /** All the gauges updates. */
  public abstract Iterable<MetricUpdate<GaugeData>> gaugeUpdates();

  /** All the sets updates. */
  public abstract Iterable<MetricUpdate<StringSetData>> stringSetUpdates();

  /** All the histogram updates. */
  public abstract Iterable<MetricUpdate<HistogramData>> perWorkerHistogramsUpdates();

  /** Create a new {@link MetricUpdates} bundle. */
  public static MetricUpdates create(
      Iterable<MetricUpdate<Long>> counterUpdates,
      Iterable<MetricUpdate<DistributionData>> distributionUpdates,
      Iterable<MetricUpdate<GaugeData>> gaugeUpdates,
      Iterable<MetricUpdate<StringSetData>> stringSetUpdates,
      Iterable<MetricUpdate<HistogramData>> perWorkerHistogramsUpdates) {
    return new AutoValue_MetricUpdates(
        counterUpdates,
        distributionUpdates,
        gaugeUpdates,
        stringSetUpdates,
        perWorkerHistogramsUpdates);
  }

  /** Returns true if there are no updates in this MetricUpdates object. */
  public boolean isEmpty() {
    return Iterables.isEmpty(counterUpdates())
        && Iterables.isEmpty(distributionUpdates())
        && Iterables.isEmpty(gaugeUpdates())
        && Iterables.isEmpty(stringSetUpdates())
        && Iterables.isEmpty(perWorkerHistogramsUpdates());
  }
}
