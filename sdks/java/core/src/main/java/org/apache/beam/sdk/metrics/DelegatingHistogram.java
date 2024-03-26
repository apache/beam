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

import java.io.Serializable;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.HistogramData;

/** Implementation of {@link Histogram} that delegates to the instance for the current context. */
@Internal
public class DelegatingHistogram implements Metric, Histogram, Serializable {
  private final MetricName name;
  private final HistogramData.BucketType bucketType;
  private final boolean processWideContainer;
  private final boolean perWorkerHistogram;

  /**
   * Create a {@code DelegatingHistogram} with {@code perWorkerHistogram} set to false.
   *
   * @param name Metric name for this metric.
   * @param bucketType Histogram bucketing strategy.
   * @param processWideContainer Whether this Counter is stored in the ProcessWide container or the
   *     current thread's container.
   */
  public DelegatingHistogram(
      MetricName name, HistogramData.BucketType bucketType, boolean processWideContainer) {
    this(name, bucketType, processWideContainer, false);
  }

  /**
   * @param name Metric name for this metric.
   * @param bucketType Histogram bucketing strategy.
   * @param processWideContainer Whether this Counter is stored in the ProcessWide container or the
   *     current thread's container.
   * @param perWorkerHistogram Whether this Histogram refers to a perWorker metric or not.
   */
  public DelegatingHistogram(
      MetricName name,
      HistogramData.BucketType bucketType,
      boolean processWideContainer,
      boolean perWorkerHistogram) {
    this.name = name;
    this.bucketType = bucketType;
    this.processWideContainer = processWideContainer;
    this.perWorkerHistogram = perWorkerHistogram;
  }

  private Optional<Histogram> getHistogram() {
    MetricsContainer container =
        processWideContainer
            ? MetricsEnvironment.getProcessWideContainer()
            : MetricsEnvironment.getCurrentContainer();
    if (container == null) {
      return Optional.empty();
    }
    if (perWorkerHistogram) {
      return Optional.of(container.getPerWorkerHistogram(name, bucketType));
    } else {
      return Optional.of(container.getHistogram(name, bucketType));
    }
  }

  @Override
  public void update(double value) {
    getHistogram().ifPresent(histogram -> histogram.update(value));
  }

  @Override
  public void update(double... values) {
    getHistogram().ifPresent(histogram -> histogram.update(values));
  }

  @Override
  public MetricName getName() {
    return name;
  }
}
