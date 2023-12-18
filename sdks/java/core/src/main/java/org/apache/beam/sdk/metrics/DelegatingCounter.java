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
import org.apache.beam.sdk.annotations.Internal;

/** Implementation of {@link Counter} that delegates to the instance for the current context. */
@Internal
public class DelegatingCounter implements Metric, Counter, Serializable {
  private final MetricName name;
  private final boolean processWideContainer;
  private final boolean perWorkerCounter;

  /**
   * Create a {@code DelegatingCounter} with {@code perWorkerCounter} and {@code
   * processWideContainer} set to false.
   *
   * @param name Metric name for this metric.
   */
  public DelegatingCounter(MetricName name) {
    this(name, false, false);
  }

  /**
   * Create a {@code DelegatingCounter} with {@code perWorkerCounter} set to false.
   *
   * @param name Metric name for this metric.
   * @param processWideContainer Whether this Counter is stored in the ProcessWide container or the
   *     current thread's container.
   */
  public DelegatingCounter(MetricName name, boolean processWideContainer) {
    this(name, processWideContainer, false);
  }

  /**
   * @param name Metric name for this metric.
   * @param processWideContainer Whether this Counter is stored in the ProcessWide container or the
   *     current thread's container.
   * @param perWorkerCounter Whether this Counter refers to a perWorker metric or not.
   */
  public DelegatingCounter(
      MetricName name, boolean processWideContainer, boolean perWorkerCounter) {
    this.name = name;
    this.processWideContainer = processWideContainer;
    this.perWorkerCounter = perWorkerCounter;
  }

  /** Increment the counter. */
  @Override
  public void inc() {
    inc(1);
  }

  /** Increment the counter by the given amount. */
  @Override
  public void inc(long n) {
    MetricsContainer container =
        this.processWideContainer
            ? MetricsEnvironment.getProcessWideContainer()
            : MetricsEnvironment.getCurrentContainer();
    if (container == null) {
      return;
    }
    if (perWorkerCounter) {
      container.getPerWorkerCounter(name).inc(n);
    } else {
      container.getCounter(name).inc(n);
    }
  }

  /* Decrement the counter. */
  @Override
  public void dec() {
    inc(-1);
  }

  /* Decrement the counter by the given amount. */
  @Override
  public void dec(long n) {
    inc(-1 * n);
  }

  @Override
  public MetricName getName() {
    return name;
  }
}
