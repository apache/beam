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
import org.apache.beam.sdk.util.HistogramData;

/** Implementation of {@link Histogram} that delegates to the instance for the current context. */
@Internal
public class DelegatingHistogram implements Metric, Histogram, Serializable {
  private final MetricName name;
  private final HistogramData.BucketType bucketType;
  private final boolean processWideContainer;

  public DelegatingHistogram(
      MetricName name, HistogramData.BucketType bucketType, boolean processWideContainer) {
    this.name = name;
    this.bucketType = bucketType;
    this.processWideContainer = processWideContainer;
  }

  @Override
  public void update(double value) {
    MetricsContainer container =
        processWideContainer
            ? MetricsEnvironment.getProcessWideContainer()
            : MetricsEnvironment.getCurrentContainer();
    if (container != null) {
      container.getHistogram(name, bucketType).update(value);
    }
  }

  @Override
  public MetricName getName() {
    return name;
  }
}
