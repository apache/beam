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
import java.util.Set;
import org.apache.beam.sdk.metrics.MetricName;

/**
 * Value class to represent Metric name and percentiles together. {@link MetricsContainerImpl} uses
 * a map of this key and the Distribution Metric.
 */
@AutoValue
public abstract class DistributionMetricKey implements Serializable {
  public abstract MetricName getMetricName();

  public abstract Set<Double> getPercentiles();

  public static DistributionMetricKey create(MetricName metricName, Set<Double> percentiles) {
    return new AutoValue_DistributionMetricKey(metricName, percentiles);
  }
}
