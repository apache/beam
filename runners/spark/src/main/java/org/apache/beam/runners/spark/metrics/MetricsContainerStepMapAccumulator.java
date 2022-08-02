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
package org.apache.beam.runners.spark.metrics;

import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.spark.util.AccumulatorV2;

/** {@link AccumulatorV2} implementation for {@link MetricsContainerStepMap}. */
public class MetricsContainerStepMapAccumulator
    extends AccumulatorV2<MetricsContainerStepMap, MetricsContainerStepMap> {
  private static final MetricsContainerStepMap empty = new SparkMetricsContainerStepMap();

  private MetricsContainerStepMap value;

  public MetricsContainerStepMapAccumulator(MetricsContainerStepMap value) {
    this.value = value;
  }

  @Override
  public boolean isZero() {
    return value.equals(empty);
  }

  @Override
  public MetricsContainerStepMapAccumulator copy() {
    MetricsContainerStepMap newContainer = new SparkMetricsContainerStepMap();
    newContainer.updateAll(value);
    return new MetricsContainerStepMapAccumulator(newContainer);
  }

  @Override
  public void reset() {
    this.value = new SparkMetricsContainerStepMap();
  }

  @Override
  public void add(MetricsContainerStepMap other) {
    this.value.updateAll(other);
  }

  @Override
  public void merge(AccumulatorV2<MetricsContainerStepMap, MetricsContainerStepMap> other) {
    this.value.updateAll(other.value());
  }

  @Override
  public MetricsContainerStepMap value() {
    return this.value;
  }
}
