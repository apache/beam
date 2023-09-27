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
package org.apache.beam.runners.flink.metrics;

import org.apache.flink.metrics.MetricGroup;

/**
 * The base helper class for holding a {@link
 * org.apache.beam.runners.core.metrics.MetricsContainerImpl MetricsContainerImpl} and forwarding
 * Beam metrics to Flink accumulators and metrics. This class is used when {@link
 * org.apache.flink.api.common.functions.RuntimeContext Flink RuntimeContext} is not available.
 *
 * @see FlinkMetricContainer
 */
public class FlinkMetricContainerWithoutAccumulator extends FlinkMetricContainerBase {
  public FlinkMetricContainerWithoutAccumulator(MetricGroup metricGroup) {
    super(metricGroup);
  }
}
