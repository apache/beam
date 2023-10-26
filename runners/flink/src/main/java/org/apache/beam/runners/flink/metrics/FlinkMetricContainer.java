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

import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for holding a {@link MetricsContainerImpl} and forwarding Beam metrics to Flink
 * accumulators and metrics.
 *
 * <p>Using accumulators can be turned off because it is memory and network intensive. The
 * accumulator results are only meaningful in batch applications or testing streaming applications
 * which have a defined end. They are not essential during execution because metrics will also be
 * reported using the configured metrics reporter.
 */
public class FlinkMetricContainer extends FlinkMetricContainerBase {
  public static final String ACCUMULATOR_NAME = "__metricscontainers";
  private static final Logger LOG = LoggerFactory.getLogger(FlinkMetricContainer.class);

  private final RuntimeContext runtimeContext;

  public FlinkMetricContainer(RuntimeContext runtimeContext) {
    super(runtimeContext.getMetricGroup());
    this.runtimeContext = runtimeContext;
  }

  /**
   * This should be called at the end of the Flink job and sets up an accumulator to push the
   * metrics to the PipelineResult. This should not be called beforehand, to avoid the overhead
   * which accumulators cause at runtime.
   */
  public void registerMetricsForPipelineResult() {
    Accumulator<MetricsContainerStepMap, MetricsContainerStepMap> metricsAccumulator =
        runtimeContext.getAccumulator(ACCUMULATOR_NAME);
    if (metricsAccumulator == null) {
      metricsAccumulator = new MetricsAccumulator();
      try {
        runtimeContext.addAccumulator(ACCUMULATOR_NAME, metricsAccumulator);
      } catch (UnsupportedOperationException e) {
        // Not supported in all environments, e.g. tests
      } catch (Exception e) {
        LOG.error("Failed to create metrics accumulator.", e);
      }
    }
    metricsAccumulator.add(metricsContainers);
  }
}
