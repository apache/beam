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

import java.io.Closeable;
import java.io.IOException;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Util for invoking {@link Source.Reader} methods that might require a {@link MetricsContainerImpl}
 * to be active. Source.Reader decorator which registers {@link MetricsContainerImpl}. It update
 * metrics to Flink metric and accumulator in start and advance.
 */
public class ReaderInvocationUtil<OutputT, ReaderT extends Source.Reader<OutputT>> {

  private final String stepName;
  private final FlinkMetricContainerBase container;
  private final Boolean enableMetrics;

  public ReaderInvocationUtil(
      String stepName, PipelineOptions options, FlinkMetricContainerBase container) {
    FlinkPipelineOptions flinkPipelineOptions = options.as(FlinkPipelineOptions.class);
    this.stepName = stepName;
    this.enableMetrics = !flinkPipelineOptions.getDisableMetrics();
    this.container = container;
  }

  public boolean invokeStart(ReaderT reader) throws IOException {
    if (enableMetrics) {
      try (Closeable ignored =
          MetricsEnvironment.scopedMetricsContainer(container.getMetricsContainer(stepName))) {
        boolean result = reader.start();
        container.updateMetrics(stepName);
        return result;
      }
    } else {
      return reader.start();
    }
  }

  public boolean invokeAdvance(ReaderT reader) throws IOException {
    if (enableMetrics) {
      try (Closeable ignored =
          MetricsEnvironment.scopedMetricsContainer(container.getMetricsContainer(stepName))) {
        boolean result = reader.advance();
        container.updateMetrics(stepName);
        return result;
      }
    } else {
      return reader.advance();
    }
  }
}
