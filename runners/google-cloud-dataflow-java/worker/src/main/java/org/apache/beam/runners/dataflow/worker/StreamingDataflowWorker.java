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
package org.apache.beam.runners.dataflow.worker;

import java.util.Optional;
import org.apache.beam.runners.core.metrics.MetricsLogger;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.streaming.harness.ApplianceWorkerHarness;
import org.apache.beam.runners.dataflow.worker.streaming.harness.DispatchedStreamingEngineWorkerHarness;
import org.apache.beam.runners.dataflow.worker.streaming.harness.FanOutStreamingEngineWorkerHarness;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerHarness;
import org.apache.beam.sdk.fn.JvmInitializers;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Main method for starting {@link StreamingWorkerHarness}. */
public final class StreamingDataflowWorker {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingDataflowWorker.class);
  private static final String ENABLE_IPV6_EXPERIMENT = "enable_private_ipv6_google_access";
  private static final String BEAM_FN_API_EXPERIMENT = "beam_fn_api";

  public static void main(String[] args) throws Exception {
    JvmInitializers.runOnStartup();
    DataflowWorkerHarnessHelper.initializeLogging(StreamingDataflowWorker.class);
    DataflowWorkerHarnessOptions options =
        DataflowWorkerHarnessHelper.initializeGlobalStateAndPipelineOptions(
            StreamingDataflowWorker.class, DataflowWorkerHarnessOptions.class);
    DataflowWorkerHarnessHelper.configureLogging(options);
    validateWorkerOptions(options);

    LOG.info("Creating StreamingWorkerHarness from options: {}", options);
    StreamingWorkerHarness worker = createStreamingWorkerHarness(options);

    // Use the MetricsLogger container which is used by BigQueryIO to periodically log process-wide
    // metrics.
    MetricsEnvironment.setProcessWideContainer(MetricsLogger.createUnboundedMetricsLogger());
    JvmInitializers.runBeforeProcessing(options);

    LOG.info("Starting worker harness in mode={}.", worker.mode());
    worker.start();
  }

  private static boolean isDirectPathPipeline(DataflowWorkerHarnessOptions options) {
    return options.isEnableStreamingEngine()
        && options.getIsWindmillServiceDirectPathEnabled()
        && Optional.ofNullable(options.getDataflowServiceOptions())
            .map(dataflowServiceOptions -> dataflowServiceOptions.contains(ENABLE_IPV6_EXPERIMENT))
            .orElse(false);
  }

  private static StreamingWorkerHarness createStreamingWorkerHarness(
      DataflowWorkerHarnessOptions options) {
    if (!options.isEnableStreamingEngine()) {
      return ApplianceWorkerHarness.fromOptions(options);
    }

    return isDirectPathPipeline(options)
        ? FanOutStreamingEngineWorkerHarness.fromOptions(options)
        : DispatchedStreamingEngineWorkerHarness.fromOptions(options);
  }

  private static void validateWorkerOptions(DataflowWorkerHarnessOptions options) {
    Preconditions.checkArgument(
        options.isStreaming(),
        "%s instantiated with options indicating batch use",
        StreamingDataflowWorker.class.getName());

    Preconditions.checkArgument(
        !DataflowRunner.hasExperiment(options, BEAM_FN_API_EXPERIMENT),
        "%s cannot be main() class with beam_fn_api enabled",
        StreamingDataflowWorker.class.getSimpleName());
  }
}
