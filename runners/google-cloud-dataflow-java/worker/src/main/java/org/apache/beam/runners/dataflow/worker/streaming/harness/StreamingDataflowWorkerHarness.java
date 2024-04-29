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

package org.apache.beam.runners.dataflow.worker.streaming.harness;

import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.MetricTrackingWindmillServerStub;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingConfigLoader;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEngineConfigLoader;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEnginePipelineConfig;
import org.apache.beam.runners.dataflow.worker.streaming.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.ActiveWorkRefresher;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamingDataflowWorkerHarness {

  private final DataflowWorkerHarnessOptions options;
  private final AtomicBoolean isRunning;
  private final long clientId;
  private final ComputationStateCache computationStateCache;
  private final WindmillServerStub windmillServer;
  private final WorkCommitter workCommitter;
  private final MetricTrackingWindmillServerStub getDataClient;
  private final StreamingWorkScheduler streamingWorkScheduler;
  private final MemoryMonitor memoryMonitor;
  private final StreamingWorkerStatusReporter workerStatusReporter;
  private final StreamingStatusPages statusPages;
  private final ActiveWorkRefresher activeWorkRefresher;
  private final ExecutorService streamingGetWorkExecutor;
  private final ExecutorService memoryMonitorExecutor;
  private final @Nullable StreamingEngineConfigLoader streamingConfigLoader;
}
