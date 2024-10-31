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
package org.apache.beam.runners.dataflow.worker.streaming.harness.environment;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingApplianceComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEngineComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.harness.SingleSourceWorkerHarness;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingApplianceWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingEngineWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ApplianceGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.StreamPoolGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ThrottlingGetDataMetricTracker;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WorkerHarnessInjectorTest {
  private static DataflowWorkerHarnessOptions testOptions() {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    options.setWindmillServiceEndpoint("windmill-service.com");
    options.setJobId("jobId");
    options.setProject("project");
    options.setWorkerId("worker");
    return options;
  }

  @Test
  public void testCreateHarnessInjector_streamingEngine() {
    DataflowWorkerHarnessOptions testOptions = testOptions();
    testOptions.setEnableStreamingEngine(true);
    WorkerHarnessInjector injector =
        WorkerHarnessInjector.createHarnessInjector(testOptions, mock(WorkUnitClient.class))
            .setGetDataMetricTracker(new ThrottlingGetDataMetricTracker(mock(MemoryMonitor.class)))
            .setMemoryMonitor(mock(MemoryMonitor.class))
            .setClientId(1)
            .build();
    assertThat(injector.configFetcher())
        .isInstanceOf(StreamingEngineComputationConfigFetcher.class);
    assertThat(injector.getDataClient()).isInstanceOf(StreamPoolGetDataClient.class);
    assertThat(injector.harness()).isInstanceOf(SingleSourceWorkerHarness.class);
    assertThat(injector.workCommitter()).isInstanceOf(StreamingEngineWorkCommitter.class);
  }

  @Test
  public void testCreateHarnessInjector_appliance() {
    DataflowWorkerHarnessOptions testOptions = testOptions();
    testOptions.setEnableStreamingEngine(false);
    WorkerHarnessInjector injector =
        WorkerHarnessInjector.createHarnessInjector(testOptions, mock(WorkUnitClient.class))
            .setGetDataMetricTracker(new ThrottlingGetDataMetricTracker(mock(MemoryMonitor.class)))
            .setMemoryMonitor(mock(MemoryMonitor.class))
            .setClientId(1)
            .build();
    assertThat(injector.configFetcher())
        .isInstanceOf(StreamingApplianceComputationConfigFetcher.class);
    assertThat(injector.getDataClient()).isInstanceOf(ApplianceGetDataClient.class);
    assertThat(injector.harness()).isInstanceOf(SingleSourceWorkerHarness.class);
    assertThat(injector.workCommitter()).isInstanceOf(StreamingApplianceWorkCommitter.class);
  }
}
