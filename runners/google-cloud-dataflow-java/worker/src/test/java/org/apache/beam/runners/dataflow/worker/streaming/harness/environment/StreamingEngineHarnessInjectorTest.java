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
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEngineComputationConfigFetcher;
import org.apache.beam.runners.dataflow.worker.streaming.harness.SingleSourceWorkerHarness;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingEngineWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.StreamPoolGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ThrottlingGetDataMetricTracker;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingEngineHarnessInjectorTest {

  @Test
  public void testCreatesWorkerHarnessInjectorForStreamingEngine() {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    options.setJobId("jobId");
    options.setProject("project");
    options.setWorkerId("worker");
    WorkerHarnessInjector injector =
        StreamingEngineHarnessInjector.builder()
            .setDataflowServiceClient(mock(WorkUnitClient.class))
            .setMemoryMonitor(mock(MemoryMonitor.class))
            .setClientId(1)
            .setClock(Instant::now)
            .setGetDataMetricTracker(new ThrottlingGetDataMetricTracker(mock(MemoryMonitor.class)))
            .setHotKeyLogger(new HotKeyLogger())
            .setMapTaskExecutorFactory(IntrinsicMapTaskExecutorFactory.defaultFactory())
            .setOptions(options)
            .setRetryLocallyDelayMs(-1)
            .build();

    assertThat(injector.configFetcher())
        .isInstanceOf(StreamingEngineComputationConfigFetcher.class);
    assertThat(injector.getDataClient()).isInstanceOf(StreamPoolGetDataClient.class);
    assertThat(injector.harness()).isInstanceOf(SingleSourceWorkerHarness.class);
    assertThat(injector.workCommitter()).isInstanceOf(StreamingEngineWorkCommitter.class);
  }
}
