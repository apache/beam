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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.StreamingScalingReportResponse;
import com.google.api.services.dataflow.model.WorkerMessageResponse;
import java.util.Collections;
import java.util.concurrent.Executors;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class StreamingWorkerStatusReporterTest {
  private final long DEFAULT_WINDMILL_QUOTA_THROTTLE_TIME = 1000;
  private final long DEFAULT_HARNESS_REPORTING_PERIOD = 10000;
  private final long DEFAULT_PER_WORKER_METRICS_PERIOD = 30000;

  private BoundedQueueExecutor mockExecutor;
  private WorkUnitClient mockWorkUnitClient;
  private FailureTracker mockFailureTracker;
  private MemoryMonitor mockMemoryMonitor;

  @Before
  public void setUp() {
    this.mockExecutor = mock(BoundedQueueExecutor.class);
    this.mockWorkUnitClient = mock(WorkUnitClient.class);
    this.mockFailureTracker = mock(FailureTracker.class);
    this.mockMemoryMonitor = mock(MemoryMonitor.class);
  }

  @Test
  public void testOverrideMaximumThreadCount() throws Exception {
    StreamingWorkerStatusReporter reporter =
        StreamingWorkerStatusReporter.builder()
            .setPublishCounters(true)
            .setDataflowServiceClient(mockWorkUnitClient)
            .setWindmillQuotaThrottleTime(() -> DEFAULT_WINDMILL_QUOTA_THROTTLE_TIME)
            .setAllStageInfo(Collections::emptyList)
            .setFailureTracker(mockFailureTracker)
            .setStreamingCounters(StreamingCounters.create())
            .setMemoryMonitor(mockMemoryMonitor)
            .setWorkExecutor(mockExecutor)
            .setExecutorFactory((threadName) -> Executors.newSingleThreadScheduledExecutor())
            .setWindmillHarnessUpdateReportingPeriodMillis(DEFAULT_HARNESS_REPORTING_PERIOD)
            .setPerWorkerMetricsUpdateReportingPeriodMillis(DEFAULT_PER_WORKER_METRICS_PERIOD)
            .build();
    StreamingScalingReportResponse streamingScalingReportResponse =
        new StreamingScalingReportResponse().setMaximumThreadCount(10);
    WorkerMessageResponse workerMessageResponse =
        new WorkerMessageResponse()
            .setStreamingScalingReportResponse(streamingScalingReportResponse);
    when(mockWorkUnitClient.reportWorkerMessage(any()))
        .thenReturn(Collections.singletonList(workerMessageResponse));
    reporter.reportPeriodicWorkerMessage();
    verify(mockExecutor).setMaximumPoolSize(10, 110);
  }

  @Test
  public void testHandleEmptyWorkerMessageResponse() throws Exception {
    StreamingWorkerStatusReporter reporter =
        StreamingWorkerStatusReporter.builder()
            .setPublishCounters(true)
            .setDataflowServiceClient(mockWorkUnitClient)
            .setWindmillQuotaThrottleTime(() -> DEFAULT_WINDMILL_QUOTA_THROTTLE_TIME)
            .setAllStageInfo(Collections::emptyList)
            .setFailureTracker(mockFailureTracker)
            .setStreamingCounters(StreamingCounters.create())
            .setMemoryMonitor(mockMemoryMonitor)
            .setWorkExecutor(mockExecutor)
            .setExecutorFactory((threadName) -> Executors.newSingleThreadScheduledExecutor())
            .setWindmillHarnessUpdateReportingPeriodMillis(DEFAULT_HARNESS_REPORTING_PERIOD)
            .setPerWorkerMetricsUpdateReportingPeriodMillis(DEFAULT_PER_WORKER_METRICS_PERIOD)
            .build();
    when(mockWorkUnitClient.reportWorkerMessage(any()))
        .thenReturn(Collections.singletonList(new WorkerMessageResponse()));
    reporter.reportPeriodicWorkerMessage();
    verify(mockExecutor, Mockito.times(0)).setMaximumPoolSize(anyInt(), anyInt());
  }
}
