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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SingleSourceWorkerHarnessTest {
  private final WorkCommitter workCommitter = mock(WorkCommitter.class);
  private final GetDataClient getDataClient = mock(GetDataClient.class);
  private final HeartbeatSender heartbeatSender = mock(HeartbeatSender.class);
  private final Runnable waitForResources = () -> {};
  private final Function<String, Optional<ComputationState>> computationStateFetcher =
      ignored -> Optional.empty();
  private final StreamingWorkScheduler streamingWorkScheduler = mock(StreamingWorkScheduler.class);

  private SingleSourceWorkerHarness createWorkerHarness(
      SingleSourceWorkerHarness.GetWorkSender getWorkSender) {
    return SingleSourceWorkerHarness.builder()
        .setWorkCommitter(workCommitter)
        .setGetDataClient(getDataClient)
        .setHeartbeatSender(heartbeatSender)
        .setWaitForResources(waitForResources)
        .setStreamingWorkScheduler(streamingWorkScheduler)
        .setComputationStateFetcher(computationStateFetcher)
        .setGetWorkSender(getWorkSender)
        .build();
  }

  @Test
  public void testDispatchLoopFailureThrowsException_appliance() {
    RuntimeException expected = new RuntimeException("something bad happened");
    SingleSourceWorkerHarness.GetWorkSender getWorkSender =
        SingleSourceWorkerHarness.GetWorkSender.forAppliance(
            () -> {
              throw expected;
            });

    SingleSourceWorkerHarness harness = createWorkerHarness(getWorkSender);
    Throwable actual = assertThrows(AssertionError.class, harness::start);
    assertThat(actual).hasCauseThat().hasCauseThat().isEqualTo(expected);
  }

  @Test
  public void testDispatchLoopFailureThrowsException_streamingEngine() {
    RuntimeException expected = new RuntimeException("something bad happened");
    SingleSourceWorkerHarness.GetWorkSender getWorkSender =
        SingleSourceWorkerHarness.GetWorkSender.forStreamingEngine(
            workItemReceiver -> {
              throw expected;
            });
    SingleSourceWorkerHarness harness = createWorkerHarness(getWorkSender);
    Throwable actual = assertThrows(AssertionError.class, harness::start);
    assertThat(actual).hasCauseThat().hasCauseThat().isEqualTo(expected);
  }
}
