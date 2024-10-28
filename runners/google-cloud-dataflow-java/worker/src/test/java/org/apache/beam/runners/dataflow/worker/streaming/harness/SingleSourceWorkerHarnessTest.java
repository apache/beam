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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.DataflowWorkerHarnessHelper;
import org.apache.beam.runners.dataflow.worker.WorkerUncaughtExceptionHandler;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.util.TerminatingExecutors;
import org.apache.beam.runners.dataflow.worker.util.common.worker.JvmRuntime;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class SingleSourceWorkerHarnessTest {
  private static final Logger LOG = LoggerFactory.getLogger(SingleSourceWorkerHarnessTest.class);
  private final WorkCommitter workCommitter = mock(WorkCommitter.class);
  private final GetDataClient getDataClient = mock(GetDataClient.class);
  private final HeartbeatSender heartbeatSender = mock(HeartbeatSender.class);
  private final Runnable waitForResources = () -> {};
  private final Function<String, Optional<ComputationState>> computationStateFetcher =
      ignored -> Optional.empty();
  private final StreamingWorkScheduler streamingWorkScheduler = mock(StreamingWorkScheduler.class);

  private SingleSourceWorkerHarness createWorkerHarness(
      SingleSourceWorkerHarness.GetWorkSender getWorkSender, JvmRuntime runtime) {
    return SingleSourceWorkerHarness.builder()
        .setWorkCommitter(workCommitter)
        .setGetDataClient(getDataClient)
        .setHeartbeatSender(heartbeatSender)
        .setWaitForResources(waitForResources)
        .setStreamingWorkScheduler(streamingWorkScheduler)
        .setComputationStateFetcher(computationStateFetcher)
        .setGetWorkSender(getWorkSender)
        .setWorkProviderExecutor(
            TerminatingExecutors.newSingleThreadedExecutorForTesting(
                runtime,
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setPriority(Thread.MIN_PRIORITY)
                    .setNameFormat("DispatchThread"),
                LOG))
        .build();
  }

  @Test
  public void testDispatchLoop_unexpectedFailureKillsJvm_appliance() {
    SingleSourceWorkerHarness.GetWorkSender getWorkSender =
        SingleSourceWorkerHarness.GetWorkSender.forAppliance(
            () -> {
              throw new RuntimeException("something bad happened");
            });

    FakeJvmRuntime fakeJvmRuntime = new FakeJvmRuntime();
    createWorkerHarness(getWorkSender, fakeJvmRuntime).start();
    assertTrue(fakeJvmRuntime.waitForRuntimeDeath(5, TimeUnit.SECONDS));
    fakeJvmRuntime.assertJvmTerminated();
  }

  @Test
  public void testDispatchLoop_unexpectedFailureKillsJvm_streamingEngine() {
    SingleSourceWorkerHarness.GetWorkSender getWorkSender =
        SingleSourceWorkerHarness.GetWorkSender.forStreamingEngine(
            workItemReceiver -> {
              throw new RuntimeException("something bad happened");
            });

    FakeJvmRuntime fakeJvmRuntime = new FakeJvmRuntime();
    createWorkerHarness(getWorkSender, fakeJvmRuntime).start();
    assertTrue(fakeJvmRuntime.waitForRuntimeDeath(5, TimeUnit.SECONDS));
    fakeJvmRuntime.assertJvmTerminated();
  }

  private static class FakeJvmRuntime implements JvmRuntime {
    private final CountDownLatch haltedLatch = new CountDownLatch(1);
    private volatile int exitStatus = 0;

    @Override
    public void halt(int status) {
      exitStatus = status;
      haltedLatch.countDown();
    }

    public boolean waitForRuntimeDeath(long timeout, TimeUnit unit) {
      try {
        return haltedLatch.await(timeout, unit);
      } catch (InterruptedException e) {
        return false;
      }
    }

    private void assertJvmTerminated() {
      assertThat(exitStatus).isEqualTo(WorkerUncaughtExceptionHandler.JVM_TERMINATED_STATUS_CODE);
    }
  }
}
