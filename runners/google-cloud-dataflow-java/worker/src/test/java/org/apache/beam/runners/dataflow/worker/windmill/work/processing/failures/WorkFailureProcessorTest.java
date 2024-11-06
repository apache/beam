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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.KeyTokenInvalidException;
import org.apache.beam.runners.dataflow.worker.WorkItemCancelledException;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WorkFailureProcessorTest {

  private static final String DEFAULT_COMPUTATION_ID = "computationId";

  private static WorkFailureProcessor createWorkFailureProcessor(
      FailureTracker failureTracker, Supplier<Instant> clock) {
    BoundedQueueExecutor workExecutor =
        new BoundedQueueExecutor(
            1,
            60,
            TimeUnit.SECONDS,
            1,
            10000000,
            new ThreadFactoryBuilder()
                .setNameFormat("DataflowWorkUnits-%d")
                .setDaemon(true)
                .build());
    return WorkFailureProcessor.builder()
        .setWorkUnitExecutor(workExecutor)
        .setFailureTracker(failureTracker)
        .setHeapDumper(Optional::empty)
        .setClock(clock)
        .setRetryLocallyDelayMs(0)
        .build();
  }

  private static WorkFailureProcessor createWorkFailureProcessor(FailureTracker failureTracker) {
    return createWorkFailureProcessor(failureTracker, Instant::now);
  }

  private static FailureTracker streamingEngineFailureReporter() {
    return StreamingEngineFailureTracker.create(10, 10);
  }

  private static FailureTracker streamingApplianceFailureReporter(boolean isWorkFailed) {
    return StreamingApplianceFailureTracker.create(
        10,
        10,
        ignored -> Windmill.ReportStatsResponse.newBuilder().setFailed(isWorkFailed).build());
  }

  private static ExecutableWork createWork(Supplier<Instant> clock, Consumer<Work> processWorkFn) {
    return ExecutableWork.create(
        Work.create(
            Windmill.WorkItem.newBuilder().setKey(ByteString.EMPTY).setWorkToken(1L).build(),
            Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
            Work.createProcessingContext(
                "computationId",
                new FakeGetDataClient(),
                ignored -> {},
                mock(HeartbeatSender.class)),
            clock,
            new ArrayList<>()),
        processWorkFn);
  }

  private static ExecutableWork createWork(Consumer<Work> processWorkFn) {
    return createWork(Instant::now, processWorkFn);
  }

  @Test
  public void logAndProcessFailure_doesNotRetryKeyTokenInvalidException() {
    Set<Work> executedWork = new HashSet<>();
    ExecutableWork work = createWork(executedWork::add);
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingEngineFailureReporter());
    Set<Work> invalidWork = new HashSet<>();
    workFailureProcessor.logAndProcessFailure(
        DEFAULT_COMPUTATION_ID, work, new KeyTokenInvalidException("key"), invalidWork::add);

    assertThat(executedWork).isEmpty();
    assertThat(invalidWork).containsExactly(work.work());
  }

  @Test
  public void logAndProcessFailure_doesNotRetryWhenWorkItemCancelled() {
    Set<Work> executedWork = new HashSet<>();
    ExecutableWork work = createWork(executedWork::add);
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingEngineFailureReporter());
    Set<Work> invalidWork = new HashSet<>();
    workFailureProcessor.logAndProcessFailure(
        DEFAULT_COMPUTATION_ID,
        work,
        new WorkItemCancelledException(work.getWorkItem().getShardingKey()),
        invalidWork::add);

    assertThat(executedWork).isEmpty();
    assertThat(invalidWork).containsExactly(work.work());
  }

  @Test
  public void logAndProcessFailure_doesNotRetryOOM() {
    Set<Work> executedWork = new HashSet<>();
    ExecutableWork work = createWork(executedWork::add);
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingEngineFailureReporter());
    Set<Work> invalidWork = new HashSet<>();
    workFailureProcessor.logAndProcessFailure(
        DEFAULT_COMPUTATION_ID, work, new OutOfMemoryError(), invalidWork::add);

    assertThat(executedWork).isEmpty();
    assertThat(invalidWork).containsExactly(work.work());
  }

  @Test
  public void logAndProcessFailure_doesNotRetryWhenFailureReporterMarksAsNonRetryable() {
    Set<Work> executedWork = new HashSet<>();
    ExecutableWork work = createWork(executedWork::add);
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingApplianceFailureReporter(true));
    Set<Work> invalidWork = new HashSet<>();
    workFailureProcessor.logAndProcessFailure(
        DEFAULT_COMPUTATION_ID, work, new RuntimeException(), invalidWork::add);

    assertThat(executedWork).isEmpty();
    assertThat(invalidWork).containsExactly(work.work());
  }

  @Test
  public void logAndProcessFailure_doesNotRetryAfterLocalRetryTimeout() {
    Set<Work> executedWork = new HashSet<>();
    ExecutableWork veryOldWork =
        createWork(() -> Instant.now().minus(Duration.standardDays(30)), executedWork::add);
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingEngineFailureReporter());
    Set<Work> invalidWork = new HashSet<>();
    workFailureProcessor.logAndProcessFailure(
        DEFAULT_COMPUTATION_ID, veryOldWork, new RuntimeException(), invalidWork::add);

    assertThat(executedWork).isEmpty();
    assertThat(invalidWork).contains(veryOldWork.work());
  }

  @Test
  public void logAndProcessFailure_retriesOnUncaughtUnhandledException_streamingEngine()
      throws InterruptedException {
    CountDownLatch runWork = new CountDownLatch(1);
    ExecutableWork work = createWork(ignored -> runWork.countDown());
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingEngineFailureReporter());
    Set<Work> invalidWork = new HashSet<>();
    workFailureProcessor.logAndProcessFailure(
        DEFAULT_COMPUTATION_ID, work, new RuntimeException(), invalidWork::add);

    runWork.await();
    assertThat(invalidWork).isEmpty();
  }

  @Test
  public void logAndProcessFailure_retriesOnUncaughtUnhandledException_streamingAppliance()
      throws InterruptedException {
    CountDownLatch runWork = new CountDownLatch(1);
    ExecutableWork work = createWork(ignored -> runWork.countDown());
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingApplianceFailureReporter(false));
    Set<Work> invalidWork = new HashSet<>();
    workFailureProcessor.logAndProcessFailure(
        DEFAULT_COMPUTATION_ID, work, new RuntimeException(), invalidWork::add);

    runWork.await();
    assertThat(invalidWork).isEmpty();
  }
}
