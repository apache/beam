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
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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
                .build(),
            /*useFairMonitor=*/ false,
            /*useKeyGroupWorkQueue=*/ false);

    return WorkFailureProcessor.forTesting(workExecutor, failureTracker, Optional::empty, clock, 0);
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
    WorkItem workItem = WorkItem.newBuilder().setKey(ByteString.EMPTY).setWorkToken(1L).build();
    return ExecutableWork.create(
        Work.create(
            workItem,
            workItem.getSerializedSize(),
            Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
            Work.createProcessingContext(
                "computationId",
                new FakeGetDataClient(),
                ignored -> {},
                mock(HeartbeatSender.class)),
            false,
            clock,
            ImmutableList.of()),
        (work, handle) -> {
          processWorkFn.accept(work);
        });
  }

  private static ExecutableWork createWork(Consumer<Work> processWorkFn) {
    return createWork(Instant::now, processWorkFn);
  }

  @Test
  public void logAndProcessFailureBatch_doesNotRetryFailedWork() throws Throwable {
    Set<Work> executedWork = new HashSet<>();
    ExecutableWork work = createWork(executedWork::add);
    work.work().setFailed();
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingEngineFailureReporter());
    Set<Work> invalidWork = new HashSet<>();
    workFailureProcessor.logAndProcessFailureBatch(
        DEFAULT_COMPUTATION_ID, List.of(work), new RuntimeException(), invalidWork::add);

    assertThat(executedWork).isEmpty();
    assertThat(invalidWork).containsExactly(work.work());
  }

  @Test
  public void logAndProcessFailureBatch_doesNotRetryOOM() {
    Set<Work> executedWork = new HashSet<>();
    ExecutableWork work = createWork(executedWork::add);
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingEngineFailureReporter());
    Set<Work> invalidWork = new HashSet<>();
    assertThrows(
        OutOfMemoryError.class,
        () ->
            workFailureProcessor.logAndProcessFailureBatch(
                DEFAULT_COMPUTATION_ID,
                Arrays.asList(work),
                new OutOfMemoryError(),
                invalidWork::add));

    assertThat(executedWork).isEmpty();
  }

  @Test
  public void logAndProcessFailureBatch_doesNotRetryWhenFailureReporterMarksAsNonRetryable()
      throws Throwable {
    Set<Work> executedWork = new HashSet<>();
    ExecutableWork work = createWork(executedWork::add);
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingApplianceFailureReporter(true));
    Set<Work> invalidWork = new HashSet<>();
    workFailureProcessor.logAndProcessFailureBatch(
        DEFAULT_COMPUTATION_ID, Arrays.asList(work), new RuntimeException(), invalidWork::add);

    assertThat(executedWork).isEmpty();
    assertThat(invalidWork).containsExactly(work.work());
  }

  @Test
  public void logAndProcessFailureBatch_doesNotRetryAfterLocalRetryTimeout() throws Throwable {
    Set<Work> executedWork = new HashSet<>();
    ExecutableWork veryOldWork =
        createWork(() -> Instant.now().minus(Duration.standardDays(30)), executedWork::add);
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingEngineFailureReporter());
    Set<Work> invalidWork = new HashSet<>();
    workFailureProcessor.logAndProcessFailureBatch(
        DEFAULT_COMPUTATION_ID,
        Arrays.asList(veryOldWork),
        new RuntimeException(),
        invalidWork::add);

    assertThat(executedWork).isEmpty();
    assertThat(invalidWork).contains(veryOldWork.work());
  }

  @Test
  public void logAndProcessFailureBatch_retriesOnUncaughtUnhandledException_streamingEngine()
      throws Throwable {
    CountDownLatch runWork = new CountDownLatch(1);
    ExecutableWork work = createWork(ignored -> runWork.countDown());
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingEngineFailureReporter());
    Set<Work> invalidWork = new HashSet<>();
    workFailureProcessor.logAndProcessFailureBatch(
        DEFAULT_COMPUTATION_ID, Arrays.asList(work), new RuntimeException(), invalidWork::add);

    runWork.await();
    assertThat(invalidWork).isEmpty();
  }

  @Test
  public void logAndProcessFailureBatch_retriesOnUncaughtUnhandledException_streamingAppliance()
      throws Throwable {
    CountDownLatch runWork = new CountDownLatch(1);
    ExecutableWork work = createWork(ignored -> runWork.countDown());
    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingApplianceFailureReporter(false));
    Set<Work> invalidWork = new HashSet<>();
    workFailureProcessor.logAndProcessFailureBatch(
        DEFAULT_COMPUTATION_ID, Arrays.asList(work), new RuntimeException(), invalidWork::add);

    runWork.await();
    assertThat(invalidWork).isEmpty();
  }

  @Test
  public void logAndProcessFailureBatch_retryAll() throws Throwable {
    CountDownLatch runWork1 = new CountDownLatch(1);
    CountDownLatch runWork2 = new CountDownLatch(1);
    ExecutableWork work1 = createWork(ignored -> runWork1.countDown());
    ExecutableWork work2 = createWork(ignored -> runWork2.countDown());

    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingEngineFailureReporter());
    Set<Work> invalidWork = new HashSet<>();

    workFailureProcessor.logAndProcessFailureBatch(
        DEFAULT_COMPUTATION_ID,
        Arrays.asList(work1, work2),
        new RuntimeException(),
        invalidWork::add);

    runWork1.await();
    runWork2.await();
    assertThat(invalidWork).isEmpty();
  }

  @Test
  public void logAndProcessFailureBatch_mixRetryAndAbort() throws Throwable {
    CountDownLatch runWork1 = new CountDownLatch(1);
    Set<Work> executedWork2 = new HashSet<>();
    ExecutableWork work1 = createWork(ignored -> runWork1.countDown());
    ExecutableWork work2 = createWork(executedWork2::add);
    work2.work().setFailed();

    WorkFailureProcessor workFailureProcessor =
        createWorkFailureProcessor(streamingEngineFailureReporter());
    Set<Work> invalidWork = new HashSet<>();

    workFailureProcessor.logAndProcessFailureBatch(
        DEFAULT_COMPUTATION_ID,
        Arrays.asList(work1, work2),
        new RuntimeException(),
        invalidWork::add);

    runWork1.await();
    assertThat(executedWork2).isEmpty();
    assertThat(invalidWork).containsExactly(work2.work());
  }
}
