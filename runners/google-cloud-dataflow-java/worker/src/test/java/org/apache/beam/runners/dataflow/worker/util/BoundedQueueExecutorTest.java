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
package org.apache.beam.runners.dataflow.worker.util;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor.BoundedQueueExecutorWorkHandleImpl;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Unit tests for {@link org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor}. */
@RunWith(Parameterized.class)
// TODO(https://github.com/apache/beam/issues/21230): Remove when new version of errorprone is
// released (2.11.0)
@SuppressWarnings("unused")
public class BoundedQueueExecutorTest {

  @Parameterized.Parameter public boolean useFairMonitor;

  @Parameterized.Parameters(name = "useFairMonitor = {0}")
  public static Collection<Object[]> useFairMonitor() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  private static final long MAXIMUM_BYTES_OUTSTANDING = 10000000;
  private static final int DEFAULT_MAX_THREADS = 2;
  private static final int DEFAULT_THREAD_EXPIRATION_SEC = 60;
  @Rule public transient Timeout globalTimeout = Timeout.seconds(300);
  private BoundedQueueExecutor executor;

  private static final Work.KeyGroup DEFAULT_KEY_GROUP = Work.KeyGroup.create(1, 2);

  private static ExecutableWork createWork(Consumer<Work> executeWorkFn) {
    return createWorkWithCompId("computationId", executeWorkFn);
  }

  private static ExecutableWork createWorkWithCompId(
      String computationId, Consumer<Work> executeWorkFn) {
    return createWorkWithCompIdAndKeyGroup(computationId, DEFAULT_KEY_GROUP, executeWorkFn);
  }

  private static ExecutableWork createWorkWithCompIdAndKeyGroup(
      String computationId, Work.KeyGroup keyGroup, Consumer<Work> executeWorkFn) {
    WorkItem workItem =
        WorkItem.newBuilder()
            .setKey(ByteString.EMPTY)
            .setShardingKey(1)
            .setWorkToken(33)
            .setCacheToken(1)
            .setKeyGroup(
                Windmill.Uint128Proto.newBuilder()
                    .setHigh(keyGroup.high())
                    .setLow(keyGroup.low())
                    .build())
            .build();
    return ExecutableWork.create(
        Work.create(
            workItem,
            workItem.getSerializedSize(),
            Watermarks.builder().setInputDataWatermark(Instant.now()).build(),
            Work.createProcessingContext(
                computationId, new FakeGetDataClient(), ignored -> {}, mock(HeartbeatSender.class)),
            false,
            Instant::now),
        (work, handle) -> {
          executeWorkFn.accept(work);
        });
  }

  private ExecutableWork createSleepProcessWork(CountDownLatch start, CountDownLatch stop) {
    return createWork(
        ignored -> {
          start.countDown();
          try {
            stop.await();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Before
  public void setUp() {
    this.executor =
        new BoundedQueueExecutor(
            DEFAULT_MAX_THREADS,
            DEFAULT_THREAD_EXPIRATION_SEC,
            TimeUnit.SECONDS,
            DEFAULT_MAX_THREADS + 100,
            MAXIMUM_BYTES_OUTSTANDING,
            new ThreadFactoryBuilder()
                .setNameFormat("DataflowWorkUnits-%d")
                .setDaemon(true)
                .build(),
            useFairMonitor,
            /* useKeyGroupWorkQueue= */ false);
  }

  @Test
  public void testScheduleWorkWhenExceedMaximumPoolSize() throws Exception {
    CountDownLatch processStart1 = new CountDownLatch(1);
    CountDownLatch processStop1 = new CountDownLatch(1);
    CountDownLatch processStart2 = new CountDownLatch(1);
    CountDownLatch processStop2 = new CountDownLatch(1);
    CountDownLatch processStart3 = new CountDownLatch(1);
    CountDownLatch processStop3 = new CountDownLatch(1);
    ExecutableWork m1 = createSleepProcessWork(processStart1, processStop1);
    ExecutableWork m2 = createSleepProcessWork(processStart2, processStop2);
    ExecutableWork m3 = createSleepProcessWork(processStart3, processStop3);

    executor.execute(m1, 1);
    processStart1.await();
    executor.execute(m2, 1);
    processStart2.await();
    // m1 and m2 have started and all threads are occupied so m3 will be queued and not executed.
    executor.execute(m3, 1);
    assertFalse(processStart3.await(1000, TimeUnit.MILLISECONDS));
    assertFalse(executor.executorQueueIsEmpty());

    // Stop m1 so there is an available thread for m3 to run.
    processStop1.countDown();
    processStart3.await();
    // m3 started.
    assertTrue(executor.executorQueueIsEmpty());
    processStop2.countDown();
    processStop3.countDown();
    executor.shutdown();
  }

  @Test
  public void testScheduleWorkWhenExceedMaximumBytesOutstanding() throws Exception {
    CountDownLatch processStart1 = new CountDownLatch(1);
    CountDownLatch processStop1 = new CountDownLatch(1);
    CountDownLatch processStart2 = new CountDownLatch(1);
    CountDownLatch processStop2 = new CountDownLatch(1);
    ExecutableWork m1 = createSleepProcessWork(processStart1, processStop1);
    ExecutableWork m2 = createSleepProcessWork(processStart2, processStop2);

    executor.execute(m1, 10000000);
    processStart1.await();
    // m1 has started and reached the maximumBytesOutstanding. Though the executor has available
    // threads, the new task will be blocked until the bytes are available.
    // Start a new thread since executor.execute() is a blocking function.
    Thread m2Runner =
        new Thread(
            () -> {
              executor.execute(m2, 1000);
            });
    m2Runner.start();
    assertFalse(processStart2.await(1000, TimeUnit.MILLISECONDS));
    // m2 will wait for monitor instead of being queued.
    assertEquals(Thread.State.WAITING, m2Runner.getState());
    assertTrue(executor.executorQueueIsEmpty());

    // Stop m1 so there are available bytes for m2 to run.
    processStop1.countDown();
    // m2 should be able to start execution
    processStart2.await();
    // ensure that the execute() call scheduling m2 returns even if the completion of m2 is blocked.
    m2Runner.join();
    processStop2.countDown();
    executor.shutdown();
  }

  @Test
  public void testOverrideMaximumPoolSize() throws Exception {
    CountDownLatch processStart1 = new CountDownLatch(1);
    CountDownLatch processStart2 = new CountDownLatch(1);
    CountDownLatch processStart3 = new CountDownLatch(1);
    CountDownLatch stop = new CountDownLatch(1);
    ExecutableWork m1 = createSleepProcessWork(processStart1, stop);
    ExecutableWork m2 = createSleepProcessWork(processStart2, stop);
    ExecutableWork m3 = createSleepProcessWork(processStart3, stop);

    // Initial state.
    assertEquals(0, executor.activeCount());
    assertEquals(2, executor.getMaximumPoolSize());

    // m1 and m2 are accepted.
    executor.execute(m1, 1);
    processStart1.await();
    assertEquals(1, executor.activeCount());
    executor.execute(m2, 1);
    processStart2.await();
    assertEquals(2, executor.activeCount());

    // Max pool size was reached so new work is queued.
    executor.execute(m3, 1);
    assertFalse(processStart3.await(1000, TimeUnit.MILLISECONDS));

    // Increase the max thread count
    executor.setMaximumPoolSize(3, 103);
    assertEquals(3, executor.getMaximumPoolSize());

    // m3 is accepted
    processStart3.await();
    assertEquals(3, executor.activeCount());

    stop.countDown();
    executor.shutdown();
  }

  @Test
  public void testRecordTotalTimeMaxActiveThreadsUsed() throws Exception {
    CountDownLatch processStart1 = new CountDownLatch(1);
    CountDownLatch processStart2 = new CountDownLatch(1);
    CountDownLatch processStart3 = new CountDownLatch(1);
    CountDownLatch stop = new CountDownLatch(1);
    ExecutableWork m1 = createSleepProcessWork(processStart1, stop);
    ExecutableWork m2 = createSleepProcessWork(processStart2, stop);
    ExecutableWork m3 = createSleepProcessWork(processStart3, stop);

    // Initial state.
    assertEquals(0, executor.activeCount());
    assertEquals(2, executor.getMaximumPoolSize());

    // m1 and m2 are accepted.
    executor.execute(m1, 1);
    processStart1.await();
    assertEquals(1, executor.activeCount());
    executor.execute(m2, 1);
    processStart2.await();
    assertEquals(2, executor.activeCount());

    // Max pool size was reached so no new work is accepted.
    executor.execute(m3, 1);
    assertFalse(processStart3.await(1000, TimeUnit.MILLISECONDS));

    assertEquals(0L, executor.allThreadsActiveTime());
    stop.countDown();
    while (executor.activeCount() != 0) {
      // Waiting for all threads to be ended.
      Thread.sleep(200);
    }
    // Max pool size was reached so the allThreadsActiveTime() was updated.
    assertThat(executor.allThreadsActiveTime(), greaterThan(0L));

    executor.shutdown();
  }

  @Test
  public void testRecordTotalTimeMaxActiveThreadsUsedWhenMaximumPoolSizeIsIncreased()
      throws Exception {
    CountDownLatch processStart1 = new CountDownLatch(1);
    CountDownLatch processStart2 = new CountDownLatch(1);
    CountDownLatch processStart3 = new CountDownLatch(1);
    CountDownLatch stop = new CountDownLatch(1);
    ExecutableWork m1 = createSleepProcessWork(processStart1, stop);
    ExecutableWork m2 = createSleepProcessWork(processStart2, stop);
    ExecutableWork m3 = createSleepProcessWork(processStart3, stop);

    // Initial state.
    assertEquals(0, executor.activeCount());
    assertEquals(2, executor.getMaximumPoolSize());

    // m1 and m2 are accepted.
    executor.execute(m1, 1);
    processStart1.await();
    assertEquals(1, executor.activeCount());
    executor.execute(m2, 1);
    processStart2.await();
    assertEquals(2, executor.activeCount());

    // Max pool size was reached so no new work is accepted.
    executor.execute(m3, 1);
    assertFalse(processStart3.await(1000, TimeUnit.MILLISECONDS));

    assertEquals(0L, executor.allThreadsActiveTime());
    // Increase the max thread count
    executor.setMaximumPoolSize(5, 105);
    stop.countDown();
    while (executor.activeCount() != 0) {
      // Waiting for all threads to be ended.
      Thread.sleep(200);
    }
    // Max pool size was updated during execution but allThreadsActiveTime() was still recorded
    // for the thread which reached the old max pool size.
    assertThat(executor.allThreadsActiveTime(), greaterThan(0L));

    executor.shutdown();
  }

  @Test
  public void testRecordTotalTimeMaxActiveThreadsUsedWhenMaximumPoolSizeIsReduced()
      throws Exception {
    CountDownLatch processStart1 = new CountDownLatch(1);
    CountDownLatch processStop1 = new CountDownLatch(1);
    CountDownLatch processStart2 = new CountDownLatch(1);
    CountDownLatch processStop2 = new CountDownLatch(1);
    CountDownLatch processStart3 = new CountDownLatch(1);
    CountDownLatch processStop3 = new CountDownLatch(1);
    ExecutableWork m1 = createSleepProcessWork(processStart1, processStop1);
    ExecutableWork m2 = createSleepProcessWork(processStart2, processStop2);
    ExecutableWork m3 = createSleepProcessWork(processStart3, processStop3);

    // Initial state.
    assertEquals(0, executor.activeCount());
    assertEquals(2, executor.getMaximumPoolSize());

    // m1 is accepted.
    executor.execute(m1, 1);
    processStart1.await();
    assertEquals(1, executor.activeCount());
    assertEquals(2, executor.getMaximumPoolSize());
    assertEquals(0L, executor.allThreadsActiveTime());

    processStop1.countDown();
    while (executor.activeCount() != 0) {
      // Waiting for all threads to be ended.
      Thread.sleep(200);
    }

    // Reduce max pool size to 1
    executor.setMaximumPoolSize(1, 105);

    assertEquals(0, executor.activeCount());
    executor.execute(m2, 1);
    processStart2.await();
    Thread.sleep(100);
    assertEquals(1, executor.activeCount());
    assertEquals(1, executor.getMaximumPoolSize());
    processStop2.countDown();

    while (executor.activeCount() != 0) {
      // Waiting for all threads to be ended.
      Thread.sleep(200);
    }

    // allThreadsActiveTime() should be recorded
    // since when the second task was running it reached the new max pool size.
    assertThat(executor.allThreadsActiveTime(), greaterThan(0L));
    executor.shutdown();
  }

  @Test
  public void testRunnableExceptionPropagationDecrementsCounters() throws Exception {
    CountDownLatch processStart = new CountDownLatch(1);
    CountDownLatch processStop = new CountDownLatch(1);

    Runnable work =
        () -> {
          processStart.countDown();
          try {
            processStop.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          throw new RuntimeException("Simulated finalizer processing exception");
        };

    executor.forceExecute(work);
    processStart.await();

    assertEquals(1, executor.elementsOutstanding());

    processStop.countDown();

    // Wait until outstanding elements are released
    while (executor.elementsOutstanding() != 0) {
      Thread.sleep(10);
    }

    assertEquals(0, executor.elementsOutstanding());
  }

  @Test
  public void testHandleMerge() throws Exception {
    BoundedQueueExecutorWorkHandleImpl handle1 = executor.createBudgetHandle(1, 100L);
    BoundedQueueExecutorWorkHandleImpl handle2 = executor.createBudgetHandle(2, 200L);

    handle1.merge(handle2);

    // Verify that handle2 has 0 budget and is closed.
    assertEquals(0, handle2.elements());
    assertEquals(0, handle2.bytes());
    assertTrue(handle2.isClosed());

    // Verify that handle1 has the combined budget and is not closed.
    assertEquals(3, handle1.elements());
    assertEquals(300L, handle1.bytes());
    assertFalse(handle1.isClosed());
  }

  @Test
  public void testRenderSummaryHtml() {
    String expectedSummaryHtml =
        "Worker Threads: 0/2<br>/n"
            + "Active Threads: 0<br>/n"
            + "Work Queue Size: 0/102<br>/n"
            + "Work Queue Bytes: 0/10000000<br>/n";
    assertEquals(expectedSummaryHtml, executor.summaryHtml());
  }

  @Test
  public void testPollWork() throws Exception {
    // Create separate BoundedQueueExecutor with 1 thread so we can block it easily
    BoundedQueueExecutor testExecutor =
        new BoundedQueueExecutor(
            1,
            60,
            TimeUnit.SECONDS,
            100,
            10000000,
            new ThreadFactoryBuilder().setNameFormat("testStealing-%d").setDaemon(true).build(),
            useFairMonitor,
            /* useKeyGroupWorkQueue= */ true);

    // 1. Create blocker task to occupy the worker thread
    CountDownLatch blockerStart = new CountDownLatch(1);
    CountDownLatch blockerStop = new CountDownLatch(1);
    ExecutableWork blockerWork =
        createWorkWithCompIdAndKeyGroup(
            "blockerComp",
            DEFAULT_KEY_GROUP,
            ignored -> {
              blockerStart.countDown();
              try {
                blockerStop.await();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    testExecutor.execute(blockerWork, 0);
    blockerStart.await();

    // 2. Create two distinct key groups
    Work.KeyGroup keyGroup1 = Work.KeyGroup.create(1, 1);
    Work.KeyGroup keyGroup2 = Work.KeyGroup.create(1, 2);

    // Create executable tasks
    CountDownLatch targetStart = new CountDownLatch(1);
    ExecutableWork work1 = createWorkWithCompIdAndKeyGroup("compA", keyGroup1, ignored -> {});
    ExecutableWork work2 =
        createWorkWithCompIdAndKeyGroup(
            "compA",
            keyGroup2,
            ignored -> {
              targetStart.countDown();
            });

    // Enqueue tasks (they will wait in the queue because the thread is blocked)
    testExecutor.execute(work1, 100);
    testExecutor.execute(work2, 150);

    // Total outstanding elements must be 3 (blocker + work1 + work2)
    assertEquals(3, testExecutor.elementsOutstanding());

    // Steal work2 using pollWork with compA and keyGroup2
    try (BoundedQueueExecutorWorkHandleImpl stealHandle = testExecutor.createBudgetHandle(0, 0L)) {
      ExecutableWork stolen = testExecutor.pollWork("compA", keyGroup2, stealHandle);
      assertNotNull(stolen);
      assertEquals(work2, stolen);

      // Run the stolen task
      stolen.run(stealHandle);
      targetStart.await();
    }

    // Steal work1 using pollWork with compA and keyGroup1
    try (BoundedQueueExecutorWorkHandleImpl stealHandle = testExecutor.createBudgetHandle(0, 0L)) {
      ExecutableWork stolen = testExecutor.pollWork("compA", keyGroup1, stealHandle);
      assertNotNull(stolen);
      assertEquals(work1, stolen);
    }

    // Unblock the blocker and shut down
    blockerStop.countDown();
    testExecutor.shutdown();
  }

  @Test
  public void testPollWorkWithLinkedBlockingQueue() throws Exception {
    BoundedQueueExecutor testExecutor =
        new BoundedQueueExecutor(
            1,
            60,
            TimeUnit.SECONDS,
            100,
            10000000,
            new ThreadFactoryBuilder().setNameFormat("testLinkedQueue-%d").setDaemon(true).build(),
            useFairMonitor,
            /* useKeyGroupWorkQueue= */ false);

    CountDownLatch blockerStart = new CountDownLatch(1);
    CountDownLatch blockerStop = new CountDownLatch(1);
    ExecutableWork blockerWork =
        createWorkWithCompIdAndKeyGroup(
            "blockerComp",
            DEFAULT_KEY_GROUP,
            ignored -> {
              blockerStart.countDown();
              try {
                blockerStop.await();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    testExecutor.execute(blockerWork, 0);
    blockerStart.await();

    Work.KeyGroup keyGroup = Work.KeyGroup.create(1, 1);
    ExecutableWork work = createWorkWithCompIdAndKeyGroup("compA", keyGroup, ignored -> {});
    testExecutor.execute(work, 100);

    try (BoundedQueueExecutorWorkHandleImpl stealHandle = testExecutor.createBudgetHandle(0, 0L)) {
      ExecutableWork stolen = testExecutor.pollWork("compA", keyGroup, stealHandle);
      assertNull(stolen);
    }

    blockerStop.countDown();
    testExecutor.shutdown();
  }
}
