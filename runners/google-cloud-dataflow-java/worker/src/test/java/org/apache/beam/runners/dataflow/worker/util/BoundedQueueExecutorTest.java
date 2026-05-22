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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor.BoundedQueueExecutorWorkHandleImpl;
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

  private static ExecutableWork createWork(Consumer<Work> executeWorkFn) {
    WorkItem workItem =
        WorkItem.newBuilder()
            .setKey(ByteString.EMPTY)
            .setShardingKey(1)
            .setWorkToken(33)
            .setCacheToken(1)
            .build();
    return ExecutableWork.create(
        Work.create(
            workItem,
            workItem.getSerializedSize(),
            Watermarks.builder().setInputDataWatermark(Instant.now()).build(),
            Work.createProcessingContext(
                "computationId",
                new FakeGetDataClient(),
                ignored -> {},
                mock(HeartbeatSender.class)),
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
            useFairMonitor);
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
  public void testPollWorkAndInlineBatchExecution() throws Exception {
    BoundedQueueExecutor testExecutor =
        new BoundedQueueExecutor(
            1,
            DEFAULT_THREAD_EXPIRATION_SEC,
            TimeUnit.SECONDS,
            10,
            MAXIMUM_BYTES_OUTSTANDING,
            new ThreadFactoryBuilder()
                .setNameFormat("testPollWorkAndInlineBatchExecution-%d")
                .setDaemon(true)
                .build(),
            useFairMonitor);

    CountDownLatch blockerStart = new CountDownLatch(1);
    CountDownLatch blockerStop = new CountDownLatch(1);
    ExecutableWork blockerWork = createSleepProcessWork(blockerStart, blockerStop);

    CountDownLatch start1 = new CountDownLatch(1);
    CountDownLatch stop1 = new CountDownLatch(1);
    ExecutableWork m1 = createSleepProcessWork(start1, stop1);

    CountDownLatch start2 = new CountDownLatch(1);
    CountDownLatch stop2 = new CountDownLatch(1);
    ExecutableWork m2 = createSleepProcessWork(start2, stop2);

    // 1. Occupy the single worker thread with blocker work so subsequent tasks remain queued.
    testExecutor.execute(blockerWork, 0);
    blockerStart.await();
    assertEquals(1, testExecutor.elementsOutstanding());
    assertEquals(0, testExecutor.bytesOutstanding());

    // 2. Enqueue tasks to stay in the queue.
    testExecutor.execute(m1, 1000);
    testExecutor.execute(m2, 2000);

    assertEquals(3, testExecutor.elementsOutstanding());
    assertEquals(3000, testExecutor.bytesOutstanding());

    // 3. Create the batch handle.
    try (BoundedQueueExecutorWorkHandleImpl batchHandle = testExecutor.createEmptyBudgetHandle()) {
      // 4. Poll tasks inline.
      Optional<ExecutableWork> polled1 = testExecutor.pollWork(batchHandle);
      assertTrue(polled1.isPresent());
      assertEquals(m1, polled1.get());

      Optional<ExecutableWork> polled2 = testExecutor.pollWork(batchHandle);
      assertTrue(polled2.isPresent());
      assertEquals(m2, polled2.get());

      // Queue should now be empty.
      Optional<ExecutableWork> polled3 = testExecutor.pollWork(batchHandle);
      assertFalse(polled3.isPresent());

      // 5. Run polled tasks inline.
      start1.countDown();
      stop1.countDown();
      polled1.get().run(batchHandle);

      start2.countDown();
      stop2.countDown();
      polled2.get().run(batchHandle);

      // Outstanding counts should NOT yet be decremented.
      assertEquals(3, testExecutor.elementsOutstanding());
      assertEquals(3000, testExecutor.bytesOutstanding());
    }

    // 6. Upon close, outstanding counts should immediately reflect the batch decrement in one shot.
    // Only the blocker task (0 bytes, 1 element) should remain outstanding.
    while (testExecutor.elementsOutstanding() != 1) {
      Thread.sleep(10);
    }
    assertEquals(1, testExecutor.elementsOutstanding());
    assertEquals(0, testExecutor.bytesOutstanding());

    // Clean up blocker.
    blockerStop.countDown();
    testExecutor.shutdown();
  }

  @Test
  public void testPollWorkAndInlineBatchExecutionWithException() throws Exception {
    BoundedQueueExecutor testExecutor =
        new BoundedQueueExecutor(
            1,
            DEFAULT_THREAD_EXPIRATION_SEC,
            TimeUnit.SECONDS,
            10,
            MAXIMUM_BYTES_OUTSTANDING,
            new ThreadFactoryBuilder()
                .setNameFormat("testPollWorkAndInlineBatchExecutionWithException-%d")
                .setDaemon(true)
                .build(),
            useFairMonitor);

    CountDownLatch blockerStart = new CountDownLatch(1);
    CountDownLatch blockerStop = new CountDownLatch(1);
    ExecutableWork blockerWork = createSleepProcessWork(blockerStart, blockerStop);

    // Occupy all worker threads
    testExecutor.execute(blockerWork, 0);
    blockerStart.await();

    ExecutableWork inlineWork1 =
        createWork(
            ignored -> {
              throw new RuntimeException("Simulated inline execution exception");
            });

    long size1 = inlineWork1.work().getSerializedWorkItemSize();
    testExecutor.execute(inlineWork1, size1);

    long outstandingBytesBefore = testExecutor.bytesOutstanding();
    int outstandingElementsBefore = testExecutor.elementsOutstanding();

    try {
      try (BoundedQueueExecutorWorkHandleImpl batchHandle =
          testExecutor.createEmptyBudgetHandle()) {
        Optional<ExecutableWork> polled1 = testExecutor.pollWork(batchHandle);
        assertTrue(polled1.isPresent());
        polled1.get().run(batchHandle);
      }
    } catch (RuntimeException e) {
      assertEquals("Simulated inline execution exception", e.getMessage());
    }

    // Outstanding elements must still be released cleanly by try-with-resources close!
    while (testExecutor.elementsOutstanding() != outstandingElementsBefore - 1) {
      Thread.sleep(10);
    }
    assertEquals(outstandingElementsBefore - 1, testExecutor.elementsOutstanding());
    assertEquals(outstandingBytesBefore - size1, testExecutor.bytesOutstanding());

    blockerStop.countDown();
    testExecutor.shutdown();
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
}
