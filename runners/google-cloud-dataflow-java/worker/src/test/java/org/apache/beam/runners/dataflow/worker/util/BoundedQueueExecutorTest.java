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
    return createWorkWithCompId("computationId", executeWorkFn);
  }

  private static ExecutableWork createWorkWithCompId(
      String computationId, Consumer<Work> executeWorkFn) {
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
  public void testTombstoneExecutionAndQueueSize() throws Exception {
    BoundedQueueExecutor testExecutor =
        new BoundedQueueExecutor(
            1, // 1 thread to strictly control execution order
            DEFAULT_THREAD_EXPIRATION_SEC,
            TimeUnit.SECONDS,
            10,
            MAXIMUM_BYTES_OUTSTANDING,
            new ThreadFactoryBuilder()
                .setNameFormat("testTombstoneExecutionAndQueueSize-%d")
                .setDaemon(true)
                .build(),
            useFairMonitor);

    CountDownLatch blockerStart = new CountDownLatch(1);
    CountDownLatch blockerStop = new CountDownLatch(1);
    ExecutableWork blockerWork =
        createWorkWithCompId(
            "comp-1",
            ignored -> {
              blockerStart.countDown();
              try {
                blockerStop.await();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    CountDownLatch targetStart = new CountDownLatch(1);
    ExecutableWork targetWork =
        createWorkWithCompId(
            "comp-2",
            ignored -> {
              targetStart.countDown();
            });

    // 1. Occupy the worker thread with blockerWork
    testExecutor.execute(blockerWork, 0);
    blockerStart.await();

    // 2. Enqueue targetWork (goes into queue)
    testExecutor.execute(targetWork, 1000);

    // Wait a moment to ensure targetWork is registered in the queue
    assertEquals(2, testExecutor.elementsOutstanding());
    assertEquals(1, testExecutor.executorQueueIsEmpty() ? 0 : 1); // it is in the queue

    // 3. Now steal targetWork using targeted pollWork from a "stealer" context
    try (BoundedQueueExecutorWorkHandleImpl stealHandle = testExecutor.createBudgetHandle(0, 0L)) {
      java.util.Optional<ExecutableWork> stolen = testExecutor.pollWork("comp-2", stealHandle);
      assertTrue(stolen.isPresent());
      assertEquals(targetWork, stolen.get());

      // Since it's stolen, the budget is transferred to stealHandle.
      // The outstanding size must still be 2 (blocker + stolen in stealHandle).
      assertEquals(2, testExecutor.elementsOutstanding());

      // 4. Run the stolen work inline in the stealer thread.
      stolen.get().run(stealHandle);
      targetStart.await();
    }

    // After stealHandle is closed, the outstanding size should decrement by 1 (the stolen task).
    // Only the blocker remains.
    while (testExecutor.elementsOutstanding() != 1) {
      Thread.sleep(10);
    }
    assertEquals(1, testExecutor.elementsOutstanding());

    // 5. Unblock the blocker. The executor worker thread should now pop the targetWork tombstone.
    // Since it is cancelled (stolen), the worker thread should skip it as a no-op.
    // The elementsOutstanding should drop to 0 after the blocker completes.
    blockerStop.countDown();

    while (testExecutor.elementsOutstanding() != 0) {
      Thread.sleep(10);
    }
    assertEquals(0, testExecutor.elementsOutstanding());
    testExecutor.shutdown();
  }

  @Test
  public void testConcurrentStealingAndPolling() throws Exception {
    final int numComputations = 10;
    final int tasksPerComp = 100;
    final int totalTasks = numComputations * tasksPerComp;

    // 4 executor worker threads to execute work
    BoundedQueueExecutor testExecutor =
        new BoundedQueueExecutor(
            4,
            DEFAULT_THREAD_EXPIRATION_SEC,
            TimeUnit.SECONDS,
            totalTasks * 2,
            MAXIMUM_BYTES_OUTSTANDING,
            new ThreadFactoryBuilder()
                .setNameFormat("testConcurrentStealingAndPolling-%d")
                .setDaemon(true)
                .build(),
            useFairMonitor);

    // Latches to coordinate
    CountDownLatch allDone = new CountDownLatch(totalTasks);

    // Enqueue work for all computations
    for (int i = 0; i < numComputations; i++) {
      final String compId = "comp-" + i;
      for (int j = 0; j < tasksPerComp; j++) {
        ExecutableWork work =
            createWorkWithCompId(
                compId,
                ignored -> {
                  allDone.countDown();
                });
        testExecutor.execute(work, 100);
      }
    }

    // Launch active "stealers" that target specific computations concurrently
    Thread[] stealers = new Thread[numComputations];
    for (int i = 0; i < numComputations; i++) {
      final String compId = "comp-" + i;
      stealers[i] =
          new Thread(
              () -> {
                try {
                  // Attempt to steal tasks for this computation
                  int stolenCount = 0;
                  while (stolenCount < tasksPerComp) {
                    try (BoundedQueueExecutorWorkHandleImpl stealHandle =
                        testExecutor.createBudgetHandle(0, 0L)) {
                      java.util.Optional<ExecutableWork> stolen =
                          testExecutor.pollWork(compId, stealHandle);
                      if (stolen.isPresent()) {
                        stolen.get().run(stealHandle);
                        stolenCount++;
                      } else {
                        // Yield if none available
                        Thread.sleep(1);
                      }
                    } catch (InterruptedException e) {
                      break;
                    }
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
      stealers[i].start();
    }

    // Wait for all tasks to complete (either executed by thread pool or stolen)
    assertTrue(allDone.await(30, TimeUnit.SECONDS));

    for (Thread stealer : stealers) {
      stealer.interrupt();
      stealer.join();
    }

    // Wait until all outstanding elements are released cleanly
    while (testExecutor.elementsOutstanding() != 0) {
      Thread.sleep(10);
    }
    assertEquals(0, testExecutor.elementsOutstanding());
    assertEquals(0, testExecutor.bytesOutstanding());
    testExecutor.shutdown();
  }

  @Test
  public void testConcurrentStealingAndPollingNoWastedThreads() throws Exception {
    final int numComputations = 10;
    final int tasksPerComp = 100;
    final int totalTasks = numComputations * tasksPerComp;

    // 4 executor worker threads to execute work
    BoundedQueueExecutor testExecutor =
        new BoundedQueueExecutor(
            4,
            DEFAULT_THREAD_EXPIRATION_SEC,
            TimeUnit.SECONDS,
            totalTasks * 2,
            MAXIMUM_BYTES_OUTSTANDING,
            new ThreadFactoryBuilder()
                .setNameFormat("testConcurrentStealingAndPollingNoWastedThreads-%d")
                .setDaemon(true)
                .build(),
            useFairMonitor);

    // Thread-safe counters for tracking executions
    java.util.concurrent.atomic.AtomicInteger totalExecuted =
        new java.util.concurrent.atomic.AtomicInteger(0);
    java.util.concurrent.atomic.AtomicInteger workerExecuted =
        new java.util.concurrent.atomic.AtomicInteger(0);
    java.util.concurrent.atomic.AtomicInteger poolWorkerExecuted =
        new java.util.concurrent.atomic.AtomicInteger(0);

    // Map to track duplicate executions
    java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicInteger>
        executionCounts = new java.util.concurrent.ConcurrentHashMap<>();

    // Latches to coordinate
    CountDownLatch allDone = new CountDownLatch(totalTasks);

    // Enqueue work for all computations
    for (int i = 0; i < numComputations; i++) {
      final String compId = "comp-" + i;
      for (int j = 0; j < tasksPerComp; j++) {
        final String taskId = compId + "-task-" + j;
        ExecutableWork work =
            createWorkWithCompId(
                compId,
                ignored -> {
                  int count =
                      executionCounts
                          .computeIfAbsent(
                              taskId, k -> new java.util.concurrent.atomic.AtomicInteger(0))
                          .incrementAndGet();
                  totalExecuted.incrementAndGet();
                  String threadName = Thread.currentThread().getName();
                  if (threadName.startsWith("testConcurrentStealingAndPollingNoWastedThreads-")) {
                    poolWorkerExecuted.incrementAndGet();
                  } else {
                    workerExecuted.incrementAndGet();
                  }
                  allDone.countDown();
                });
        testExecutor.execute(work, 100);
      }
    }

    // Launch active "stealers" that target specific computations concurrently
    Thread[] stealers = new Thread[numComputations];
    for (int i = 0; i < numComputations; i++) {
      final String compId = "comp-" + i;
      stealers[i] =
          new Thread(
              () -> {
                try {
                  while (!Thread.currentThread().isInterrupted()) {
                    try (BoundedQueueExecutorWorkHandleImpl stealHandle =
                        testExecutor.createBudgetHandle(0, 0L)) {
                      java.util.Optional<ExecutableWork> stolen =
                          testExecutor.pollWork(compId, stealHandle);
                      if (stolen.isPresent()) {
                        stolen.get().run(stealHandle);
                      } else {
                        Thread.sleep(1);
                      }
                    } catch (InterruptedException e) {
                      break;
                    }
                  }
                } catch (Exception e) {
                  // Ignore or log
                }
              });
      stealers[i].start();
    }

    // Wait for all tasks to complete (either executed by thread pool or stolen)
    assertTrue(allDone.await(60, TimeUnit.SECONDS));

    // Stop stealers
    for (Thread stealer : stealers) {
      stealer.interrupt();
      stealer.join();
    }

    // Verify execution invariants
    assertEquals("Total executions must match total tasks", totalTasks, totalExecuted.get());
    assertEquals("No duplicate executions allowed", totalTasks, executionCounts.size());
    for (java.util.Map.Entry<String, java.util.concurrent.atomic.AtomicInteger> entry :
        executionCounts.entrySet()) {
      assertEquals(
          "Task " + entry.getKey() + " executed more than once", 1, entry.getValue().get());
    }

    // Verify that completed task count of the inner executor perfectly matches poolWorkerExecuted
    java.lang.reflect.Field executorField = BoundedQueueExecutor.class.getDeclaredField("executor");
    executorField.setAccessible(true);
    java.util.concurrent.ThreadPoolExecutor innerExecutor =
        (java.util.concurrent.ThreadPoolExecutor) executorField.get(testExecutor);

    // Wait a tiny bit for any pending pool tasks to fully complete their afterExecute/bookkeeping
    Thread.sleep(100);

    long completedTasks = innerExecutor.getCompletedTaskCount();
    assertEquals(
        "Completed task count of the pool must perfectly match the number of worker-executed tasks (exactly 0 wasted activations)",
        (long) poolWorkerExecuted.get(),
        completedTasks);

    // Wait until all outstanding elements are released cleanly
    while (testExecutor.elementsOutstanding() != 0) {
      Thread.sleep(10);
    }
    assertEquals(0, testExecutor.elementsOutstanding());
    assertEquals(0, testExecutor.bytesOutstanding());
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
