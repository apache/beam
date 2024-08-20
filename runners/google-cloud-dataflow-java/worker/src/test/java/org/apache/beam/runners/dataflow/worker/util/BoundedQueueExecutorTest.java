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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor}. */
@RunWith(JUnit4.class)
// TODO(https://github.com/apache/beam/issues/21230): Remove when new version of errorprone is
// released (2.11.0)
@SuppressWarnings("unused")
public class BoundedQueueExecutorTest {
  private static final long MAXIMUM_BYTES_OUTSTANDING = 10000000;
  private static final int DEFAULT_MAX_THREADS = 2;
  private static final int DEFAULT_THREAD_EXPIRATION_SEC = 60;
  @Rule public transient Timeout globalTimeout = Timeout.seconds(300);
  private BoundedQueueExecutor executor;

  private static ExecutableWork createWork(Consumer<Work> executeWorkFn) {
    return ExecutableWork.create(
        Work.create(
            Windmill.WorkItem.newBuilder()
                .setKey(ByteString.EMPTY)
                .setShardingKey(1)
                .setWorkToken(33)
                .setCacheToken(1)
                .build(),
            Watermarks.builder().setInputDataWatermark(Instant.now()).build(),
            Work.createProcessingContext(
                "computationId",
                new FakeGetDataClient(),
                ignored -> {},
                mock(HeartbeatSender.class)),
            Instant::now,
            Collections.emptyList()),
        executeWorkFn);
  }

  private Runnable createSleepProcessWorkFn(CountDownLatch start, CountDownLatch stop) {
    return () -> {
      start.countDown();
      try {
        stop.await();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
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
                .build());
  }

  @Test
  public void testScheduleWorkWhenExceedMaximumPoolSize() throws Exception {
    CountDownLatch processStart1 = new CountDownLatch(1);
    CountDownLatch processStop1 = new CountDownLatch(1);
    CountDownLatch processStart2 = new CountDownLatch(1);
    CountDownLatch processStop2 = new CountDownLatch(1);
    CountDownLatch processStart3 = new CountDownLatch(1);
    CountDownLatch processStop3 = new CountDownLatch(1);
    Runnable m1 = createSleepProcessWorkFn(processStart1, processStop1);
    Runnable m2 = createSleepProcessWorkFn(processStart2, processStop2);
    Runnable m3 = createSleepProcessWorkFn(processStart3, processStop3);

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
    Runnable m1 = createSleepProcessWorkFn(processStart1, processStop1);
    Runnable m2 = createSleepProcessWorkFn(processStart2, processStop2);

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
    processStart2.await();
    // m2 started.
    assertEquals(Thread.State.TERMINATED, m2Runner.getState());
    processStop2.countDown();
    executor.shutdown();
  }

  @Test
  public void testOverrideMaximumPoolSize() throws Exception {
    CountDownLatch processStart1 = new CountDownLatch(1);
    CountDownLatch processStart2 = new CountDownLatch(1);
    CountDownLatch processStart3 = new CountDownLatch(1);
    CountDownLatch stop = new CountDownLatch(1);
    Runnable m1 = createSleepProcessWorkFn(processStart1, stop);
    Runnable m2 = createSleepProcessWorkFn(processStart2, stop);
    Runnable m3 = createSleepProcessWorkFn(processStart3, stop);

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
    Runnable m1 = createSleepProcessWorkFn(processStart1, stop);
    Runnable m2 = createSleepProcessWorkFn(processStart2, stop);
    Runnable m3 = createSleepProcessWorkFn(processStart3, stop);

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
  public void testRecordTotalTimeMaxActiveThreadsUsedWhenMaximumPoolSizeUpdated() throws Exception {
    CountDownLatch processStart1 = new CountDownLatch(1);
    CountDownLatch processStart2 = new CountDownLatch(1);
    CountDownLatch processStart3 = new CountDownLatch(1);
    CountDownLatch stop = new CountDownLatch(1);
    Runnable m1 = createSleepProcessWorkFn(processStart1, stop);
    Runnable m2 = createSleepProcessWorkFn(processStart2, stop);
    Runnable m3 = createSleepProcessWorkFn(processStart3, stop);

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
  public void testRenderSummaryHtml() {
    String expectedSummaryHtml =
        "Worker Threads: 0/2<br>/n"
            + "Active Threads: 0<br>/n"
            + "Work Queue Size: 0/102<br>/n"
            + "Work Queue Bytes: 0/10000000<br>/n";
    assertEquals(expectedSummaryHtml, executor.summaryHtml());
  }

  @Test
  public void testExecute_updatesThreadNameForExecutableWork() throws InterruptedException {
    CountDownLatch waitForWorkExecution = new CountDownLatch(1);
    ExecutableWork executableWork =
        createWork(
            work -> {
              assertTrue(
                  Thread.currentThread()
                      .getName()
                      .contains(
                          BoundedQueueExecutor.debugFormattedWorkToken(
                              work.getWorkItem().getWorkToken())));
              waitForWorkExecution.countDown();
            });
    executor.execute(executableWork, executableWork.getWorkItem().getSerializedSize());
    waitForWorkExecution.await();
  }

  @Test
  public void testForceExecute_updatesThreadNameForExecutableWork() throws InterruptedException {
    CountDownLatch waitForWorkExecution = new CountDownLatch(1);
    ExecutableWork executableWork =
        createWork(
            work -> {
              assertTrue(
                  Thread.currentThread()
                      .getName()
                      .contains(
                          BoundedQueueExecutor.debugFormattedWorkToken(
                              work.getWorkItem().getWorkToken())));
              waitForWorkExecution.countDown();
            });
    executor.forceExecute(executableWork, executableWork.getWorkItem().getSerializedSize());
    waitForWorkExecution.await();
  }
}
