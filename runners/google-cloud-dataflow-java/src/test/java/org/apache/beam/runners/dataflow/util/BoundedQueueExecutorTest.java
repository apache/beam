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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Unit tests for {@link org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor}. */
@RunWith(Parameterized.class)
// TODO(https://github.com/apache/beam/issues/21230): Remove when new version of errorprone is
// released (2.11.0)
@SuppressWarnings("unused")
public class BoundedQueueExecutorTest {
  private static final long MAXIMUM_BYTES_OUTSTANDING = 10000000;
  private static final int DEFAULT_MAX_THREADS = 2;
  private static final int DEFAULT_THREAD_EXPIRATION_SEC = 300;

  private BoundedQueueExecutor executor;

  static Work createMockWork(long workToken, Consumer<Work> processWorkFn) {
    return Work.create(
        Windmill.WorkItem.newBuilder().setKey(ByteString.EMPTY).setWorkToken(workToken).build(),
        Instant::now,
        Collections.emptyList(),
        processWorkFn);
  }

  static Consumer<Work> sleepProcessWorkFn =
      (CountDownLatch latch, AtomicBoolean stop) -> {
        latch.countDown();
        int count = 0;
        while (!stop.get()) {
          count += 1;
        }
      };

  @Before
  public void setUp() {
    this.executor =
        new BoundedQueueExecutor(
            DEFAULT_MAX_THREADS,
            DEFAULT_THREAD_EXPIRATION_SEC,
            TimeUnit.SECONDS,
            DEFAULT_MAX_THREADS,
            MAXIMUM_BYTES_OUTSTANDING,
            new ThreadFactoryBuilder()
                .setNameFormat("DataflowWorkUnits-%d")
                .setDaemon(true)
                .build());
  }

  @Test
  public void testOverrideMaximumThreadCount() throws Exception {
    CountDownLatch processStart1 = new CountDownLatch(1);
    CountDownLatch processStart2 = new CountDownLatch(1);
    CountDownLatch processStart3 = new CountDownLatch(1);
    AtomicBoolean stop = new AtomicBoolean(false);
    Work m1 = createMockWork(1, sleepProcessWorkFn(processStart1, stop));
    Work m2 = createMockWork(2, sleepProcessWorkFn(processStart2, stop));
    Work m3 = createMockWork(3, sleepProcessWorkFn(processStart3, stop));

    // Initial state.
    assertEquals(0, executor.activeCount());
    assertEquals(2, executor.maximumThreadCount());

    // m1 and m2 are accepted.
    executor.execute(m1, m1.getWorkItem().getSerializedSize());
    processStart1.await();
    assertEquals(1, executor.activeCount());
    executor.execute(m2, m2.getWorkItem().getSerializedSize());
    processStart2.await();
    assertEquals(2, executor.activeCount());

    // Max pool size was reached so no new work is accepted.
    executor.execute(m3, m3.getWorkItem().getSerializedSize());
    assertEquals(1, processStart3.getCount());

    // Increase the max thread count
    executor.setMaximumPoolSize(3);
    assertEquals(3, executor.maximumThreadCount());

    // m3 is accepted
    executor.execute(m3, m3.getWorkItem().getSerializedSize());
    processStart3.await();
    assertEquals(3, executor.activeCount());

    stop.set(true);
    executor.shutdown();
  }

  @Test
  public void testRecordTotalTimeMaxActiveThreadsUsed() throws Exception {
    CountDownLatch processStart1 = new CountDownLatch(1);
    CountDownLatch processStart2 = new CountDownLatch(1);
    CountDownLatch processStart3 = new CountDownLatch(1);
    AtomicBoolean stop = new AtomicBoolean(false);
    Work m1 = createMockWork(1, sleepProcessWorkFn(processStart1, stop));
    Work m2 = createMockWork(2, sleepProcessWorkFn(processStart2, stop));
    Work m3 = createMockWork(3, sleepProcessWorkFn(processStart3, stop));

    // Initial state.
    assertEquals(0, executor.activeCount());
    assertEquals(2, executor.maximumThreadCount());

    // m1 and m2 are accepted.
    executor.execute(m1, m1.getWorkItem().getSerializedSize());
    processStart1.await();
    assertEquals(1, executor.activeCount());
    executor.execute(m2, m2.getWorkItem().getSerializedSize());
    processStart2.await();
    assertEquals(2, executor.activeCount());

    // Max pool size was reached so no new work is accepted.
    executor.execute(m3, m3.getWorkItem().getSerializedSize());
    assertEquals(1, processStart3.getCount());

    assertEquals(0, executor.allThreadsActiveTime());
    stop.set(true);
    // Max pool size was reached so the allThreadsActiveTime() was updated.
    assertThat(executor.allThreadsActiveTime(), greaterThan(0));

    executor.shutdown();
  }

  @Test
  public void testTotalTimeMaxActiveThreadsUsedRemainUnchangedWithIncreasingMaximumThreadCount()
      throws Exception {
    CountDownLatch processStart1 = new CountDownLatch(1);
    CountDownLatch processStart2 = new CountDownLatch(1);
    CountDownLatch processStart3 = new CountDownLatch(1);
    AtomicBoolean stop = new AtomicBoolean(false);
    Work m1 = createMockWork(1, sleepProcessWorkFn(processStart1, stop));
    Work m2 = createMockWork(2, sleepProcessWorkFn(processStart2, stop));
    Work m3 = createMockWork(3, sleepProcessWorkFn(processStart3, stop));

    // Initial state.
    assertEquals(0, executor.activeCount());
    assertEquals(2, executor.maximumThreadCount());

    // m1 and m2 are accepted.
    executor.execute(m1, m1.getWorkItem().getSerializedSize());
    processStart1.await();
    assertEquals(1, executor.activeCount());
    executor.execute(m2, m2.getWorkItem().getSerializedSize());
    processStart2.await();
    assertEquals(2, executor.activeCount());

    // Max pool size was reached so no new work is accepted.
    executor.execute(m3, m3.getWorkItem().getSerializedSize());
    assertEquals(1, processStart3.getCount());

    assertEquals(0, executor.allThreadsActiveTime());
    // Increase the max thread count
    executor.setMaximumPoolSize(5);
    stop.set(true);
    // Max pool size was updated during execution so allThreadsActiveTime() remains unchanged.
    assertThat(executor.allThreadsActiveTime(), equalTo(0));

    executor.shutdown();
  }
}
