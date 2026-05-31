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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor.QueuedWork;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@SuppressWarnings({
  "ReturnValueIgnored",
  "UnusedVariable",
  "FutureReturnValueIgnored",
  "CatchAndPrintStackTrace",
  "ThreadPriorityCheck",
  "nullness"
})
public class ComputationWorkQueueTest {

  private BoundedQueueExecutor executor;

  @Before
  public void setUp() {
    executor =
        new BoundedQueueExecutor(
            2,
            60,
            TimeUnit.SECONDS,
            100,
            10000000,
            new ThreadFactoryBuilder().setNameFormat("Test-%d").setDaemon(true).build(),
            false);
  }

  private static final Work.KeyGroup DEFAULT_KEY_GROUP = Work.KeyGroup.create(1, 2);

  private QueuedWork createQueuedWork(String computationId, long workBytes) {
    return createQueuedWork(computationId, DEFAULT_KEY_GROUP, workBytes);
  }

  private QueuedWork createQueuedWork(
      String computationId, Work.KeyGroup keyGroup, long workBytes) {
    WorkItem workItem =
        WorkItem.newBuilder()
            .setKey(ByteString.EMPTY)
            .setShardingKey(1)
            .setWorkToken(33)
            .setCacheToken(1)
            .setKeyGroup(
                org.apache.beam.runners.dataflow.worker.windmill.Windmill.Uint128Proto.newBuilder()
                    .setHigh(keyGroup.high())
                    .setLow(keyGroup.low())
                    .build())
            .build();
    ExecutableWork work =
        ExecutableWork.create(
            Work.create(
                workItem,
                workItem.getSerializedSize(),
                Watermarks.builder().setInputDataWatermark(Instant.now()).build(),
                Work.createProcessingContext(
                    computationId,
                    new FakeGetDataClient(),
                    ignored -> {},
                    mock(HeartbeatSender.class)),
                false,
                Instant::now),
            (w, h) -> {});
    return new QueuedWork(work, executor.createBudgetHandle(1, workBytes));
  }

  private static class MockRunnable implements Runnable {
    final String id;

    MockRunnable(String id) {
      this.id = id;
    }

    @Override
    public void run() {}
  }

  @Test
  public void testBasicOfferAndPoll() {
    ComputationWorkQueue queue = new ComputationWorkQueue();
    assertTrue(queue.isEmpty());
    assertEquals(0, queue.size());

    MockRunnable task1 = new MockRunnable("1");
    MockRunnable task2 = new MockRunnable("2");

    assertTrue(queue.offer(task1));
    assertTrue(queue.offer(task2));
    assertEquals(2, queue.size());

    assertEquals(task1, queue.poll());
    assertEquals(task2, queue.poll());
    assertNull(queue.poll());
    assertTrue(queue.isEmpty());
  }

  @Test
  public void testRemove() {
    ComputationWorkQueue queue = new ComputationWorkQueue();
    MockRunnable task1 = new MockRunnable("1");
    MockRunnable task2 = new MockRunnable("2");

    queue.offer(task1);
    queue.offer(task2);

    assertTrue(queue.remove(task1));
    assertEquals(1, queue.size());
    assertEquals(task2, queue.poll());
    assertFalse(queue.remove(task1)); // Already gone
  }

  @Test
  public void testDrainTo() {
    ComputationWorkQueue queue = new ComputationWorkQueue();
    MockRunnable task1 = new MockRunnable("1");
    MockRunnable task2 = new MockRunnable("2");
    queue.offer(task1);
    queue.offer(task2);

    List<Runnable> drained = new ArrayList<>();
    assertEquals(2, queue.drainTo(drained));
    assertEquals(2, drained.size());
    assertEquals(task1, drained.get(0));
    assertEquals(task2, drained.get(1));
    assertTrue(queue.isEmpty());
  }

  @Test
  public void testIteratorSafeTraversalAndImmutable() {
    ComputationWorkQueue queue = new ComputationWorkQueue();
    MockRunnable task1 = new MockRunnable("1");
    MockRunnable task2 = new MockRunnable("2");
    queue.offer(task1);
    queue.offer(task2);

    java.util.Iterator<Runnable> it = queue.iterator();
    assertTrue(it.hasNext());
    assertEquals(task1, it.next());
    assertTrue(it.hasNext());
    assertEquals(task2, it.next());
    assertFalse(it.hasNext());

    // Assert that mutating the iterator throws UnsupportedOperationException
    it = queue.iterator();
    assertTrue(it.hasNext());
    it.next();
    try {
      it.remove();
      org.junit.Assert.fail("Iterator must be immutable");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testPollWorkTargeted() {
    ComputationWorkQueue queue = new ComputationWorkQueue();

    QueuedWork workA1 = createQueuedWork("compA", 100);
    QueuedWork workB1 = createQueuedWork("compB", 200);
    QueuedWork workA2 = createQueuedWork("compA", 150);

    queue.offer(workA1);
    queue.offer(workB1);
    queue.offer(workA2);

    assertEquals(3, queue.size());

    // Targeted poll A
    QueuedWork polledA1 = queue.pollWork("compA", DEFAULT_KEY_GROUP);
    assertNotNull(polledA1);
    assertEquals("compA", polledA1.getWork().getComputationId());
    assertEquals(100, polledA1.getHandle().bytes());

    // Verify size decremented
    assertEquals(2, queue.size());

    // Poll next should be B1 (since A1 was stolen, B1 is now first global)
    assertEquals(workB1, queue.poll());
    assertEquals(1, queue.size());

    // Last should be A2
    assertEquals(workA2, queue.poll());
    assertTrue(queue.isEmpty());
  }

  @Test
  public void testMemoryPruningLeavesZeroLeaks() {
    ComputationWorkQueue queue = new ComputationWorkQueue();
    QueuedWork workA1 = createQueuedWork("compA", 100);
    queue.offer(workA1);

    // Steal A1
    QueuedWork polled = queue.pollWork("compA", DEFAULT_KEY_GROUP);
    assertNotNull(polled);
    assertTrue(queue.isEmpty());

    // Offering another work with different computation ID
    QueuedWork workB1 = createQueuedWork("compB", 200);
    queue.offer(workB1);
    assertEquals(1, queue.size());

    // Steal B1
    QueuedWork polledB = queue.pollWork("compB", DEFAULT_KEY_GROUP);
    assertNotNull(polledB);
    assertTrue(queue.isEmpty());
  }

  @Test
  public void testConcurrentStress() throws InterruptedException {
    final ComputationWorkQueue queue = new ComputationWorkQueue();
    final int producerThreads = 4;
    final int consumerThreads = 4;
    final int tasksPerProducer = 1000;
    final int totalTasks = producerThreads * tasksPerProducer;

    ExecutorService executorService =
        Executors.newFixedThreadPool(producerThreads + consumerThreads);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(producerThreads + consumerThreads);
    final AtomicInteger consumedCount = new AtomicInteger(0);

    // Start producers
    for (int i = 0; i < producerThreads; i++) {
      executorService.submit(
          () -> {
            try {
              startLatch.await();
              for (int j = 0; j < tasksPerProducer; j++) {
                String compId = "comp-" + (j % 5);
                queue.offer(createQueuedWork(compId, 10));
              }
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    // Start consumers (mix of poll and pollWork)
    for (int i = 0; i < consumerThreads; i++) {
      final int consumerId = i;
      executorService.submit(
          () -> {
            try {
              startLatch.await();
              while (consumedCount.get() < totalTasks) {
                Runnable task;
                if (consumerId % 2 == 0) {
                  // Targeted poll
                  String compId = "comp-" + (consumedCount.get() % 5);
                  task = queue.pollWork(compId, DEFAULT_KEY_GROUP);
                } else {
                  // Global poll
                  task = queue.poll();
                }
                if (task != null) {
                  consumedCount.incrementAndGet();
                } else {
                  Thread.yield();
                }
              }
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));

    assertEquals(0, queue.size());
    assertTrue(queue.isEmpty());
  }

  @Test
  public void testTakeBlocksAndWakesUp() throws InterruptedException {
    final ComputationWorkQueue queue = new ComputationWorkQueue();
    final MockRunnable task = new MockRunnable("take-task");
    final java.util.concurrent.atomic.AtomicReference<Runnable> result =
        new java.util.concurrent.atomic.AtomicReference<>();
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch finished = new CountDownLatch(1);

    Thread t =
        new Thread(
            () -> {
              started.countDown();
              try {
                result.set(queue.take());
              } catch (InterruptedException e) {
                // Ignore
              } finally {
                finished.countDown();
              }
            });
    t.setDaemon(true);
    t.start();

    assertTrue(started.await(2, TimeUnit.SECONDS));
    // Give thread a moment to enter await()
    Thread.sleep(100);
    assertEquals(Thread.State.WAITING, t.getState());

    queue.offer(task);

    assertTrue(finished.await(2, TimeUnit.SECONDS));
    assertEquals(task, result.get());
  }

  @Test
  public void testPollWithTimeout() throws InterruptedException {
    final ComputationWorkQueue queue = new ComputationWorkQueue();
    final MockRunnable task = new MockRunnable("poll-task");
    final java.util.concurrent.atomic.AtomicReference<Runnable> result =
        new java.util.concurrent.atomic.AtomicReference<>();
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch finished = new CountDownLatch(1);

    // 1. Verify timeout returns null
    Thread t1 =
        new Thread(
            () -> {
              started.countDown();
              try {
                result.set(queue.poll(50, TimeUnit.MILLISECONDS));
              } catch (InterruptedException e) {
                // Ignore
              } finally {
                finished.countDown();
              }
            });
    t1.setDaemon(true);
    t1.start();

    assertTrue(started.await(2, TimeUnit.SECONDS));
    Thread.sleep(10);
    assertEquals(Thread.State.TIMED_WAITING, t1.getState());

    assertTrue(finished.await(2, TimeUnit.SECONDS));
    assertNull(result.get());

    // 2. Verify timed poll receives task offered concurrently
    final CountDownLatch started2 = new CountDownLatch(1);
    final CountDownLatch finished2 = new CountDownLatch(1);
    final java.util.concurrent.atomic.AtomicReference<Runnable> result2 =
        new java.util.concurrent.atomic.AtomicReference<>();

    Thread t2 =
        new Thread(
            () -> {
              started2.countDown();
              try {
                result2.set(queue.poll(2, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                // Ignore
              } finally {
                finished2.countDown();
              }
            });
    t2.setDaemon(true);
    t2.start();

    assertTrue(started2.await(2, TimeUnit.SECONDS));
    Thread.sleep(50);
    assertEquals(Thread.State.TIMED_WAITING, t2.getState());

    queue.offer(task);

    assertTrue(finished2.await(2, TimeUnit.SECONDS));
    assertEquals(task, result2.get());
  }

  @Test
  public void testPollWorkWithKeyGroup() {
    ComputationWorkQueue queue = new ComputationWorkQueue();

    Work.KeyGroup keyGroup1 = Work.KeyGroup.create(1, 1);
    Work.KeyGroup keyGroup2 = Work.KeyGroup.create(1, 2);

    QueuedWork workA1 = createQueuedWork("compA", keyGroup1, 100);
    QueuedWork workA2 = createQueuedWork("compA", keyGroup2, 150);

    queue.offer(workA1);
    queue.offer(workA2);

    assertEquals(2, queue.size());

    // Poll with keyGroup2 first - should return workA2
    QueuedWork polledA2 = queue.pollWork("compA", keyGroup2);
    assertNotNull(polledA2);
    assertEquals(workA2, polledA2);
    assertEquals(1, queue.size());

    // Poll with keyGroup2 again - should return null
    assertNull(queue.pollWork("compA", keyGroup2));

    // Poll with keyGroup1 - should return workA1
    QueuedWork polledA1 = queue.pollWork("compA", keyGroup1);
    assertNotNull(polledA1);
    assertEquals(workA1, polledA1);
    assertTrue(queue.isEmpty());
  }
}
