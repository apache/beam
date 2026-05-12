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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor.QueuedWork;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class KeyGroupWorkQueueTest {

  @Parameters(name = "fairQueue={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  @Parameterized.Parameter public boolean fairQueue;

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
            fairQueue,
            /* useKeyGroupWorkQueue= */ true);
  }

  private static final Work.KeyGroup TEST_KEY_GROUP = Work.KeyGroup.create(1, 2);

  private QueuedWork createQueuedWork(String computationId, long workBytes) {
    return createQueuedWork(computationId, TEST_KEY_GROUP, workBytes);
  }

  private QueuedWork createQueuedWork(
      String computationId, Work.@Nullable KeyGroup keyGroup, long workBytes) {
    WorkItem.Builder workItemBuilder =
        WorkItem.newBuilder()
            .setKey(ByteString.EMPTY)
            .setShardingKey(1)
            .setWorkToken(33)
            .setCacheToken(1);
    if (keyGroup != null) {
      workItemBuilder.setKeyGroup(
          org.apache.beam.runners.dataflow.worker.windmill.Windmill.Uint128Proto.newBuilder()
              .setHigh(keyGroup.high())
              .setLow(keyGroup.low())
              .build());
    }
    WorkItem workItem = workItemBuilder.build();
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

  private static class NoOpRunnable implements Runnable {
    final String id;

    NoOpRunnable(String id) {
      this.id = id;
    }

    @Override
    public void run() {}

    @Override
    public String toString() {
      return "NoOpRunnable(" + id + ")";
    }
  }

  @Test
  public void testBasicOfferAndPoll() {
    KeyGroupWorkQueue queue = new KeyGroupWorkQueue(fairQueue);
    assertTrue(queue.isEmpty());
    assertEquals(0, queue.size());

    NoOpRunnable task1 = new NoOpRunnable("1");
    NoOpRunnable task2 = new NoOpRunnable("2");

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
    KeyGroupWorkQueue queue = new KeyGroupWorkQueue(fairQueue);
    NoOpRunnable task1 = new NoOpRunnable("1");
    NoOpRunnable task2 = new NoOpRunnable("2");

    queue.offer(task1);
    queue.offer(task2);

    assertTrue(queue.remove(task1));
    assertEquals(1, queue.size());
    assertEquals(task2, queue.poll());
    assertFalse(queue.remove(task1)); // Already gone
  }

  @Test
  public void testDrainTo() {
    KeyGroupWorkQueue queue = new KeyGroupWorkQueue(fairQueue);
    NoOpRunnable task1 = new NoOpRunnable("1");
    NoOpRunnable task2 = new NoOpRunnable("2");
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
    KeyGroupWorkQueue queue = new KeyGroupWorkQueue(fairQueue);
    NoOpRunnable task1 = new NoOpRunnable("1");
    NoOpRunnable task2 = new NoOpRunnable("2");
    queue.offer(task1);
    queue.offer(task2);

    Iterator<Runnable> it = queue.iterator();
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
      fail("Iterator must be immutable");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testPollWorkTargeted() {
    KeyGroupWorkQueue queue = new KeyGroupWorkQueue(fairQueue);

    QueuedWork workA1 = createQueuedWork("compA", 100);
    QueuedWork workB1 = createQueuedWork("compB", 200);
    QueuedWork workA2 = createQueuedWork("compA", 150);

    queue.offer(workA1);
    queue.offer(workB1);
    queue.offer(workA2);

    assertEquals(3, queue.size());
    assertFalse(queue.isEmpty());

    // Targeted poll A
    QueuedWork polledA1 = queue.pollWork("compA", TEST_KEY_GROUP);
    assertNotNull(polledA1);
    assertEquals("compA", polledA1.getWork().getComputationId());
    assertEquals(100, polledA1.getHandle().bytes());
    assertEquals(2, queue.size());
    assertFalse(queue.isEmpty());

    // Poll next should be B1 (since A1 was stolen, B1 is now first global)
    assertEquals(workB1, queue.poll());
    assertEquals(1, queue.size());
    assertFalse(queue.isEmpty());

    // Last should be A2
    assertEquals(workA2, queue.poll());
    assertEquals(0, queue.size());
    assertTrue(queue.isEmpty());

    assertEquals(null, queue.poll());
  }

  @Test
  public void testConcurrentStress() throws InterruptedException, ExecutionException {
    KeyGroupWorkQueue queue = new KeyGroupWorkQueue(fairQueue);
    int producerThreads = 4;
    int consumerThreads = 4;
    int tasksPerProducer = 1000;
    int totalTasks = producerThreads * tasksPerProducer;
    Runnable poisonPill =
        new Runnable() {
          @Override
          public void run() {}

          @Override
          public String toString() {
            return "POISON_PILL";
          }
        };

    ExecutorService executorService =
        Executors.newFixedThreadPool(producerThreads + consumerThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch producersDoneLatch = new CountDownLatch(producerThreads);
    CountDownLatch consumersDoneLatch = new CountDownLatch(consumerThreads);
    CountDownLatch consumedLatch = new CountDownLatch(totalTasks);
    List<Future<?>> futures = new ArrayList<>();

    // Start consumers
    for (int i = 0; i < consumerThreads; i++) {
      int consumerId = i;
      futures.add(
          executorService.submit(
              () -> {
                try {
                  startLatch.await();
                  int iteration = consumerId % 4;
                  while (true) {
                    int strategy = iteration;
                    iteration = (iteration + 1) % 4;
                    Runnable task = null;
                    if (strategy == 0) {
                      String compId = "comp-" + (consumedLatch.getCount() % 5);
                      task = queue.pollWork(compId, TEST_KEY_GROUP);
                    } else if (strategy == 1) {
                      task = queue.poll();
                    } else if (strategy == 2) {
                      task = queue.poll(10, TimeUnit.MICROSECONDS);
                    } else if (strategy == 3) {
                      task = queue.take();
                    }

                    if (task == poisonPill) {
                      break;
                    }
                    if (task != null) {
                      consumedLatch.countDown();
                    }
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                } finally {
                  consumersDoneLatch.countDown();
                }
              }));
    }

    // Start producers
    for (int i = 0; i < producerThreads; i++) {
      futures.add(
          executorService.submit(
              () -> {
                try {
                  startLatch.await();
                  for (int j = 0; j < tasksPerProducer; j++) {
                    String compId = "comp-" + (j % 5);
                    queue.offer(createQueuedWork(compId, 10));
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                } finally {
                  producersDoneLatch.countDown();
                }
              }));
    }

    // Release the start latch to start the test
    startLatch.countDown();

    // Wait for all tasks to be consumed
    assertTrue(consumedLatch.await(30, TimeUnit.SECONDS));

    // Send poison pills to stop all consumers
    for (int i = 0; i < consumerThreads; i++) {
      queue.offer(poisonPill);
    }

    // Wait for consumers to finish
    assertTrue(consumersDoneLatch.await(30, TimeUnit.SECONDS));
    // Wait for producers to finish
    assertTrue(producersDoneLatch.await(30, TimeUnit.SECONDS));

    // Check for exceptions in threads
    for (Future<?> future : futures) {
      future.get();
    }

    executorService.shutdown();
    assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));

    assertEquals(0, queue.size());
    assertTrue(queue.isEmpty());
  }

  @Test
  public void testTakeBlocksAndWakesUp() throws InterruptedException {
    final KeyGroupWorkQueue queue = new KeyGroupWorkQueue(fairQueue);
    final NoOpRunnable task = new NoOpRunnable("take-task");
    final AtomicReference<Runnable> result = new AtomicReference<>();
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

    assertTrue(started.await(30, TimeUnit.SECONDS));
    waitForThreadState(t, State.WAITING);

    queue.offer(task);

    assertTrue(finished.await(30, TimeUnit.SECONDS));
    assertEquals(task, result.get());
  }

  @Test
  public void testPollWithTimeout() throws InterruptedException {
    final KeyGroupWorkQueue queue = new KeyGroupWorkQueue(fairQueue);
    final NoOpRunnable task = new NoOpRunnable("poll-task");
    final AtomicReference<Runnable> result = new AtomicReference<>();
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch finished = new CountDownLatch(1);

    // 1. Verify timeout returns null
    Thread t1 =
        new Thread(
            () -> {
              started.countDown();
              try {
                result.set(queue.poll(500, TimeUnit.MILLISECONDS));
              } catch (InterruptedException e) {
                // Ignore
              } finally {
                finished.countDown();
              }
            });
    t1.setDaemon(true);
    t1.start();

    assertTrue(started.await(30, TimeUnit.SECONDS));
    waitForThreadState(t1, State.TIMED_WAITING);

    assertTrue(finished.await(30, TimeUnit.SECONDS));
    assertNull(result.get());

    // 2. Verify timed poll receives task offered concurrently
    final CountDownLatch started2 = new CountDownLatch(1);
    final CountDownLatch finished2 = new CountDownLatch(1);
    final AtomicReference<Runnable> result2 = new AtomicReference<>();

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

    assertTrue(started2.await(30, TimeUnit.SECONDS));
    waitForThreadState(t2, State.TIMED_WAITING);

    queue.offer(task);

    assertTrue(finished2.await(30, TimeUnit.SECONDS));
    assertEquals(task, result2.get());
  }

  @Test
  public void testPollWorkWithKeyGroup() {
    KeyGroupWorkQueue queue = new KeyGroupWorkQueue(fairQueue);

    Work.KeyGroup keyGroup1 = Work.KeyGroup.create(1, 1);
    Work.KeyGroup keyGroup2 = Work.KeyGroup.create(1, 2);
    Work.KeyGroup keyGroupNotExist = Work.KeyGroup.create(3, 4);

    QueuedWork workA1 = createQueuedWork("compA", keyGroup1, 100);
    QueuedWork workA2 = createQueuedWork("compA", keyGroup2, 150);

    queue.offer(workA1);
    queue.offer(workA2);

    assertEquals(2, queue.size());

    QueuedWork polledNotExist = queue.pollWork("compA", keyGroupNotExist);
    assertNull(polledNotExist);
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

    polledNotExist = queue.pollWork("compA", keyGroupNotExist);
    assertNull(polledNotExist);
    assertTrue(queue.isEmpty());
  }

  private void waitForThreadState(Thread t, State state) throws InterruptedException {
    long timeoutMs = 30000;
    long start = System.currentTimeMillis();
    while (t.getState() != state) {
      if (System.currentTimeMillis() - start > timeoutMs) {
        fail("Thread did not reach " + state + " state within " + timeoutMs + "ms");
      }
      Thread.sleep(1);
    }
  }
}
