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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor.QueuedWork;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KeyGroupWorkIndexTest {

  private static final String COMP_A = "compA";
  private static final String COMP_B = "compB";
  private static final Work.KeyGroup KEY_GROUP_1 = Work.KeyGroup.create(1, 1);
  private static final Work.KeyGroup KEY_GROUP_2 = Work.KeyGroup.create(1, 2);

  @Rule public transient Timeout globalTimeout = Timeout.seconds(300);

  // Creates the budget handles of the QueuedWork created by the tests. Its executor is never used.
  private final BoundedQueueExecutor owner =
      new BoundedQueueExecutor(
          1,
          60,
          TimeUnit.SECONDS,
          100,
          10000000,
          new ThreadFactoryBuilder()
              .setNameFormat("KeyGroupWorkIndexTest-%d")
              .setDaemon(true)
              .build(),
          /* useFairMonitor= */ false,
          /* multiKeyBundleEnabled= */ true);
  private final KeyGroupWorkIndex index = new KeyGroupWorkIndex();
  private long nextWorkToken = 0;

  @After
  public void tearDown() throws InterruptedException {
    owner.shutdown();
  }

  private QueuedWork createQueuedWork(String computationId, Work.KeyGroup keyGroup) {
    long workToken = nextWorkToken++;
    WorkItem workItem =
        WorkItem.newBuilder()
            .setKey(ByteString.EMPTY)
            .setShardingKey(workToken)
            .setWorkToken(workToken)
            .setCacheToken(1)
            .setKeyGroup(
                Windmill.Uint128Proto.newBuilder()
                    .setHigh(keyGroup.high())
                    .setLow(keyGroup.low())
                    .build())
            .build();
    Work.ProcessingContext processingContext =
        Work.createProcessingContext(
            computationId, new FakeGetDataClient(), ignored -> {}, mock(HeartbeatSender.class));
    ExecutableWork work =
        ExecutableWork.create(
            Work.create(
                workItem,
                workItem.getSerializedSize(),
                Watermarks.builder().setInputDataWatermark(Instant.now()).build(),
                processingContext,
                false,
                Instant::now,
                ImmutableList.of()),
            (w, handle) -> {});
    QueuedWork queuedWork =
        new QueuedWork(work, owner.createBudgetHandle(work.work(), /* bytes= */ 0), index);
    assertTrue(queuedWork.isStealable());
    return queuedWork;
  }

  private static int workToken(QueuedWork queuedWork) {
    return (int) queuedWork.getWork().work().getWorkItem().getWorkToken();
  }

  @Test
  public void testPollAndClaim_returnsWorkInFifoOrder() {
    QueuedWork work1 = createQueuedWork(COMP_A, KEY_GROUP_1);
    QueuedWork work2 = createQueuedWork(COMP_A, KEY_GROUP_1);
    QueuedWork work3 = createQueuedWork(COMP_A, KEY_GROUP_1);
    index.add(work1);
    index.add(work2);
    index.add(work3);

    assertSame(work1, index.pollAndClaim(COMP_A, KEY_GROUP_1));
    assertSame(work2, index.pollAndClaim(COMP_A, KEY_GROUP_1));
    assertSame(work3, index.pollAndClaim(COMP_A, KEY_GROUP_1));
    assertNull(index.pollAndClaim(COMP_A, KEY_GROUP_1));
    assertTrue(work1.isClaimed());
    assertTrue(work2.isClaimed());
    assertTrue(work3.isClaimed());
    assertTrue(index.isEmpty());
  }

  @Test
  public void testPollAndClaim_isolatesComputationsAndKeyGroups() {
    QueuedWork workA1 = createQueuedWork(COMP_A, KEY_GROUP_1);
    QueuedWork workA2 = createQueuedWork(COMP_A, KEY_GROUP_2);
    QueuedWork workB1 = createQueuedWork(COMP_B, KEY_GROUP_1);
    index.add(workA1);
    index.add(workA2);
    index.add(workB1);

    assertNull(index.pollAndClaim(COMP_A, Work.KeyGroup.create(3, 4)));
    assertNull(index.pollAndClaim(COMP_B, KEY_GROUP_2));
    assertSame(workA2, index.pollAndClaim(COMP_A, KEY_GROUP_2));
    assertNull(index.pollAndClaim(COMP_A, KEY_GROUP_2));
    assertSame(workB1, index.pollAndClaim(COMP_B, KEY_GROUP_1));
    assertNull(index.pollAndClaim(COMP_B, KEY_GROUP_1));
    assertSame(workA1, index.pollAndClaim(COMP_A, KEY_GROUP_1));
    assertNull(index.pollAndClaim(COMP_A, KEY_GROUP_1));
    assertTrue(index.isEmpty());
  }

  @Test
  public void testPollAndClaim_skipsWorkClaimedByExecutor() {
    QueuedWork work1 = createQueuedWork(COMP_A, KEY_GROUP_1);
    QueuedWork work2 = createQueuedWork(COMP_A, KEY_GROUP_1);
    index.add(work1);
    index.add(work2);

    assertTrue(work1.tryClaimForExecution());

    assertSame(work2, index.pollAndClaim(COMP_A, KEY_GROUP_1));
    assertNull(index.pollAndClaim(COMP_A, KEY_GROUP_1));
    assertTrue(index.isEmpty());
    index.drainClaimed(work1);
    assertTrue(index.isEmpty());

    QueuedWork work3 = createQueuedWork(COMP_A, KEY_GROUP_1);
    QueuedWork work4 = createQueuedWork(COMP_A, KEY_GROUP_1);
    index.add(work3);
    index.add(work4);

    assertTrue(work4.tryClaimForExecution());
    index.drainClaimed(work4);
    assertFalse(index.isEmpty());

    assertSame(work3, index.pollAndClaim(COMP_A, KEY_GROUP_1));
    assertTrue(index.isEmpty());
  }

  @Test
  public void testAdd_skipsClaimedWork() {
    QueuedWork work = createQueuedWork(COMP_A, KEY_GROUP_1);
    assertTrue(work.tryClaimForExecution());

    index.add(work);

    assertTrue(index.isEmpty());
    assertNull(index.pollAndClaim(COMP_A, KEY_GROUP_1));
  }

  @Test
  public void testDrainClaimed_drainsFromHead() {
    QueuedWork work1 = createQueuedWork(COMP_A, KEY_GROUP_1);
    QueuedWork work2 = createQueuedWork(COMP_A, KEY_GROUP_1);
    index.add(work1);
    index.add(work2);

    assertTrue(work2.tryClaimForExecution());
    index.drainClaimed(work2);
    assertFalse(index.isEmpty());
    assertTrue(work1.tryClaimForExecution());
    index.drainClaimed(work1);
    assertTrue(index.isEmpty());

    QueuedWork work3 = createQueuedWork(COMP_A, KEY_GROUP_1);
    index.add(work3);
    assertSame(work3, index.pollAndClaim(COMP_A, KEY_GROUP_1));
    assertTrue(index.isEmpty());
  }

  @Test
  public void testConcurrentAddDrainAndPollAndClaim_claimsEachWorkExactlyOnce() throws Exception {
    int numProducers = 2;
    int numExecutors = 2;
    int numStealers = 2;
    int numWorksPerProducer = 10000;
    ImmutableList<String> computations = ImmutableList.of(COMP_A, COMP_B);
    ImmutableList<Work.KeyGroup> keyGroups =
        ImmutableList.of(KEY_GROUP_1, KEY_GROUP_2, Work.KeyGroup.create(2, 1));

    List<List<QueuedWork>> worksByProducer = new ArrayList<>();
    for (int producer = 0; producer < numProducers; producer++) {
      List<QueuedWork> works = new ArrayList<>();
      for (int i = 0; i < numWorksPerProducer; i++) {
        works.add(
            createQueuedWork(
                computations.get(i % computations.size()), keyGroups.get(i % keyGroups.size())));
      }
      worksByProducer.add(works);
    }
    int numWorks = numProducers * numWorksPerProducer;
    // Simulates the executor queue: work is queued before it is indexed.
    ConcurrentLinkedQueue<QueuedWork> executorQueue = new ConcurrentLinkedQueue<>();
    AtomicIntegerArray executedCounts = new AtomicIntegerArray(numWorks);
    AtomicIntegerArray stolenCounts = new AtomicIntegerArray(numWorks);
    AtomicBoolean producersDone = new AtomicBoolean(false);
    AtomicBoolean executorsDone = new AtomicBoolean(false);

    ExecutorService threads =
        Executors.newFixedThreadPool(numProducers + numExecutors + numStealers);
    List<Future<?>> producerFutures = new ArrayList<>();
    for (List<QueuedWork> works : worksByProducer) {
      producerFutures.add(
          threads.submit(
              () -> {
                for (QueuedWork work : works) {
                  executorQueue.add(work);
                  index.add(work);
                }
              }));
    }
    List<Future<?>> executorFutures = new ArrayList<>();
    for (int i = 0; i < numExecutors; i++) {
      executorFutures.add(
          threads.submit(
              () -> {
                while (!producersDone.get() || !executorQueue.isEmpty()) {
                  QueuedWork work = executorQueue.poll();
                  if (work != null && work.tryClaimForExecution()) {
                    index.drainClaimed(work);
                    executedCounts.incrementAndGet(workToken(work));
                  }
                }
              }));
    }
    List<Future<?>> stealerFutures = new ArrayList<>();
    for (int i = 0; i < numStealers; i++) {
      stealerFutures.add(
          threads.submit(
              () -> {
                while (!executorsDone.get()) {
                  ThreadLocalRandom random = ThreadLocalRandom.current();
                  QueuedWork work =
                      index.pollAndClaim(
                          computations.get(random.nextInt(computations.size())),
                          keyGroups.get(random.nextInt(keyGroups.size())));
                  if (work != null) {
                    stolenCounts.incrementAndGet(workToken(work));
                  }
                }
              }));
    }

    for (Future<?> future : producerFutures) {
      future.get();
    }
    producersDone.set(true);
    for (Future<?> future : executorFutures) {
      future.get();
    }
    executorsDone.set(true);
    for (Future<?> future : stealerFutures) {
      future.get();
    }
    threads.shutdown();
    assertTrue(threads.awaitTermination(30, TimeUnit.SECONDS));

    for (int workToken = 0; workToken < numWorks; workToken++) {
      assertEquals(
          "work token " + workToken,
          1,
          executedCounts.get(workToken) + stolenCounts.get(workToken));
    }
    assertTrue(index.isEmpty());
  }
}
