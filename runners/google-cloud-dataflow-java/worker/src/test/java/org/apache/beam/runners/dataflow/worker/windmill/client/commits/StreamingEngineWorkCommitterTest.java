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
package org.apache.beam.runners.dataflow.worker.windmill.client.commits;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.api.services.dataflow.model.MapTask;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.FakeWindmillServer;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.WeightedSemaphore;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.WorkId;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingEngineWorkCommitterTest {

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  @Rule public ErrorCollector errorCollector = new ErrorCollector();
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private WorkCommitter workCommitter;
  private FakeWindmillServer fakeWindmillServer;
  private Supplier<CloseableStream<CommitWorkStream>> commitWorkStreamFactory;

  private static void waitForExpectedSetSize(Set<?> s, int expectedSize) {
    long deadline = System.currentTimeMillis() + 100 * 1000; // 100 seconds
    while (s.size() < expectedSize) {
      try {
        Thread.sleep(10);
        if (System.currentTimeMillis() > deadline) {
          throw new RuntimeException(
              "Timed out waiting for expected set size to be: "
                  + expectedSize
                  + " but was: "
                  + s.size());
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    assertThat(s).hasSize(expectedSize);
  }

  private static Work createMockWork(long workToken) {
    WorkItem workItem =
        WorkItem.newBuilder()
            .setKey(ByteString.EMPTY)
            .setWorkToken(workToken)
            .setCacheToken(1L)
            .setShardingKey(2L)
            .build();
    return Work.create(
        workItem,
        workItem.getSerializedSize(),
        Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
        Work.createProcessingContext(
            "computationId",
            new FakeGetDataClient(),
            ignored -> {
              throw new UnsupportedOperationException();
            },
            mock(HeartbeatSender.class)),
        false,
        Instant::now,
        ImmutableList.of());
  }

  private static ComputationState createComputationState(String computationId) {
    return new ComputationState(
        computationId,
        new MapTask().setSystemName("system").setStageName("stage"),
        mock(BoundedQueueExecutor.class),
        ImmutableMap.of(),
        null);
  }

  private static CompleteCommit asCompleteCommit(
      String computationId, Work work, Windmill.CommitStatus status) {
    Windmill.CommitStatus finalStatus = work.isFailed() ? Windmill.CommitStatus.ABORTED : status;
    return CompleteCommit.create(
        computationId, work.getShardedKey(), work.id(), finalStatus, /* retryableFailure= */ false);
  }

  @Before
  public void setUp() throws IOException {
    fakeWindmillServer =
        new FakeWindmillServer(
            errorCollector, ignored -> Optional.of(mock(ComputationState.class)));
    commitWorkStreamFactory =
        WindmillStreamPool.create(
                1, Duration.standardMinutes(1), fakeWindmillServer::commitWorkStream)
            ::getCloseableStream;
  }

  private WorkCommitter createWorkCommitter(Consumer<CompleteCommit> onCommitComplete) {
    return StreamingEngineWorkCommitter.builder()
        .setCommitByteSemaphore(Commits.maxCommitByteSemaphore())
        .setCommitWorkStreamFactory(commitWorkStreamFactory)
        .setOnCommitComplete(onCommitComplete)
        .build();
  }

  @Test
  public void testCommit_sendsCommitsToStreamingEngine() {
    Set<CompleteCommit> completeCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
    workCommitter = createWorkCommitter(completeCommits::add);
    List<Commit> commits = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      Work work = createMockWork(i);
      WorkItemCommitRequest commitRequest =
          WorkItemCommitRequest.newBuilder()
              .setKey(work.getWorkItem().getKey())
              .setShardingKey(work.getWorkItem().getShardingKey())
              .setWorkToken(work.getWorkItem().getWorkToken())
              .setCacheToken(work.getWorkItem().getCacheToken())
              .build();
      commits.add(Commit.create(commitRequest, createComputationState("computationId-" + i), work));
    }

    workCommitter.start();
    commits.parallelStream().forEach(workCommitter::commit);

    Map<Long, WorkItemCommitRequest> committed =
        fakeWindmillServer.waitForAndGetCommits(commits.size());
    waitForExpectedSetSize(completeCommits, 5);

    for (Commit commit : commits) {
      assertThat(commit.workBatch()).hasSize(1);
      WorkItemCommitRequest request =
          committed.get(commit.workBatch().get(0).getWorkItem().getWorkToken());
      assertNotNull(request);
      assertThat(request).isEqualTo(commit.singleKeyRequest());
      assertThat(completeCommits)
          .contains(
              asCompleteCommit(
                  commit.computationId(), commit.workBatch().get(0), Windmill.CommitStatus.OK));
    }

    workCommitter.stop();
  }

  @Test
  public void testCommit_handlesFailedCommits() {
    Set<CompleteCommit> completeCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
    workCommitter = createWorkCommitter(completeCommits::add);
    List<Commit> commits = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      Work work = createMockWork(i);
      // Fail half of the work.
      if (i % 2 == 0) {
        work.setFailed();
      }
      WorkItemCommitRequest commitRequest =
          WorkItemCommitRequest.newBuilder()
              .setKey(work.getWorkItem().getKey())
              .setShardingKey(work.getWorkItem().getShardingKey())
              .setWorkToken(work.getWorkItem().getWorkToken())
              .setCacheToken(work.getWorkItem().getCacheToken())
              .build();
      commits.add(Commit.create(commitRequest, createComputationState("computationId-" + i), work));
    }

    workCommitter.start();
    commits.parallelStream().forEach(workCommitter::commit);

    Map<Long, WorkItemCommitRequest> committed =
        fakeWindmillServer.waitForAndGetCommits(commits.size() / 2);
    waitForExpectedSetSize(completeCommits, 10);

    for (Commit commit : commits) {
      assertThat(commit.workBatch()).hasSize(1);
      if (commit.workBatch().get(0).isFailed()) {
        assertThat(completeCommits)
            .contains(
                asCompleteCommit(
                    commit.computationId(),
                    commit.workBatch().get(0),
                    Windmill.CommitStatus.ABORTED));
        assertThat(committed)
            .doesNotContainKey(commit.workBatch().get(0).getWorkItem().getWorkToken());
      } else {
        assertThat(completeCommits)
            .contains(
                asCompleteCommit(
                    commit.computationId(), commit.workBatch().get(0), Windmill.CommitStatus.OK));
        assertThat(committed)
            .containsEntry(
                commit.workBatch().get(0).getWorkItem().getWorkToken(), commit.singleKeyRequest());
      }
    }

    workCommitter.stop();
  }

  @Test
  public void testCommit_handlesCompleteCommits_commitStatusNotOK() {
    Set<CompleteCommit> completeCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
    workCommitter = createWorkCommitter(completeCommits::add);
    Map<WorkId, Windmill.CommitStatus> expectedCommitStatus = new HashMap<>();
    Random commitStatusSelector = new Random();
    int commitStatusSelectorBound = Windmill.CommitStatus.values().length - 1;
    // Compute the CommitStatus randomly, to test plumbing of different commitStatuses to
    // StreamingEngine.
    Function<Work, Windmill.CommitStatus> computeCommitStatusForTest =
        work -> {
          Windmill.CommitStatus commitStatus =
              work.getWorkItem().getWorkToken() % 2 == 0
                  ? Windmill.CommitStatus.values()[
                      commitStatusSelector.nextInt(commitStatusSelectorBound)]
                  : OK;
          expectedCommitStatus.put(work.id(), commitStatus);
          return commitStatus;
        };

    List<Commit> commits = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      Work work = createMockWork(i);
      WorkItemCommitRequest commitRequest =
          WorkItemCommitRequest.newBuilder()
              .setKey(work.getWorkItem().getKey())
              .setShardingKey(work.getWorkItem().getShardingKey())
              .setWorkToken(work.getWorkItem().getWorkToken())
              .setCacheToken(work.getWorkItem().getCacheToken())
              .build();
      commits.add(Commit.create(commitRequest, createComputationState("computationId-" + i), work));
      fakeWindmillServer
          .whenCommitWorkStreamCalled()
          .put(work.id(), computeCommitStatusForTest.apply(work));
    }

    workCommitter.start();
    commits.parallelStream().forEach(workCommitter::commit);

    Map<Long, WorkItemCommitRequest> committed =
        fakeWindmillServer.waitForAndGetCommits(commits.size());
    waitForExpectedSetSize(completeCommits, commits.size());

    for (Commit commit : commits) {
      assertThat(commit.workBatch()).hasSize(1);
      WorkItemCommitRequest request =
          committed.get(commit.workBatch().get(0).getWorkItem().getWorkToken());
      assertNotNull(request);
      assertThat(request).isEqualTo(commit.singleKeyRequest());
      assertThat(completeCommits)
          .contains(
              asCompleteCommit(
                  commit.computationId(),
                  commit.workBatch().get(0),
                  expectedCommitStatus.get(commit.workBatch().get(0).id())));
    }

    workCommitter.stop();
  }

  @Test
  public void testStop_drainsCommitQueue() {
    // Use this fake to queue up commits on the committer.
    Supplier<CommitWorkStream> fakeCommitWorkStream =
        () ->
            new CommitWorkStream() {

              @Override
              public void start() {}

              @Override
              public RequestBatcher batcher() {
                return new RequestBatcher() {
                  @Override
                  public boolean commitWorkItem(
                      String computation,
                      WorkItemCommitRequest request,
                      Consumer<Windmill.CommitStatus> onDone) {
                    return false;
                  }

                  @Override
                  public boolean commitMultiKeyWorkItem(
                      String computation,
                      Windmill.MultiKeyWorkItemCommitRequest request,
                      Consumer<Windmill.CommitStatus> onDone) {
                    return false;
                  }

                  @Override
                  public void flush() {}
                };
              }

              @Override
              public void halfClose() {}

              @Override
              public boolean awaitTermination(int time, TimeUnit unit) {
                return false;
              }

              @Override
              public Instant startTime() {
                return Instant.now();
              }

              @Override
              public String backendWorkerToken() {
                return "";
              }

              @Override
              public void shutdown() {}
            };

    commitWorkStreamFactory =
        WindmillStreamPool.create(1, Duration.standardMinutes(1), fakeCommitWorkStream)
            ::getCloseableStream;

    Set<CompleteCommit> completeCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
    workCommitter = createWorkCommitter(completeCommits::add);

    List<Commit> commits = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      Work work = createMockWork(i);
      WorkItemCommitRequest commitRequest =
          WorkItemCommitRequest.newBuilder()
              .setKey(work.getWorkItem().getKey())
              .setShardingKey(work.getWorkItem().getShardingKey())
              .setWorkToken(work.getWorkItem().getWorkToken())
              .setCacheToken(work.getWorkItem().getCacheToken())
              .build();
      commits.add(Commit.create(commitRequest, createComputationState("computationId-" + i), work));
    }

    workCommitter.start();
    commits.parallelStream().forEach(workCommitter::commit);
    workCommitter.stop();

    assertThat(commits.size()).isEqualTo(completeCommits.size());
    for (CompleteCommit completeCommit : completeCommits) {
      assertThat(completeCommit.status()).isEqualTo(Windmill.CommitStatus.ABORTED);
      assertThat(completeCommit.retryableFailure()).isFalse();
    }

    for (Commit commit : commits) {
      assertThat(commit.workBatch()).hasSize(1);
      assertTrue(commit.workBatch().get(0).isFailed());
    }
  }

  @Test
  public void testMultipleCommitSendersSingleStream() {
    commitWorkStreamFactory =
        WindmillStreamPool.create(
                1, Duration.standardMinutes(1), fakeWindmillServer::commitWorkStream)
            ::getCloseableStream;
    Set<CompleteCommit> completeCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
    workCommitter =
        StreamingEngineWorkCommitter.builder()
            .setCommitByteSemaphore(Commits.maxCommitByteSemaphore())
            .setCommitWorkStreamFactory(commitWorkStreamFactory)
            .setNumCommitSenders(5)
            .setOnCommitComplete(completeCommits::add)
            .build();

    List<Commit> commits = new ArrayList<>();
    for (int i = 1; i <= 500; i++) {
      Work work = createMockWork(i);
      WorkItemCommitRequest commitRequest =
          WorkItemCommitRequest.newBuilder()
              .setKey(work.getWorkItem().getKey())
              .setShardingKey(work.getWorkItem().getShardingKey())
              .setWorkToken(work.getWorkItem().getWorkToken())
              .setCacheToken(work.getWorkItem().getCacheToken())
              .build();
      commits.add(Commit.create(commitRequest, createComputationState("computationId-" + i), work));
    }

    workCommitter.start();
    commits.parallelStream().forEach(workCommitter::commit);
    Map<Long, WorkItemCommitRequest> committed =
        fakeWindmillServer.waitForAndGetCommits(commits.size());
    waitForExpectedSetSize(completeCommits, commits.size());

    for (Commit commit : commits) {
      assertThat(commit.workBatch()).hasSize(1);
      WorkItemCommitRequest request =
          committed.get(commit.workBatch().get(0).getWorkItem().getWorkToken());
      assertNotNull(request);
      assertThat(request).isEqualTo(commit.singleKeyRequest());
      assertThat(completeCommits)
          .contains(
              asCompleteCommit(
                  commit.computationId(), commit.workBatch().get(0), Windmill.CommitStatus.OK));
    }

    workCommitter.stop();
  }

  @Test
  public void testStop_drainsCommitQueue_concurrentCommit()
      throws InterruptedException, ExecutionException, TimeoutException {
    Set<CompleteCommit> completeCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
    workCommitter =
        StreamingEngineWorkCommitter.builder()
            // Set the semaphore to only allow a single commit at a time.
            // This creates a bottleneck on purpose to trigger race conditions during shutdown.
            .setCommitByteSemaphore(WeightedSemaphore.create(1, (commit) -> 1))
            .setCommitWorkStreamFactory(commitWorkStreamFactory)
            .setOnCommitComplete(completeCommits::add)
            .build();

    int numThreads = 5;
    ExecutorService producer = Executors.newFixedThreadPool(numThreads);
    AtomicBoolean producing = new AtomicBoolean(true);
    AtomicLong sentCommits = new AtomicLong(0);

    workCommitter.start();

    AtomicLong workToken = new AtomicLong(0);
    List<Future<?>> futures = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      futures.add(
          producer.submit(
              () -> {
                while (producing.get()) {
                  Work work = createMockWork(workToken.getAndIncrement());
                  WorkItemCommitRequest commitRequest =
                      WorkItemCommitRequest.newBuilder()
                          .setKey(work.getWorkItem().getKey())
                          .setShardingKey(work.getWorkItem().getShardingKey())
                          .setWorkToken(work.getWorkItem().getWorkToken())
                          .setCacheToken(work.getWorkItem().getCacheToken())
                          .build();
                  Commit commit =
                      Commit.create(commitRequest, createComputationState("computationId"), work);
                  workCommitter.commit(commit);
                  sentCommits.incrementAndGet();
                }
              }));
    }

    // Let it run for a bit
    Thread.sleep(100);

    workCommitter.stop();
    producing.set(false);
    producer.shutdown();
    assertTrue(producer.awaitTermination(10, TimeUnit.SECONDS));
    for (Future<?> future : futures) {
      future.get(10, TimeUnit.SECONDS);
    }

    waitForExpectedSetSize(completeCommits, sentCommits.intValue());
  }

  @Test
  public void testCommit_multiKeyCommitSuccess() {
    Set<CompleteCommit> completeCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
    workCommitter = createWorkCommitter(completeCommits::add);

    Work workA = createMockWork(101L);
    Work workB = createMockWork(102L);
    Work workC = createMockWork(103L);

    Windmill.MultiKeyWorkItemCommitRequest multiKeyRequest =
        Windmill.MultiKeyWorkItemCommitRequest.newBuilder()
            .addRequests(
                Windmill.WorkItemCommitRequest.newBuilder()
                    .setKey(workA.getWorkItem().getKey())
                    .setShardingKey(workA.getWorkItem().getShardingKey())
                    .setWorkToken(workA.getWorkItem().getWorkToken())
                    .setCacheToken(workA.getWorkItem().getCacheToken())
                    .build())
            .addRequests(
                Windmill.WorkItemCommitRequest.newBuilder()
                    .setKey(workB.getWorkItem().getKey())
                    .setShardingKey(workB.getWorkItem().getShardingKey())
                    .setWorkToken(workB.getWorkItem().getWorkToken())
                    .setCacheToken(workB.getWorkItem().getCacheToken())
                    .build())
            .addRequests(
                Windmill.WorkItemCommitRequest.newBuilder()
                    .setKey(workC.getWorkItem().getKey())
                    .setShardingKey(workC.getWorkItem().getShardingKey())
                    .setWorkToken(workC.getWorkItem().getWorkToken())
                    .setCacheToken(workC.getWorkItem().getCacheToken())
                    .build())
            .build();

    Commit commit =
        Commit.createMultiKey(
            multiKeyRequest,
            createComputationState("computationId"),
            ImmutableList.of(workA, workB, workC));

    workCommitter.start();
    workCommitter.commit(commit);

    // Wait for the server to receive and process the commits
    fakeWindmillServer.waitForAndGetCommits(3);
    waitForExpectedSetSize(completeCommits, 3);

    // Verify that FakeWindmillServer received all 3 work requests in multiKeyCommitsReceived
    List<Windmill.MultiKeyWorkItemCommitRequest> multiKeyCommits =
        fakeWindmillServer.getMultiKeyCommitsReceived();
    assertThat(multiKeyCommits).hasSize(1);
    assertThat(multiKeyCommits.get(0)).isEqualTo(multiKeyRequest);

    // Verify all three works are completed successfully
    assertThat(completeCommits)
        .containsExactly(
            CompleteCommit.create(
                "computationId",
                workA.getShardedKey(),
                workA.id(),
                CommitStatus.OK,
                /* retryableFailure= */ false),
            CompleteCommit.create(
                "computationId",
                workB.getShardedKey(),
                workB.id(),
                CommitStatus.OK,
                /* retryableFailure= */ false),
            CompleteCommit.create(
                "computationId",
                workC.getShardedKey(),
                workC.id(),
                CommitStatus.OK,
                /* retryableFailure= */ false));

    // There should be no more commits in the queue
    assertEquals(0, workCommitter.currentActiveCommitBytes());
    workCommitter.stop();
  }

  @Test
  public void testCommit_multiKeyCommitFailedWork() {
    Set<CompleteCommit> completeCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
    workCommitter = createWorkCommitter(completeCommits::add);

    Work workA = createMockWork(101L);
    Work workB = createMockWork(102L);
    Work workC = createMockWork(103L);

    // Mark non-primary key B as failed
    workB.setFailed();

    Windmill.MultiKeyWorkItemCommitRequest multiKeyRequest =
        Windmill.MultiKeyWorkItemCommitRequest.newBuilder()
            .addRequests(
                Windmill.WorkItemCommitRequest.newBuilder()
                    .setKey(workA.getWorkItem().getKey())
                    .setShardingKey(workA.getWorkItem().getShardingKey())
                    .setWorkToken(workA.getWorkItem().getWorkToken())
                    .setCacheToken(workA.getWorkItem().getCacheToken())
                    .build())
            .addRequests(
                Windmill.WorkItemCommitRequest.newBuilder()
                    .setKey(workB.getWorkItem().getKey())
                    .setShardingKey(workB.getWorkItem().getShardingKey())
                    .setWorkToken(workB.getWorkItem().getWorkToken())
                    .setCacheToken(workB.getWorkItem().getCacheToken())
                    .build())
            .addRequests(
                Windmill.WorkItemCommitRequest.newBuilder()
                    .setKey(workC.getWorkItem().getKey())
                    .setShardingKey(workC.getWorkItem().getShardingKey())
                    .setWorkToken(workC.getWorkItem().getWorkToken())
                    .setCacheToken(workC.getWorkItem().getCacheToken())
                    .build())
            .build();

    Commit commit =
        Commit.createMultiKey(
            multiKeyRequest,
            createComputationState("computationId"),
            ImmutableList.of(workA, workB, workC));

    workCommitter.start();
    workCommitter.commit(commit);

    // The entire batch must be aborted immediately without making network calls
    waitForExpectedSetSize(completeCommits, 3);

    // Verify all three works are aborted individually
    assertThat(completeCommits)
        .containsExactly(
            CompleteCommit.create(
                "computationId",
                workA.getShardedKey(),
                workA.id(),
                CommitStatus.ABORTED,
                /* retryableFailure= */ true),
            CompleteCommit.create(
                "computationId",
                workB.getShardedKey(),
                workB.id(),
                CommitStatus.ABORTED,
                /* retryableFailure= */ false),
            CompleteCommit.create(
                "computationId",
                workC.getShardedKey(),
                workC.id(),
                CommitStatus.ABORTED,
                /* retryableFailure= */ true));

    // There should be no more commits in the queue
    assertEquals(0, workCommitter.currentActiveCommitBytes());
    workCommitter.stop();
  }

  @Test
  public void testCommit_multiKeyCommitStatusNotOK() {
    Set<CompleteCommit> completeCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
    workCommitter = createWorkCommitter(completeCommits::add);

    Work workA = createMockWork(101L);
    Work workB = createMockWork(102L);
    Work workC = createMockWork(103L);

    Windmill.MultiKeyWorkItemCommitRequest multiKeyRequest =
        Windmill.MultiKeyWorkItemCommitRequest.newBuilder()
            .addRequests(
                Windmill.WorkItemCommitRequest.newBuilder()
                    .setKey(workA.getWorkItem().getKey())
                    .setShardingKey(workA.getWorkItem().getShardingKey())
                    .setWorkToken(workA.getWorkItem().getWorkToken())
                    .setCacheToken(workA.getWorkItem().getCacheToken())
                    .build())
            .addRequests(
                Windmill.WorkItemCommitRequest.newBuilder()
                    .setKey(workB.getWorkItem().getKey())
                    .setShardingKey(workB.getWorkItem().getShardingKey())
                    .setWorkToken(workB.getWorkItem().getWorkToken())
                    .setCacheToken(workB.getWorkItem().getCacheToken())
                    .build())
            .addRequests(
                Windmill.WorkItemCommitRequest.newBuilder()
                    .setKey(workC.getWorkItem().getKey())
                    .setShardingKey(workC.getWorkItem().getShardingKey())
                    .setWorkToken(workC.getWorkItem().getWorkToken())
                    .setCacheToken(workC.getWorkItem().getCacheToken())
                    .build())
            .build();

    Commit commit =
        Commit.createMultiKey(
            multiKeyRequest,
            createComputationState("computationId"),
            ImmutableList.of(workA, workB, workC));

    // Respond to multi key commit with NOT_FOUND status.
    fakeWindmillServer.setMultiKeyCommitStatus(CommitStatus.NOT_FOUND);

    workCommitter.start();
    workCommitter.commit(commit);

    // Wait for the server to receive and process the commits
    fakeWindmillServer.waitForAndGetCommits(3);
    waitForExpectedSetSize(completeCommits, 3);

    // Verify that FakeWindmillServer received the multi-key commit
    List<Windmill.MultiKeyWorkItemCommitRequest> multiKeyCommits =
        fakeWindmillServer.getMultiKeyCommitsReceived();
    assertThat(multiKeyCommits).hasSize(1);
    assertThat(multiKeyCommits.get(0)).isEqualTo(multiKeyRequest);

    // Verify all three works in the multi-key commit are completed with NOT_FOUND status
    assertThat(completeCommits)
        .containsExactly(
            CompleteCommit.create(
                "computationId",
                workA.getShardedKey(),
                workA.id(),
                CommitStatus.NOT_FOUND,
                /* retryableFailure= */ false),
            CompleteCommit.create(
                "computationId",
                workB.getShardedKey(),
                workB.id(),
                CommitStatus.NOT_FOUND,
                /* retryableFailure= */ false),
            CompleteCommit.create(
                "computationId",
                workC.getShardedKey(),
                workC.id(),
                CommitStatus.NOT_FOUND,
                /* retryableFailure= */ false));

    // There should be no more commits in the queue
    assertEquals(0, workCommitter.currentActiveCommitBytes());
    workCommitter.stop();
  }
}
