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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.api.services.dataflow.model.MapTask;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.FakeWindmillServer;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.WorkId;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
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

  private static Work createMockWork(long workToken) {
    return Work.create(
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.EMPTY)
            .setWorkToken(workToken)
            .setCacheToken(1L)
            .setShardingKey(2L)
            .build(),
        Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
        Work.createProcessingContext(
            "computationId",
            new FakeGetDataClient(),
            ignored -> {
              throw new UnsupportedOperationException();
            },
            mock(HeartbeatSender.class)),
        Instant::now,
        Collections.emptyList());
  }

  private static ComputationState createComputationState(String computationId) {
    return new ComputationState(
        computationId,
        new MapTask().setSystemName("system").setStageName("stage"),
        mock(BoundedQueueExecutor.class),
        ImmutableMap.of(),
        null);
  }

  private static CompleteCommit asCompleteCommit(Commit commit, Windmill.CommitStatus status) {
    if (commit.work().isFailed()) {
      return CompleteCommit.forFailedWork(commit);
    }

    return CompleteCommit.create(commit, status);
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
    Set<CompleteCommit> completeCommits = new HashSet<>();
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

    for (Commit commit : commits) {
      WorkItemCommitRequest request = committed.get(commit.work().getWorkItem().getWorkToken());
      assertNotNull(request);
      assertThat(request).isEqualTo(commit.request());
      assertThat(completeCommits).contains(asCompleteCommit(commit, Windmill.CommitStatus.OK));
    }

    workCommitter.stop();
  }

  @Test
  public void testCommit_handlesFailedCommits() {
    Set<CompleteCommit> completeCommits = new HashSet<>();
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

    for (Commit commit : commits) {
      if (commit.work().isFailed()) {
        assertThat(completeCommits)
            .contains(asCompleteCommit(commit, Windmill.CommitStatus.ABORTED));
        assertThat(committed).doesNotContainKey(commit.work().getWorkItem().getWorkToken());
      } else {
        assertThat(completeCommits).contains(asCompleteCommit(commit, Windmill.CommitStatus.OK));
        assertThat(committed)
            .containsEntry(commit.work().getWorkItem().getWorkToken(), commit.request());
      }
    }

    workCommitter.stop();
  }

  @Test
  public void testCommit_handlesCompleteCommits_commitStatusNotOK() {
    Set<CompleteCommit> completeCommits = new HashSet<>();
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

    for (Commit commit : commits) {
      WorkItemCommitRequest request = committed.get(commit.work().getWorkItem().getWorkToken());
      assertNotNull(request);
      assertThat(request).isEqualTo(commit.request());
      assertThat(completeCommits)
          .contains(asCompleteCommit(commit, expectedCommitStatus.get(commit.work().id())));
    }
    assertThat(completeCommits.size()).isEqualTo(commits.size());

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

    Set<CompleteCommit> completeCommits = new HashSet<>();
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
    }

    for (Commit commit : commits) {
      assertTrue(commit.work().isFailed());
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

    for (Commit commit : commits) {
      WorkItemCommitRequest request = committed.get(commit.work().getWorkItem().getWorkToken());
      assertNotNull(request);
      assertThat(request).isEqualTo(commit.request());
      assertThat(completeCommits).contains(asCompleteCommit(commit, Windmill.CommitStatus.OK));
    }

    workCommitter.stop();
  }
}
