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
import static org.junit.Assert.assertNotNull;

import com.google.api.services.dataflow.model.MapTask;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.FakeWindmillServer;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamPool;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class StreamingEngineWorkCommitterTest {

  @Rule public ErrorCollector errorCollector = new ErrorCollector();
  private StreamingEngineWorkCommitter workCommitter;
  private boolean shouldCommitWork;
  private FakeWindmillServer fakeWindmillServer;
  private Supplier<CloseableStream<CommitWorkStream>> commitWorkStreamFactory;

  private static Work createMockWork(long workToken, Consumer<Work> processWorkFn) {
    return Work.create(
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.EMPTY)
            .setWorkToken(workToken)
            .setShardingKey(workToken)
            .setCacheToken(workToken)
            .build(),
        Instant::now,
        Collections.emptyList(),
        processWorkFn);
  }

  private static ComputationState createComputationState(String computationId) {
    return new ComputationState(
        computationId,
        new MapTask().setSystemName("system").setStageName("stage"),
        Mockito.mock(BoundedQueueExecutor.class),
        ImmutableMap.of(),
        null);
  }

  @Before
  public void setUp() {
    shouldCommitWork = true;
    fakeWindmillServer =
        new FakeWindmillServer(
            errorCollector, ignored -> Optional.of(Mockito.mock(ComputationState.class)));
    commitWorkStreamFactory =
        WindmillStreamPool.create(
                1, Duration.standardMinutes(1), fakeWindmillServer::commitWorkStream)
            ::getCloseableStream;
  }

  @After
  public void cleanUp() {
    shouldCommitWork = false;
    workCommitter.stop();
  }

  private StreamingEngineWorkCommitter createWorkCommitter(
      CountDownLatch onReady,
      Consumer<Commit> onFailedCommit,
      Consumer<CompleteCommit> onCommitComplete) {
    return StreamingEngineWorkCommitter.create(
        commitWorkStreamFactory,
        1,
        () -> shouldCommitWork,
        onFailedCommit,
        onCommitComplete,
        onReady);
  }

  @Test
  public void testCommit_sendsCommitsToStreamingEngine() {
    CountDownLatch started = new CountDownLatch(1);
    workCommitter = createWorkCommitter(started, ignored -> {}, ignored -> {});
    List<Commit> commits = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      Work work = createMockWork(i, ignored -> {});
      WorkItemCommitRequest commitRequest =
          WorkItemCommitRequest.newBuilder()
              .setKey(work.getWorkItem().getKey())
              .setShardingKey(work.getWorkItem().getShardingKey())
              .setWorkToken(work.getWorkItem().getWorkToken())
              .setCacheToken(work.getWorkItem().getCacheToken())
              .build();
      commits.add(Commit.create(commitRequest, createComputationState("computationId-" + i), work));
    }

    started.countDown();
    commits.forEach(workCommitter::commit);

    Map<Long, WorkItemCommitRequest> committed =
        fakeWindmillServer.waitForAndGetCommits(commits.size());

    for (Commit commit : commits) {
      WorkItemCommitRequest request = committed.get(commit.work().getWorkItem().getWorkToken());
      assertNotNull(request);
      assertThat(request).isEqualTo(commit.request());
    }
  }

  @Test
  public void testCommit_handlesFailedCommits() {
    CountDownLatch started = new CountDownLatch(1);
    Set<Commit> failedCommits = new HashSet<>();
    workCommitter = createWorkCommitter(started, failedCommits::add, ignored -> {});
    List<Commit> commits = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      Work work = createMockWork(i, ignored -> {});
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

    started.countDown();
    commits.forEach(workCommitter::commit);

    Map<Long, WorkItemCommitRequest> committed =
        fakeWindmillServer.waitForAndGetCommits(commits.size() / 2);

    for (Commit commit : commits) {
      if (commit.work().isFailed()) {
        assertThat(failedCommits).contains(commit);
        assertThat(committed).doesNotContainKey(commit.work().getWorkItem().getWorkToken());
      }
    }
  }

  @Test
  public void testCommit_handlesCompleteCommits() {
    CountDownLatch started = new CountDownLatch(1);
    Set<Commit> completeCommits = new HashSet<>();
    workCommitter =
        createWorkCommitter(
            started, ignored -> {}, completeCommit -> completeCommits.add(completeCommit.commit()));
    List<Commit> commits = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      Work work = createMockWork(i, ignored -> {});
      WorkItemCommitRequest commitRequest =
          WorkItemCommitRequest.newBuilder()
              .setKey(work.getWorkItem().getKey())
              .setShardingKey(work.getWorkItem().getShardingKey())
              .setWorkToken(work.getWorkItem().getWorkToken())
              .setCacheToken(work.getWorkItem().getCacheToken())
              .build();
      commits.add(Commit.create(commitRequest, createComputationState("computationId-" + i), work));
    }

    started.countDown();
    commits.forEach(workCommitter::commit);

    Map<Long, WorkItemCommitRequest> committed =
        fakeWindmillServer.waitForAndGetCommits(commits.size());

    for (Commit commit : commits) {
      WorkItemCommitRequest request = committed.get(commit.work().getWorkItem().getWorkToken());
      assertNotNull(request);
      assertThat(request).isEqualTo(commit.request());
      assertThat(completeCommits).contains(commit);
    }
  }
}
