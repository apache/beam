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
import static org.mockito.Mockito.mock;

import com.google.api.services.dataflow.model.MapTask;
import com.google.common.truth.Correspondence;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.beam.runners.dataflow.worker.FakeWindmillServer;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingApplianceWorkCommitterTest {
  @Rule public ErrorCollector errorCollector = new ErrorCollector();
  private FakeWindmillServer fakeWindmillServer;
  private StreamingApplianceWorkCommitter workCommitter;

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

  private StreamingApplianceWorkCommitter createWorkCommitter(
      Consumer<CompleteCommit> onCommitComplete) {
    return StreamingApplianceWorkCommitter.create(fakeWindmillServer::commitWork, onCommitComplete);
  }

  @Before
  public void setUp() {
    fakeWindmillServer =
        new FakeWindmillServer(
            errorCollector, ignored -> Optional.of(mock(ComputationState.class)));
  }

  @After
  public void cleanUp() {
    workCommitter.stop();
  }

  @Test
  public void testCommit() {
    List<CompleteCommit> completeCommits = new ArrayList<>();
    workCommitter = createWorkCommitter(completeCommits::add);
    List<Commit> commits = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      Work work = createMockWork(i);
      Windmill.WorkItemCommitRequest commitRequest =
          Windmill.WorkItemCommitRequest.newBuilder()
              .setKey(work.getWorkItem().getKey())
              .setShardingKey(work.getWorkItem().getShardingKey())
              .setWorkToken(work.getWorkItem().getWorkToken())
              .setCacheToken(work.getWorkItem().getCacheToken())
              .build();
      commits.add(Commit.create(commitRequest, createComputationState("computationId-" + i), work));
    }

    workCommitter.start();
    commits.forEach(workCommitter::commit);

    Map<Long, Windmill.WorkItemCommitRequest> committed =
        fakeWindmillServer.waitForAndGetCommits(commits.size());

    for (Commit commit : commits) {
      Windmill.WorkItemCommitRequest request =
          committed.get(commit.work().getWorkItem().getWorkToken());
      assertNotNull(request);
      assertThat(request).isEqualTo(commit.request());
    }

    assertThat(completeCommits).hasSize(commits.size());
    assertThat(completeCommits)
        .comparingElementsUsing(
            Correspondence.from(
                (CompleteCommit completeCommit, Commit commit) ->
                    completeCommit.computationId().equals(commit.computationId())
                        && completeCommit.status() == Windmill.CommitStatus.OK
                        && completeCommit.workId().equals(commit.work().id())
                        && completeCommit
                            .shardedKey()
                            .equals(
                                ShardedKey.create(
                                    commit.request().getKey(), commit.request().getShardingKey())),
                "expected to equal"))
        .containsExactlyElementsIn(commits);
  }
}
