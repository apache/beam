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
package org.apache.beam.runners.dataflow.worker.streaming;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.api.services.dataflow.model.MapTask;
import java.util.Collections;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ComputationStateTest {

  private final BoundedQueueExecutor mockExecutor = mock(BoundedQueueExecutor.class);
  private final WindmillStateCache.ForComputation mockStateCache =
      mock(WindmillStateCache.ForComputation.class);
  private final HeartbeatSender mockHeartbeatSender = mock(HeartbeatSender.class);

  private ComputationState computationState;

  private static ShardedKey shardedKey(String str, long shardKey) {
    return ShardedKey.create(ByteString.copyFromUtf8(str), shardKey);
  }

  private ExecutableWork createWork(Windmill.WorkItem workItem) {
    return ExecutableWork.create(
        Work.create(
            workItem,
            workItem.getSerializedSize(),
            Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
            Work.createProcessingContext(
                "computationId", new FakeGetDataClient(), ignored -> {}, mockHeartbeatSender),
            false,
            Instant::now,
            ImmutableList.of()),
        (work, handle) -> {});
  }

  private static Windmill.WorkItem createWorkItem(
      long workToken, long cacheToken, ShardedKey shardedKey) {
    return Windmill.WorkItem.newBuilder()
        .setShardingKey(shardedKey.shardingKey())
        .setKey(shardedKey.key())
        .setWorkToken(workToken)
        .setCacheToken(cacheToken)
        .build();
  }

  @Before
  public void setUp() {
    MapTask mapTask = new MapTask();
    mapTask.setStageName("stage");
    mapTask.setSystemName("system");
    computationState =
        new ComputationState(
            "computationId", mapTask, mockExecutor, Collections.emptyMap(), mockStateCache);
  }

  @Test
  public void testReexecuteActiveWork_workNotActive() {
    ShardedKey shardedKey = shardedKey("key", 1L);
    WorkId workId = WorkId.builder().setWorkToken(1L).setCacheToken(1L).build();

    computationState.reexecuteActiveWork(shardedKey, workId);

    verifyNoInteractions(mockExecutor);
  }

  @Test
  public void testReexecuteActiveWork_workActive() {
    ShardedKey shardedKey = shardedKey("key", 1L);
    Windmill.WorkItem workItem = createWorkItem(1L, 1L, shardedKey);
    ExecutableWork work = createWork(workItem);

    // Activate work first. This will execute it once.
    computationState.activateWork(work);
    verify(mockExecutor).execute(work, work.work().getSerializedWorkItemSize());

    // Now re-execute
    computationState.reexecuteActiveWork(shardedKey, work.id());
    verify(mockExecutor).forceExecute(work, work.work().getSerializedWorkItemSize());

    verifyNoMoreInteractions(mockExecutor);
  }
}
