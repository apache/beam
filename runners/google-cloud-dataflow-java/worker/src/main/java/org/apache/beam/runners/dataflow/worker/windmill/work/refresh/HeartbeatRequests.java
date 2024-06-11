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
package org.apache.beam.runners.dataflow.worker.windmill.work.refresh;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.joda.time.Instant;

/** Helper factory class for creating heartbeat requests. */
@Internal
public final class HeartbeatRequests {

  private HeartbeatRequests() {}

  static ImmutableListMultimap<HeartbeatSender, HeartbeatRequest> getRefreshableKeyHeartbeats(
      ImmutableListMultimap<ShardedKey, Work> activeWork,
      Instant refreshDeadline,
      DataflowExecutionStateSampler sampler) {
    return activeWork.asMap().entrySet().stream()
        .flatMap(e -> toHeartbeatRequest(e, refreshDeadline, sampler))
        .collect(toImmutableListMultimap(Pair::getKey, Pair::getValue));
  }

  private static Stream<Pair<HeartbeatSender, HeartbeatRequest>> toHeartbeatRequest(
      Map.Entry<ShardedKey, Collection<Work>> shardedKeyAndWorkQueue,
      Instant refreshDeadline,
      DataflowExecutionStateSampler sampler) {
    ShardedKey shardedKey = shardedKeyAndWorkQueue.getKey();
    Collection<Work> workQueue = shardedKeyAndWorkQueue.getValue();
    return getRefreshableWork(workQueue, refreshDeadline)
        // Don't send heartbeats for queued work we already know is failed.
        .filter(work -> !work.isFailed())
        .map(
            work ->
                Pair.of(work.heartbeatSender(), createHeartbeatRequest(shardedKey, work, sampler)));
  }

  private static HeartbeatRequest createHeartbeatRequest(
      ShardedKey shardedKey, Work work, DataflowExecutionStateSampler sampler) {
    return HeartbeatRequest.newBuilder()
        .setShardingKey(shardedKey.shardingKey())
        .setWorkToken(work.getWorkItem().getWorkToken())
        .setCacheToken(work.getWorkItem().getCacheToken())
        .addAllLatencyAttribution(work.getLatencyAttributions(/* isHeartbeat= */ true, sampler))
        .build();
  }

  private static Stream<Work> getRefreshableWork(
      Collection<Work> workQueue, Instant refreshDeadline) {
    return workQueue.stream().filter(work -> work.isRefreshable(refreshDeadline));
  }
}
