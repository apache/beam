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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableTable.toImmutableTable;

import com.google.auto.value.AutoValue;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.RefreshableWork;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table;
import org.joda.time.Instant;

/** Helper factory class for creating heartbeat requests. */
@Internal
public final class HeartbeatRequests {

  private HeartbeatRequests() {}

  static Table<HeartbeatSender, RefreshableWork, HeartbeatRequest> getRefreshableKeyHeartbeats(
      ImmutableListMultimap<ShardedKey, RefreshableWork> activeWork,
      Instant refreshDeadline,
      DataflowExecutionStateSampler sampler) {
    return activeWork.asMap().entrySet().stream()
        .flatMap(e -> toHeartbeatRow(e, refreshDeadline, sampler))
        .collect(toImmutableTable(HeartbeatRow::sender, HeartbeatRow::work, HeartbeatRow::request));
  }

  private static Stream<HeartbeatRow> toHeartbeatRow(
      Map.Entry<ShardedKey, Collection<RefreshableWork>> shardedKeyAndWorkQueue,
      Instant refreshDeadline,
      DataflowExecutionStateSampler sampler) {
    ShardedKey shardedKey = shardedKeyAndWorkQueue.getKey();
    Collection<RefreshableWork> workQueue = shardedKeyAndWorkQueue.getValue();
    return workQueue.stream()
        .filter(work -> work.isRefreshable(refreshDeadline))
        .map(work -> HeartbeatRow.create(work, createHeartbeatRequest(shardedKey, work, sampler)));
  }

  private static HeartbeatRequest createHeartbeatRequest(
      ShardedKey shardedKey, RefreshableWork work, DataflowExecutionStateSampler sampler) {
    return HeartbeatRequest.newBuilder()
        .setShardingKey(shardedKey.shardingKey())
        .setWorkToken(work.id().workToken())
        .setCacheToken(work.id().cacheToken())
        .addAllLatencyAttribution(work.getLatencyAttributions(/* isHeartbeat= */ true, sampler))
        .build();
  }

  @AutoValue
  abstract static class HeartbeatRow {

    private static HeartbeatRow create(RefreshableWork work, HeartbeatRequest request) {
      return new AutoValue_HeartbeatRequests_HeartbeatRow(work.heartbeatSender(), work, request);
    }

    abstract HeartbeatSender sender();

    abstract RefreshableWork work();

    abstract HeartbeatRequest request();
  }
}
