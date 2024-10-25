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

import com.google.auto.value.AutoValue;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.RefreshableWork;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;

/** Heartbeat requests and the work that was used to generate the heartbeat requests. */
@AutoValue
abstract class Heartbeats {

  static Heartbeats.Builder builder() {
    return new AutoValue_Heartbeats.Builder();
  }

  abstract ImmutableList<RefreshableWork> work();

  abstract ImmutableListMultimap<String, Windmill.HeartbeatRequest> heartbeatRequests();

  final int size() {
    return heartbeatRequests().asMap().size();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract ImmutableList.Builder<RefreshableWork> workBuilder();

    abstract ImmutableListMultimap.Builder<String, Windmill.HeartbeatRequest>
        heartbeatRequestsBuilder();

    final Builder add(
        String computationId, RefreshableWork work, DataflowExecutionStateSampler sampler) {
      workBuilder().add(work);
      heartbeatRequestsBuilder().put(computationId, createHeartbeatRequest(work, sampler));
      return this;
    }

    private Windmill.HeartbeatRequest createHeartbeatRequest(
        RefreshableWork work, DataflowExecutionStateSampler sampler) {
      return Windmill.HeartbeatRequest.newBuilder()
          .setShardingKey(work.getShardedKey().shardingKey())
          .setWorkToken(work.id().workToken())
          .setCacheToken(work.id().cacheToken())
          .addAllLatencyAttribution(work.getHeartbeatLatencyAttributions(sampler))
          .build();
    }

    abstract Heartbeats build();
  }
}
