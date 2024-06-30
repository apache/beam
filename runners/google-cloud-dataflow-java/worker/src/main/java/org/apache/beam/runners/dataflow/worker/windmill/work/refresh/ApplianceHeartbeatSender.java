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

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.annotations.Internal;

/** Streaming appliance implementation of {@link HeartbeatSender}. */
@Internal
public final class ApplianceHeartbeatSender implements HeartbeatSender {
  private final Consumer<Windmill.GetDataRequest> sendHeartbeatFn;

  public ApplianceHeartbeatSender(Consumer<Windmill.GetDataRequest> sendHeartbeatFn) {
    this.sendHeartbeatFn = sendHeartbeatFn;
  }

  @Override
  public void sendHeartbeats(Map<String, List<Windmill.HeartbeatRequest>> heartbeats) {
    // This code path is only used by appliance which sends heartbeats (used to refresh active
    // work) as KeyedGetDataRequests. So we must translate the HeartbeatRequest to a
    // KeyedGetDataRequest here regardless of the value of sendKeyedGetDataRequests.
    Windmill.GetDataRequest.Builder builder = Windmill.GetDataRequest.newBuilder();

    for (Map.Entry<String, List<Windmill.HeartbeatRequest>> entry : heartbeats.entrySet()) {
      Windmill.ComputationGetDataRequest.Builder perComputationBuilder =
          Windmill.ComputationGetDataRequest.newBuilder();
      perComputationBuilder.setComputationId(entry.getKey());
      for (Windmill.HeartbeatRequest request : entry.getValue()) {
        perComputationBuilder.addRequests(
            Windmill.KeyedGetDataRequest.newBuilder()
                .setShardingKey(request.getShardingKey())
                .setWorkToken(request.getWorkToken())
                .setCacheToken(request.getCacheToken())
                .addAllLatencyAttribution(request.getLatencyAttributionList())
                .build());
      }
      builder.addRequests(perComputationBuilder.build());
    }

    sendHeartbeatFn.accept(builder.build());
  }
}
