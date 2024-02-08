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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationHeartbeatResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatResponse;
import org.apache.beam.sdk.annotations.Internal;

@Internal
public final class WorkHeartbeatProcessor implements Consumer<List<ComputationHeartbeatResponse>> {
  private final Function<String, Optional<ComputationState>> computationStateFetcher;

  public WorkHeartbeatProcessor(
      Function<String, Optional<ComputationState>> computationStateFetcher) {
    this.computationStateFetcher = computationStateFetcher;
  }

  @Override
  public void accept(List<ComputationHeartbeatResponse> responses) {
    for (ComputationHeartbeatResponse computationHeartbeatResponse : responses) {
      // Maps sharding key to (work token, cache token) for work that should be marked failed.
      Map<Long, List<FailedWorkToken>> failedWork = new HashMap<>();
      for (HeartbeatResponse heartbeatResponse :
          computationHeartbeatResponse.getHeartbeatResponsesList()) {
        if (heartbeatResponse.getFailed()) {
          failedWork
              .computeIfAbsent(heartbeatResponse.getShardingKey(), key -> new ArrayList<>())
              .add(
                  FailedWorkToken.newBuilder()
                      .setWorkToken(heartbeatResponse.getWorkToken())
                      .setCacheToken(heartbeatResponse.getCacheToken())
                      .build());
        }
      }

      computationStateFetcher
          .apply(computationHeartbeatResponse.getComputationId())
          .ifPresent(state -> state.failWork(failedWork));
    }
  }
}
