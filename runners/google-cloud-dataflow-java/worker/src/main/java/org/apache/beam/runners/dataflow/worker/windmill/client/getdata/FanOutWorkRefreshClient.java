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
package org.apache.beam.runners.dataflow.worker.windmill.client.getdata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.Heartbeats;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * {@link WorkRefreshClient} that fans out heartbeats to all {@link HeartbeatSender}(s) in parallel
 * passed into {@link #refreshActiveWork(Map)}
 */
@Internal
public final class FanOutWorkRefreshClient implements WorkRefreshClient {
  private static final String FAN_OUT_REFRESH_WORK_EXECUTOR_NAME =
      "FanOutActiveWorkRefreshExecutor";

  private final ThrottlingGetDataMetricTracker getDataMetricTracker;
  private final ExecutorService fanOutActiveWorkRefreshExecutor;

  public FanOutWorkRefreshClient(ThrottlingGetDataMetricTracker getDataMetricTracker) {
    this.getDataMetricTracker = getDataMetricTracker;
    this.fanOutActiveWorkRefreshExecutor =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat(FAN_OUT_REFRESH_WORK_EXECUTOR_NAME).build());
  }

  @Override
  public void refreshActiveWork(Map<HeartbeatSender, Heartbeats> heartbeats) {
    List<CompletableFuture<Void>> fanOutRefreshActiveWork = new ArrayList<>();
    for (Map.Entry<HeartbeatSender, Heartbeats> heartbeat : heartbeats.entrySet()) {
      fanOutRefreshActiveWork.add(sendHeartbeatOnStreamFuture(heartbeat));
    }

    // Don't block until we kick off all the refresh active work RPCs.
    @SuppressWarnings("rawtypes")
    CompletableFuture<Void> parallelFanOutRefreshActiveWork =
        CompletableFuture.allOf(fanOutRefreshActiveWork.toArray(new CompletableFuture[0]));
    parallelFanOutRefreshActiveWork.join();
  }

  private CompletableFuture<Void> sendHeartbeatOnStreamFuture(
      Map.Entry<HeartbeatSender, Heartbeats> heartbeat) {
    return CompletableFuture.runAsync(
        () -> {
          try (AutoCloseable ignored =
              getDataMetricTracker.trackHeartbeats(heartbeat.getValue().size())) {
            HeartbeatSender sender = heartbeat.getKey();
            Heartbeats heartbeats = heartbeat.getValue();
            sender.sendHeartbeats(heartbeats);
          } catch (Exception e) {
            throw new GetDataClient.GetDataException("Error refreshing heartbeats.", e);
          }
        },
        fanOutActiveWorkRefreshExecutor);
  }
}
