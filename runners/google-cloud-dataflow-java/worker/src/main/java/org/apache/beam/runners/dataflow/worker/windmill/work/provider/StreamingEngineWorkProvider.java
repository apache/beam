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
package org.apache.beam.runners.dataflow.worker.windmill.work.provider;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.joda.time.Instant;

class StreamingEngineWorkProvider extends SingleSourceWorkProvider {

  private static final int GET_WORK_STREAM_TIMEOUT_MINUTES = 3;

  private final Function<WorkItemReceiver, WindmillStream.GetWorkStream> getWorkStreamFactory;

  StreamingEngineWorkProvider(
      WorkCommitter workCommitter,
      GetDataClient getDataClient,
      HeartbeatSender heartbeatSender,
      StreamingWorkScheduler streamingWorkScheduler,
      Runnable waitForResources,
      Function<String, Optional<ComputationState>> computationStateFetcher,
      Function<WorkItemReceiver, WindmillStream.GetWorkStream> getWorkStreamFactory) {
    super(
        workCommitter,
        getDataClient,
        heartbeatSender,
        streamingWorkScheduler,
        waitForResources,
        computationStateFetcher);
    this.getWorkStreamFactory = getWorkStreamFactory;
  }

  @Override
  protected void dispatchLoop() {
    while (isRunning.get()) {
      WindmillStream.GetWorkStream stream =
          getWorkStreamFactory.apply(
              (String computation,
                  Instant inputDataWatermark,
                  Instant synchronizedProcessingTime,
                  Windmill.WorkItem workItem,
                  Collection<Windmill.LatencyAttribution> getWorkStreamLatencies) ->
                  computationStateFetcher
                      .apply(computation)
                      .ifPresent(
                          computationState -> {
                            waitForResources.run();
                            streamingWorkScheduler.scheduleWork(
                                computationState,
                                workItem,
                                Watermarks.builder()
                                    .setInputDataWatermark(inputDataWatermark)
                                    .setSynchronizedProcessingTime(synchronizedProcessingTime)
                                    .setOutputDataWatermark(workItem.getOutputDataWatermark())
                                    .build(),
                                Work.createProcessingContext(
                                    computationState.getComputationId(),
                                    getDataClient,
                                    workCommitter::commit,
                                    heartbeatSender),
                                getWorkStreamLatencies);
                          }));
      try {
        // Reconnect every now and again to enable better load balancing.
        // If at any point the server closes the stream, we will reconnect immediately; otherwise
        // we half-close the stream after some time and create a new one.
        if (!stream.awaitTermination(GET_WORK_STREAM_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
          stream.halfClose();
        }
      } catch (InterruptedException e) {
        // Continue processing until !running.get()
      }
    }
  }

  @Override
  protected String debugName() {
    return "StreamingEngine";
  }
}
