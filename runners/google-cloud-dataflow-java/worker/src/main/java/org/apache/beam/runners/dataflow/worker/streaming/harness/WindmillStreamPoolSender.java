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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns and maintains a pool of streams used to fetch {@link
 * org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem}(s) from a specific source.
 */
@Internal
@ThreadSafe
final class WindmillStreamPoolSender implements WindmillStreamSender, StreamSender {
  private static final Logger LOG = LoggerFactory.getLogger(WindmillStreamPoolSender.class);
  private final AtomicBoolean started;
  private final AtomicReference<GetWorkBudget> getWorkBudget;
  private final GetWorkStream getWorkStream;
  private final WorkCommitter workCommitter;
  private final GetDataClient getDataClient;
  private final HeartbeatSender heartbeatSender;
  private final StreamingWorkScheduler streamingWorkScheduler;
  private final Runnable waitForResources;
  private final Function<String, Optional<ComputationState>> computationStateFetcher;

  private WindmillStreamPoolSender(
      WindmillConnection connection,
      GetWorkRequest getWorkRequest,
      AtomicReference<GetWorkBudget> getWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkCommitter workCommitter,
      GetDataClient getDataClient,
      HeartbeatSender heartbeatSender,
      StreamingWorkScheduler streamingWorkScheduler,
      Runnable waitForResources,
      Function<String, Optional<ComputationState>> computationStateFetcher) {
    this.started = new AtomicBoolean(false);
    this.getWorkBudget = getWorkBudget;
    this.getDataClient = getDataClient;
    this.heartbeatSender = heartbeatSender;
    this.streamingWorkScheduler = streamingWorkScheduler;
    this.waitForResources = waitForResources;
    this.computationStateFetcher = computationStateFetcher;
    this.workCommitter = workCommitter;

    WorkItemReceiver processWorkItem =
        (computationId,
            inputDataWatermark,
            synchronizedProcessingTime,
            workItem,
            getWorkStreamLatencies) ->
            this.computationStateFetcher
                .apply(computationId)
                .ifPresent(
                    computationState -> {
                      this.waitForResources.run();
                      this.streamingWorkScheduler.scheduleWork(
                          computationState,
                          workItem,
                          Watermarks.builder()
                              .setInputDataWatermark(Preconditions.checkNotNull(inputDataWatermark))
                              .setSynchronizedProcessingTime(synchronizedProcessingTime)
                              .setOutputDataWatermark(workItem.getOutputDataWatermark())
                              .build(),
                          Work.createProcessingContext(
                              computationId,
                              this.getDataClient,
                              workCommitter::commit,
                              this.heartbeatSender),
                          getWorkStreamLatencies);
                    });
    this.getWorkStream =
        streamingEngineStreamFactory.createGetWorkStream(
            connection.stub(),
            getWorkRequest,
            processWorkItem);
  }

  static WindmillStreamPoolSender create(
      WindmillConnection connection,
      GetWorkRequest getWorkRequest,
      GetWorkBudget getWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkCommitter workCommitter,
      GetDataClient getDataClient,
      HeartbeatSender heartbeatSender,
      StreamingWorkScheduler streamingWorkScheduler,
      Runnable waitForResources,
      Function<String, Optional<ComputationState>> computationStateFetcher) {
    return new WindmillStreamPoolSender(
        connection,
        getWorkRequest,
        new AtomicReference<>(getWorkBudget),
        streamingEngineStreamFactory,
        workCommitter,
        getDataClient,
        heartbeatSender,
        streamingWorkScheduler,
        waitForResources,
        computationStateFetcher);
  }

  @SuppressWarnings("ReturnValueIgnored")
  @Override
  public void start() {
    getWorkStream.start();
    workCommitter.start();

    started.set(true);
  }

  @Override
  public void close() {

    if (started.get()) {
      getWorkStream.shutdown();
      workCommitter.stop();
    }
  }

  @Override
  public void setBudget(long items, long bytes) {
    GetWorkBudget budget = GetWorkBudget.builder().setItems(items).setBytes(bytes).build();
    getWorkBudget.set(budget);
    if (started.get()) {
      getWorkStream.setBudget(budget);
    }
  }

  @Override
  public long getCurrentActiveCommitBytes() {
    return workCommitter.currentActiveCommitBytes();
  }
}
