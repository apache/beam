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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
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
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Owns and maintains a pool of streams used to fetch {@link
 * org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem}(s) from a specific source.
 */
@Internal
@ThreadSafe
@SuppressWarnings("unused")
public final class WindmillStreamPoolSender implements WindmillStreamSender {
  private final AtomicReference<GetWorkBudget> getWorkBudget;
  private final WindmillConnection connection;
  private final GetWorkRequest getWorkRequest;
  private final GrpcWindmillStreamFactory streamingEngineStreamFactory;
  private final WorkCommitter workCommitter;
  private final GetDataClient getDataClient;
  private final HeartbeatSender heartbeatSender;
  private final StreamingWorkScheduler streamingWorkScheduler;
  private final Runnable waitForResources;
  private final Function<String, Optional<ComputationState>> computationStateFetcher;
  private @Nullable GetWorkStream getWorkStream;
  private @Nullable SingleSourceWorkerHarness singleSourceWorkerHarness;

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
    this.connection = connection;
    this.getWorkRequest = getWorkRequest;
    this.getWorkBudget = getWorkBudget;
    this.streamingEngineStreamFactory = streamingEngineStreamFactory;
    this.getDataClient = getDataClient;
    this.heartbeatSender = heartbeatSender;
    this.streamingWorkScheduler = streamingWorkScheduler;
    this.waitForResources = waitForResources;
    this.computationStateFetcher = computationStateFetcher;
    this.workCommitter = workCommitter;
  }

  public static WindmillStreamPoolSender create(
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
    WindmillStreamPoolSender windmillStreamPoolSender =
        new WindmillStreamPoolSender(
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
    windmillStreamPoolSender.singleSourceWorkerHarness =
        SingleSourceWorkerHarness.builder()
            .setWorkCommitter(workCommitter)
            .setGetDataClient(getDataClient)
            .setHeartbeatSender(heartbeatSender)
            .setStreamingWorkScheduler(streamingWorkScheduler)
            .setWaitForResources(waitForResources)
            .setComputationStateFetcher(computationStateFetcher)
            .setGetWorkSender(
                SingleSourceWorkerHarness.GetWorkSender.forStreamingEngine(
                    windmillStreamPoolSender::createGetWorkStream))
            .build();
    return windmillStreamPoolSender;
  }

  private GetWorkStream createGetWorkStream(WorkItemReceiver workItemReceiver) {
    // Creating a local variable first instead of directly assigning it to the instance variable
    // to keep the compiler happy which otherwise complains NonNull value is being assinged to
    // Nullable.
    GetWorkStream workStream =
        streamingEngineStreamFactory.createGetWorkStream(
            connection.currentStub(), getWorkRequest, workItemReceiver);
    workStream.start();
    this.getWorkStream = workStream;
    return workStream;
  }

  @Override
  public synchronized void start() {
    if (singleSourceWorkerHarness != null) {
      singleSourceWorkerHarness.start();
    }
  }

  @Override
  public synchronized void close() {
    if (singleSourceWorkerHarness != null) {
      singleSourceWorkerHarness.shutdown();
    }
  }

  @Override
  public synchronized void setBudget(long items, long bytes) {
    GetWorkBudget budget = GetWorkBudget.builder().setItems(items).setBytes(bytes).build();
    getWorkBudget.set(budget);
    if (this.getWorkStream != null) {
      getWorkStream.setBudget(budget);
    }
  }

  @Override
  public long getCurrentActiveCommitBytes() {
    return workCommitter.currentActiveCommitBytes();
  }
}
