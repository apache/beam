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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.AsyncCommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.CompleteCommit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.StreamingEngineWorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;

/**
 * Implementation of {@link
 * org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream} that
 * manages its own commit queue, and asynchronously CommitWork RPCs to Streaming Engine as callers
 * queue commits on the internal queue.
 *
 * <p>Callers should call {@link #queueCommit(Commit)} when work is ready to be committed.
 */
public class GrpcDirectCommitWorkStream extends GrpcCommitWorkStream
    implements AsyncCommitWorkStream {
  private final StreamingEngineWorkCommitter streamingEngineWorkCommitter;

  private GrpcDirectCommitWorkStream(
      Function<StreamObserver<StreamingCommitResponse>, StreamObserver<StreamingCommitWorkRequest>>
          startCommitWorkRpcFn,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      ThrottleTimer commitWorkThrottleTimer,
      JobHeader jobHeader,
      AtomicLong idGenerator,
      int streamingRpcBatchLimit,
      Consumer<Commit> onFailedCommit,
      Consumer<CompleteCommit> onCommitComplete) {
    super(
        startCommitWorkRpcFn,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        commitWorkThrottleTimer,
        jobHeader,
        idGenerator,
        streamingRpcBatchLimit);
    this.streamingEngineWorkCommitter =
        StreamingEngineWorkCommitter.createDirect(
            () -> this, 1, () -> !super.clientClosed.get(), onFailedCommit, onCommitComplete);
  }

  public static GrpcDirectCommitWorkStream create(
      Function<StreamObserver<StreamingCommitResponse>, StreamObserver<StreamingCommitWorkRequest>>
          startCommitWorkRpcFn,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      ThrottleTimer commitWorkThrottleTimer,
      JobHeader jobHeader,
      AtomicLong idGenerator,
      int streamingRpcBatchLimit,
      Consumer<Commit> onFailedCommit,
      Consumer<CompleteCommit> onCommitComplete) {
    GrpcDirectCommitWorkStream commitWorkStream =
        new GrpcDirectCommitWorkStream(
            startCommitWorkRpcFn,
            backoff,
            streamObserverFactory,
            streamRegistry,
            logEveryNStreamFailures,
            commitWorkThrottleTimer,
            jobHeader,
            idGenerator,
            streamingRpcBatchLimit,
            onFailedCommit,
            onCommitComplete);
    commitWorkStream.startStream();
    return commitWorkStream;
  }

  @Override
  public void queueCommit(Commit commit) {
    commit.work().setState(Work.State.QUEUED);
    streamingEngineWorkCommitter.commit(commit);
  }

  @Override
  public long currentActiveCommitBytes() {
    return streamingEngineWorkCommitter.currentActiveCommitBytes();
  }

  @Override
  public synchronized void close() {
    super.close();
    streamingEngineWorkCommitter.stop();
  }
}
