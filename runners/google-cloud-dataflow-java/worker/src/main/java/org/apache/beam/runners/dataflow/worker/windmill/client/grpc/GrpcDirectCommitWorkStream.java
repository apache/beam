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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.streaming.Commit;
import org.apache.beam.runners.dataflow.worker.streaming.WeightedBoundedQueue;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.CompleteCommit;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.AsyncCommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  @VisibleForTesting static final int COMMIT_BATCH_SIZE = 5;
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDirectCommitWorkStream.class);
  private static final int MAX_COMMIT_QUEUE_BYTES = 500 << 20; // 500MB
  private final WeightedBoundedQueue<Commit> commitQueue;
  private final ExecutorService commitSender;
  private final Consumer<Commit> onFailedCommit;
  private final Consumer<CompleteCommit> onCommitComplete;
  private final AtomicLong activeCommitBytes;

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
    this.commitQueue =
        WeightedBoundedQueue.create(
            MAX_COMMIT_QUEUE_BYTES, commit -> Math.min(MAX_COMMIT_QUEUE_BYTES, commit.getSize()));
    this.commitSender =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("DirectCommitWorkSenderThread")
                .setUncaughtExceptionHandler(
                    (t, e) ->
                        LOG.error(
                            "{} failed due to uncaught exception during execution. ",
                            t.getName(),
                            e))
                .build());
    this.onFailedCommit = onFailedCommit;
    this.onCommitComplete = onCommitComplete;
    this.activeCommitBytes = new AtomicLong();
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
    commitWorkStream.startCommitSender();
    return commitWorkStream;
  }

  @Override
  public void queueCommit(Commit commit) {
    commitQueue.put(commit);
  }

  @Override
  public long currentActiveCommitBytes() {
    return activeCommitBytes.get();
  }

  @Override
  public synchronized void close() {
    super.close();
    commitSender.shutdownNow();
  }

  @SuppressWarnings("FutureReturnValueIgnored" // Starts the background thread.
  )
  private void startCommitSender() {
    commitSender.submit(
        () -> {
          @Nullable Commit lastAttempedCommit = waitForInitialCommit();
          while (!super.clientClosed.get()) {
            // We initialize the commit stream only after we have a commit to make sure it is fresh.
            if (!addCommitToStream(lastAttempedCommit)) {
              throw new IllegalStateException(
                  "Initial commit on flushed stream should always be accepted.");
            }
            // Batch additional commits to the stream and possibly make an un-batched commit the
            // next initial commit.
            lastAttempedCommit = batchCommitsToStream();
            super.flush();
          }
        });
  }

  private @Nullable Commit waitForInitialCommit() {
    while (!super.clientClosed.get()) {
      try {
        return commitQueue.take();
      } catch (InterruptedException ignored) {
      }
    }

    return null;
  }

  private boolean addCommitToStream(@Nullable Commit commit) {
    Preconditions.checkNotNull(commit);
    // Drop commits for failed work. Such commits will be dropped by Windmill anyway.
    if (commit.work().isFailed()) {
      onFailedCommit.accept(commit);
      return true;
    }

    commit.work().setState(Work.State.COMMITTING);
    activeCommitBytes.addAndGet(commit.getSize());
    boolean isCommitSuccessful =
        super.commitWorkItem(
            commit.computationId(),
            commit.request(),
            (status) -> onCommitComplete.accept(CompleteCommit.create(commit, status)));

    // Back out the stats changes since the commit wasn't consumed.
    if (!isCommitSuccessful) {
      commit.work().setState(Work.State.COMMIT_QUEUED);
    }

    activeCommitBytes.addAndGet(-commit.getSize());
    return isCommitSuccessful;
  }

  private Commit batchCommitsToStream() {
    int commits = 1;
    while (!super.clientClosed.get()) {
      Commit commit;
      try {
        commit =
            commits < COMMIT_BATCH_SIZE
                ? commitQueue.poll(10 - 2L * commits, TimeUnit.MILLISECONDS)
                : commitQueue.poll();
      } catch (InterruptedException e) {
        // Continue processing until stream is closed.
        continue;
      }
      if (commit == null || !addCommitToStream(commit)) {
        return commit;
      }
      commits++;
    }

    return null;
  }
}
