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
package org.apache.beam.runners.dataflow.worker.windmill.client.commits;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.WeightedBoundedQueue;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.WorkId;
import org.apache.beam.runners.dataflow.worker.util.TerminatingExecutors;
import org.apache.beam.runners.dataflow.worker.util.TerminatingExecutors.TerminatingExecutorService;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitWorkRequest;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Streaming appliance implementation of {@link WorkCommitter}. */
@Internal
@ThreadSafe
public final class StreamingApplianceWorkCommitter implements WorkCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingApplianceWorkCommitter.class);
  private static final long TARGET_COMMIT_BUNDLE_BYTES = 32 << 20;
  private static final int MAX_COMMIT_QUEUE_BYTES = 500 << 20; // 500MB

  private final Consumer<CommitWorkRequest> commitWorkFn;
  private final WeightedBoundedQueue<Commit> commitQueue;
  private final TerminatingExecutorService commitWorkers;
  private final AtomicLong activeCommitBytes;
  private final Consumer<CompleteCommit> onCommitComplete;

  private StreamingApplianceWorkCommitter(
      Consumer<CommitWorkRequest> commitWorkFn, Consumer<CompleteCommit> onCommitComplete) {
    this.commitWorkFn = commitWorkFn;
    this.commitQueue =
        WeightedBoundedQueue.create(
            MAX_COMMIT_QUEUE_BYTES, commit -> Math.min(MAX_COMMIT_QUEUE_BYTES, commit.getSize()));
    this.commitWorkers =
        TerminatingExecutors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setPriority(Thread.MAX_PRIORITY)
                .setNameFormat("CommitThread-%d"),
            LOG);
    this.activeCommitBytes = new AtomicLong();
    this.onCommitComplete = onCommitComplete;
  }

  public static StreamingApplianceWorkCommitter create(
      Consumer<CommitWorkRequest> commitWork, Consumer<CompleteCommit> onCommitComplete) {
    return new StreamingApplianceWorkCommitter(commitWork, onCommitComplete);
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    if (!commitWorkers.isShutdown()) {
      commitWorkers.submit(this::commitLoop);
    }
  }

  @Override
  public void commit(Commit commit) {
    commitQueue.put(commit);
  }

  @Override
  public long currentActiveCommitBytes() {
    return activeCommitBytes.get();
  }

  @Override
  public void stop() {
    commitWorkers.shutdownNow();
  }

  @Override
  public int parallelism() {
    return 1;
  }

  private void commitLoop() {
    Map<ComputationState, Windmill.ComputationCommitWorkRequest.Builder> computationRequestMap =
        new HashMap<>();
    while (true) {
      computationRequestMap.clear();
      CommitWorkRequest.Builder commitRequestBuilder = CommitWorkRequest.newBuilder();
      long commitBytes = 0;
      // Block until we have a commit, then batch with additional commits.
      Commit commit;
      try {
        commit = commitQueue.take();
      } catch (InterruptedException e) {
        return;
      }
      while (commit != null) {
        ComputationState computationState = commit.computationState();
        commit.work().setState(Work.State.COMMITTING);
        Windmill.ComputationCommitWorkRequest.Builder computationRequestBuilder =
            computationRequestMap.get(computationState);
        if (computationRequestBuilder == null) {
          computationRequestBuilder = commitRequestBuilder.addRequestsBuilder();
          computationRequestBuilder.setComputationId(computationState.getComputationId());
          computationRequestMap.put(computationState, computationRequestBuilder);
        }
        computationRequestBuilder.addRequests(commit.request());
        // Send the request if we've exceeded the bytes or there is no more
        // pending work.  commitBytes is a long, so this cannot overflow.
        commitBytes += commit.getSize();
        if (commitBytes >= TARGET_COMMIT_BUNDLE_BYTES) {
          break;
        }
        commit = commitQueue.poll();
      }
      commitWork(commitRequestBuilder.build(), commitBytes);
      completeWork(computationRequestMap);
    }
  }

  private void commitWork(CommitWorkRequest commitRequest, long commitBytes) {
    LOG.trace("Commit: {}", commitRequest);
    activeCommitBytes.addAndGet(commitBytes);
    commitWorkFn.accept(commitRequest);
    activeCommitBytes.addAndGet(-commitBytes);
  }

  private void completeWork(
      Map<ComputationState, Windmill.ComputationCommitWorkRequest.Builder> committedWork) {
    for (Map.Entry<ComputationState, Windmill.ComputationCommitWorkRequest.Builder> entry :
        committedWork.entrySet()) {
      for (Windmill.WorkItemCommitRequest workRequest : entry.getValue().getRequestsList()) {
        // Appliance errors are propagated by exception on entire batch.
        onCommitComplete.accept(
            CompleteCommit.create(
                entry.getKey().getComputationId(),
                ShardedKey.create(workRequest.getKey(), workRequest.getShardingKey()),
                WorkId.builder()
                    .setCacheToken(workRequest.getCacheToken())
                    .setWorkToken(workRequest.getWorkToken())
                    .build(),
                Windmill.CommitStatus.OK));
      }
    }
  }
}
