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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.streaming.WeightedBoundedQueue;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming engine implementation of {@link WorkCommitter}. Commits work back to Streaming Engine
 * backend.
 */
@Internal
@ThreadSafe
public final class StreamingEngineWorkCommitter implements WorkCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingEngineWorkCommitter.class);
  private static final int TARGET_COMMIT_BATCH_KEYS = 5;
  private static final int MAX_COMMIT_QUEUE_BYTES = 500 << 20; // 500MB

  private final Supplier<CloseableStream<CommitWorkStream>> commitWorkStreamFactory;
  private final WeightedBoundedQueue<Commit> commitQueue;
  private final ExecutorService commitSenders;
  private final AtomicLong activeCommitBytes;
  private final Consumer<CompleteCommit> onCommitComplete;
  private final int numCommitSenders;

  private StreamingEngineWorkCommitter(
      Supplier<CloseableStream<CommitWorkStream>> commitWorkStreamFactory,
      int numCommitSenders,
      Consumer<CompleteCommit> onCommitComplete) {
    this.commitWorkStreamFactory = commitWorkStreamFactory;
    this.commitQueue =
        WeightedBoundedQueue.create(
            MAX_COMMIT_QUEUE_BYTES, commit -> Math.min(MAX_COMMIT_QUEUE_BYTES, commit.getSize()));
    this.commitSenders =
        Executors.newFixedThreadPool(
            numCommitSenders,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setPriority(Thread.MAX_PRIORITY)
                .setNameFormat("CommitThread-%d")
                .build());
    this.activeCommitBytes = new AtomicLong();
    this.onCommitComplete = onCommitComplete;
    this.numCommitSenders = numCommitSenders;
  }

  public static StreamingEngineWorkCommitter create(
      Supplier<CloseableStream<CommitWorkStream>> commitWorkStreamFactory,
      int numCommitSenders,
      Consumer<CompleteCommit> onCommitComplete) {
    return new StreamingEngineWorkCommitter(
        commitWorkStreamFactory, numCommitSenders, onCommitComplete);
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    if (!commitSenders.isShutdown()) {
      for (int i = 0; i < numCommitSenders; i++) {
        commitSenders.submit(this::streamingCommitLoop);
      }
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
    if (!commitSenders.isTerminated()) {
      commitSenders.shutdownNow();
      try {
        commitSenders.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn(
            "Commit senders didn't complete shutdown within 10 seconds, continuing to drain queue",
            e);
      }
    }
    drainCommitQueue();
  }

  private void drainCommitQueue() {
    Commit queuedCommit = commitQueue.poll();
    while (queuedCommit != null) {
      failCommit(queuedCommit);
      queuedCommit = commitQueue.poll();
    }
  }

  private void failCommit(Commit commit) {
    commit.work().setFailed();
    onCommitComplete.accept(CompleteCommit.forFailedWork(commit));
  }

  @Override
  public int parallelism() {
    return numCommitSenders;
  }

  private void streamingCommitLoop() {
    @Nullable Commit initialCommit = null;
    try {
      while (true) {
        if (initialCommit == null) {
          try {
            // Block until we have a commit or are shutting down.
            initialCommit = commitQueue.take();
          } catch (InterruptedException e) {
            return;
          }
        }
        Preconditions.checkNotNull(initialCommit);

        if (initialCommit.work().isFailed()) {
          onCommitComplete.accept(CompleteCommit.forFailedWork(initialCommit));
          initialCommit = null;
          continue;
        }

        try (CloseableStream<CommitWorkStream> closeableCommitStream =
            commitWorkStreamFactory.get()) {
          CommitWorkStream commitStream = closeableCommitStream.stream();
          try (CommitWorkStream.RequestBatcher batcher = commitStream.batcher()) {
            if (!tryAddToCommitBatch(initialCommit, batcher)) {
              throw new AssertionError(
                  "Initial commit on flushed stream should always be accepted.");
            }
            // Batch additional commits to the stream and possibly make an un-batched commit the
            // next initial commit.
            initialCommit = expandBatch(batcher);
          }
        } catch (Exception e) {
          LOG.error("Error occurred sending commits.", e);
        }
      }
    } finally {
      if (initialCommit != null) {
        failCommit(initialCommit);
      }
    }
  }

  /** Adds the commit to the batch if it fits, returning true if it is consumed. */
  private boolean tryAddToCommitBatch(Commit commit, CommitWorkStream.RequestBatcher batcher) {
    Preconditions.checkNotNull(commit);
    commit.work().setState(Work.State.COMMITTING);
    activeCommitBytes.addAndGet(commit.getSize());
    boolean isCommitAccepted =
        batcher.commitWorkItem(
            commit.computationId(),
            commit.request(),
            (commitStatus) -> {
              onCommitComplete.accept(CompleteCommit.create(commit, commitStatus));
              activeCommitBytes.addAndGet(-commit.getSize());
            });

    // Since the commit was not accepted, revert the changes made above.
    if (!isCommitAccepted) {
      commit.work().setState(Work.State.COMMIT_QUEUED);
      activeCommitBytes.addAndGet(-commit.getSize());
    }

    return isCommitAccepted;
  }

  // Helper to batch additional commits into the commit batch as long as they fit.
  // Returns a commit that was removed from the queue but not consumed or null.
  private Commit expandBatch(CommitWorkStream.RequestBatcher batcher) {
    int commits = 1;
    while (true) {
      Commit commit;
      try {
        if (commits < TARGET_COMMIT_BATCH_KEYS) {
          commit = commitQueue.poll(10 - 2L * commits, TimeUnit.MILLISECONDS);
        } else {
          commit = commitQueue.poll();
        }
      } catch (InterruptedException e) {
        return null;
      }

      if (commit == null) {
        return null;
      }

      // Drop commits for failed work. Such commits will be dropped by Windmill anyway.
      if (commit.work().isFailed()) {
        onCommitComplete.accept(CompleteCommit.forFailedWork(commit));
        continue;
      }

      if (!tryAddToCommitBatch(commit, batcher)) {
        return commit;
      }
      commits++;
    }
  }
}
