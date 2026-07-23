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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.streaming.WeightedBoundedQueue;
import org.apache.beam.runners.dataflow.worker.streaming.WeightedSemaphore;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus;
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
  private static final String NO_BACKEND_WORKER_TOKEN = "";

  private final Supplier<CloseableStream<CommitWorkStream>> commitWorkStreamFactory;
  private final WeightedBoundedQueue<Commit> commitQueue;
  private final ExecutorService commitSenders;
  private final AtomicLong activeCommitBytes;
  private final Consumer<CompleteCommit> onCommitComplete;
  private final int numCommitSenders;
  private final AtomicBoolean isRunning;

  StreamingEngineWorkCommitter(
      Supplier<CloseableStream<CommitWorkStream>> commitWorkStreamFactory,
      int numCommitSenders,
      Consumer<CompleteCommit> onCommitComplete,
      String backendWorkerToken,
      WeightedSemaphore<Commit> commitByteSemaphore) {
    this.commitWorkStreamFactory = commitWorkStreamFactory;
    this.commitQueue = WeightedBoundedQueue.create(commitByteSemaphore);
    this.commitSenders =
        Executors.newFixedThreadPool(
            numCommitSenders,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setPriority(Thread.MAX_PRIORITY)
                .setNameFormat(
                    backendWorkerToken.isEmpty()
                        ? "CommitThread-%d"
                        : "CommitThread-" + backendWorkerToken + "-%d")
                .build());
    this.activeCommitBytes = new AtomicLong();
    this.onCommitComplete = onCommitComplete;
    this.numCommitSenders = numCommitSenders;
    this.isRunning = new AtomicBoolean(false);
  }

  public static Builder builder() {
    return new AutoBuilder_StreamingEngineWorkCommitter_Builder()
        .setBackendWorkerToken(NO_BACKEND_WORKER_TOKEN)
        .setNumCommitSenders(1);
  }

  @Override
  public void start() {
    Preconditions.checkState(
        isRunning.compareAndSet(false, true), "Multiple calls to WorkCommitter.start().");
    for (int i = 0; i < numCommitSenders; i++) {
      commitSenders.execute(this::streamingCommitLoop);
    }
  }

  @Override
  public void commit(Commit commit) {
    if (shouldFailCommit(commit)) {
      failQueuedCommit(commit);
    } else {
      commitQueue.put(commit);
    }

    // Do this check after adding to commitQueue, else commitQueue.put() can race with
    // drainCommitQueue() in stop() and leave commits orphaned in the queue.
    if (!this.isRunning.get()) {
      LOG.debug("Trying to queue commit on shutdown, failing commit={}", commit);
      drainCommitQueue();
    }
  }

  @Override
  public long currentActiveCommitBytes() {
    return activeCommitBytes.get();
  }

  @Override
  public void stop() {
    Preconditions.checkState(isRunning.compareAndSet(true, false));
    commitSenders.shutdownNow();
    try {
      commitSenders.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn(
          "Commit senders didn't complete shutdown within 10 seconds, continuing to drain queue.",
          e);
    }
    drainCommitQueue();
  }

  private void drainCommitQueue() {
    Commit queuedCommit = commitQueue.poll();
    while (queuedCommit != null) {
      failQueuedCommit(queuedCommit);
      queuedCommit = commitQueue.poll();
    }
  }

  private void failQueuedCommit(Commit commit) {
    if (!isRunning.get()) {
      // Shutting down, fail everything unconditionally to prevent infinite loops
      for (Work w : commit.workBatch()) {
        w.setFailed();
        onCommitComplete.accept(
            CompleteCommit.create(
                commit.computationId(),
                w.getShardedKey(),
                w.id(),
                CommitStatus.ABORTED,
                /* retryableFailure= */ false));
      }
      return;
    }

    // Still running, only fail actually failed work, and request re-execution for valid ones
    for (Work w : commit.workBatch()) {
      if (w.isFailed()) {
        onCommitComplete.accept(
            CompleteCommit.create(
                commit.computationId(),
                w.getShardedKey(),
                w.id(),
                CommitStatus.ABORTED,
                /* retryableFailure= */ false));
      } else {
        LOG.debug("Requesting re-execution for valid work {} from failed commit", w.id());
        onCommitComplete.accept(
            CompleteCommit.create(
                commit.computationId(),
                w.getShardedKey(),
                w.id(),
                CommitStatus.ABORTED,
                /* retryableFailure= */ true));
      }
    }
  }

  @Override
  public int parallelism() {
    return numCommitSenders;
  }

  private void streamingCommitLoop() {
    @Nullable Commit initialCommit = null;
    try {
      while (isRunning.get()) {
        if (initialCommit == null) {
          try {
            // Block until we have a commit or are shutting down.
            initialCommit = commitQueue.take();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }

        // take() blocks until a value is available in the commitQueue.
        Preconditions.checkNotNull(initialCommit);

        if (shouldFailCommit(initialCommit)) {
          failQueuedCommit(initialCommit);
          initialCommit = null;
          continue;
        }

        try (CloseableStream<CommitWorkStream> closeableCommitStream =
                commitWorkStreamFactory.get();
            CommitWorkStream.RequestBatcher batcher = closeableCommitStream.stream().batcher()) {
          if (!tryAddToCommitBatch(initialCommit, batcher)) {
            throw new AssertionError("Initial commit on flushed stream should always be accepted.");
          }
          // Batch additional commits to the stream and possibly make an un-batched commit the
          // next initial commit.
          initialCommit = expandBatch(batcher);
        } catch (Exception e) {
          LOG.error("Error occurred sending commits.", e);
        }
      }
    } finally {
      if (initialCommit != null) {
        failQueuedCommit(initialCommit);
      }
    }
  }

  boolean shouldFailCommit(Commit commit) {
    for (Work w : commit.workBatch()) {
      if (w.isFailed()) {
        return true;
      }
    }
    return false;
  }

  /** Adds the commit to the batch if it fits, returning true if it is consumed. */
  private boolean tryAddToCommitBatch(Commit commit, CommitWorkStream.RequestBatcher batcher) {
    Preconditions.checkNotNull(commit);
    for (Work w : commit.workBatch()) {
      w.setState(Work.State.COMMITTING);
    }
    activeCommitBytes.addAndGet(commit.getSerializedByteSize());
    boolean isCommitAccepted;
    if (commit.multiKeyRequest() != null) {
      isCommitAccepted =
          batcher.commitMultiKeyWorkItem(
              commit.computationId(),
              checkStateNotNull(commit.multiKeyRequest()),
              commitStatus -> {
                for (Work w : commit.workBatch()) {
                  onCommitComplete.accept(
                      CompleteCommit.create(
                          commit.computationId(),
                          w.getShardedKey(),
                          w.id(),
                          commitStatus,
                          /* retryableFailure= */ false));
                }
                activeCommitBytes.addAndGet(-commit.getSerializedByteSize());
              });
    } else {
      isCommitAccepted =
          batcher.commitWorkItem(
              commit.computationId(),
              checkStateNotNull(commit.singleKeyRequest()),
              commitStatus -> {
                Work w = commit.workBatch().get(0);
                onCommitComplete.accept(
                    CompleteCommit.create(
                        commit.computationId(),
                        w.getShardedKey(),
                        w.id(),
                        commitStatus,
                        /* retryableFailure= */ false));
                activeCommitBytes.addAndGet(-commit.getSerializedByteSize());
              });
    }

    // Since the commit was not accepted, revert the changes made above.
    if (!isCommitAccepted) {
      for (Work w : commit.workBatch()) {
        w.setState(Work.State.COMMIT_QUEUED);
      }
      activeCommitBytes.addAndGet(-commit.getSerializedByteSize());
    }

    return isCommitAccepted;
  }

  /**
   * Helper to batch additional commits into the commit batch as long as they fit. Returns a commit
   * that was removed from the queue but not consumed or null.
   */
  private @Nullable Commit expandBatch(CommitWorkStream.RequestBatcher batcher) {
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
        Thread.currentThread().interrupt();
        return null;
      }

      if (commit == null) {
        return null;
      }

      // Drop commits for failed work. Such commits will be dropped by Windmill anyway.
      if (shouldFailCommit(commit)) {
        failQueuedCommit(commit);
        continue;
      }

      if (!tryAddToCommitBatch(commit, batcher)) {
        return commit;
      }
      commits++;
    }
  }

  @AutoBuilder
  public interface Builder {

    Builder setCommitWorkStreamFactory(
        Supplier<CloseableStream<CommitWorkStream>> commitWorkStreamFactory);

    Builder setCommitByteSemaphore(WeightedSemaphore<Commit> commitByteSemaphore);

    Builder setNumCommitSenders(int numCommitSenders);

    Builder setOnCommitComplete(Consumer<CompleteCommit> onCommitComplete);

    Builder setBackendWorkerToken(String backendWorkerToken);

    StreamingEngineWorkCommitter autoBuild();

    default WorkCommitter build() {
      return autoBuild();
    }
  }
}
