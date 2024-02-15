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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.streaming.WeightedBoundedQueue;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming engine implementation of {@link WorkCommitter}. Commits work back to Streaming Engine
 * backend.
 */
public final class StreamingEngineWorkCommitter implements WorkCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingEngineWorkCommitter.class);
  private static final int COMMIT_BATCH_SIZE = 5;

  private final Supplier<CloseableStream<CommitWorkStream>> commitWorkStreamFactory;
  private final WeightedBoundedQueue<Commit> commitQueue;
  private final ExecutorService commitSenders;
  private final AtomicLong activeCommitBytes;
  private final Supplier<Boolean> shouldCommitWork;
  private final Consumer<Commit> onFailedCommit;
  private final Consumer<CompleteCommit> onCommitComplete;
  private final int numCommitSenders;

  private StreamingEngineWorkCommitter(
      Supplier<CloseableStream<CommitWorkStream>> commitWorkStreamFactory,
      int numCommitSenders,
      Supplier<Boolean> shouldCommitWork,
      Consumer<Commit> onFailedCommit,
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
    this.shouldCommitWork = shouldCommitWork;
    this.onFailedCommit = onFailedCommit;
    this.onCommitComplete = onCommitComplete;
    this.numCommitSenders = numCommitSenders;
  }

  public static StreamingEngineWorkCommitter create(
      Supplier<CloseableStream<CommitWorkStream>> commitWorkStreamFactory,
      int numCommitSenders,
      Supplier<Boolean> shouldCommitWork,
      Consumer<Commit> onFailedCommit,
      Consumer<CompleteCommit> onCommitComplete,
      CountDownLatch ready) {
    StreamingEngineWorkCommitter workCommitter =
        new StreamingEngineWorkCommitter(
            commitWorkStreamFactory,
            numCommitSenders,
            shouldCommitWork,
            onFailedCommit,
            onCommitComplete);
    workCommitter.startCommitSenders(ready);
    return workCommitter;
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
    commitSenders.shutdownNow();
  }

  @Override
  public int parallelism() {
    return numCommitSenders;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void startCommitSenders(CountDownLatch ready) {
    for (int i = 0; i < numCommitSenders; i++) {
      commitSenders.submit(
          () -> {
            while (true) {
              try {
                ready.await();
                break;
              } catch (InterruptedException ignore) {
              }
            }
            streamingCommitLoop();
          });
    }
  }

  private void streamingCommitLoop() {
    Commit initialCommit = null;
    while (shouldCommitWork.get()) {
      if (initialCommit == null) {
        try {
          initialCommit = commitQueue.take();
        } catch (InterruptedException e) {
          continue;
        }
      }

      try (CloseableStream<CommitWorkStream> closeableCommitStream =
          commitWorkStreamFactory.get()) {
        CommitWorkStream commitStream = closeableCommitStream.stream().get();
        if (!tryAddToCommitStream(initialCommit, commitStream)) {
          throw new AssertionError("Initial commit on flushed stream should always be accepted.");
        }
        // Batch additional commits to the stream and possibly make an un-batched commit the next
        // initial commit.
        initialCommit = batchCommitsToStream(commitStream);
        commitStream.flush();
      } catch (Exception e) {
        LOG.error("Error occurred fetching a CommitWorkStream.", e);
      }
    }
  }

  /** Adds the commit to the commitStream if it fits, returning true if it is consumed. */
  private boolean tryAddToCommitStream(Commit commit, CommitWorkStream commitStream) {
    Preconditions.checkNotNull(commit);
    // Drop commits for failed work. Such commits will be dropped by Windmill anyway.
    if (commit.work().isFailed()) {
      onFailedCommit.accept(commit);
      return true;
    }

    commit.work().setState(Work.State.COMMITTING);
    activeCommitBytes.addAndGet(commit.getSize());
    boolean isCommitSuccessful =
        commitStream.commitWorkItem(
            commit.computationId(),
            commit.request(),
            (commitStatus) -> onCommitComplete.accept(CompleteCommit.create(commit, commitStatus)));

    if (!isCommitSuccessful) {
      commit.work().setState(Work.State.COMMIT_QUEUED);
    }

    activeCommitBytes.addAndGet(-commit.getSize());
    return isCommitSuccessful;
  }

  // Helper to batch additional commits into the commit stream as long as they fit.
  // Returns a commit that was removed from the queue but not consumed or null.
  private Commit batchCommitsToStream(CommitWorkStream commitStream) {
    int commits = 1;
    while (shouldCommitWork.get()) {
      Commit commit;
      try {
        if (commits < COMMIT_BATCH_SIZE) {
          commit = commitQueue.poll(10 - 2L * commits, TimeUnit.MILLISECONDS);
        } else {
          commit = commitQueue.poll();
        }
      } catch (InterruptedException e) {
        // Continue processing until !running.get()
        continue;
      }
      if (commit == null || !tryAddToCommitStream(commit, commitStream)) {
        return commit;
      }
      commits++;
    }
    return null;
  }
}
