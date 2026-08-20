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
package org.apache.beam.runners.spark.structuredstreaming.translation;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.beam.runners.spark.SparkCommonPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The streaming counterpart of {@link EvaluationContext}: instead of forcing a one-shot batch
 * evaluation of every leaf dataset, it starts one Spark Structured Streaming query per leaf and
 * blocks until all of them reach a terminal state.
 *
 * <h2>Sink choice</h2>
 *
 * <p>Every query uses the {@code noop} sink, never {@code memory}. A Beam pipeline emits through
 * its own sinks inside the leaf {@code DoFn}s, so a leaf dataset's rows have already served their
 * purpose by the time the Spark sink sees them; the {@code memory} sink would accumulate every one
 * of them in driver memory for nobody to read. Note that {@code MemoryWriterCommitMessage} is
 * registered by {@code SparkSessionFactory.SparkKryoRegistrator} anyway, so switching a query to
 * the {@code memory} sink for debugging no longer trips {@code
 * spark.kryo.registrationRequired=true}.
 *
 * <h2>Termination</h2>
 *
 * <p>Queries read from sources with opaque epoch offsets that never settle (see {@code
 * UnboundedSourceDataset}), so {@code StreamingQueryManager#awaitAnyTermination} without outside
 * help would hang forever. Two independent knobs exist to terminate a query:
 *
 * <ul>
 *   <li>{@link #stop()}, invoked by {@code SparkStructuredStreamingPipelineResult#cancel()}.
 *   <li>The idle-stop listener registered in {@link #evaluate()} when {@code
 *       SparkStructuredStreamingPipelineOptions#getStreamingStopAfterIdleBatches()} is {@code >=
 *       0}: it counts consecutive micro-batches with zero input rows per query and gracefully stops
 *       that one query once the threshold is reached. This is how streaming tests in this module
 *       terminate on their own.
 * </ul>
 *
 * <h2>Draining on {@link #stop()}</h2>
 *
 * <p>{@link #stop()} does not attempt a full {@code Trigger.AvailableNow()} drain pass that
 * processes every already-buffered offset before halting: doing so would require stopping the query
 * and restarting it with a different trigger against the same checkpoint, which is more machinery
 * than this POC's lifecycle warrants and risks checkpoint-compatibility bugs of its own. Instead,
 * {@link #stop()} relies on {@link StreamingQuery#stop()}'s own graceful behaviour, which lets a
 * micro-batch that is already in flight finish normally instead of interrupting it, and only then
 * halts the query. Data that was not yet pulled into an in-flight micro-batch at the moment {@link
 * #stop()} is called is simply left unprocessed. This is a documented limitation of the POC, not an
 * oversight.
 */
@Internal
public class StreamingEvaluationContext extends EvaluationContext {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingEvaluationContext.class);

  // How long one awaitTermination poll blocks on a single query before moving on to the next one,
  // see awaitTermination(List). Short enough to surface a failure promptly, long enough to not
  // busy-spin while every query is healthy.
  private static final long AWAIT_POLL_TIMEOUT_MILLIS = 100;

  private final SparkStructuredStreamingPipelineOptions options;

  // Guards both `queries` and `stopped` so evaluate() (which appends to `queries` as it starts
  // queries) and stop() (which may run concurrently on another thread, see the class javadoc on
  // thread-safety below) never race on which queries have been started or already stopped.
  private final Object lock = new Object();
  private final List<StreamingQuery> queries = new ArrayList<>();
  private boolean stopped = false;

  // Set by checkpointBaseDir() when no checkpointDir was configured, so a temporary directory was
  // created as a fallback. Written once in evaluate() before any query starts and read back in the
  // finally block of that same method on the same thread, so unlike `queries` and `stopped` it does
  // not need `lock`. Never set when the user configured a checkpointDir, which evaluate() must
  // never delete.
  private @Nullable Path tempCheckpointDir;

  StreamingEvaluationContext(
      Collection<? extends NamedDataset<?>> leaves,
      SparkSession session,
      SparkCommonPipelineOptions options) {
    super(leaves, session);
    this.options = options.as(SparkStructuredStreamingPipelineOptions.class);
  }

  /**
   * Starts one streaming query per leaf dataset and blocks until every one of them has reached a
   * terminal state, either because {@link #stop()} was called (typically via {@code cancel()}) or
   * because the idle-stop listener stopped it after enough consecutive empty micro-batches.
   */
  @Override
  public void evaluate() {
    String checkpointBaseDir = checkpointBaseDir(options);
    int idleStopThreshold = options.getStreamingStopAfterIdleBatches();

    StreamingQueryListener idleStopListener = null;
    if (idleStopThreshold >= 0) {
      idleStopListener = new IdleStopListener(idleStopThreshold);
      getSparkSession().streams().addListener(idleStopListener);
    }

    try {
      int leafIndex = 0;
      for (NamedDataset<?> ds : leaves()) {
        Dataset<?> dataset = ds.dataset();
        if (dataset == null) {
          continue;
        }
        synchronized (lock) {
          if (stopped) {
            // stop() already ran (e.g. an immediate cancel()); do not start further queries.
            break;
          }
        }
        if (!dataset.isStreaming()) {
          // Defensive fallback: a leaf that turns out not to be streaming (e.g. a bounded side
          // collection) is simply evaluated the batch way instead of starting a query for it.
          EvaluationContext.evaluate(ds.name(), dataset);
          continue;
        }

        StreamingQuery query = startQuery(dataset, checkpointBaseDir, leafIndex++, options);
        boolean alreadyStopped;
        synchronized (lock) {
          queries.add(query);
          alreadyStopped = stopped;
        }
        if (alreadyStopped) {
          // stop() ran in the window between the check above and this query actually starting.
          stopQuery(query);
        }
      }

      List<StreamingQuery> toAwait;
      synchronized (lock) {
        toAwait = new ArrayList<>(queries);
      }
      awaitTermination(toAwait);
    } finally {
      if (idleStopListener != null) {
        getSparkSession().streams().removeListener(idleStopListener);
      }
      if (tempCheckpointDir != null) {
        deleteTempCheckpointDir(tempCheckpointDir);
      }
    }
  }

  /**
   * Stops all queries started by {@link #evaluate()}.
   *
   * <p>Idempotent and safe to call from a thread other than the one running {@link #evaluate()}:
   * {@code cancel()} calls this from the main thread while {@code evaluate()} is blocked awaiting
   * termination on the pipeline execution thread. See the class javadoc for the drain limitation.
   */
  @Override
  public void stop() {
    List<StreamingQuery> toStop;
    synchronized (lock) {
      if (stopped) {
        return;
      }
      stopped = true;
      toStop = new ArrayList<>(queries);
    }
    for (StreamingQuery query : toStop) {
      stopQuery(query);
    }
  }

  private StreamingQuery startQuery(
      Dataset<?> dataset,
      String checkpointBaseDir,
      int leafIndex,
      SparkStructuredStreamingPipelineOptions options) {
    try {
      return dataset
          .writeStream()
          .format("noop")
          .outputMode("append")
          .option("checkpointLocation", checkpointBaseDir + "/" + leafIndex)
          .trigger(Trigger.ProcessingTime(options.getMaxBatchDurationMillis()))
          .start();
    } catch (TimeoutException e) {
      throw new RuntimeException(
          "Failed to start streaming query for leaf dataset index " + leafIndex, e);
    }
  }

  /**
   * Blocks until every query in {@code toAwait} has terminated, polling them round robin with a
   * short timeout rather than awaiting them one after the other: a plain {@link
   * StreamingQuery#awaitTermination()} on the first query would block for as long as that query
   * keeps running and not surface a failure of a later query until then. Polling bounds the latency
   * of surfacing any query's failure by roughly one round of poll timeouts, no matter which query
   * fails.
   *
   * <p>On the first query that fails, {@link #stop()} makes sure sibling queries do not keep
   * running, and the failure is rethrown. Deliberately not {@code
   * StreamingQueryManager#awaitAnyTermination}: that relies on the session global {@code
   * resetTerminated} bookkeeping and would interfere with concurrent tests sharing one session.
   */
  private void awaitTermination(List<StreamingQuery> toAwait) {
    List<StreamingQuery> active = new ArrayList<>(toAwait);
    while (!active.isEmpty()) {
      Iterator<StreamingQuery> iterator = active.iterator();
      while (iterator.hasNext()) {
        StreamingQuery query = iterator.next();
        try {
          if (query.awaitTermination(AWAIT_POLL_TIMEOUT_MILLIS)) {
            // Terminated without an exception, nothing left to await for this query.
            iterator.remove();
          }
        } catch (StreamingQueryException e) {
          LOG.error("Streaming query {} terminated with an exception.", query.id(), e);
          // Make sure sibling queries do not keep running once one of them has failed.
          stop();
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Best-effort, idempotent stop of a single query, see the class javadoc for what "best-effort"
   * means here.
   */
  private void stopQuery(StreamingQuery query) {
    try {
      if (query.isActive()) {
        query.stop();
      }
    } catch (TimeoutException | RuntimeException e) {
      LOG.warn(
          "Error while stopping streaming query {}: {}",
          query.id(),
          String.valueOf(e.getMessage()));
    }
  }

  private void stopQueryById(UUID id) {
    StreamingQuery match = null;
    synchronized (lock) {
      for (StreamingQuery query : queries) {
        if (query.id().equals(id)) {
          match = query;
          break;
        }
      }
    }
    if (match != null) {
      stopQuery(match);
    }
  }

  private String checkpointBaseDir(SparkCommonPipelineOptions options) {
    String dir = options.getCheckpointDir();
    if (dir == null || dir.isEmpty()) {
      try {
        tempCheckpointDir = Files.createTempDirectory("beam-spark4-streaming-checkpoint");
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      dir = tempCheckpointDir.toString();
      LOG.warn("No checkpoint directory configured, falling back to temporary directory {}.", dir);
    }
    return dir;
  }

  /**
   * Best-effort, recursive delete of the fallback temporary checkpoint directory created by {@link
   * #checkpointBaseDir}. RocksDB and Spark write nested state and log files under it, so plain
   * {@link Files#delete} is not enough. Failures are logged rather than thrown: cleanup is a
   * courtesy, not something that should fail a pipeline that otherwise ran to completion.
   */
  private static void deleteTempCheckpointDir(Path dir) {
    try (Stream<Path> paths = Files.walk(dir)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.delete(path);
                } catch (IOException e) {
                  LOG.warn(
                      "Failed to delete temporary checkpoint path {}: {}",
                      path,
                      String.valueOf(e.getMessage()));
                }
              });
    } catch (IOException e) {
      LOG.warn(
          "Failed to clean up temporary checkpoint directory {}: {}",
          dir,
          String.valueOf(e.getMessage()));
    }
  }

  /**
   * Counts, per query, the number of consecutive micro-batches with zero input rows, and gracefully
   * stops a query once its count reaches {@code threshold}. The count for a query resets to zero as
   * soon as one of its micro-batches has rows.
   *
   * <p>Stopping happens on a dedicated thread rather than inline in {@link #onQueryProgress}:
   * {@link StreamingQuery#stop()} blocks until the query's execution thread has shut down, which
   * should not happen on the listener bus thread that dispatches these callbacks.
   */
  private final class IdleStopListener extends StreamingQueryListener {
    private final int threshold;
    private final Map<UUID, AtomicInteger> idleCounts = new ConcurrentHashMap<>();

    IdleStopListener(int threshold) {
      this.threshold = threshold;
    }

    @Override
    public void onQueryStarted(QueryStartedEvent event) {}

    @Override
    public void onQueryProgress(QueryProgressEvent event) {
      UUID id = event.progress().id();
      if (event.progress().numInputRows() == 0) {
        int count = idleCounts.computeIfAbsent(id, unused -> new AtomicInteger()).incrementAndGet();
        if (count >= threshold) {
          idleCounts.remove(id);
          Thread stopThread = new Thread(() -> stopQueryById(id), "beam-idle-stop-" + id);
          stopThread.setDaemon(true);
          stopThread.start();
        }
      } else {
        idleCounts.remove(id);
      }
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {
      idleCounts.remove(event.id());
    }
  }
}
