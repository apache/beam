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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.spark.SparkCommonPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts one Spark Structured Streaming query per leaf dataset and blocks until all of them reach a
 * terminal state. Queries end through {@link #stop()} or the idle stop listener.
 *
 * <p>Leaf {@code i} checkpoints under {@code <checkpointDir>/i}, in pipeline graph order. A changed
 * pipeline needs a new checkpoint directory, as with any Spark streaming query.
 */
@Internal
public class StreamingEvaluationContext extends EvaluationContext {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingEvaluationContext.class);

  private static final long AWAIT_POLL_TIMEOUT_MILLIS = 100;

  private final SparkStructuredStreamingPipelineOptions options;

  // Guards queries and stopped.
  private final Object lock = new Object();
  private final List<StreamingQuery> queries = new ArrayList<>();
  private boolean stopped = false;

  StreamingEvaluationContext(
      Collection<? extends NamedDataset<?>> leaves,
      SparkSession session,
      SparkCommonPipelineOptions options) {
    super(leaves, session);
    this.options = options.as(SparkStructuredStreamingPipelineOptions.class);
  }

  /** Starts one streaming query per leaf dataset and blocks until all queries terminate. */
  @Override
  public void evaluate() {
    String checkpointBaseDir = options.getCheckpointDir();
    checkArgument(
        checkpointBaseDir != null && !checkpointBaseDir.isEmpty(),
        "checkpointDir must be set for a streaming pipeline");
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
            break;
          }
        }
        if (!dataset.isStreaming()) {
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
          stopQuery(query);
        }
      }

      List<StreamingQuery> toAwait;
      synchronized (lock) {
        toAwait = new ArrayList<>(queries);
      }
      awaitTermination(toAwait);
    } catch (RuntimeException e) {
      stop();
      throw e;
    } finally {
      if (idleStopListener != null) {
        getSparkSession().streams().removeListener(idleStopListener);
      }
    }
  }

  /**
   * Stops all queries started by {@link #evaluate()}. This method is idempotent and thread safe.
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
          .option(
              "checkpointLocation",
              new Path(checkpointBaseDir, Integer.toString(leafIndex)).toString())
          .trigger(Trigger.ProcessingTime(options.getMaxBatchDurationMillis()))
          .start();
    } catch (TimeoutException e) {
      throw new RuntimeException(
          "Failed to start streaming query for leaf dataset index " + leafIndex, e);
    }
  }

  /** Blocks until every query in toAwait has terminated. Sibling queries stop on failure. */
  private void awaitTermination(List<StreamingQuery> toAwait) {
    List<StreamingQuery> active = new ArrayList<>(toAwait);
    while (!active.isEmpty()) {
      Iterator<StreamingQuery> iterator = active.iterator();
      while (iterator.hasNext()) {
        StreamingQuery query = iterator.next();
        try {
          if (query.awaitTermination(AWAIT_POLL_TIMEOUT_MILLIS)) {
            iterator.remove();
          }
        } catch (StreamingQueryException e) {
          LOG.error("Streaming query {} terminated with an exception.", query.id(), e);
          stop();
          throw new RuntimeException(e);
        }
      }
    }
  }

  /** Stops a single query if active. */
  private void stopQuery(StreamingQuery query) {
    try {
      if (query.isActive()) {
        query.stop();
      }
    } catch (TimeoutException | RuntimeException e) {
      LOG.warn("Failed to stop streaming query {}.", query.id(), e);
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

  /**
   * Stops a query after {@code threshold} consecutive triggers without input rows. A trigger
   * without data is reported as a progress event with zero rows when the source offset moved and as
   * an idle event otherwise, both count.
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
        countIdle(id);
      } else {
        idleCounts.remove(id);
      }
    }

    @Override
    public void onQueryIdle(QueryIdleEvent event) {
      countIdle(event.id());
    }

    private void countIdle(UUID id) {
      int count = idleCounts.computeIfAbsent(id, unused -> new AtomicInteger()).incrementAndGet();
      if (count >= threshold) {
        idleCounts.remove(id);
        Thread stopThread = new Thread(() -> stopQueryById(id), "beam-idle-stop-" + id);
        stopThread.setDaemon(true);
        stopThread.start();
      }
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {
      idleCounts.remove(event.id());
    }
  }
}
