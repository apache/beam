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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The rows of an unbounded query, handed over while its pipeline runs.
 *
 * <p>{@link BeamEnumerableConverter} runs the pipeline on the DirectRunner. Its last step adds each
 * row and its {@link ValueKind} here, and a JDBC result set or the {@link Viewer} takes them out.
 * This only works on the DirectRunner, because it runs the pipeline in this JVM.
 */
@Internal
public final class LiveResult {
  /** Shows the rows of unbounded queries while their pipelines run, in place of JDBC results. */
  public interface Viewer {
    /**
     * Shows the rows until the user is done or {@link LiveResult#isCancelRequested()} turns true.
     * It runs on the statement's thread. Once it returns, the pipeline is cancelled and the
     * statement returns no rows, or fails if the pipeline failed.
     */
    void show(LiveResult result) throws IOException;
  }

  /** A row and the kind of change it is. */
  public static final class Change {
    private final ValueKind kind;
    private final Row row;

    Change(ValueKind kind, Row row) {
      this.kind = kind;
      this.row = row;
    }

    public ValueKind getKind() {
      return kind;
    }

    public Row getRow() {
      return row;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(LiveResult.class);

  /** How many rows wait to be taken out before the pipeline waits for room. */
  private static final int CAPACITY = 10_000;

  /** The live results of running pipelines, by pipeline options id. */
  private static final Map<Long, LiveResult> RESULTS = new ConcurrentHashMap<>();

  private static volatile @Nullable Viewer viewer;

  private final Schema schema;
  private final AtomicBoolean cancelFlag;
  private final long id;
  private final LinkedBlockingDeque<Change> changes = new LinkedBlockingDeque<>(CAPACITY);
  private final CountDownLatch finished = new CountDownLatch(1);
  private volatile @Nullable PipelineResult pipelineResult;
  private volatile State state = State.RUNNING;
  private volatile @Nullable RuntimeException failure;
  private volatile boolean closed;

  private LiveResult(Schema schema, AtomicBoolean cancelFlag, long id) {
    this.schema = schema;
    this.cancelFlag = cancelFlag;
    this.id = id;
  }

  /**
   * Sets the viewer for the rows of unbounded queries. The Beam SQL shell sets one when it runs in
   * a terminal. Without one, the rows are returned through JDBC as they arrive.
   */
  public static void setViewer(@Nullable Viewer newViewer) {
    viewer = newViewer;
  }

  static @Nullable Viewer getViewer() {
    return viewer;
  }

  static @Nullable LiveResult forOptionsId(long optionsId) {
    return RESULTS.get(optionsId);
  }

  /**
   * Starts the pipeline without waiting for it, and watches it until it ends.
   *
   * @param cancelFlag set when the statement is cancelled, for example with Ctrl+C in the shell
   */
  static LiveResult start(Pipeline pipeline, Schema schema, AtomicBoolean cancelFlag) {
    LiveResult result = new LiveResult(schema, cancelFlag, pipeline.getOptions().getOptionsId());
    RESULTS.put(result.id, result);
    PipelineResult pipelineResult;
    try {
      pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);
      pipelineResult = pipeline.run();
    } catch (RuntimeException e) {
      RESULTS.remove(result.id);
      throw e;
    }
    result.pipelineResult = pipelineResult;
    Thread watcher = new Thread(() -> result.watch(pipelineResult), "BeamSqlLiveResultWatcher");
    watcher.setDaemon(true);
    watcher.start();
    return result;
  }

  private void watch(PipelineResult pipelineResult) {
    @Nullable State end = null;
    try {
      end = pipelineResult.waitUntilFinish();
    } catch (RuntimeException e) {
      failure = e;
      end = State.FAILED;
    } finally {
      state = end == null ? State.UNKNOWN : end;
      finished.countDown();
    }
  }

  /**
   * Adds a row, waiting for room if the buffer is full. The pipeline calls this from many threads.
   */
  void add(ValueKind kind, Row row) {
    Change change = new Change(kind, row);
    try {
      while (!closed) {
        if (changes.offerLast(change, 100, TimeUnit.MILLISECONDS)) {
          return;
        }
      }
    } catch (InterruptedException e) {
      // The pipeline is being cancelled.
      Thread.currentThread().interrupt();
    }
  }

  /** The schema of the query's rows. */
  public Schema getSchema() {
    return schema;
  }

  /** Waits up to {@code timeout} for the next row, and returns null if none came. */
  public @Nullable Change poll(long timeout, TimeUnit unit) throws InterruptedException {
    return changes.pollFirst(timeout, unit);
  }

  /** Moves the rows that are waiting into {@code sink}, without waiting for more. */
  public void drainTo(Collection<? super Change> sink) {
    changes.drainTo(sink);
  }

  /** Whether the statement was cancelled, for example with Ctrl+C in the shell. */
  public boolean isCancelRequested() {
    return cancelFlag.get();
  }

  /** Whether the pipeline has ended. Rows may still be waiting to be taken out. */
  public boolean isFinished() {
    return finished.getCount() == 0;
  }

  /** {@link State#RUNNING} until the pipeline ends, and then the state it ended in. */
  public State getState() {
    return isFinished() ? state : State.RUNNING;
  }

  /** The error that the pipeline failed with, or null if it hasn't failed. */
  public @Nullable RuntimeException getFailure() {
    return isFinished() ? failure : null;
  }

  /** Stops taking rows, cancels the pipeline and waits up to 10 seconds for it to stop. */
  void close() {
    if (closed) {
      return;
    }
    closed = true;
    RESULTS.remove(id);
    PipelineResult result = pipelineResult;
    if (result != null && !isFinished()) {
      try {
        result.cancel();
        finished.await(10, TimeUnit.SECONDS);
      } catch (IOException | RuntimeException e) {
        LOG.warn("Failed to cancel the pipeline", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    changes.clear();
  }
}
