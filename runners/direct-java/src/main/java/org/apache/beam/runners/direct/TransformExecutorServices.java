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
package org.apache.beam.runners.direct;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static factory methods for constructing instances of {@link TransformExecutorService}. */
final class TransformExecutorServices {
  private TransformExecutorServices() {
    // Do not instantiate
  }

  /**
   * Returns an EvaluationState that evaluates {@link TransformExecutor TransformExecutors} in
   * parallel.
   */
  public static TransformExecutorService parallel(ExecutorService executor) {
    return new ParallelTransformExecutor(executor);
  }

  /**
   * Returns an EvaluationState that evaluates {@link TransformExecutor TransformExecutors} in
   * serial.
   */
  public static TransformExecutorService serial(ExecutorService executor) {
    return new SerialTransformExecutor(executor);
  }

  /**
   * A {@link TransformExecutorService} with unlimited parallelism. Any {@link TransformExecutor}
   * scheduled will be immediately submitted to the {@link ExecutorService}.
   *
   * <p>A principal use of this is for the evaluation of an unkeyed Step. Unkeyed computations are
   * processed in parallel.
   */
  private static class ParallelTransformExecutor implements TransformExecutorService {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelTransformExecutor.class);

    private final ExecutorService executor;
    private final AtomicBoolean active = new AtomicBoolean(true);

    private ParallelTransformExecutor(ExecutorService executor) {
      this.executor = executor;
    }

    @Override
    // TODO: [BEAM-4563] Pass Future back to consumer to check for async errors
    @SuppressWarnings("FutureReturnValueIgnored")
    public void schedule(TransformExecutor work) {
      if (active.get()) {
        try {
          executor.submit(work);
        } catch (RejectedExecutionException rejected) {
          boolean stillActive = active.get();
          if (stillActive) {
            throw new IllegalStateException(
                String.format(
                    "Execution of Work %s was rejected, but the %s is still active",
                    work, ParallelTransformExecutor.class.getSimpleName()));
          } else {
            LOG.debug(
                "Rejected execution of Work {} on executor {}. "
                    + "Suppressed exception because evaluator is not active",
                work,
                this);
          }
        }
      }
    }

    @Override
    public void complete(TransformExecutor completed) {}

    @Override
    public void shutdown() {
      active.set(false);
    }
  }

  /**
   * A {@link TransformExecutorService} with a single work queue. Any {@link TransformExecutor}
   * scheduled will be placed on the work queue. Only one item of work will be submitted to the
   * {@link ExecutorService} at any time.
   *
   * <p>A principal use of this is for the serial evaluation of a (Step, Key) pair. Keyed
   * computations are processed serially per step.
   */
  private static class SerialTransformExecutor implements TransformExecutorService {
    private final ExecutorService executor;

    private AtomicReference<TransformExecutor> currentlyEvaluating;
    private final Queue<TransformExecutor> workQueue;
    private boolean active = true;

    private SerialTransformExecutor(ExecutorService executor) {
      this.executor = executor;
      this.currentlyEvaluating = new AtomicReference<>();
      this.workQueue = new ConcurrentLinkedQueue<>();
    }

    /**
     * Schedules the work, adding it to the work queue if there is a bundle currently being
     * evaluated and scheduling it immediately otherwise.
     */
    @Override
    public void schedule(TransformExecutor work) {
      workQueue.offer(work);
      updateCurrentlyEvaluating();
    }

    @Override
    public void complete(TransformExecutor completed) {
      if (!currentlyEvaluating.compareAndSet(completed, null)) {
        throw new IllegalStateException(
            "Finished work "
                + completed
                + " but could not complete due to unexpected currently executing "
                + currentlyEvaluating.get());
      }
      updateCurrentlyEvaluating();
    }

    @Override
    public void shutdown() {
      synchronized (this) {
        active = false;
      }
      workQueue.clear();
    }

    // TODO: [BEAM-4563] Pass Future back to consumer to check for async errors
    @SuppressWarnings("FutureReturnValueIgnored")
    private void updateCurrentlyEvaluating() {
      if (currentlyEvaluating.get() == null) {
        // Only synchronize if we need to update what's currently evaluating
        synchronized (this) {
          TransformExecutor newWork = workQueue.poll();
          if (active && newWork != null) {
            if (currentlyEvaluating.compareAndSet(null, newWork)) {
              executor.submit(newWork);
            } else {
              workQueue.offer(newWork);
            }
          }
        }
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(SerialTransformExecutor.class)
          .add("currentlyEvaluating", currentlyEvaluating)
          .add("workQueue", workQueue)
          .toString();
    }
  }
}
