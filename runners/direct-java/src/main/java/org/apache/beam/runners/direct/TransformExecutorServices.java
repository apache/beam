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

import com.google.common.base.MoreObjects;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Static factory methods for constructing instances of {@link TransformExecutorService}.
 */
final class TransformExecutorServices {
  private TransformExecutorServices() {
    // Do not instantiate
  }

  /**
   * Returns an EvaluationState that evaluates {@link TransformExecutor TransformExecutors} in
   * parallel.
   */
  public static TransformExecutorService parallel(ExecutorService executor) {
    return new ParallelEvaluationState(executor);
  }

  /**
   * Returns an EvaluationState that evaluates {@link TransformExecutor TransformExecutors} in
   * serial.
   */
  public static TransformExecutorService serial(ExecutorService executor) {
    return new SerialEvaluationState(executor);
  }

  /**
   * A {@link TransformExecutorService} with unlimited parallelism. Any {@link TransformExecutor}
   * scheduled will be immediately submitted to the {@link ExecutorService}.
   *
   * <p>A principal use of this is for the evaluation of an unkeyed Step. Unkeyed computations are
   * processed in parallel.
   */
  private static class ParallelEvaluationState implements TransformExecutorService {
    private final ExecutorService executor;

    private ParallelEvaluationState(ExecutorService executor) {
      this.executor = executor;
    }

    @Override
    public void schedule(TransformExecutor<?> work) {
      executor.submit(work);
    }

    @Override
    public void complete(TransformExecutor<?> completed) {
    }
  }

  /**
   * A {@link TransformExecutorService} with a single work queue. Any {@link TransformExecutor}
   * scheduled will be placed on the work queue. Only one item of work will be submitted to the
   * {@link ExecutorService} at any time.
   *
   * <p>A principal use of this is for the serial evaluation of a (Step, Key) pair.
   * Keyed computations are processed serially per step.
   */
  private static class SerialEvaluationState implements TransformExecutorService {
    private final ExecutorService executor;

    private AtomicReference<TransformExecutor<?>> currentlyEvaluating;
    private final Queue<TransformExecutor<?>> workQueue;

    private SerialEvaluationState(ExecutorService executor) {
      this.executor = executor;
      this.currentlyEvaluating = new AtomicReference<>();
      this.workQueue = new ConcurrentLinkedQueue<>();
    }

    /**
     * Schedules the work, adding it to the work queue if there is a bundle currently being
     * evaluated and scheduling it immediately otherwise.
     */
    @Override
    public void schedule(TransformExecutor<?> work) {
      workQueue.offer(work);
      updateCurrentlyEvaluating();
    }

    @Override
    public void complete(TransformExecutor<?> completed) {
      if (!currentlyEvaluating.compareAndSet(completed, null)) {
        throw new IllegalStateException(
            "Finished work "
                + completed
                + " but could not complete due to unexpected currently executing "
                + currentlyEvaluating.get());
      }
      updateCurrentlyEvaluating();
    }

    private void updateCurrentlyEvaluating() {
      if (currentlyEvaluating.get() == null) {
        // Only synchronize if we need to update what's currently evaluating
        synchronized (this) {
          TransformExecutor<?> newWork = workQueue.poll();
          if (newWork != null) {
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
      return MoreObjects.toStringHelper(SerialEvaluationState.class)
          .add("currentlyEvaluating", currentlyEvaluating)
          .add("workQueue", workQueue)
          .toString();
    }
  }
}
