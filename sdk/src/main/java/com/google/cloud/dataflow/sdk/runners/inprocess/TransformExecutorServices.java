/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.common.base.MoreObjects;

import java.util.Map;
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
  public static TransformExecutorService parallel(
      ExecutorService executor, Map<TransformExecutor<?>, Boolean> scheduled) {
    return new ParallelEvaluationState(executor, scheduled);
  }

  /**
   * Returns an EvaluationState that evaluates {@link TransformExecutor TransformExecutors} in
   * serial.
   */
  public static TransformExecutorService serial(
      ExecutorService executor, Map<TransformExecutor<?>, Boolean> scheduled) {
    return new SerialEvaluationState(executor, scheduled);
  }

  /**
   * The evaluation of a step where the input is unkeyed. Unkeyed inputs can be evaluated in
   * parallel with an arbitrary amount of parallelism.
   */
  private static class ParallelEvaluationState implements TransformExecutorService {
    private final ExecutorService executor;
    private final Map<TransformExecutor<?>, Boolean> scheduled;

    private ParallelEvaluationState(
        ExecutorService executor, Map<TransformExecutor<?>, Boolean> scheduled) {
      this.executor = executor;
      this.scheduled = scheduled;
    }

    @Override
    public void schedule(TransformExecutor<?> work) {
      executor.submit(work);
      scheduled.put(work, true);
    }

    @Override
    public void complete(TransformExecutor<?> completed) {
      scheduled.remove(completed);
    }
  }

  /**
   * The evaluation of a (Step, Key) pair. (Step, Key) computations are processed serially;
   * scheduling a computation will add it to the Work Queue if a computation is in progress, and
   * completing a computation will schedule the next item in the work queue, if it exists.
   */
  private static class SerialEvaluationState implements TransformExecutorService {
    private final ExecutorService executor;
    private final Map<TransformExecutor<?>, Boolean> scheduled;

    private AtomicReference<TransformExecutor<?>> currentlyEvaluating;
    private final Queue<TransformExecutor<?>> workQueue;

    private SerialEvaluationState(
        ExecutorService executor, Map<TransformExecutor<?>, Boolean> scheduled) {
      this.scheduled = scheduled;
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
      scheduled.remove(completed);
      updateCurrentlyEvaluating();
    }

    private void updateCurrentlyEvaluating() {
      if (currentlyEvaluating.get() == null) {
        // Only synchronize if we need to update what's currently evaluating
        synchronized (this) {
          TransformExecutor<?> newWork = workQueue.poll();
          if (newWork != null) {
            if (currentlyEvaluating.compareAndSet(null, newWork)) {
              scheduled.put(newWork, true);
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

