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
package org.apache.beam.runners.direct.portable;

import java.io.Serializable;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ComparisonChain;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Ordering;
import org.joda.time.Instant;

/**
 * Executes callbacks that occur based on the progression of the watermark per-step.
 *
 * <p>Callbacks are registered by calls to {@link #callOnGuaranteedFiring(PTransformNode,
 * BoundedWindow, WindowingStrategy, Runnable)}, and are executed after a call to {@link
 * #fireForWatermark(PTransformNode, Instant)} with the same {@link PTransformNode} and a watermark
 * sufficient to ensure that the trigger for the windowing strategy would have been produced.
 *
 * <p>NOTE: {@link WatermarkCallbackExecutor} does not track the latest observed watermark for any
 * {@link PTransformNode} - any call to {@link #callOnGuaranteedFiring(PTransformNode,
 * BoundedWindow, WindowingStrategy, Runnable)} that could have potentially already fired should be
 * followed by a call to {@link #fireForWatermark(PTransformNode, Instant)} for the same transform
 * with the current value of the watermark.
 */
class WatermarkCallbackExecutor {
  /** Create a new {@link WatermarkCallbackExecutor}. */
  public static WatermarkCallbackExecutor create(Executor executor) {
    return new WatermarkCallbackExecutor(executor);
  }

  private final ConcurrentMap<PTransformNode, PriorityQueue<WatermarkCallback>> callbacks;
  private final Executor executor;

  private WatermarkCallbackExecutor(Executor executor) {
    this.callbacks = new ConcurrentHashMap<>();
    this.executor = executor;
  }

  /**
   * Execute the provided {@link Runnable} after the next call to {@link
   * #fireForWatermark(PTransformNode, Instant)} where the window is guaranteed to have produced
   * output.
   */
  public void callOnGuaranteedFiring(
      PTransformNode step,
      BoundedWindow window,
      WindowingStrategy<?, ?> windowingStrategy,
      Runnable runnable) {
    WatermarkCallback callback =
        WatermarkCallback.onGuaranteedFiring(window, windowingStrategy, runnable);

    PriorityQueue<WatermarkCallback> callbackQueue = callbacks.get(step);
    if (callbackQueue == null) {
      callbackQueue = new PriorityQueue<>(11, new CallbackOrdering());
      if (callbacks.putIfAbsent(step, callbackQueue) != null) {
        callbackQueue = callbacks.get(step);
      }
    }

    synchronized (callbackQueue) {
      callbackQueue.offer(callback);
    }
  }

  /**
   * Execute the provided {@link Runnable} after the next call to {@link
   * #fireForWatermark(PTransformNode, Instant)} where the window is guaranteed to be expired.
   */
  public void callOnWindowExpiration(
      PTransformNode step,
      BoundedWindow window,
      WindowingStrategy<?, ?> windowingStrategy,
      Runnable runnable) {
    WatermarkCallback callback =
        WatermarkCallback.afterWindowExpiration(window, windowingStrategy, runnable);

    PriorityQueue<WatermarkCallback> callbackQueue = callbacks.get(step);
    if (callbackQueue == null) {
      callbackQueue = new PriorityQueue<>(11, new CallbackOrdering());
      if (callbacks.putIfAbsent(step, callbackQueue) != null) {
        callbackQueue = callbacks.get(step);
      }
    }

    synchronized (callbackQueue) {
      callbackQueue.offer(callback);
    }
  }

  /**
   * Schedule all pending callbacks that must have produced output by the time of the provided
   * watermark.
   */
  public void fireForWatermark(PTransformNode step, Instant watermark) {
    PriorityQueue<WatermarkCallback> callbackQueue = callbacks.get(step);
    if (callbackQueue == null) {
      return;
    }
    synchronized (callbackQueue) {
      while (!callbackQueue.isEmpty() && callbackQueue.peek().shouldFire(watermark)) {
        executor.execute(callbackQueue.poll().getCallback());
      }
    }
  }

  private static class WatermarkCallback {
    public static <W extends BoundedWindow> WatermarkCallback onGuaranteedFiring(
        BoundedWindow window, WindowingStrategy<?, W> strategy, Runnable callback) {
      @SuppressWarnings("unchecked")
      Instant firingAfter = strategy.getTrigger().getWatermarkThatGuaranteesFiring((W) window);
      return new WatermarkCallback(firingAfter, callback);
    }

    public static <W extends BoundedWindow> WatermarkCallback afterWindowExpiration(
        BoundedWindow window, WindowingStrategy<?, W> strategy, Runnable callback) {
      // Fire one milli past the end of the window. This ensures that all window expiration
      // timers are delivered first
      Instant firingAfter = window.maxTimestamp().plus(strategy.getAllowedLateness()).plus(1L);
      return new WatermarkCallback(firingAfter, callback);
    }

    private final Instant fireAfter;
    private final Runnable callback;

    private WatermarkCallback(Instant fireAfter, Runnable callback) {
      this.fireAfter = fireAfter;
      this.callback = callback;
    }

    public boolean shouldFire(Instant currentWatermark) {
      return currentWatermark.isAfter(fireAfter)
          || currentWatermark.equals(BoundedWindow.TIMESTAMP_MAX_VALUE);
    }

    public Runnable getCallback() {
      return callback;
    }
  }

  private static class CallbackOrdering extends Ordering<WatermarkCallback>
      implements Serializable {
    @Override
    public int compare(WatermarkCallback left, WatermarkCallback right) {
      return ComparisonChain.start()
          .compare(left.fireAfter, right.fireAfter)
          .compare(left.callback, right.callback, Ordering.arbitrary())
          .result();
    }
  }
}
