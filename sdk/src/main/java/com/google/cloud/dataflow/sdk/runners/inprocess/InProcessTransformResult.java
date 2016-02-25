/*
 * Copyright (C) 2015 Google Inc.
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

import com.google.cloud.dataflow.sdk.runners.inprocess.InMemoryWatermarkManager.TimerUpdate;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.state.CopyOnAccessInMemoryStateInternals;

import org.joda.time.Instant;

import javax.annotation.Nullable;

/**
 * The result of evaluating an {@link AppliedPTransform} with a {@link TransformEvaluator}.
 */
public interface InProcessTransformResult {
  /**
   * Returns the {@link AppliedPTransform} that produced this result.
   */
  AppliedPTransform<?, ?, ?> getTransform();

  /**
   * Returns the {@link UncommittedBundle (uncommitted) Bundles} output by this transform. These
   * will be committed by the evaluation context as part of completing this result.
   */
  Iterable<? extends UncommittedBundle<?>> getOutputBundles();

  /**
   * Returns the {@link CounterSet} used by this {@link PTransform}, or null if this transform did
   * not use a {@link CounterSet}.
   */
  @Nullable CounterSet getCounters();

  /**
   * Returns the Watermark Hold for the transform at the time this result was produced.
   *
   * If the transform does not set any watermark hold, returns
   * {@link BoundedWindow#TIMESTAMP_MAX_VALUE}.
   */
  Instant getWatermarkHold();

  /**
   * Returns the State used by the transform.
   *
   * If this evaluation did not access state, this may return null.
   */
  CopyOnAccessInMemoryStateInternals<?> getState();

  /**
   * Returns a TimerUpdateBuilder that was produced as a result of this evaluation. If the
   * evaluation was triggered due to the delivery of one or more timers, those timers must be added
   * to the builder before it is complete.
   *
   * <p>If this evaluation did not add or remove any timers, returns an empty TimerUpdate.
   */
  TimerUpdate getTimerUpdate();

}
