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

import java.util.Set;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.direct.CommittedResult.OutputType;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * The result of evaluating an {@link AppliedPTransform} with a {@link TransformEvaluator}.
 *
 * <p>Every transform evaluator has a defined input type, but {@link ParDo} has multiple outputs so
 * there is not necesssarily a defined output type.
 */
interface TransformResult<InputT> {
  /**
   * Returns the {@link AppliedPTransform} that produced this result.
   *
   * <p>This is treated as an opaque identifier so evaluators can delegate to other evaluators that
   * may not have compatible types.
   */
  AppliedPTransform<?, ?, ?> getTransform();

  /**
   * Returns the {@link UncommittedBundle (uncommitted) Bundles} output by this transform. These
   * will be committed by the evaluation context as part of completing this result.
   *
   * <p>Note that the bundles need not have a uniform type, for example in the case of multi-output
   * {@link ParDo}.
   */
  Iterable<? extends UncommittedBundle<?>> getOutputBundles();

  /**
   * Returns elements that were provided to the {@link TransformEvaluator} as input but were not
   * processed.
   */
  Iterable<? extends WindowedValue<InputT>> getUnprocessedElements();

  /** Returns the logical metric updates. */
  MetricUpdates getLogicalMetricUpdates();

  /**
   * Returns the Watermark Hold for the transform at the time this result was produced.
   *
   * <p>If the transform does not set any watermark hold, returns {@link
   * BoundedWindow#TIMESTAMP_MAX_VALUE}.
   */
  Instant getWatermarkHold();

  /**
   * Returns the State used by the transform.
   *
   * <p>If this evaluation did not access state, this may return null.
   */
  @Nullable
  CopyOnAccessInMemoryStateInternals getState();

  /**
   * Returns a TimerUpdateBuilder that was produced as a result of this evaluation. If the
   * evaluation was triggered due to the delivery of one or more timers, those timers must be added
   * to the builder before it is complete.
   *
   * <p>If this evaluation did not add or remove any timers, returns an empty TimerUpdate.
   */
  TimerUpdate getTimerUpdate();

  /**
   * Returns the types of output produced by this {@link PTransform}. This may not include {@link
   * OutputType#BUNDLE}, as empty bundles may be dropped when the transform is committed.
   */
  Set<OutputType> getOutputTypes();

  /**
   * Returns a new TransformResult based on this one but overwriting any existing logical metric
   * updates with {@code metricUpdates}.
   */
  TransformResult<InputT> withLogicalMetricUpdates(MetricUpdates metricUpdates);
}
