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

import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.state.CopyOnAccessInMemoryStateInternals;
import org.apache.beam.sdk.values.PCollectionView;

import org.joda.time.Instant;
import javax.annotation.Nullable;

/**
 * The result of evaluating an {@link AppliedPTransform} with a {@link TransformEvaluator}.
 */
public interface TransformResult {
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
   * Returns elements that were provided to the {@link TransformEvaluator} as input but were not
   * processed.
   */
  Iterable<? extends WindowedValue<?>> getUnprocessedElements();

  /**
   * Returns the {@link AggregatorContainer.Mutator} used by this {@link PTransform}, or null if
   * this transform did not use an {@link AggregatorContainer.Mutator}.
   */
  @Nullable AggregatorContainer.Mutator getAggregatorChanges();

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
  @Nullable
  CopyOnAccessInMemoryStateInternals<?> getState();

  /**
   * Returns a TimerUpdateBuilder that was produced as a result of this evaluation. If the
   * evaluation was triggered due to the delivery of one or more timers, those timers must be added
   * to the builder before it is complete.
   *
   * <p>If this evaluation did not add or remove any timers, returns an empty TimerUpdate.
   */
  TimerUpdate getTimerUpdate();

  /**
   * Returns whether output was produced by the evaluation of this transform. True if
   * {@link #getOutputBundles()} is nonempty, or if pipeline-visible state has changed (for example,
   * the contents of a {@link PCollectionView} were updated).
   */
  boolean producedOutput();
}
