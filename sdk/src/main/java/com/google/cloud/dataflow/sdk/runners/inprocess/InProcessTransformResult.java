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

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.Bundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;

import org.joda.time.Instant;

import javax.annotation.Nullable;

/**
 * The result of evaluating an {@link AppliedPTransform} with a {@link TransformEvaluator}.
 */
public interface InProcessTransformResult {
  /**
   * @return the {@link AppliedPTransform} that produced this result
   */
  AppliedPTransform<?, ?, ?> getTransform();

  /**
   * @return the {@link Bundle Bundles} produced by this transform
   */
  Iterable<? extends Bundle<?>> getBundles();

  /**
   * @return the {@link CounterSet} used by this {@link PTransform}, or null if this transform did
   *         not use a {@link CounterSet}
   */
  @Nullable CounterSet getCounters();

  /**
   * @return the Watermark Hold for the transform at the time this result was produced
   */
  Instant getWatermarkHold();
}
