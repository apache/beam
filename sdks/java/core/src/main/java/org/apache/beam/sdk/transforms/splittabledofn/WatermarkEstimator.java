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
package org.apache.beam.sdk.transforms.splittabledofn;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

/**
 * A {@link WatermarkEstimator} which is used for estimating output watermarks of a splittable
 * {@link DoFn}. See {@link WatermarkEstimators} for commonly used watermark estimators.
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
public interface WatermarkEstimator<WatermarkEstimatorStateT> {
  /**
   * Return estimated output watermark. This method must return monotonically increasing watermarks
   * across instances that are constructed from prior state.
   */
  Instant currentWatermark();

  /**
   * Get current state of the {@link WatermarkEstimator} instance, which can be used to recreate the
   * {@link WatermarkEstimator} when processing the restriction. See {@link
   * DoFn.NewWatermarkEstimator} for additional details.
   *
   * <p>The internal state of the estimator must not be mutated by this method.
   *
   * <p>The state returned must not be mutated.
   */
  WatermarkEstimatorStateT getState();
}
