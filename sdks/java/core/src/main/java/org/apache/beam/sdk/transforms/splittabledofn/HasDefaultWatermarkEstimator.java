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

import org.apache.beam.sdk.transforms.DoFn;

/**
 * Interface for watermark estimator state for which a default implementation of {@link
 * DoFn.NewWatermarkEstimator} is available, depending only on the watermark estimator state itself.
 */
public interface HasDefaultWatermarkEstimator<
    WatermarkEstimatorStateT,
    WatermarkEstimatorT extends WatermarkEstimator<WatermarkEstimatorStateT>> {
  /** Creates a new watermark estimator for {@code this}. */
  WatermarkEstimatorT newWatermarkEstimator();
}
