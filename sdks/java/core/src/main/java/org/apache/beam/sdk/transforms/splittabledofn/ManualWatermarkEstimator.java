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
 * A {@link WatermarkEstimator} which is controlled manually from within a {@link DoFn}. The {@link
 * DoFn} must invoke {@link #setWatermark} to advance the watermark. See {@link
 * WatermarkEstimators.Manual} for a concrete implementation.
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
public interface ManualWatermarkEstimator<WatermarkEstimatorStateT>
    extends WatermarkEstimator<WatermarkEstimatorStateT> {

  /**
   * Sets a timestamp before or at the timestamps of all future elements produced by the associated
   * DoFn.
   *
   * <p>This can be approximate. If records are output that violate this guarantee, they will be
   * considered late, which will affect how they will be processed. See <a
   * href="https://beam.apache.org/documentation/programming-guide/#watermarks-and-late-data">watermarks
   * and late data</a> for more information on late data and how to handle it.
   *
   * <p>However, this value should be as late as possible. Downstream windows may not be able to
   * close until this watermark passes their end.
   */
  void setWatermark(Instant watermark);
}
