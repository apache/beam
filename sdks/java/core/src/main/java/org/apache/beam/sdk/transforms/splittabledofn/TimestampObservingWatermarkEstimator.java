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
 * A {@link WatermarkEstimator} that observes the timestamps of all records output from a {@link
 * DoFn}.
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
public interface TimestampObservingWatermarkEstimator<WatermarkEstimatorStateT>
    extends WatermarkEstimator<WatermarkEstimatorStateT> {

  /**
   * Update watermark estimate with latest output timestamp. This is called with the timestamp of
   * every element output from the DoFn.
   */
  void observeTimestamp(Instant timestamp);
}
