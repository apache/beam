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
package com.google.cloud.dataflow.sdk.util.state;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.annotations.Experimental.Kind;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;

import org.joda.time.Instant;

/**
 * A {@link State} accepting and aggregating output timestamps, which determines
 * the time to which the output watermark must be held.
 *
 * <p><b><i>For internal use only. This API may change at any time.</i></b>
 */
@Experimental(Kind.STATE)
public interface WatermarkStateInternal extends MergeableState<Instant, Instant> {

  /**
   * Release all holds for windows which have been merged away and incorporate their
   * combined values (according to {@link OutputTimeFn#merge}) into the result window hold.
   */
  void releaseExtraneousHolds();
}
