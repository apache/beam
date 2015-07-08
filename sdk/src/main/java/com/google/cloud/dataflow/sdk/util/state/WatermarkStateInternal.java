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
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFns;

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
   * Release any holds that have become extraneous so they do not prevent progress of the
   * output watermark.
   *
   * <p>For example, when using {@link OutputTimeFns#outputAtEndOfWindow()}, there will be holds
   * in place at the end of every initial window that merges into the result window. These holds
   * need to be released. It is implementation-dependent how (or whether) this happens.
   *
   * <p>This method is permitted to be "best effort" but should always try to release holds
   * as far as possible to allow the output watermark to make progress.
   */
  void releaseExtraneousHolds();
}
