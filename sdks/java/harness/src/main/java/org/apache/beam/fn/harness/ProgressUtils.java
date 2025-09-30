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
package org.apache.beam.fn.harness;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;

/** Miscellaneous methods for working with progress. */
@Internal
public abstract class ProgressUtils {
  static RestrictionTracker.Progress scaleProgress(
      RestrictionTracker.Progress progress, int currentWindowIndex, int stopWindowIndex) {
    checkArgument(
        currentWindowIndex < stopWindowIndex,
        "Current window index (%s) must be less than stop window index (%s)",
        currentWindowIndex,
        stopWindowIndex);

    double totalWorkPerWindow = progress.getWorkCompleted() + progress.getWorkRemaining();
    double completed = totalWorkPerWindow * currentWindowIndex + progress.getWorkCompleted();
    double remaining =
        totalWorkPerWindow * (stopWindowIndex - currentWindowIndex - 1)
            + progress.getWorkRemaining();
    return RestrictionTracker.Progress.from(completed, remaining);
  }
}
