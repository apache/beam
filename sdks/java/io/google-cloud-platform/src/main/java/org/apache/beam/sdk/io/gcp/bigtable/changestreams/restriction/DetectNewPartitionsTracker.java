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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.nullness.qual.Nullable;

@Internal
public class DetectNewPartitionsTracker extends GrowableOffsetRangeTracker {

  public DetectNewPartitionsTracker(long start) {
    super(start, new UnboundedRangeEndEstimator());
  }

  @Override
  // Suppress build error that prevents this from being nullable. For some reason it
  // believes GrowableOffsetRangeTracker trySplit is NonNull even though it is nullable
  @SuppressWarnings("override.return")
  public @Nullable SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {
    // Refuse to split because we only ever want one active restriction for DNP
    if (fractionOfRemainder != 0) {
      return null;
    }
    return super.trySplit(fractionOfRemainder);
  }

  // DNP should always be unbounded so we hardcode Long.MAX_VALUE
  private static class UnboundedRangeEndEstimator implements RangeEndEstimator {

    @Override
    public long estimate() {
      return Long.MAX_VALUE;
    }
  }
}
