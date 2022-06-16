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
package org.apache.beam.runners.dataflow.worker;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Some timestamp conversion helpers for working with Windmill. */
public class WindmillTimeUtils {
  /** Convert a Windmill watermark to a harness watermark. */
  public static @Nullable Instant windmillToHarnessWatermark(long watermarkUs) {
    if (watermarkUs == Long.MIN_VALUE) {
      // Not yet known.
      return null;
    } else {
      return windmillToHarnessTimestamp(watermarkUs);
    }
  }

  /**
   * Convert a Windmill message timestamp to a harness timestamp.
   *
   * <p>For soundness we require the test {@code harness message timestamp >= harness output
   * watermark} to imply {@code windmill message timestamp >= windmill output watermark}. Thus we
   * round timestamps down and output watermarks up.
   */
  public static Instant windmillToHarnessTimestamp(long timestampUs) {
    // Windmill should never send us an unknown timestamp.
    Preconditions.checkArgument(timestampUs != Long.MIN_VALUE);
    Instant result = new Instant(divideAndRoundDown(timestampUs, 1000));
    if (result.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      // End of time.
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    }
    return result;
  }

  /** Convert a harness timestamp to a Windmill timestamp. */
  public static long harnessToWindmillTimestamp(Instant timestamp) {
    if (!timestamp.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      // End of time.
      return Long.MAX_VALUE;
    } else if (timestamp.getMillis() < Long.MIN_VALUE / 1000) {
      return Long.MIN_VALUE + 1;
    } else {
      return timestamp.getMillis() * 1000;
    }
  }

  private static long divideAndRoundDown(long number, long divisor) {
    return number / divisor - ((number < 0 && number % divisor != 0) ? 1 : 0);
  }
}
