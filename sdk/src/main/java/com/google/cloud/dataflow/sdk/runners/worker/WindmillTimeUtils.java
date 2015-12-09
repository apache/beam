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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.common.base.Preconditions;

import org.joda.time.Instant;

import javax.annotation.Nullable;

/**
 * Some timestamp conversion helpers for working with Windmill.
 */
class WindmillTimeUtils {
  /**
   * Convert a Windmill output watermark to a harness watermark.
   *
   * <p>Windmill tracks time in microseconds while the harness uses milliseconds.
   * Windmill will 'speculatively' hold the output watermark for a computation to the
   * earliest input message timestamp, provided that message timestamp is at or after
   * the current output watermark. Thus for soundness we must ensure
   * 'Windmill considers message late' implies 'harness considers message late'. Thus we
   * round up when converting from microseconds to milliseconds.
   *
   * <p>In other words, harness output watermark >= windmill output watermark.
   */
  @Nullable
  static Instant windmillToHarnessOutputWatermark(long watermarkUs) {
    if (watermarkUs == Long.MIN_VALUE) {
      // Unknown.
      return null;
    } else if (watermarkUs == Long.MAX_VALUE) {
      // End of time.
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    } else {
      // Round up to nearest millisecond.
      return new Instant((watermarkUs + 999) / 1000);
    }
  }

  /**
   * Convert a Windmill input watermark to a harness input watermark.
   *
   * <p>We round down, thus harness input watermark <= windmill output watermark.
   */
  @Nullable
  static Instant windmillToHarnessInputWatermark(long watermarkUs) {
    if (watermarkUs == Long.MIN_VALUE) {
      // Unknown.
      return null;
    } else if (watermarkUs == Long.MAX_VALUE) {
      // End of time.
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    } else {
      // Round down to nearest millisecond.
      return new Instant(watermarkUs / 1000);
    }
  }

  /**
   * Convert a Windmill message timestamp to a harness timestamp.
   *
   * <p>For soundness we require the test
   * {@code harness message timestamp >= harness output watermark} to imply
   * {@code windmill message timestamp >= windmill output watermark}. Thus
   * we round timestamps down and output watermarks up.
   */
  static Instant windmillToHarnessTimestamp(long timestampUs) {
    // Windmill should never send us an unknown timestamp.
    Preconditions.checkArgument(timestampUs != Long.MIN_VALUE);
    if (timestampUs == Long.MAX_VALUE) {
      // End of time.
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    } else {
      // Round down to nearest millisecond.
      return new Instant(timestampUs / 1000);
    }
  }

  /**
   * Convert a harness timestamp to a Windmill timestamp.
   */
  static long harnessToWindmillTimestamp(Instant timestamp) {
    if (timestamp.equals(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      // End of time.
      return Long.MAX_VALUE;
    } else {
      return timestamp.getMillis() * 1000;
    }
  }
}
