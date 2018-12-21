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
package org.apache.beam.sdk.io.synthetic;

import java.io.Serializable;
import java.util.Optional;
import java.util.stream.LongStream;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * This watermark is based on synthetic data that is used by synthetic sources. It uses the
 * deterministic data distribution to "predict" what event times will data have and then uses it to
 * determine what the watermark for a particular element is.
 */
class SyntheticWatermark implements Serializable {

  private SyntheticSourceOptions options;

  private final long endOffset;

  private Instant watermark;

  SyntheticWatermark(SyntheticSourceOptions options, long endOffset) {
    this.options = options;
    this.endOffset = endOffset;
    this.watermark = new Instant(0);
  }

  /** Calculates new watermark value and returns it if it's greater than the previous one. */
  Instant calculateNew(long currentOffset, Instant processingTime) {

    // the source has seen all elements so the watermark is "+infinity"
    if (currentOffset >= endOffset) {
      watermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
      return watermark;
    }

    Instant newWatermark =
        findLowestEventTimeInAdvance(currentOffset, processingTime)
            .minus(Duration.millis(options.watermarkDriftMillis));

    if (newWatermark.getMillis() > watermark.getMillis()) {
      watermark = newWatermark;
    }

    return watermark;
  }

  /**
   * Searches forward to see what would be the event time for next some elements in advance (based
   * on the processing time delay distribution). Then it returns min event time that has been
   * spotted while searching.
   */
  private Instant findLowestEventTimeInAdvance(long offset, Instant processingTime) {
    Optional<org.joda.time.Instant> minEventTime =
        LongStream.range(offset, offset + options.watermarkSearchInAdvanceCount)
            .mapToObj(element -> eventTime(element, processingTime))
            .min(Instant::compareTo);

    return minEventTime.orElse(eventTime(offset, processingTime));
  }

  private Instant eventTime(long offset, Instant processingTime) {
    return processingTime.minus(options.nextProcessingTimeDelay(offset));
  }
}
