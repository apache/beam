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

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A window observing variant of {@link NonWindowObservingTestSplittableDoFn} which uses the side
 * inputs to choose the checkpoint upper bound.
 */
class WindowObservingTestSplittableDoFn extends NonWindowObservingTestSplittableDoFn {

  private static final long SPLIT_ELEMENT = 3;

  private final PCollectionView<String> singletonSideInput;
  private static final long PROCESSED_WINDOW = 1;
  private boolean splitAtTruncate = false;
  private long processedWindowCount = 0;

  WindowObservingTestSplittableDoFn(PCollectionView<String> singletonSideInput) {
    this.singletonSideInput = singletonSideInput;
  }

  static WindowObservingTestSplittableDoFn forSplitAtTruncate(
      PCollectionView<String> singletonSideInput) {
    WindowObservingTestSplittableDoFn doFn =
        new WindowObservingTestSplittableDoFn(singletonSideInput);
    doFn.splitAtTruncate = true;
    return doFn;
  }

  @Override
  @ProcessElement
  public ProcessContinuation processElement(
      ProcessContext context,
      RestrictionTracker<OffsetRange, Long> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator)
      throws Exception {
    enterProcessAndBlockIfEnabled();
    long checkpointUpperBound = Long.parseLong(context.sideInput(singletonSideInput));
    long position = tracker.currentRestriction().getFrom();
    boolean claimStatus = true;
    while (!shouldAbortProcessing()) {
      claimStatus = tracker.tryClaim(position);
      if (!claimStatus) {
        break;
      } else if (position == SPLIT_ELEMENT) {
        splitElementProcessed();
        waitForTrySplitPerformed();
      }
      context.outputWithTimestamp(
          context.element() + ":" + position,
          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(position)));
      watermarkEstimator.setWatermark(
          GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(position)));
      position += 1L;
      if (position == checkpointUpperBound) {
        break;
      }
    }
    if (!claimStatus) {
      return ProcessContinuation.stop();
    } else {
      return ProcessContinuation.resume().withResumeDelay(Duration.millis(54321L));
    }
  }

  @Override
  public Duration getAllowedTimestampSkew() {
    return Duration.millis(Long.MAX_VALUE);
  }

  @Override
  @TruncateRestriction
  public RestrictionTracker.TruncateResult<OffsetRange> truncateRestriction(
      @Restriction OffsetRange range) throws Exception {
    // Waiting for split when we are on the second window.
    if (splitAtTruncate && processedWindowCount == PROCESSED_WINDOW) {
      splitElementProcessed();
      waitForTrySplitPerformed();
    }
    processedWindowCount += 1;
    return RestrictionTracker.TruncateResult.of(
        new OffsetRange(range.getFrom(), range.getTo() / 2));
  }
}
