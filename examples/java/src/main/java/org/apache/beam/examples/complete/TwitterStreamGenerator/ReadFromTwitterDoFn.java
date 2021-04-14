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
package org.apache.beam.examples.complete.TwitterStreamGenerator;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

/** Splittable dofn that read live data off twitter* */
@DoFn.UnboundedPerElement
final class ReadFromTwitterDoFn extends DoFn<TwitterConfig, Status> {
  static Long maxTweetsCount = Long.MAX_VALUE;

  ReadFromTwitterDoFn(Long maxTweetsCount) {
    ReadFromTwitterDoFn.maxTweetsCount = maxTweetsCount;
  }

  ReadFromTwitterDoFn() {}

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(ReadFromTwitterDoFn.class);

  static class OffsetTracker extends RestrictionTracker<OffsetRange, Long> implements Serializable {
    private OffsetRange restriction;

    OffsetTracker(OffsetRange holder) {
      this.restriction = holder;
    }

    @Override
    public boolean tryClaim(Long position) {
      LOG.info("-------------- Claiming " + position + " used to have: " + restriction);
      long fetchedRecords = this.restriction == null ? 0 : this.restriction.getTo() + 1;
      if (fetchedRecords > maxTweetsCount) {
        return false;
      }
      this.restriction = new OffsetRange(0, fetchedRecords);
      return true;
    }

    @Override
    public OffsetRange currentRestriction() {
      return restriction;
    }

    @Override
    public SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {
      LOG.info("-------------- Trying to split: fractionOfRemainder=" + fractionOfRemainder);
      return SplitResult.of(new OffsetRange(0, 0), restriction);
    }

    @Override
    public void checkDone() throws IllegalStateException {}

    @Override
    public IsBounded isBounded() {
      return IsBounded.UNBOUNDED;
    }
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  private static Instant ensureTimestampWithinBounds(Instant timestamp) {
    if (timestamp.isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
      timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
    } else if (timestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      timestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
    }
    return timestamp;
  }

  @NewWatermarkEstimator
  public WatermarkEstimators.Manual newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new WatermarkEstimators.Manual(ensureTimestampWithinBounds(watermarkEstimatorState));
  }

  @DoFn.GetInitialRestriction
  public OffsetRange getInitialRestriction() throws IOException {
    return new OffsetRange(0, 0);
  }

  @DoFn.NewTracker
  public RestrictionTracker<OffsetRange, Long> newTracker(
      @DoFn.Restriction OffsetRange restriction) {
    return new OffsetTracker(restriction);
  }

  @DoFn.ProcessElement
  public DoFn.ProcessContinuation processElement(
      @Element TwitterConfig twitterConfig,
      DoFn.OutputReceiver<Status> out,
      RestrictionTracker<OffsetRange, Long> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    LOG.info("In Read From Twitter Do Fn");
    TwitterConnection twitterConnection = TwitterConnection.getInstance(twitterConfig);
    BlockingQueue<Status> queue = twitterConnection.getQueue();
    while (!queue.isEmpty()) {
      Status status = queue.poll();
      if (status != null) {
        if (!tracker.tryClaim(status.getId())) {
          twitterConnection.closeStream();
          return DoFn.ProcessContinuation.stop();
        }
        Instant currentInstant = Instant.ofEpochMilli(status.getCreatedAt().getTime());
        watermarkEstimator.setWatermark(currentInstant);
        out.outputWithTimestamp(status, currentInstant);
      }
    }
    return DoFn.ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1));
  }
}
