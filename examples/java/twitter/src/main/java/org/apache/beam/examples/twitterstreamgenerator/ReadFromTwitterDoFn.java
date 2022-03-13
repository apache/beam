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
package org.apache.beam.examples.twitterstreamgenerator;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

/** Splittable dofn that read live data off twitter. * */
@DoFn.UnboundedPerElement
final class ReadFromTwitterDoFn extends DoFn<TwitterConfig, String> {

  private final DateTime startTime;

  ReadFromTwitterDoFn() {
    this.startTime = new DateTime();
  }
  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(ReadFromTwitterDoFn.class);

  static class OffsetHolder implements Serializable {
    public final @Nullable TwitterConfig twitterConfig;
    public final @Nullable Long fetchedRecords;

    OffsetHolder(@Nullable TwitterConfig twitterConfig, @Nullable Long fetchedRecords) {
      this.twitterConfig = twitterConfig;
      this.fetchedRecords = fetchedRecords;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OffsetHolder that = (OffsetHolder) o;
      return Objects.equals(twitterConfig, that.twitterConfig)
          && Objects.equals(fetchedRecords, that.fetchedRecords);
    }

    @Override
    public int hashCode() {
      return Objects.hash(twitterConfig, fetchedRecords);
    }
  }

  static class OffsetTracker extends RestrictionTracker<OffsetHolder, TwitterConfig>
      implements Serializable {
    private OffsetHolder restriction;
    private final DateTime startTime;

    OffsetTracker(OffsetHolder holder, DateTime startTime) {
      this.restriction = holder;
      this.startTime = startTime;
    }

    @Override
    public boolean tryClaim(TwitterConfig twitterConfig) {
      LOG.debug(
          "-------------- Claiming {} used to have: {}",
          twitterConfig.hashCode(),
          restriction.fetchedRecords);
      long fetchedRecords =
          this.restriction == null || this.restriction.fetchedRecords == null
              ? 0
              : this.restriction.fetchedRecords + 1;
      long elapsedTime = System.currentTimeMillis() - startTime.getMillis();
      final long millis = 60 * 1000;
      LOG.debug(
          "-------------- Time running: {} / {}",
          elapsedTime,
          (twitterConfig.getMinutesToRun() * millis));
      if (fetchedRecords > twitterConfig.getTweetsCount()
          || elapsedTime > twitterConfig.getMinutesToRun() * millis) {
        return false;
      }
      this.restriction = new OffsetHolder(twitterConfig, fetchedRecords);
      return true;
    }

    @Override
    public OffsetHolder currentRestriction() {
      return restriction;
    }

    @Override
    public SplitResult<OffsetHolder> trySplit(double fractionOfRemainder) {
      LOG.debug("-------------- Trying to split: fractionOfRemainder={}", fractionOfRemainder);
      return SplitResult.of(new OffsetHolder(null, 0L), restriction);
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
  public OffsetHolder getInitialRestriction(@Element TwitterConfig twitterConfig)
      throws IOException {
    return new OffsetHolder(null, 0L);
  }

  @DoFn.NewTracker
  public RestrictionTracker<OffsetHolder, TwitterConfig> newTracker(
      @Element TwitterConfig twitterConfig, @DoFn.Restriction OffsetHolder restriction) {
    return new OffsetTracker(restriction, startTime);
  }

  @GetRestrictionCoder
  public Coder<OffsetHolder> getRestrictionCoder() {
    return SerializableCoder.of(OffsetHolder.class);
  }

  @DoFn.ProcessElement
  public DoFn.ProcessContinuation processElement(
      @Element TwitterConfig twitterConfig,
      DoFn.OutputReceiver<String> out,
      RestrictionTracker<OffsetRange, TwitterConfig> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    LOG.debug("In Read From Twitter Do Fn");
    TwitterConnection twitterConnection = TwitterConnection.getInstance(twitterConfig);
    BlockingQueue<Status> queue = twitterConnection.getQueue();
    if (queue.isEmpty()) {
      if (checkIfDone(twitterConnection, twitterConfig, tracker)) {
        return DoFn.ProcessContinuation.stop();
      }
    }
    while (!queue.isEmpty()) {
      Status status = queue.poll();
      if (checkIfDone(twitterConnection, twitterConfig, tracker)) {
        return DoFn.ProcessContinuation.stop();
      }
      if (status != null) {
        Instant currentInstant = Instant.ofEpochMilli(status.getCreatedAt().getTime());
        watermarkEstimator.setWatermark(currentInstant);
        out.outputWithTimestamp(status.getText(), currentInstant);
      }
    }
    return DoFn.ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1));
  }

  boolean checkIfDone(
      TwitterConnection twitterConnection,
      TwitterConfig twitterConfig,
      RestrictionTracker<OffsetRange, TwitterConfig> tracker) {
    if (!tracker.tryClaim(twitterConfig)) {
      twitterConnection.closeStream();
      return true;
    } else {
      return false;
    }
  }
}
