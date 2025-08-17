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

import static org.junit.Assert.fail;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * The trySplit testing of this splittable DoFn is done when processing the {@link
 * NonWindowObservingTestSplittableDoFn#SPLIT_ELEMENT}. Always checkpoints at element {@link
 * NonWindowObservingTestSplittableDoFn#CHECKPOINT_UPPER_BOUND}.
 *
 * <p>The expected thread flow is:
 *
 * <ul>
 *   <li>splitting thread: {@link
 *       NonWindowObservingTestSplittableDoFn#waitForSplitElementToBeProcessed()}
 *   <li>process element thread: {@link
 *       NonWindowObservingTestSplittableDoFn#splitElementProcessed()}
 *   <li>splitting thread: perform try split
 *   <li>splitting thread: {@link NonWindowObservingTestSplittableDoFn#trySplitPerformed()} *
 *   <li>process element thread: {@link
 *       NonWindowObservingTestSplittableDoFn#waitForTrySplitPerformed()}
 * </ul>
 */
class NonWindowObservingTestSplittableDoFn extends DoFn<String, String> {
  private static final ConcurrentMap<String, Latches> DOFN_INSTANCE_TO_LATCHES =
      new ConcurrentHashMap<>();
  private static final long SPLIT_ELEMENT = 3;
  private static final long CHECKPOINT_UPPER_BOUND = 8;

  static class Latches {
    public Latches() {}

    CountDownLatch blockProcessLatch = new CountDownLatch(0);
    CountDownLatch processEnteredLatch = new CountDownLatch(1);
    CountDownLatch splitElementProcessedLatch = new CountDownLatch(1);
    CountDownLatch trySplitPerformedLatch = new CountDownLatch(1);
    AtomicBoolean abortProcessing = new AtomicBoolean();
  }

  private Latches getLatches() {
    return DOFN_INSTANCE_TO_LATCHES.computeIfAbsent(this.uuid, (uuid) -> new Latches());
  }

  public void splitElementProcessed() {
    getLatches().splitElementProcessedLatch.countDown();
  }

  public void waitForSplitElementToBeProcessed() throws InterruptedException {
    if (!getLatches().splitElementProcessedLatch.await(30, TimeUnit.SECONDS)) {
      fail("Failed to wait for trySplit to occur.");
    }
  }

  public void trySplitPerformed() {
    getLatches().trySplitPerformedLatch.countDown();
  }

  public void waitForTrySplitPerformed() throws InterruptedException {
    if (!getLatches().trySplitPerformedLatch.await(30, TimeUnit.SECONDS)) {
      fail("Failed to wait for trySplit to occur.");
    }
  }

  // Must be called before process is invoked. Will prevent process from doing anything until
  // unblockProcess is
  // called.
  public void setupBlockProcess() {
    getLatches().blockProcessLatch = new CountDownLatch(1);
  }

  public void enterProcessAndBlockIfEnabled() throws InterruptedException {
    getLatches().processEnteredLatch.countDown();
    if (!getLatches().blockProcessLatch.await(30, TimeUnit.SECONDS)) {
      fail("Failed to wait for unblockProcess to occur.");
    }
  }

  public void waitForProcessEntered() throws InterruptedException {
    if (!getLatches().processEnteredLatch.await(5, TimeUnit.SECONDS)) {
      fail("Failed to wait for process to begin.");
    }
  }

  public void unblockProcess() throws InterruptedException {
    getLatches().blockProcessLatch.countDown();
  }

  public void setAbortProcessing() {
    getLatches().abortProcessing.set(true);
  }

  public boolean shouldAbortProcessing() {
    return getLatches().abortProcessing.get();
  }

  private final String uuid;

  NonWindowObservingTestSplittableDoFn() {
    this.uuid = UUID.randomUUID().toString();
  }

  @ProcessElement
  public ProcessContinuation processElement(
      ProcessContext context,
      RestrictionTracker<OffsetRange, Long> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator)
      throws Exception {
    long checkpointUpperBound = CHECKPOINT_UPPER_BOUND;
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

  @GetInitialRestriction
  public OffsetRange restriction(@Element String element) {
    return new OffsetRange(0, Integer.parseInt(element));
  }

  @NewTracker
  public RestrictionTracker<OffsetRange, Long> newTracker(@Restriction OffsetRange restriction) {
    return new OffsetRangeTracker(restriction);
  }

  @SplitRestriction
  public void splitRange(@Restriction OffsetRange range, OutputReceiver<OffsetRange> receiver) {
    receiver.output(new OffsetRange(range.getFrom(), (range.getFrom() + range.getTo()) / 2));
    receiver.output(new OffsetRange((range.getFrom() + range.getTo()) / 2, range.getTo()));
  }

  @TruncateRestriction
  public RestrictionTracker.TruncateResult<OffsetRange> truncateRestriction(
      @Restriction OffsetRange range) throws Exception {
    return RestrictionTracker.TruncateResult.of(
        new OffsetRange(range.getFrom(), range.getTo() / 2));
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState() {
    return GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1));
  }

  @NewWatermarkEstimator
  public WatermarkEstimators.Manual newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermark) {
    return new WatermarkEstimators.Manual(watermark);
  }
}
