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
package org.apache.beam.runners.samza.runtime;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.samza.operators.Scheduler;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bundle management for the {@link DoFnOp} that handles lifecycle of a bundle. It also serves as a
 * proxy for the {@link DoFnOp} to process watermark and decides to 1. Hold watermark if there is at
 * least one bundle in progress. 2. Propagates the watermark to downstream DAG, if all the previous
 * bundles have completed.
 *
 * <p>This class is not thread safe and the current implementation relies on the assumption that
 * messages are dispatched to BundleManager in a single threaded mode.
 *
 * @param <OutT> output type of the {@link DoFnOp}
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PortableBundleManager<OutT> implements BundleManager<OutT> {
  private static final Logger LOG = LoggerFactory.getLogger(PortableBundleManager.class);
  private static final long MIN_BUNDLE_CHECK_TIME_MS = 10L;

  private final long maxBundleSize;
  private final long maxBundleTimeMs;
  private final BundleProgressListener<OutT> bundleProgressListener;
  private final Scheduler<KeyedTimerData<Void>> bundleTimerScheduler;
  private final String bundleCheckTimerId;

  // Number elements belonging to the current active bundle
  private AtomicLong currentBundleElementCount;
  // Number of bundles that are in progress but not yet finished
  private AtomicLong pendingBundleCount;
  // Denotes the start time of the current active bundle
  private AtomicLong bundleStartTime;
  // Denotes if there is an active in progress bundle. Note at a given time, we can have multiple
  // bundle in progress.
  // This flag denotes if there is a bundle that is current and hasn't been closed.
  private AtomicBoolean isBundleStarted;
  // Holder for watermark which gets propagated when the bundle is finished.
  private volatile Instant bundleWatermarkHold;

  public PortableBundleManager(
      BundleProgressListener<OutT> bundleProgressListener,
      long maxBundleSize,
      long maxBundleTimeMs,
      Scheduler<KeyedTimerData<Void>> bundleTimerScheduler,
      String bundleCheckTimerId) {
    this.maxBundleSize = maxBundleSize;
    this.maxBundleTimeMs = maxBundleTimeMs;
    this.bundleProgressListener = bundleProgressListener;
    this.bundleTimerScheduler = bundleTimerScheduler;
    this.bundleCheckTimerId = bundleCheckTimerId;

    if (maxBundleSize > 1) {
      scheduleNextBundleCheck();
    }

    // instance variable initialization for bundle tracking
    this.bundleStartTime = new AtomicLong(Long.MAX_VALUE);
    this.currentBundleElementCount = new AtomicLong(0);
    this.isBundleStarted = new AtomicBoolean(false);
    this.pendingBundleCount = new AtomicLong(0);
  }

  /*
   * Schedule in processing time to check whether the current bundle should be closed. Note that
   * we only approximately achieve max bundle time by checking as frequent as half of the max bundle
   * time set by users. This would violate the max bundle time by up to half of it but should
   * acceptable in most cases (and cheaper than scheduling a timer at the beginning of every bundle).
   */
  private void scheduleNextBundleCheck() {
    final Instant nextBundleCheckTime =
        Instant.now().plus(Duration.millis(maxBundleTimeMs / 2 + MIN_BUNDLE_CHECK_TIME_MS));
    final TimerInternals.TimerData timerData =
        TimerInternals.TimerData.of(
            this.bundleCheckTimerId,
            StateNamespaces.global(),
            nextBundleCheckTime,
            nextBundleCheckTime,
            TimeDomain.PROCESSING_TIME);
    bundleTimerScheduler.schedule(
        new KeyedTimerData<>(new byte[0], null, timerData), nextBundleCheckTime.getMillis());
  }

  @Override
  public void tryStartBundle() {
    inconsistentStateCheck();

    LOG.debug(
        "tryStartBundle: elementCount={}, Bundle={}", currentBundleElementCount, this.toString());

    if (isBundleStarted.compareAndSet(false, true)) {
      LOG.debug("Starting a new bundle.");
      bundleStartTime.set(System.currentTimeMillis());
      pendingBundleCount.getAndIncrement();
      bundleProgressListener.onBundleStarted();
    }

    currentBundleElementCount.incrementAndGet();
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<OutT> emitter) {
    // propagate watermark immediately if no bundle is in progress and all the previous bundles have
    // completed.
    if (shouldProcessWatermark()) {
      LOG.debug("Propagating watermark: {} directly since no bundle in progress.", watermark);
      bundleProgressListener.onWatermark(watermark, emitter);
      return;
    }

    // hold back the watermark since there is either a bundle in progress or previously closed
    // bundles are unfinished.
    this.bundleWatermarkHold = watermark;
  }

  @Override
  public void processTimer(KeyedTimerData<Void> keyedTimerData, OpEmitter<OutT> emitter) {
    inconsistentStateCheck();
    // this is internal timer in processing time to check whether a bundle should be closed
    if (bundleCheckTimerId.equals(keyedTimerData.getTimerData().getTimerId())) {
      tryFinishBundle(emitter);
      scheduleNextBundleCheck();
    }
  }

  /**
   * Signal the bundle manager to handle failure. We discard the output collected as part of
   * processing the current element and reset the bundle count.
   *
   * @param t failure cause
   */
  @Override
  public void signalFailure(Throwable t) {
    inconsistentStateCheck();
    LOG.error("Encountered error during processing the message. Discarding the output due to: ", t);

    isBundleStarted.set(false);
    currentBundleElementCount.set(0);
    bundleStartTime.set(Long.MAX_VALUE);
    pendingBundleCount.decrementAndGet();
  }

  @Override
  public void tryFinishBundle(OpEmitter<OutT> emitter) {
    LOG.debug("tryFinishBundle: elementCount={}", currentBundleElementCount);
    inconsistentStateCheck();
    if (shouldFinishBundle() && isBundleStarted.compareAndSet(true, false)) {
      LOG.debug("Finishing the current bundle. Bundle={}", this);
      currentBundleElementCount.set(0);
      bundleStartTime.set(Long.MAX_VALUE);

      Instant watermarkHold = bundleWatermarkHold;
      bundleWatermarkHold = null;

      pendingBundleCount.decrementAndGet();

      bundleProgressListener.onBundleFinished(emitter);
      if (watermarkHold != null) {
        bundleProgressListener.onWatermark(watermarkHold, emitter);
      }
    }
  }

  public void inconsistentStateCheck() {
    if (!isBundleStarted.get() && currentBundleElementCount.get() != 0) {
      LOG.warn(
          "Bundle is in a inconsistent state. isBundleStarted = false, but currentBundleElementCount = {}",
          currentBundleElementCount);
    }
  }

  private boolean shouldProcessWatermark() {
    return !isBundleStarted.get() && pendingBundleCount.get() == 0;
  }

  /**
   * We close the current bundle in progress if one of the following criteria is met 1. The bundle
   * count &ge; maxBundleSize 2. Time elapsed since the bundle started is &ge; maxBundleTimeMs 3.
   * Watermark hold equals to TIMESTAMP_MAX_VALUE which usually is the case for bounded jobs
   *
   * @return true - if one of the criteria above is satisfied; false - otherwise
   */
  private boolean shouldFinishBundle() {
    return (currentBundleElementCount.get() >= maxBundleSize
        || System.currentTimeMillis() - bundleStartTime.get() >= maxBundleTimeMs
        || BoundedWindow.TIMESTAMP_MAX_VALUE.equals(bundleWatermarkHold));
  }
}
