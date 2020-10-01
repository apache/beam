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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
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
 * <p>A bundle is considered complete only when the outputs corresponding to each element in the
 * bundle have been resolved and the watermark associated with the bundle(if any) is propagated
 * downstream. The output of an element is considered resolved based on the nature of the ParDoFn 1.
 * In case of synchronous ParDo, outputs of the element is resolved immediately after the
 * processElement returns. 2. In case of asynchronous ParDo, outputs of the element is resolved when
 * all the future emitted by the processElement is resolved.
 *
 * <p>This class is not thread safe and the current implementation relies on the assumption that
 * messages are dispatched to BundleManager in a single threaded mode.
 *
 * @param <OutT> output type of the {@link DoFnOp}
 */
public class BundleManager<OutT> {
  private static final Logger LOG = LoggerFactory.getLogger(BundleManager.class);
  private static final long MIN_BUNDLE_CHECK_TIME_MS = 10L;

  private final long maxBundleSize;
  private final long maxBundleTimeMs;
  private final BundleProgressListener<OutT> bundleProgressListener;
  private final FutureCollector<OutT> futureCollector;
  private final Scheduler<KeyedTimerData<Void>> bundleTimerScheduler;
  private final String bundleCheckTimerId;

  // Number elements belonging to the current active bundle
  private transient AtomicLong currentBundleElementCount;
  // Number of bundles that are in progress but not yet finished
  private transient AtomicLong pendingBundleCount;
  // Denotes the start time of the current active bundle
  private transient AtomicLong bundleStartTime;
  // Denotes if there is an active in progress bundle. Note at a given time, we can have multiple
  // bundle in progress.
  // This flag denotes if there is a bundle that is current and hasn't been closed.
  private transient AtomicBoolean isBundleStarted;
  // Holder for watermark which gets propagated when the bundle is finished.
  private transient Instant bundleWatermarkHold;
  // A future that is completed once all futures belonging to the current active bundle are
  // completed.  The value is null if there are no futures in the current active bundle.
  private transient AtomicReference<CompletionStage<Void>> currentActiveBundleDoneFutureReference;
  private transient CompletionStage<Void> watermarkFuture;

  public BundleManager(
      BundleProgressListener<OutT> bundleProgressListener,
      FutureCollector<OutT> futureCollector,
      long maxBundleSize,
      long maxBundleTimeMs,
      Scheduler<KeyedTimerData<Void>> bundleTimerScheduler,
      String bundleCheckTimerId) {
    this.maxBundleSize = maxBundleSize;
    this.maxBundleTimeMs = maxBundleTimeMs;
    this.bundleProgressListener = bundleProgressListener;
    this.bundleTimerScheduler = bundleTimerScheduler;
    this.bundleCheckTimerId = bundleCheckTimerId;
    this.futureCollector = futureCollector;

    if (maxBundleSize > 1) {
      scheduleNextBundleCheck();
    }

    // instance variable initialization for bundle tracking
    this.bundleStartTime = new AtomicLong(Long.MAX_VALUE);
    this.currentActiveBundleDoneFutureReference = new AtomicReference<>();
    this.currentBundleElementCount = new AtomicLong(0L);
    this.isBundleStarted = new AtomicBoolean(false);
    this.pendingBundleCount = new AtomicLong(0L);
    this.watermarkFuture = CompletableFuture.completedFuture(null);
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

  void tryStartBundle() {
    futureCollector.prepare();

    if (isBundleStarted.compareAndSet(false, true)) {
      LOG.debug("Starting a new bundle.");
      // make sure the previous bundle is sealed and futures are cleared
      Preconditions.checkArgument(
          currentActiveBundleDoneFutureReference.get() == null,
          "Current active bundle done future should be null before starting a new bundle.");
      bundleStartTime.set(System.currentTimeMillis());
      pendingBundleCount.incrementAndGet();
      bundleProgressListener.onBundleStarted();
    }

    currentBundleElementCount.incrementAndGet();
  }

  void processWatermark(Instant watermark, OpEmitter<OutT> emitter) {
    // propagate watermark immediately if no bundle is in progress and all the previous bundles have
    // completed.
    if (!isBundleStarted() && pendingBundleCount.get() == 0) {
      LOG.debug("Propagating watermark: {} directly since no bundle in progress.", watermark);
      bundleProgressListener.onWatermark(watermark, emitter);
      return;
    }

    // hold back the watermark since there is either a bundle in progress or previously closed
    // bundles are unfinished.
    this.bundleWatermarkHold = watermark;

    // for batch mode, the max watermark should force the bundle to close
    if (BoundedWindow.TIMESTAMP_MAX_VALUE.equals(watermark)) {
      /*
       * Due to lack of async watermark function, we block on the previous watermark futures before propagating the watermark
       * downstream. If a bundle is in progress tryFinishBundle() fill force the bundle to close and emit watermark.
       * If no bundle in progress, we progress watermark explicitly after the completion of previous watermark futures.
       */
      if (isBundleStarted()) {
        LOG.info(
            "Received max watermark. Triggering finish bundle before flushing the watermark downstream.");
        tryFinishBundle(emitter);
        watermarkFuture.toCompletableFuture().join();
      } else {
        LOG.info(
            "Received max watermark. Waiting for previous bundles to complete before flushing the watermark downstream.");
        watermarkFuture.toCompletableFuture().join();
        bundleProgressListener.onWatermark(watermark, emitter);
      }
    }
  }

  void processTimer(KeyedTimerData<Void> keyedTimerData, OpEmitter<OutT> emitter) {
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
  void signalFailure(Throwable t) {
    LOG.error("Encountered error during processing the message. Discarding the output due to: ", t);
    futureCollector.discard();
    // reset the bundle start flag only if the bundle has started
    isBundleStarted.compareAndSet(true, false);

    // bundle start may not necessarily mean we have actually started the bundle since some of the
    // invariant check conditions within bundle start could throw exceptions. so rely on bundle
    // start time
    if (bundleStartTime.get() != Long.MAX_VALUE) {
      currentBundleElementCount.set(0L);
      bundleStartTime.set(Long.MAX_VALUE);
      pendingBundleCount.decrementAndGet();
      currentActiveBundleDoneFutureReference.set(null);
    }
  }

  void tryFinishBundle(OpEmitter<OutT> emitter) {

    // we need to seal the output for each element within a bundle irrespective of the whether we
    // decide to finish the
    // bundle or not
    CompletionStage<Collection<WindowedValue<OutT>>> outputFuture = futureCollector.finish();

    if (shouldFinishBundle() && isBundleStarted.compareAndSet(true, false)) {
      LOG.debug("Finishing the current bundle.");

      // reset the bundle count
      // seal the bundle and emit the result future (collection of results)
      // chain the finish bundle invocation on the finish bundle
      currentBundleElementCount.set(0L);
      bundleStartTime.set(Long.MAX_VALUE);
      Instant watermarkHold = bundleWatermarkHold;
      bundleWatermarkHold = null;

      CompletionStage<Void> currentActiveBundleDoneFuture =
          currentActiveBundleDoneFutureReference.get();
      outputFuture =
          outputFuture.thenCombine(
              currentActiveBundleDoneFuture != null
                  ? currentActiveBundleDoneFuture
                  : CompletableFuture.completedFuture(null),
              (res, ignored) -> {
                bundleProgressListener.onBundleFinished(emitter);
                return res;
              });

      BiConsumer<Collection<WindowedValue<OutT>>, Void> watermarkPropagationFn;
      if (watermarkHold == null) {
        watermarkPropagationFn = (ignored, res) -> pendingBundleCount.decrementAndGet();
      } else {
        watermarkPropagationFn =
            (ignored, res) -> {
              LOG.debug("Propagating watermark: {} to downstream.", watermarkHold);
              bundleProgressListener.onWatermark(watermarkHold, emitter);
              pendingBundleCount.decrementAndGet();
            };
      }

      // We chain the current watermark emission with previous watermark and the output futures
      // since bundles can finish out of order but we still want the watermark to be emitted in
      // order.
      watermarkFuture = outputFuture.thenAcceptBoth(watermarkFuture, watermarkPropagationFn);
      currentActiveBundleDoneFutureReference.set(null);
    } else if (isBundleStarted.get()) {
      CompletableFuture<Void> newFuture = new CompletableFuture<>();
      CompletionStage<Void> oldFuture = currentActiveBundleDoneFutureReference.getAndSet(newFuture);
      outputFuture
          .thenCombine(
              oldFuture != null ? oldFuture : CompletableFuture.completedFuture(null),
              (ignored1, ignored2) -> newFuture.complete(null))
          .exceptionally(newFuture::completeExceptionally);
    }

    // emit the future to the propagate it to rest of the DAG
    emitter.emitFuture(outputFuture);
  }

  @VisibleForTesting
  long getCurrentBundleElementCount() {
    return currentBundleElementCount.longValue();
  }

  @VisibleForTesting
  @Nullable
  CompletionStage<Void> getCurrentBundleDoneFuture() {
    return currentActiveBundleDoneFutureReference.get();
  }

  @VisibleForTesting
  void setCurrentBundleDoneFuture(CompletionStage<Void> currentBundleResultFuture) {
    this.currentActiveBundleDoneFutureReference.set(currentBundleResultFuture);
  }

  @VisibleForTesting
  long getPendingBundleCount() {
    return pendingBundleCount.longValue();
  }

  @VisibleForTesting
  void setPendingBundleCount(long value) {
    pendingBundleCount.set(value);
  }

  @VisibleForTesting
  boolean isBundleStarted() {
    return isBundleStarted.get();
  }

  @VisibleForTesting
  void setBundleWatermarkHold(Instant watermark) {
    this.bundleWatermarkHold = watermark;
  }

  /**
   * We close the current bundle in progress if one of the following criteria is met 1. The bundle
   * count &ge; maxBundleSize 2. Time elapsed since the bundle started is &ge; maxBundleTimeMs 3.
   * Watermark hold equals to TIMESTAMP_MAX_VALUE which usually is the case for bounded jobs
   *
   * @return true - if one of the criteria above is satisfied; false - otherwise
   */
  private boolean shouldFinishBundle() {
    return isBundleStarted.get()
        && (currentBundleElementCount.get() >= maxBundleSize
            || System.currentTimeMillis() - bundleStartTime.get() >= maxBundleTimeMs
            || BoundedWindow.TIMESTAMP_MAX_VALUE.equals(bundleWatermarkHold));
  }

  /**
   * A listener used to track the lifecycle of a bundle. Typically, the lifecycle of a bundle
   * consists of 1. Start bundle - Invoked when the bundle is started 2. Finish bundle - Invoked
   * when the bundle is complete. Refer to the docs under {@link BundleManager} for definition on
   * when a bundle is considered complete. 3. onWatermark - Invoked when watermark is ready to be
   * propagated to downstream DAG. Refer to the docs under {@link BundleManager} on when watermark
   * is held vs propagated.
   *
   * @param <OutT>
   */
  public interface BundleProgressListener<OutT> {
    void onBundleStarted();

    void onBundleFinished(OpEmitter<OutT> emitter);

    void onWatermark(Instant watermark, OpEmitter<OutT> emitter);
  }
}
