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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.samza.operators.Scheduler;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@linkplain BundleManager}. */
public final class BundleManagerTest {
  private static final long MAX_BUNDLE_SIZE = 3;
  private static final long MAX_BUNDLE_TIME_MS = 2000;
  private static final String BUNDLE_CHECK_TIMER_ID = "bundle-check-test-timer";

  private FutureCollector<String> mockFutureCollector;
  private BundleManager<String> bundleManager;
  private BundleManager.BundleProgressListener<String> bundleProgressListener;
  private Scheduler<KeyedTimerData<Void>> mockScheduler;

  @Before
  public void setUp() {
    mockFutureCollector = mock(FutureCollector.class);
    bundleProgressListener = mock(BundleManager.BundleProgressListener.class);
    mockScheduler = mock(Scheduler.class);
    bundleManager =
        new BundleManager<>(
            bundleProgressListener,
            mockFutureCollector,
            MAX_BUNDLE_SIZE,
            MAX_BUNDLE_TIME_MS,
            mockScheduler,
            BUNDLE_CHECK_TIMER_ID);
  }

  @Test
  public void testTryStartBundleStartsBundle() {
    bundleManager.tryStartBundle();

    verify(bundleProgressListener, times(1)).onBundleStarted();
    assertEquals(
        "Expected the number of element in the current bundle to be 1",
        1L,
        bundleManager.getCurrentBundleElementCount());
    assertEquals(
        "Expected the pending bundle count to be 1", 1L, bundleManager.getPendingBundleCount());
    assertTrue("tryStartBundle() did not start the bundle", bundleManager.isBundleStarted());
  }

  @Test
  public void testTryStartBundleThrowsExceptionAndSignalError() {
    bundleManager.setCurrentBundleDoneFuture(CompletableFuture.completedFuture(null));
    try {
      bundleManager.tryStartBundle();
    } catch (IllegalArgumentException e) {
      bundleManager.signalFailure(e);
    }

    // verify if the signal failure only resets appropriate attributes of bundle
    verify(mockFutureCollector, times(1)).prepare();
    verify(mockFutureCollector, times(1)).discard();
    assertEquals(
        "Expected the number of element in the current bundle to 0",
        0L,
        bundleManager.getCurrentBundleElementCount());
    assertEquals(
        "Expected pending bundle count to be 0", 0L, bundleManager.getPendingBundleCount());
    assertFalse("Error didn't reset the bundle as expected.", bundleManager.isBundleStarted());
  }

  @Test
  public void testTryStartBundleThrowsExceptionFromTheListener() {
    doThrow(new RuntimeException("User start bundle threw an exception"))
        .when(bundleProgressListener)
        .onBundleStarted();

    try {
      bundleManager.tryStartBundle();
    } catch (RuntimeException e) {
      bundleManager.signalFailure(e);
    }

    // verify if the signal failure only resets appropriate attributes of bundle
    verify(mockFutureCollector, times(1)).prepare();
    verify(mockFutureCollector, times(1)).discard();
    assertEquals(
        "Expected the number of element in the current bundle to 0",
        0L,
        bundleManager.getCurrentBundleElementCount());
    assertEquals(
        "Expected pending bundle count to be 0", 0L, bundleManager.getPendingBundleCount());
    assertFalse("Error didn't reset the bundle as expected.", bundleManager.isBundleStarted());
  }

  @Test
  public void testMultipleStartBundle() {
    bundleManager.tryStartBundle();
    bundleManager.tryStartBundle();

    // second invocation should not start the bundle
    verify(bundleProgressListener, times(1)).onBundleStarted();
    assertEquals(
        "Expected the number of element in the current bundle to be 2",
        2L,
        bundleManager.getCurrentBundleElementCount());
    assertEquals(
        "Expected the pending bundle count to be 1", 1L, bundleManager.getPendingBundleCount());
    assertTrue("tryStartBundle() did not start the bundle", bundleManager.isBundleStarted());
  }

  /*
   * Setup the bundle manager with default max bundle size as 3 and max bundle close timeout to 2 seconds.
   * The test verifies the following
   *  1. Bundle gets closed on tryFinishBundle()
   *     a. pending bundle count == 0
   *     b. element in current bundle == 0
   *     c. isBundleStarted == false
   *  2. onBundleFinished callback is invoked on the progress listener
   */
  @Test
  public void testTryFinishBundleClosesBundle() {
    OpEmitter<String> mockEmitter = mock(OpEmitter.class);
    when(mockFutureCollector.finish())
        .thenReturn(
            CompletableFuture.completedFuture(Collections.singleton(mock(WindowedValue.class))));

    bundleManager.tryStartBundle();
    bundleManager.tryStartBundle();
    bundleManager.tryStartBundle();
    bundleManager.tryFinishBundle(mockEmitter);

    verify(mockEmitter, times(1)).emitFuture(anyObject());
    verify(bundleProgressListener, times(1)).onBundleFinished(mockEmitter);
    assertEquals(
        "Expected the number of element in the current bundle to be 0",
        0L,
        bundleManager.getCurrentBundleElementCount());
    assertEquals(
        "Expected the pending bundle count to be 0", 0L, bundleManager.getPendingBundleCount());
    assertFalse("tryFinishBundle() did not close the bundle", bundleManager.isBundleStarted());
  }

  @Test
  public void testTryFinishBundleClosesBundleOnMaxWatermark() {
    OpEmitter<String> mockEmitter = mock(OpEmitter.class);
    when(mockFutureCollector.finish())
        .thenReturn(
            CompletableFuture.completedFuture(Collections.singleton(mock(WindowedValue.class))));
    bundleManager.setBundleWatermarkHold(BoundedWindow.TIMESTAMP_MAX_VALUE);

    bundleManager.tryStartBundle();
    bundleManager.tryStartBundle();
    bundleManager.tryFinishBundle(mockEmitter);

    verify(mockEmitter, times(1)).emitFuture(anyObject());
    verify(bundleProgressListener, times(1)).onBundleFinished(mockEmitter);
    assertEquals(
        "Expected the number of element in the current bundle to be 0",
        0L,
        bundleManager.getCurrentBundleElementCount());
    assertEquals(
        "Expected the pending bundle count to be 0", 0L, bundleManager.getPendingBundleCount());
    assertFalse("tryFinishBundle() did not close the bundle", bundleManager.isBundleStarted());
  }

  /*
   * Set up the bundle manager with defaults and ensure the bundle manager doesn't close the current active bundle.
   */
  @Test
  public void testTryFinishBundleShouldNotCloseBundle() {
    OpEmitter<String> mockEmitter = mock(OpEmitter.class);
    when(mockFutureCollector.finish())
        .thenReturn(
            CompletableFuture.completedFuture(Collections.singleton(mock(WindowedValue.class))));

    bundleManager.tryStartBundle();
    bundleManager.tryFinishBundle(mockEmitter);

    verify(mockFutureCollector, times(1)).finish();
    verify(mockEmitter, times(1)).emitFuture(anyObject());
    verify(bundleProgressListener, times(0)).onBundleFinished(mockEmitter);
    assertEquals(
        "Expected the number of element in the current bundle to be 1",
        1L,
        bundleManager.getCurrentBundleElementCount());
    assertEquals(
        "Expected the pending bundle count to be 1", 1L, bundleManager.getPendingBundleCount());
    assertTrue("tryFinishBundle() did not close the bundle", bundleManager.isBundleStarted());
  }

  @Test
  public void testTryFinishBundleWhenNoBundleInProgress() {
    OpEmitter<String> mockEmitter = mock(OpEmitter.class);
    when(mockFutureCollector.finish())
        .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

    bundleManager.tryFinishBundle(mockEmitter);

    verify(mockEmitter, times(1)).emitFuture(anyObject());
    assertNull(
        "tryFinishBundle() should not set the future when no bundle in progress",
        bundleManager.getCurrentBundleDoneFuture());
  }

  @Test
  public void testProcessWatermarkWhenNoBundleInProgress() {
    Instant now = Instant.now();
    OpEmitter<String> mockEmitter = mock(OpEmitter.class);
    bundleManager.processWatermark(now, mockEmitter);
    verify(bundleProgressListener, times(1)).onWatermark(now, mockEmitter);
  }

  /*
   * The test validates processing watermark during an active bundle in progress and also validates
   * if the watermark hold is propagated down stream after the output futures are resolved.
   */
  @Test
  public void testProcessWatermarkWithPendingBundles() {
    CountDownLatch latch = new CountDownLatch(1);
    Instant watermark = Instant.now();
    OpEmitter<String> mockEmitter = mock(OpEmitter.class);

    // We need to capture the finish bundle future to know if we can check for output watermark
    // and verify other callbacks get invoked.
    Class<CompletionStage<Collection<WindowedValue<String>>>> outputFutureClass =
        (Class<CompletionStage<Collection<WindowedValue<String>>>>) (Class) CompletionStage.class;
    ArgumentCaptor<CompletionStage<Collection<WindowedValue<String>>>> captor =
        ArgumentCaptor.forClass(outputFutureClass);

    when(mockFutureCollector.finish())
        .thenReturn(
            CompletableFuture.supplyAsync(
                () -> {
                  try {
                    latch.await();
                  } catch (InterruptedException e) {
                    throw new AssertionError("Test interrupted when waiting for latch");
                  }

                  return Collections.singleton(mock(WindowedValue.class));
                }));

    testWatermarkHoldWhenPendingBundleInProgress(mockEmitter, captor, watermark);
    testWatermarkHoldPropagatesAfterFutureResolution(mockEmitter, captor, latch, watermark);
  }

  @Test
  public void testMaxWatermarkPropagationForPendingBundle() {
    Instant watermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
    OpEmitter<String> mockEmitter = mock(OpEmitter.class);
    bundleManager.setPendingBundleCount(1);
    bundleManager.processWatermark(watermark, mockEmitter);
    verify(bundleProgressListener, times(1)).onWatermark(watermark, mockEmitter);
  }

  @Test
  public void testMaxWatermarkWithBundleInProgress() {
    Instant watermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
    OpEmitter<String> mockEmitter = mock(OpEmitter.class);

    when(mockFutureCollector.finish())
        .thenReturn(
            CompletableFuture.completedFuture(Collections.singleton(mock(WindowedValue.class))));

    bundleManager.tryStartBundle();
    bundleManager.tryStartBundle();

    // should force close bundle
    bundleManager.processWatermark(watermark, mockEmitter);
    verify(bundleProgressListener, times(1)).onWatermark(watermark, mockEmitter);
  }

  @Test
  public void testProcessTimerWithBundleTimeElapsed() {
    BundleManager<String> bundleManager =
        new BundleManager<>(
            bundleProgressListener,
            mockFutureCollector,
            MAX_BUNDLE_SIZE,
            0,
            mockScheduler,
            BUNDLE_CHECK_TIMER_ID);
    OpEmitter<String> mockEmitter = mock(OpEmitter.class);
    KeyedTimerData<Void> mockTimer = mock(KeyedTimerData.class);
    TimerInternals.TimerData mockTimerData = mock(TimerInternals.TimerData.class);

    when(mockFutureCollector.finish())
        .thenReturn(
            CompletableFuture.completedFuture(Collections.singleton(mock(WindowedValue.class))));
    when(mockTimerData.getTimerId()).thenReturn(BUNDLE_CHECK_TIMER_ID);
    when(mockTimer.getTimerData()).thenReturn(mockTimerData);

    bundleManager.tryStartBundle();
    bundleManager.processTimer(mockTimer, mockEmitter);

    verify(mockEmitter, times(1)).emitFuture(anyObject());
    verify(bundleProgressListener, times(1)).onBundleFinished(mockEmitter);
    assertEquals(
        "Expected the number of element in the current bundle to be 0",
        0L,
        bundleManager.getCurrentBundleElementCount());
    assertEquals(
        "Expected the pending bundle count to be 0", 0L, bundleManager.getPendingBundleCount());
    assertFalse("tryFinishBundle() did not close the bundle", bundleManager.isBundleStarted());
  }

  @Test
  public void testProcessTimerWithTimeLessThanMaxBundleTime() {
    OpEmitter<String> mockEmitter = mock(OpEmitter.class);
    KeyedTimerData<Void> mockTimer = mock(KeyedTimerData.class);
    TimerInternals.TimerData mockTimerData = mock(TimerInternals.TimerData.class);

    when(mockTimerData.getTimerId()).thenReturn(BUNDLE_CHECK_TIMER_ID);
    when(mockTimer.getTimerData()).thenReturn(mockTimerData);

    when(mockFutureCollector.finish())
        .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

    bundleManager.tryStartBundle();
    bundleManager.processTimer(mockTimer, mockEmitter);

    verify(mockFutureCollector, times(1)).finish();
    verify(mockEmitter, times(1)).emitFuture(anyObject());
    verify(bundleProgressListener, times(0)).onBundleFinished(mockEmitter);
    assertEquals(
        "Expected the number of element in the current bundle to be 1",
        1L,
        bundleManager.getCurrentBundleElementCount());
    assertEquals(
        "Expected the pending bundle count to be 1", 1L, bundleManager.getPendingBundleCount());
    assertTrue("tryFinishBundle() closed the bundle", bundleManager.isBundleStarted());
  }

  @Test
  public void testProcessTimerIgnoresNonBundleTimers() {
    OpEmitter<String> mockEmitter = mock(OpEmitter.class);
    KeyedTimerData<Void> mockTimer = mock(KeyedTimerData.class);
    TimerInternals.TimerData mockTimerData = mock(TimerInternals.TimerData.class);

    when(mockTimerData.getTimerId()).thenReturn("NotBundleTimer");
    when(mockTimer.getTimerData()).thenReturn(mockTimerData);

    bundleManager.tryStartBundle();
    bundleManager.processTimer(mockTimer, mockEmitter);

    verify(mockFutureCollector, times(0)).finish();
    verify(mockEmitter, times(0)).emitFuture(anyObject());
    verify(bundleProgressListener, times(0)).onBundleFinished(mockEmitter);
    assertEquals(
        "Expected the number of element in the current bundle to be 1",
        1L,
        bundleManager.getCurrentBundleElementCount());
    assertEquals(
        "Expected the pending bundle count to be 1", 1L, bundleManager.getPendingBundleCount());
    assertTrue("tryFinishBundle() closed the bundle", bundleManager.isBundleStarted());
  }

  @Test
  public void testSignalFailureResetsTheBundleAndCollector() {
    bundleManager.tryStartBundle();

    bundleManager.signalFailure(mock(Throwable.class));
    verify(mockFutureCollector, times(1)).prepare();
    verify(mockFutureCollector, times(1)).discard();
    assertEquals(
        "Expected the number of element in the current bundle to 0",
        0L,
        bundleManager.getCurrentBundleElementCount());
    assertEquals(
        "Expected pending bundle count to be 0", 0L, bundleManager.getPendingBundleCount());
    assertFalse("Error didn't reset the bundle as expected.", bundleManager.isBundleStarted());
  }

  /*
   * We validate the following
   *  1. Process watermark is held since there is a pending bundle.
   *  2. Watermark propagates down stream once the output future is resolved.
   *  3. The watermark propagated is the one that was held before closing the bundle
   *  4. onBundleFinished and onWatermark callbacks are triggered
   *  5. Pending bundle count is decremented once the future is resolved
   */
  private void testWatermarkHoldPropagatesAfterFutureResolution(
      OpEmitter<String> mockEmitter,
      ArgumentCaptor<CompletionStage<Collection<WindowedValue<String>>>> captor,
      CountDownLatch latch,
      Instant sealedWatermark) {
    Instant higherWatermark = Instant.now();

    // Process watermark should result in watermark hold again since pending bundle count > 1
    bundleManager.processWatermark(higherWatermark, mockEmitter);
    verify(bundleProgressListener, times(0)).onWatermark(higherWatermark, mockEmitter);

    // Resolving the process output futures should result in watermark propagation
    latch.countDown();
    CompletionStage<Void> validationFuture =
        captor
            .getValue()
            .thenAccept(
                results -> {
                  verify(bundleProgressListener, times(1)).onBundleFinished(mockEmitter);
                  verify(bundleProgressListener, times(1))
                      .onWatermark(sealedWatermark, mockEmitter);
                  assertEquals(
                      "Expected the pending bundle count to be 0",
                      0L,
                      bundleManager.getPendingBundleCount());
                });

    validationFuture.toCompletableFuture().join();
  }

  /*
   * We validate the following
   *  1. Watermark is held since there is a bundle in progress
   *  2. Callbacks are not invoked when tryFinishBundle() is invoked since the future is unresolved
   *  3. Watermark hold is sealed and output future is emitted
   */
  private void testWatermarkHoldWhenPendingBundleInProgress(
      OpEmitter<String> mockEmitter,
      ArgumentCaptor<CompletionStage<Collection<WindowedValue<String>>>> captor,
      Instant watermark) {
    // Starts the bundle and reach the max bundle size so that tryFinishBundle() seals the current
    // bundle
    bundleManager.tryStartBundle();
    bundleManager.tryStartBundle();
    bundleManager.tryStartBundle();

    bundleManager.processWatermark(watermark, mockEmitter);
    verify(bundleProgressListener, times(0)).onWatermark(watermark, mockEmitter);

    // Bundle is still unresolved although sealed since count down the latch is not yet decremented.
    bundleManager.tryFinishBundle(mockEmitter);
    verify(mockFutureCollector, times(1)).finish();
    verify(mockEmitter, times(1)).emitFuture(captor.capture());
    assertFalse("tryFinishBundle() closed the bundle", bundleManager.isBundleStarted());
  }
}
