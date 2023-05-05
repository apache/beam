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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.core.TimerInternals;
import org.apache.samza.operators.Scheduler;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PortableBundleManagerTest {

  @Mock BundleManager.BundleProgressListener<String> bundleProgressListener;
  @Mock Scheduler<KeyedTimerData<Void>> bundleTimerScheduler;
  @Mock OpEmitter<String> emitter;

  PortableBundleManager<String> portableBundleManager;

  private static final String TIMER_ID = "timerId";

  private static final long MAX_BUNDLE_TIME_MS = 100000;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void test() {
    portableBundleManager =
        new PortableBundleManager<>(
            bundleProgressListener, 1, MAX_BUNDLE_TIME_MS, bundleTimerScheduler, TIMER_ID);
    portableBundleManager.tryStartBundle();

    verify(bundleProgressListener, times(1)).onBundleStarted();
  }

  @Test
  public void testWhen() {
    portableBundleManager =
        new PortableBundleManager<>(
            bundleProgressListener, 4, MAX_BUNDLE_TIME_MS, bundleTimerScheduler, TIMER_ID);
    portableBundleManager.tryStartBundle();
    portableBundleManager.tryStartBundle();

    verify(bundleProgressListener, times(1)).onBundleStarted();
  }

  @Test
  public void testWhenElementCountNotReachedTHenBundleDoesntFinish() {
    portableBundleManager =
        new PortableBundleManager<>(
            bundleProgressListener, 4, MAX_BUNDLE_TIME_MS, bundleTimerScheduler, TIMER_ID);
    portableBundleManager.tryStartBundle();
    portableBundleManager.tryStartBundle();
    portableBundleManager.tryFinishBundle(emitter);

    verify(bundleProgressListener, times(1)).onBundleStarted();
    verify(bundleProgressListener, times(0)).onBundleFinished(any());
  }

  @Test
  public void testWhenElementCountReachedThenFinishBundle() {
    portableBundleManager =
        new PortableBundleManager<>(
            bundleProgressListener, 4, MAX_BUNDLE_TIME_MS, bundleTimerScheduler, TIMER_ID);
    portableBundleManager.tryStartBundle();
    portableBundleManager.tryStartBundle();
    portableBundleManager.tryStartBundle();
    portableBundleManager.tryStartBundle();
    portableBundleManager.tryFinishBundle(emitter);

    verify(bundleProgressListener, times(1)).onBundleStarted();
    verify(bundleProgressListener, times(1)).onBundleFinished(any());
  }

  @Test
  public void testWhenBundleTimeReachedThenFinishBundle() throws Exception {
    portableBundleManager =
        new PortableBundleManager<>(bundleProgressListener, 4, 1, bundleTimerScheduler, TIMER_ID);
    portableBundleManager.tryStartBundle();
    Thread.sleep(2);
    portableBundleManager.tryFinishBundle(emitter);

    verify(bundleProgressListener, times(1)).onBundleStarted();
    verify(bundleProgressListener, times(1)).onBundleFinished(any());
  }

  @Test
  public void testWhenSignalFailureThenResetBundle() throws Exception {
    portableBundleManager =
        new PortableBundleManager<>(bundleProgressListener, 4, 1, bundleTimerScheduler, TIMER_ID);
    portableBundleManager.tryStartBundle();
    portableBundleManager.signalFailure(new Exception());
    portableBundleManager.tryStartBundle();

    verify(bundleProgressListener, times(2)).onBundleStarted();
  }

  @Test
  public void testProcessWatermarkWhenBundleNotStarted() {
    Instant watermark = new Instant();
    portableBundleManager =
        new PortableBundleManager<>(bundleProgressListener, 4, 1, bundleTimerScheduler, TIMER_ID);
    portableBundleManager.processWatermark(watermark, emitter);
    verify(bundleProgressListener, times(1)).onWatermark(eq(watermark), eq(emitter));
  }

  @Test
  public void testQueueWatermarkWhenBundleStarted() {
    Instant watermark = new Instant();
    portableBundleManager =
        new PortableBundleManager<>(bundleProgressListener, 1, 1, bundleTimerScheduler, TIMER_ID);

    portableBundleManager.tryStartBundle();
    portableBundleManager.processWatermark(watermark, emitter);
    verify(bundleProgressListener, times(0)).onWatermark(eq(watermark), eq(emitter));

    portableBundleManager.tryFinishBundle(emitter);
    verify(bundleProgressListener, times(1)).onWatermark(eq(watermark), eq(emitter));
  }

  @Test
  public void testProcessTimerTriesFinishBundle() {
    portableBundleManager =
        new PortableBundleManager<>(bundleProgressListener, 1, 1, bundleTimerScheduler, TIMER_ID);

    portableBundleManager.tryStartBundle();
    KeyedTimerData<Void> keyedTimerData = mock(KeyedTimerData.class);
    TimerInternals.TimerData timerData = mock(TimerInternals.TimerData.class);
    when(keyedTimerData.getTimerData()).thenReturn(timerData);
    when(timerData.getTimerId()).thenReturn(TIMER_ID);

    portableBundleManager.processTimer(keyedTimerData, emitter);
    verify(bundleProgressListener, times(1)).onBundleFinished(any());
    verify(bundleTimerScheduler).schedule(any(KeyedTimerData.class), anyLong());
  }

  @Test
  public void testDifferentTimerIdIsIgnored() {
    portableBundleManager =
        new PortableBundleManager<>(bundleProgressListener, 1, 1, bundleTimerScheduler, TIMER_ID);

    portableBundleManager.tryStartBundle();
    KeyedTimerData<Void> keyedTimerData = mock(KeyedTimerData.class);
    TimerInternals.TimerData timerData = mock(TimerInternals.TimerData.class);
    when(keyedTimerData.getTimerData()).thenReturn(timerData);
    when(timerData.getTimerId()).thenReturn("NOT_TIMER_ID");

    portableBundleManager.processTimer(keyedTimerData, emitter);
    verify(bundleProgressListener, times(0)).onBundleFinished(any());
  }
}
