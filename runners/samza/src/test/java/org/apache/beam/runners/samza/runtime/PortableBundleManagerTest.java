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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.samza.operators.Scheduler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PortableBundleManagerTest {

  @Mock BundleManager.BundleProgressListener<String> bundleProgressListener;
  @Mock Scheduler<KeyedTimerData<Void>> bundleTimerScheduler;

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
}
