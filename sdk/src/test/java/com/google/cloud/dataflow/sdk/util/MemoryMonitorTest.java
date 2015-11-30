/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import static com.google.cloud.dataflow.sdk.testing.SystemNanoTimeSleeper.sleepMillis;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test the memory monitor will block threads when the server is in a (faked) GC thrashing state.
 */
@RunWith(JUnit4.class)
public class MemoryMonitorTest {
  static class FakeGCStatsProvider implements MemoryMonitor.GCStatsProvider {
    AtomicBoolean inGCThrashingState = new AtomicBoolean(false);
    long lastCallTimestamp = System.currentTimeMillis();
    long lastGCResult = 0;

    @Override
    public long totalGCTimeMilliseconds() {
      if (inGCThrashingState.get()) {
        long now = System.currentTimeMillis();
        lastGCResult += now - lastCallTimestamp;
        lastCallTimestamp = now;
      }
      return lastGCResult;
    }
  }

  private FakeGCStatsProvider provider;
  private MemoryMonitor monitor;
  private Thread thread;

  @Before
  public void setup() {
    provider = new FakeGCStatsProvider();
    // Update every 10ms, never shutdown VM.
    monitor = new MemoryMonitor(provider, 10, 0);
    thread = new Thread(monitor);
    thread.start();
  }

  @Test(timeout = 1000)
  public void detectGCThrashing() throws InterruptedException {
    sleepMillis(100);
    monitor.waitForResources("Test1");
    provider.inGCThrashingState.set(true);
    sleepMillis(100);
    final Semaphore s = new Semaphore(0);
    new Thread(new Runnable() {
      @Override
      public void run() {
        monitor.waitForResources("Test2");
        s.release();
      }
    }).start();
    assertFalse(s.tryAcquire(100, TimeUnit.MILLISECONDS));
    provider.inGCThrashingState.set(false);
    assertTrue(s.tryAcquire(100, TimeUnit.MILLISECONDS));
    monitor.waitForResources("Test3");
  }
}
