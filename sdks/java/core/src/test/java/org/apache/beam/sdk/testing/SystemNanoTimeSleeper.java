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
package org.apache.beam.sdk.testing;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.beam.sdk.util.Sleeper;

/**
 * This class provides an expensive sleeper to deal with issues around Java's accuracy of {@link
 * System#currentTimeMillis} and methods such as {@link Object#wait} and {@link Thread#sleep} which
 * depend on it. This <a href="https://blogs.oracle.com/dholmes/entry/inside_the_hotspot_vm_clocks">
 * article</a> goes into further detail about this issue.
 *
 * <p>This {@link Sleeper} uses {@link System#nanoTime} as the timing source and {@link
 * LockSupport#parkNanos} as the wait method. Note that usage of this sleeper may impact performance
 * because of the relatively more expensive methods being invoked when compared to {@link
 * Thread#sleep}.
 */
public class SystemNanoTimeSleeper implements Sleeper {
  public static final Sleeper INSTANCE = new SystemNanoTimeSleeper();

  /** Limit visibility to prevent instantiation. */
  private SystemNanoTimeSleeper() {}

  @Override
  public void sleep(long millis) throws InterruptedException {
    long currentTime;
    long endTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(millis, TimeUnit.MILLISECONDS);
    while ((currentTime = System.nanoTime()) < endTime) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      LockSupport.parkNanos(endTime - currentTime);
    }
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  /**
   * Causes the currently executing thread to sleep (temporarily cease execution) for the specified
   * number of milliseconds. The thread does not lose ownership of any monitors.
   */
  public static void sleepMillis(long millis) throws InterruptedException {
    INSTANCE.sleep(millis);
  }
}
