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
package org.apache.beam.sdk.io.synthetic.delay;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.synthetic.SyntheticOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Duration;

/** Utility functions used in {@link org.apache.beam.sdk.io.synthetic}. */
public class SyntheticDelay {

  // cpu delay implementation:

  private static final long MASK = (1L << 16) - 1L;
  private static final long HASH = 0x243F6A8885A308D3L;
  private static final long INIT_PLAINTEXT = 50000L;

  /**
   * Implements a mechanism to delay a thread in various fashions. * {@code CPU}: Burn CPU while
   * waiting. * {@code SLEEP}: Sleep uninterruptibly while waiting. * {@code MIXED}: Switch between
   * burning CPU and sleeping every millisecond to emulate a desired CPU utilization specified by
   * {@code cpuUtilizationInMixedDelay}.
   *
   * @return Millis spent sleeping, does not include time spent spinning.
   */
  public static long delay(
      Duration delay,
      double cpuUtilizationInMixedDelay,
      SyntheticOptions.DelayType delayType,
      Random rnd) {
    switch (delayType) {
      case CPU:
        cpuDelay(delay.getMillis());
        return 0;
      case SLEEP:
        Uninterruptibles.sleepUninterruptibly(
            Math.max(0L, delay.getMillis()), TimeUnit.MILLISECONDS);
        return delay.getMillis();
      case MIXED:
        // Mixed mode: for each millisecond of delay randomly choose to spin or sleep.
        // This is enforced at millisecond granularity since that is the minimum duration that
        // Thread.sleep() can sleep. Millisecond is also the unit of processing delay.
        long sleepMillis = 0;
        for (long i = 0; i < delay.getMillis(); i++) {
          if (rnd.nextDouble() < cpuUtilizationInMixedDelay) {
            delay(new Duration(1), 0.0, SyntheticOptions.DelayType.CPU, rnd);
          } else {
            sleepMillis += delay(new Duration(1), 0.0, SyntheticOptions.DelayType.SLEEP, rnd);
          }
        }
        return sleepMillis;
      default:
        throw new IllegalArgumentException("Unknown delay type " + delayType);
    }
  }

  /** Keep cpu busy for {@code delayMillis} by calculating lots of hashes. */
  private static void cpuDelay(long delayMillis) {
    // Note that the delay is enforced in terms of walltime. That implies this thread may not
    // keep CPU busy if it gets preempted by other threads. There is more of chance of this
    // occurring in a streaming pipeline as there could be lots of threads running this. The loop
    // measures cpu time spent for each iteration, so that these effects are some what minimized.

    long cpuMicros = delayMillis * 1000;
    Stopwatch timer = Stopwatch.createUnstarted();

    while (timer.elapsed(TimeUnit.MICROSECONDS) < cpuMicros) {
      // Find a long which hashes to HASH in lowest MASK bits.
      // Values chosen to roughly take 1ms on typical workstation.
      timer.start();
      long p = INIT_PLAINTEXT;
      while (true) {
        long t = Hashing.murmur3_128().hashLong(p).asLong();
        if ((t & MASK) == (HASH & MASK)) {
          break;
        }
        p++;
      }
      timer.stop();
    }
  }
}
