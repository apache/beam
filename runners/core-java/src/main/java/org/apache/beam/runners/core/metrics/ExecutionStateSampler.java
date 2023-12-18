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
package org.apache.beam.runners.core.metrics;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTimeUtils.MillisProvider;

/** Monitors the execution of one or more execution threads. */
public class ExecutionStateSampler {

  private final Set<ExecutionStateTracker> activeTrackers = ConcurrentHashMap.newKeySet();

  private static final MillisProvider SYSTEM_MILLIS_PROVIDER = System::currentTimeMillis;

  private static final ExecutionStateSampler INSTANCE =
      new ExecutionStateSampler(SYSTEM_MILLIS_PROVIDER);

  protected final MillisProvider clock;
  @VisibleForTesting protected volatile long lastSampleTimeMillis;

  protected ExecutionStateSampler(MillisProvider clock) {
    this.clock = clock;
  }

  public static ExecutionStateSampler instance() {
    return INSTANCE;
  }

  @VisibleForTesting
  public static ExecutionStateSampler newForTest() {
    return new ExecutionStateSampler(SYSTEM_MILLIS_PROVIDER);
  }

  @VisibleForTesting
  public static ExecutionStateSampler newForTest(MillisProvider clock) {
    return new ExecutionStateSampler(checkNotNull(clock));
  }

  // The sampling period can be reset with flag --experiment state_sampling_period_millis=<value>.
  private static long periodMs = 200;

  private @Nullable Future<Void> executionSamplerFuture = null;

  /** Set the state sampler sampling period. */
  public static void setSamplingPeriod(long samplingPeriodMillis) {
    periodMs = samplingPeriodMillis;
  }

  /** Reset the state sampler. */
  public void reset() {
    lastSampleTimeMillis = 0;
  }

  /**
   * Called to start the ExecutionStateSampler. Until the returned {@link Closeable} is closed, the
   * state sampler will periodically sample the current state of all the threads it has been asked
   * to manage.
   */
  public void start() {
    start(
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("state-sampler-%d").build()));
  }

  /**
   * The {@link ExecutorService} should be configured to create daemon threads, and should ideally
   * create threads named something like {@code "state-sampler-%d"}.
   */
  @VisibleForTesting
  synchronized void start(ExecutorService executor) {
    if (executionSamplerFuture != null) {
      return;
    }

    executionSamplerFuture =
        executor.submit(
            () -> {
              lastSampleTimeMillis = clock.getMillis();
              long targetTimeMillis = lastSampleTimeMillis + periodMs;
              while (!Thread.interrupted()) {
                long currentTimeMillis = clock.getMillis();
                long difference = targetTimeMillis - currentTimeMillis;
                if (difference > 0) {
                  try {
                    Thread.sleep(difference);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                } else {
                  // Call doSampling if more than PERIOD_MS have passed.
                  doSampling(currentTimeMillis - lastSampleTimeMillis);
                  lastSampleTimeMillis = currentTimeMillis;
                  targetTimeMillis = lastSampleTimeMillis + periodMs;
                }
              }
              return null;
            });
  }

  public synchronized void stop() {
    if (executionSamplerFuture == null) {
      return;
    }

    executionSamplerFuture.cancel(true);
    try {
      executionSamplerFuture.get(5 * periodMs, TimeUnit.MILLISECONDS);
    } catch (CancellationException e) {
      // This was expected -- we were cancelling the thread.
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(
          "Failed to stop state sampling after waiting 5 sampling periods.", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Exception in state sampler", e);
    } finally {
      executionSamplerFuture = null;
    }
  }

  /** Add the tracker to the sampling set. */
  protected void addTracker(ExecutionStateTracker tracker) {
    this.activeTrackers.add(tracker);
  }

  /** Remove the tracker from the sampling set. */
  protected void removeTracker(ExecutionStateTracker tracker) {
    activeTrackers.remove(tracker);

    // Attribute any remaining time since the last sampling while removing the tracker.
    //
    // There is a race condition here; if sampling happens in the time between when we remove the
    // tracker from activeTrackers and read the lastSampleTicks value, the sampling time will
    // be lost for the tracker being removed. This is acceptable as sampling is already an
    // approximation of actual execution time.
    long millisSinceLastSample = clock.getMillis() - this.lastSampleTimeMillis;
    if (millisSinceLastSample > 0) {
      tracker.takeSample(millisSinceLastSample);
    }
  }

  /** Attributing sampling time to trackers. */
  @VisibleForTesting
  public void doSampling(long millisSinceLastSample) {
    for (ExecutionStateTracker tracker : activeTrackers) {
      tracker.takeSample(millisSinceLastSample);
    }
  }
}
