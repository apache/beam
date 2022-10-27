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

package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.core.ApiClock;
import com.google.api.gax.rpc.Watchdog;
import com.google.api.gax.rpc.WatchdogProvider;
import java.util.concurrent.ScheduledExecutorService;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.threeten.bp.Duration;
import com.google.common.base.Preconditions;

/**
 * This class is copied from the {@link com.google.api.gax.rpc.InstantiatingWatchdogProvider},
 * however it says to not auto close, so that the {@link org.apache.beam.sdk.util.UnboundedScheduledExecutorService}
 * we pass it doesn't try to get closed as well.
 */
public class NonClosingWatchdogProvider implements WatchdogProvider {
  private final ApiClock clock;
  private final ScheduledExecutorService executor;
  private final Duration checkInterval;

  public static WatchdogProvider create() {
    return new NonClosingWatchdogProvider(null, null, null);
  }

  private NonClosingWatchdogProvider(
      @Nullable ApiClock clock,
      @Nullable ScheduledExecutorService executor,
      @Nullable Duration checkInterval) {
    this.clock = clock;
    this.executor = executor;
    this.checkInterval = checkInterval;
  }

  @Override
  public boolean needsClock() {
    return clock == null;
  }

  @Override
  public WatchdogProvider withClock(ApiClock clock) {
    return new NonClosingWatchdogProvider(
        Preconditions.checkNotNull(clock), executor, checkInterval);
  }

  @Override
  public boolean needsCheckInterval() {
    return checkInterval == null;
  }

  @Override
  public WatchdogProvider withCheckInterval(Duration checkInterval) {
    return new NonClosingWatchdogProvider(
        clock, executor, Preconditions.checkNotNull(checkInterval));
  }

  @Override
  public boolean needsExecutor() {
    return executor == null;
  }

  @Override
  public WatchdogProvider withExecutor(ScheduledExecutorService executor) {
    return new NonClosingWatchdogProvider(
        clock, Preconditions.checkNotNull(executor), checkInterval);
  }

  @SuppressWarnings("ConstantConditions")
  @Nullable
  @Override
  public Watchdog getWatchdog() {
    Preconditions.checkState(!needsClock(), "A clock is needed");
    Preconditions.checkState(!needsCheckInterval(), "A check interval is needed");
    Preconditions.checkState(!needsExecutor(), "An executor is needed");

    // Watchdog is disabled
    if (checkInterval.isZero()) {
      return null;
    }

    return Watchdog.create(clock, checkInterval, executor);
  }

  @Override
  public boolean shouldAutoClose() {
    return false;
  }

}