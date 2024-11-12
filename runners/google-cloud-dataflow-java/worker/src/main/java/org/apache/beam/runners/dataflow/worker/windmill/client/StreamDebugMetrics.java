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
package org.apache.beam.runners.dataflow.worker.windmill.client;

import com.google.auto.value.AutoValue;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.DateTime;
import org.joda.time.Instant;

/** Records stream metrics for debugging. */
@ThreadSafe
final class StreamDebugMetrics {
  private final Supplier<Instant> clock;

  @GuardedBy("this")
  private int errorCount = 0;

  @GuardedBy("this")
  private int restartCount = 0;

  @GuardedBy("this")
  private long sleepUntil = 0;

  @GuardedBy("this")
  private String lastRestartReason = "";

  @GuardedBy("this")
  private @Nullable DateTime lastRestartTime = null;

  @GuardedBy("this")
  private long lastResponseTimeMs = 0;

  @GuardedBy("this")
  private long lastSendTimeMs = 0;

  @GuardedBy("this")
  private long startTimeMs = 0;

  @GuardedBy("this")
  private @Nullable DateTime shutdownTime = null;

  @GuardedBy("this")
  private boolean clientClosed = false;

  private StreamDebugMetrics(Supplier<Instant> clock) {
    this.clock = clock;
  }

  static StreamDebugMetrics create() {
    return new StreamDebugMetrics(Instant::now);
  }

  @VisibleForTesting
  static StreamDebugMetrics forTesting(Supplier<Instant> fakeClock) {
    return new StreamDebugMetrics(fakeClock);
  }

  private static long debugDuration(long nowMs, long startMs) {
    return startMs <= 0 ? -1 : Math.max(0, nowMs - startMs);
  }

  private long nowMs() {
    return clock.get().getMillis();
  }

  synchronized void recordSend() {
    lastSendTimeMs = nowMs();
  }

  synchronized void recordStart() {
    startTimeMs = nowMs();
    lastResponseTimeMs = 0;
  }

  synchronized void recordResponse() {
    lastResponseTimeMs = nowMs();
  }

  synchronized void recordRestartReason(String error) {
    lastRestartReason = error;
    lastRestartTime = clock.get().toDateTime();
  }

  synchronized long getStartTimeMs() {
    return startTimeMs;
  }

  synchronized long getLastSendTimeMs() {
    return lastSendTimeMs;
  }

  synchronized void recordSleep(long sleepMs) {
    sleepUntil = nowMs() + sleepMs;
  }

  synchronized int incrementAndGetRestarts() {
    return restartCount++;
  }

  synchronized int incrementAndGetErrors() {
    return errorCount++;
  }

  synchronized void recordShutdown() {
    shutdownTime = clock.get().toDateTime();
  }

  synchronized void recordHalfClose() {
    clientClosed = true;
  }

  synchronized Optional<String> responseDebugString(long nowMillis) {
    return lastResponseTimeMs == 0
        ? Optional.empty()
        : Optional.of("received response " + (nowMillis - lastResponseTimeMs) + "ms ago");
  }

  private synchronized Optional<RestartMetrics> getRestartMetrics() {
    if (restartCount > 0) {
      return Optional.of(
          RestartMetrics.create(restartCount, lastRestartReason, lastRestartTime, errorCount));
    }

    return Optional.empty();
  }

  synchronized Snapshot getSummaryMetrics() {
    long nowMs = clock.get().getMillis();
    return Snapshot.create(
        debugDuration(nowMs, startTimeMs),
        debugDuration(nowMs, lastSendTimeMs),
        debugDuration(nowMs, lastResponseTimeMs),
        getRestartMetrics(),
        sleepUntil - nowMs(),
        shutdownTime,
        clientClosed);
  }

  @AutoValue
  abstract static class Snapshot {
    private static Snapshot create(
        long streamAge,
        long timeSinceLastSend,
        long timeSinceLastResponse,
        Optional<RestartMetrics> restartMetrics,
        long sleepLeft,
        @Nullable DateTime shutdownTime,
        boolean isClientClosed) {
      return new AutoValue_StreamDebugMetrics_Snapshot(
          streamAge,
          timeSinceLastSend,
          timeSinceLastResponse,
          restartMetrics,
          sleepLeft,
          Optional.ofNullable(shutdownTime),
          isClientClosed);
    }

    abstract long streamAge();

    abstract long timeSinceLastSend();

    abstract long timeSinceLastResponse();

    abstract Optional<RestartMetrics> restartMetrics();

    abstract long sleepLeft();

    abstract Optional<DateTime> shutdownTime();

    abstract boolean isClientClosed();
  }

  @AutoValue
  abstract static class RestartMetrics {
    private static RestartMetrics create(
        int restartCount,
        String restartReason,
        @Nullable DateTime lastRestartTime,
        int errorCount) {
      return new AutoValue_StreamDebugMetrics_RestartMetrics(
          restartCount, restartReason, Optional.ofNullable(lastRestartTime), errorCount);
    }

    abstract int restartCount();

    abstract String lastRestartReason();

    abstract Optional<DateTime> lastRestartTime();

    abstract int errorCount();
  }
}
