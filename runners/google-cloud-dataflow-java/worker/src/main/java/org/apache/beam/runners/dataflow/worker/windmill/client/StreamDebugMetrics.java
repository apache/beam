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
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.joda.time.DateTime;
import org.joda.time.Instant;

/** Records stream metrics for debugging. */
@ThreadSafe
final class StreamDebugMetrics {
  private final AtomicInteger restartCount = new AtomicInteger();
  private final AtomicInteger errorCount = new AtomicInteger();

  @GuardedBy("this")
  private long sleepUntil = 0;

  @GuardedBy("this")
  private String lastRestartReason = "";

  @GuardedBy("this")
  private DateTime lastRestartTime = null;

  @GuardedBy("this")
  private long lastResponseTimeMs = 0;

  @GuardedBy("this")
  private long lastSendTimeMs = 0;

  @GuardedBy("this")
  private long startTimeMs = 0;

  @GuardedBy("this")
  private DateTime shutdownTime = null;

  private static long debugDuration(long nowMs, long startMs) {
    return startMs <= 0 ? -1 : Math.max(0, nowMs - startMs);
  }

  private static long nowMs() {
    return Instant.now().getMillis();
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
    lastRestartTime = DateTime.now();
  }

  synchronized long startTimeMs() {
    return startTimeMs;
  }

  synchronized long lastSendTimeMs() {
    return lastSendTimeMs;
  }

  synchronized void recordSleep(long sleepMs) {
    sleepUntil = nowMs() + sleepMs;
  }

  int incrementAndGetRestarts() {
    return restartCount.incrementAndGet();
  }

  int incrementAndGetErrors() {
    return errorCount.incrementAndGet();
  }

  synchronized void recordShutdown() {
    shutdownTime = DateTime.now();
  }

  synchronized String responseDebugString(long nowMillis) {
    return lastResponseTimeMs == 0
        ? "never received response"
        : "received response " + (nowMillis - lastResponseTimeMs) + "ms ago";
  }

  private Optional<RestartMetrics> getRestartMetrics() {
    if (restartCount.get() > 0) {
      synchronized (this) {
        return Optional.of(
            RestartMetrics.create(
                restartCount.get(), lastRestartReason, lastRestartTime, errorCount.get()));
      }
    }

    return Optional.empty();
  }

  synchronized Snapshot getSummaryMetrics() {
    long nowMs = Instant.now().getMillis();
    return Snapshot.create(
        debugDuration(nowMs, startTimeMs),
        debugDuration(nowMs, lastSendTimeMs),
        debugDuration(nowMs, lastResponseTimeMs),
        getRestartMetrics(),
        sleepUntil - nowMs(),
        shutdownTime);
  }

  @AutoValue
  abstract static class Snapshot {
    private static Snapshot create(
        long streamAge,
        long timeSinceLastSend,
        long timeSinceLastResponse,
        Optional<RestartMetrics> restartMetrics,
        long sleepLeft,
        @Nullable DateTime shutdownTime) {
      return new AutoValue_StreamDebugMetrics_Snapshot(
          streamAge,
          timeSinceLastSend,
          timeSinceLastResponse,
          restartMetrics,
          sleepLeft,
          Optional.ofNullable(shutdownTime));
    }

    abstract long streamAge();

    abstract long timeSinceLastSend();

    abstract long timeSinceLastResponse();

    abstract Optional<RestartMetrics> restartMetrics();

    abstract long sleepLeft();

    abstract Optional<DateTime> shutdownTime();
  }

  @AutoValue
  abstract static class RestartMetrics {
    private static RestartMetrics create(
        int restartCount, String restartReason, DateTime lastRestartTime, int errorCount) {
      return new AutoValue_StreamDebugMetrics_RestartMetrics(
          restartCount, restartReason, lastRestartTime, errorCount);
    }

    abstract int restartCount();

    abstract String lastRestartReason();

    abstract DateTime lastRestartTime();

    abstract int errorCount();
  }
}
