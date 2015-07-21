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

import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.common.base.MoreObjects;

import org.joda.time.Instant;

import java.util.Objects;

/**
 * Encapsulate interaction with time within the execution environment.
 *
 * <p> This class allows setting and deleting timers, and also retrieving an
 * estimate of the current time.
 */
public interface TimerInternals {

  /**
   * Writes out a timer to be fired when the watermark reaches the given
   * timestamp.
   *
   * <p> The combination of {@code namespace}, {@code timestamp} and {@code domain} uniquely
   * identify a timer. Multiple timers set for the same parameters can be safely deduplicated.
   */
  void setTimer(TimerData timerKey);

  /**
   * Deletes the given timer.
   */
  void deleteTimer(TimerData timerKey);

  /**
   * Returns the current timestamp in the {@link TimeDomain#PROCESSING_TIME} time domain.
   */
  Instant currentProcessingTime();

  /**
   * Returns an estimate of the current timestamp in the {@link TimeDomain#EVENT_TIME} time domain.
   */
  Instant currentWatermarkTime();

  /**
   * Data about a timer as represented within {@link TimerInternals}.
   */
  public static class TimerData implements Comparable<TimerData> {
    private final StateNamespace namespace;
    private final Instant timestamp;
    private final TimeDomain domain;

    private TimerData(StateNamespace namespace, Instant timestamp, TimeDomain domain) {
      this.namespace = namespace;
      this.timestamp = timestamp;
      this.domain = domain;
    }

    public StateNamespace getNamespace() {
      return namespace;
    }

    public Instant getTimestamp() {
      return timestamp;
    }

    public TimeDomain getDomain() {
      return domain;
    }

    /**
     * Construct the {@code TimerKey} for the given parameters.
     */
    public static TimerData of(StateNamespace namespace, Instant timestamp, TimeDomain domain) {
      return new TimerData(namespace, timestamp, domain);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof TimerData)) {
        return false;
      }

      TimerData that = (TimerData) obj;
      return Objects.equals(this.domain, that.domain)
          && this.timestamp.isEqual(that.timestamp)
          && Objects.equals(this.namespace, that.namespace);
    }

    @Override
    public int hashCode() {
      return Objects.hash(domain, timestamp, namespace);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("namespace", namespace)
          .add("timestamp", timestamp)
          .add("domain", domain)
          .toString();
    }

    @Override
    public int compareTo(TimerData o) {
      return Long.compare(timestamp.getMillis(), o.getTimestamp().getMillis());
    }
  }
}
