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
package org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.AfterFirstCompositeTrigger;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.PeriodicTimeTrigger;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.TimeTrigger;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.Trigger;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.singleton;

/** Time based tumbling windowing. Windows can't overlap. */
@Audience(Audience.Type.CLIENT)
public class Time<T> implements Windowing<T, TimeInterval> {

  private final long durationMillis;
  @Nullable private Duration earlyTriggeringPeriod;

  private Time(long durationMillis) {
    checkArgument(durationMillis > 0, "Windowing with zero duration");
    this.durationMillis = durationMillis;
  }

  public static <T> Time<T> of(Duration duration) {
    return new Time<>(duration.toMillis());
  }

  /**
   * Early results will be triggered periodically until the window is finally closed.
   *
   * @param <T> the type of elements dealt with
   * @param timeout the period after which to periodically trigger windows
   * @return this instance (for method chaining purposes)
   */
  @SuppressWarnings("unchecked")
  public <T> Time<T> earlyTriggering(Duration timeout) {
    this.earlyTriggeringPeriod = Objects.requireNonNull(timeout);
    // ~ the cast is safe, this windowing implementation is self contained,
    // i.e. cannot be subclasses, and is not dependent the actual <T> at all
    return (Time) this;
  }

  @Override
  public Iterable<TimeInterval> assignWindowsToElement(WindowedElement<?, T> el) {
    long stamp = el.getTimestamp();
    long start = stamp - (stamp + durationMillis) % durationMillis;
    long end = start + durationMillis;
    return singleton(new TimeInterval(start, end));
  }

  @Override
  public Trigger<TimeInterval> getTrigger() {
    if (earlyTriggeringPeriod != null) {
      return new AfterFirstCompositeTrigger<>(
          Arrays.asList(
              new TimeTrigger(), new PeriodicTimeTrigger(earlyTriggeringPeriod.toMillis())));
    }
    return new TimeTrigger();
  }

  @Nullable
  public Duration getEarlyTriggeringPeriod() {
    return earlyTriggeringPeriod;
  }

  public long getDuration() {
    return durationMillis;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Time) {
      Time other = (Time) obj;
      return other.durationMillis == durationMillis
          && other.earlyTriggeringPeriod == earlyTriggeringPeriod;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(durationMillis, earlyTriggeringPeriod);
  }
}
