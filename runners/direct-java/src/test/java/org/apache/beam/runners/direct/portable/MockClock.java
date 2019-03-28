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
package org.apache.beam.runners.direct.portable;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.runners.direct.Clock;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A clock that returns a constant value for now which can be set with calls to {@link
 * #set(Instant)}.
 *
 * <p>For uses of the {@link Clock} interface in unit tests.
 */
class MockClock implements Clock {

  private Instant now;

  public static MockClock fromInstant(Instant initial) {
    return new MockClock(initial);
  }

  private MockClock(Instant initialNow) {
    this.now = initialNow;
  }

  public void set(Instant newNow) {
    checkArgument(
        !newNow.isBefore(now),
        "Cannot move MockClock backwards in time from %s to %s",
        now,
        newNow);
    this.now = newNow;
  }

  public void advance(Duration duration) {
    checkArgument(
        duration.getMillis() > 0,
        "Cannot move MockClock backwards in time by duration %s",
        duration);
    set(now.plus(duration));
  }

  @Override
  public Instant now() {
    return now;
  }
}
