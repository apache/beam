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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link PTransform} which produces a sequence of elements at fixed runtime intervals.
 *
 * <p>If applyWindowing() is specified, each element will be assigned to its own fixed window.
 *
 * <p>See {@link PeriodicSequence}.
 */
public class PeriodicImpulse extends PTransform<PBegin, PCollection<Instant>> {

  Instant startTimestamp = Instant.now();
  Instant stopTimestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
  Duration fireInterval = Duration.standardMinutes(1);
  boolean applyWindowing = false;

  private PeriodicImpulse() {}

  public static PeriodicImpulse create() {
    return new PeriodicImpulse();
  }

  public PeriodicImpulse startAt(Instant startTime) {
    this.startTimestamp = startTime;
    return this;
  }

  public PeriodicImpulse stopAt(Instant stopTime) {
    this.stopTimestamp = stopTime;
    return this;
  }

  public PeriodicImpulse withInterval(Duration interval) {
    this.fireInterval = interval;
    return this;
  }

  public PeriodicImpulse applyWindowing() {
    this.applyWindowing = true;
    return this;
  }

  @Override
  public PCollection<Instant> expand(PBegin input) {
    PCollection<Instant> result =
        input
            .apply(
                Create.<PeriodicSequence.SequenceDefinition>of(
                    new PeriodicSequence.SequenceDefinition(
                        startTimestamp, stopTimestamp, fireInterval)))
            .apply(PeriodicSequence.create());

    if (this.applyWindowing) {
      result =
          result.apply(
              Window.<Instant>into(FixedWindows.of(Duration.millis(fireInterval.getMillis()))));
    }

    return result;
  }
}
