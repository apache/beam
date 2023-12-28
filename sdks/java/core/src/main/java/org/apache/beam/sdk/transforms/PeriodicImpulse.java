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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
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

  Instant startTimestamp;
  Instant stopTimestamp;
  @Nullable Duration stopDuration;
  Duration fireInterval;
  boolean applyWindowing = false;
  boolean catchUpToNow = true;

  private PeriodicImpulse() {
    this.startTimestamp = Instant.now();
    this.stopTimestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
    this.fireInterval = Duration.standardMinutes(1);
  }

  public static PeriodicImpulse create() {
    return new PeriodicImpulse();
  }

  /**
   * Assign a timestamp when the pipeliene starts to produce data.
   *
   * <p>Cannot be used along with {@link #stopAfter}.
   */
  public PeriodicImpulse startAt(Instant startTime) {
    checkArgument(stopDuration == null, "startAt and stopAfter cannot be set at the same time");
    this.startTimestamp = startTime;
    return this;
  }

  /**
   * Assign a timestamp when the pipeliene stops producing data.
   *
   * <p>Cannot be used along with {@link #stopAfter}.
   */
  public PeriodicImpulse stopAt(Instant stopTime) {
    checkArgument(stopDuration == null, "stopAt and stopAfter cannot be set at the same time");
    this.stopTimestamp = stopTime;
    return this;
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Assign a time interval at which the pipeliene produces data. This is different from setting
   * {@link #startAt} and {@link #stopAt}, as the first timestamp is determined at run time
   * (pipeline starts processing).
   */
  @Internal
  public PeriodicImpulse stopAfter(Duration duration) {
    this.stopDuration = duration;
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

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>The default behavior is that PeriodicImpulse emits all instants until Instant.now(), then
   * starts firing at the specified interval. If this is set to false, the PeriodicImpulse will
   * perform the interval wait before firing each instant.
   */
  @Internal
  public PeriodicImpulse catchUpToNow(boolean catchUpToNow) {
    this.catchUpToNow = catchUpToNow;
    return this;
  }

  @Override
  public PCollection<Instant> expand(PBegin input) {
    PCollection<PeriodicSequence.SequenceDefinition> seqDef;
    if (stopDuration != null) {
      // nonnull guaranteed
      Duration d = stopDuration;
      seqDef =
          input
              .apply(Impulse.create())
              .apply(ParDo.of(new RuntimeSequenceFn(d, fireInterval, catchUpToNow)));
    } else {
      seqDef =
          input.apply(
              Create.of(
                  new PeriodicSequence.SequenceDefinition(
                      startTimestamp, stopTimestamp, fireInterval, catchUpToNow)));
    }
    PCollection<Instant> result = seqDef.apply(PeriodicSequence.create());

    if (this.applyWindowing) {
      result =
          result.apply(Window.into(FixedWindows.of(Duration.millis(fireInterval.getMillis()))));
    }
    return result;
  }

  /**
   * A DoFn generated a SequenceDefinition at run time. This enables set first element timestamp at
   * pipeline start processing data.
   */
  private static class RuntimeSequenceFn extends DoFn<byte[], PeriodicSequence.SequenceDefinition> {
    Duration stopDuration;
    Duration fireInterval;
    boolean catchUpToNow;

    RuntimeSequenceFn(Duration stopDuration, Duration fireInterval, boolean catchUpToNow) {
      this.stopDuration = stopDuration;
      this.fireInterval = fireInterval;
      this.catchUpToNow = catchUpToNow;
    }

    @ProcessElement
    public void process(ProcessContext c) {
      Instant now = Instant.now();
      c.output(
          new PeriodicSequence.SequenceDefinition(
              now, now.plus(stopDuration), fireInterval, catchUpToNow));
    }
  }
}
