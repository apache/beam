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

import avro.shaded.com.google.common.collect.Lists;
import java.util.List;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * <p>A {@link PTransform} which produces a sequence of elements at fixed
 * runtime intervals.
 *
 * If applyWindowing() is specified, each element will be assigned to its own
 * fixed window.
 *
 * See {@link PeriodicSequence}.
 */
public class PeriodicImpulse extends PTransform<PBegin, PCollection<Instant>> {

  Instant start_timestamp = Instant.now();
  Instant stop_timestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
  long fire_interval_millisec = 1000;
  boolean apply_windowing = false;

  private PeriodicImpulse() {}

  public static PeriodicImpulse create() {
    return new PeriodicImpulse();
  }

  public PeriodicImpulse startAt(Instant start_time) {
    this.start_timestamp = start_time;
    return this;
  }

  public PeriodicImpulse stopAt(Instant stop_time) {
    this.stop_timestamp = stop_time;
    return this;
  }

  public PeriodicImpulse withInterval(Duration interval) {
    this.fire_interval_millisec = interval.getMillis();
    return this;
  }

  public PeriodicImpulse applyWindowing() {
    this.apply_windowing = true;
    return this;
  }

  @Override
  public PCollection<Instant> expand(PBegin input) {
    Long[] seqInitArgsArr =
        new Long[] {
          start_timestamp.getMillis(), stop_timestamp.getMillis(), fire_interval_millisec
        };
    List<Long> seqInitArgs = Lists.newArrayList(seqInitArgsArr);
    PCollection<Instant> result =
        input.apply(Create.<List<Long>>of(seqInitArgs)).apply(PeriodicSequence.create());

    if (this.apply_windowing) {
      result =
          result.apply(
              Window.<Instant>into(FixedWindows.of(Duration.millis(fire_interval_millisec))));
    }

    return result;
  }
}
