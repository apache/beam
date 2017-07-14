/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.jstorm.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.joda.time.Instant;

/**
 * JStorm implementation of {@link WatermarkHoldState}.
 */
class JStormWatermarkHoldState implements WatermarkHoldState {

  private final StateNamespace namespace;
  private final GroupingState<Instant, Instant> watermarkHoldsState;
  private final TimestampCombiner timestampCombiner;
  private final TimerService timerService;

  JStormWatermarkHoldState(
      StateNamespace namespace,
      GroupingState<Instant, Instant> watermarkHoldsState,
      TimestampCombiner timestampCombiner,
      TimerService timerService) {
    this.namespace = checkNotNull(namespace, "namespace");
    this.watermarkHoldsState = checkNotNull(watermarkHoldsState, "watermarkHoldsState");
    this.timestampCombiner = checkNotNull(timestampCombiner, "timestampCombiner");
    this.timerService = checkNotNull(timerService, "timerService");
  }

  @Override
  public TimestampCombiner getTimestampCombiner() {
    return timestampCombiner;
  }

  @Override
  public void add(Instant instant) {
    timerService.addWatermarkHold(namespace.stringKey(), instant);
    watermarkHoldsState.add(instant);
  }

  @Override
  public ReadableState<Boolean> isEmpty() {
    return watermarkHoldsState.isEmpty();
  }

  @Override
  public Instant read() {
    return watermarkHoldsState.read();
  }

  @Override
  public WatermarkHoldState readLater() {
    // TODO: support prefetch.
    return this;
  }

  @Override
  public void clear() {
    timerService.clearWatermarkHold(namespace.stringKey());
    watermarkHoldsState.clear();
  }
}
