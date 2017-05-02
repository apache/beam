/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.beam.translation.runtime.state;

import com.alibaba.jstorm.beam.translation.runtime.TimerService;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.util.state.GroupingState;
import org.apache.beam.sdk.util.state.ReadableState;
import org.apache.beam.sdk.util.state.WatermarkHoldState;
import org.joda.time.Instant;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * JStorm implementation of {@link WatermarkHoldState}.
 */
public class JStormWatermarkHoldState<W extends BoundedWindow> implements WatermarkHoldState<W> {

    private final StateNamespace namespace;
    private final GroupingState<Instant, Instant> watermarkHoldsState;
    private final OutputTimeFn<? super W> outputTimeFn;
    private final TimerService timerService;

    JStormWatermarkHoldState(
            StateNamespace namespace,
            GroupingState<Instant, Instant> watermarkHoldsState,
            OutputTimeFn<? super W> outputTimeFn,
            TimerService timerService) {
        this.namespace = checkNotNull(namespace, "namespace");
        this.watermarkHoldsState = checkNotNull(watermarkHoldsState, "watermarkHoldsState");
        this.outputTimeFn = checkNotNull(outputTimeFn, "outputTimeFn");
        this.timerService = checkNotNull(timerService, "timerService");
    }

    @Override
    public OutputTimeFn<? super W> getOutputTimeFn() {
        return outputTimeFn;
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
    public WatermarkHoldState<W> readLater() {
        // TODO: support prefetch.
        return this;
    }

    @Override
    public void clear() {
        timerService.clearWatermarkHold(namespace.stringKey());
        watermarkHoldsState.clear();
    }
}
