/*
 * Copyright (C) 2016 Google Inc.
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

import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.io.CountingInput;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Values;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.transforms.WithTimestamps;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.collect.Iterables;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests for {@link GatherAllPanes}.
 */
@RunWith(JUnit4.class)
public class GatherAllPanesTest implements Serializable {
  @Test
  public void singlePaneSingleReifiedPane() {
    TestPipeline p = TestPipeline.create();
    PCollection<Iterable<WindowedValue<Iterable<Long>>>> accumulatedPanes =
        p.apply(CountingInput.upTo(20000))
            .apply(
                WithTimestamps.of(
                    new SerializableFunction<Long, Instant>() {
                      @Override
                      public Instant apply(Long input) {
                        return new Instant(input * 10);
                      }
                    }))
            .apply(
                Window.<Long>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply(WithKeys.<Void, Long>of((Void) null).withKeyType(new TypeDescriptor<Void>() {}))
            .apply(GroupByKey.<Void, Long>create())
            .apply(Values.<Iterable<Long>>create())
            .apply(GatherAllPanes.<Iterable<Long>>globally());

    DataflowAssert.that(accumulatedPanes)
        .satisfies(
            new SerializableFunction<Iterable<Iterable<WindowedValue<Iterable<Long>>>>, Void>() {
              @Override
              public Void apply(Iterable<Iterable<WindowedValue<Iterable<Long>>>> input) {
                for (Iterable<WindowedValue<Iterable<Long>>> windowedInput : input) {
                  if (Iterables.size(windowedInput) > 1) {
                    fail("Expected all windows to have exactly one pane, got " + windowedInput);
                    return null;
                  }
                }
                return null;
              }
            });
  }

  @Test
  public void multiplePanesMultipleReifiedPane() {
    TestPipeline p = TestPipeline.create();

    PCollection<Iterable<WindowedValue<Iterable<Long>>>> accumulatedPanes =
        p.apply(CountingInput.upTo(20000))
            .apply(
                WithTimestamps.of(
                    new SerializableFunction<Long, Instant>() {
                      @Override
                      public Instant apply(Long input) {
                        return new Instant(input * 10);
                      }
                    }))
            .apply(
                Window.<Long>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(AfterPane.elementCountAtLeast(1)))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply(WithKeys.<Void, Long>of((Void) null).withKeyType(new TypeDescriptor<Void>() {}))
            .apply(GroupByKey.<Void, Long>create())
            .apply(Values.<Iterable<Long>>create())
            .apply(GatherAllPanes.<Iterable<Long>>globally());

    DataflowAssert.that(accumulatedPanes)
        .satisfies(
            new SerializableFunction<Iterable<Iterable<WindowedValue<Iterable<Long>>>>, Void>() {
              @Override
              public Void apply(Iterable<Iterable<WindowedValue<Iterable<Long>>>> input) {
                for (Iterable<WindowedValue<Iterable<Long>>> windowedInput : input) {
                  if (Iterables.size(windowedInput) > 1) {
                    return null;
                  }
                }
                fail("Expected at least one window to have multiple panes");
                return null;
              }
            });
  }
}
