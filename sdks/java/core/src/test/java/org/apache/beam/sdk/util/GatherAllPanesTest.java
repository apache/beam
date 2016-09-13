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
package org.apache.beam.sdk.util;

import static org.junit.Assert.fail;

import com.google.common.collect.Iterables;
import java.io.Serializable;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link GatherAllPanes}.
 */
@RunWith(JUnit4.class)
public class GatherAllPanesTest implements Serializable {
  @Test
  @Category(NeedsRunner.class)
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

    PAssert.that(accumulatedPanes)
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

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void multiplePanesMultipleReifiedPane() {
    TestPipeline p = TestPipeline.create();

    PCollection<Long> someElems = p.apply("someLongs", CountingInput.upTo(20000));
    PCollection<Long> otherElems = p.apply("otherLongs", CountingInput.upTo(20000));
    PCollection<Iterable<WindowedValue<Iterable<Long>>>> accumulatedPanes =
        PCollectionList.of(someElems)
            .and(otherElems)
            .apply(Flatten.<Long>pCollections())
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

    PAssert.that(accumulatedPanes)
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

    p.run();
  }
}
