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
package org.apache.beam.runners.core;

import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WindowMatchers}. */
@RunWith(JUnit4.class)
public class WindowMatchersTest {

  @Test
  public void testIsWindowedValueExact() {
    long timestamp = 100;
    long windowStart = 0;
    long windowEnd = 200;

    assertThat(
        WindowedValue.of(
            "hello",
            new Instant(timestamp),
            new IntervalWindow(new Instant(windowStart), new Instant(windowEnd)),
            PaneInfo.NO_FIRING),
        WindowMatchers.isWindowedValue(
            "hello",
            new Instant(timestamp),
            ImmutableList.of(new IntervalWindow(new Instant(windowStart), new Instant(windowEnd))),
            PaneInfo.NO_FIRING));
  }

  @Test
  public void testIsWindowedValueReorderedWindows() {
    long timestamp = 100;
    long windowStart = 0;
    long windowEnd = 200;
    long windowStart2 = 50;
    long windowEnd2 = 150;

    assertThat(
        WindowedValue.of(
            "hello",
            new Instant(timestamp),
            ImmutableList.of(
                new IntervalWindow(new Instant(windowStart), new Instant(windowEnd)),
                new IntervalWindow(new Instant(windowStart2), new Instant(windowEnd2))),
            PaneInfo.NO_FIRING),
        WindowMatchers.isWindowedValue(
            "hello",
            new Instant(timestamp),
            ImmutableList.of(
                new IntervalWindow(new Instant(windowStart), new Instant(windowEnd)),
                new IntervalWindow(new Instant(windowStart2), new Instant(windowEnd2))),
            PaneInfo.NO_FIRING));
  }
}
