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
package org.apache.beam.runners.gearpump.translators.utils;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import io.gearpump.streaming.dsl.window.impl.Window;
import java.time.Instant;
import java.util.List;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Test;

/** Tests for {@link TranslatorUtils}. */
public class TranslatorUtilsTest {

  private static final List<KV<org.joda.time.Instant, Instant>> TEST_VALUES =
      Lists.newArrayList(
          KV.of(new org.joda.time.Instant(0), Instant.EPOCH),
          KV.of(new org.joda.time.Instant(42), Instant.ofEpochMilli(42)),
          KV.of(new org.joda.time.Instant(Long.MIN_VALUE), Instant.ofEpochMilli(Long.MIN_VALUE)),
          KV.of(new org.joda.time.Instant(Long.MAX_VALUE), Instant.ofEpochMilli(Long.MAX_VALUE)));

  @Test
  public void testJodaTimeAndJava8TimeConversion() {
    for (KV<org.joda.time.Instant, Instant> kv : TEST_VALUES) {
      assertThat(TranslatorUtils.jodaTimeToJava8Time(kv.getKey()), equalTo(kv.getValue()));
      assertThat(TranslatorUtils.java8TimeToJodaTime(kv.getValue()), equalTo(kv.getKey()));
    }
  }

  @Test
  public void testBoundedWindowToGearpumpWindow() {
    assertThat(
        TranslatorUtils.boundedWindowToGearpumpWindow(
            new IntervalWindow(
                new org.joda.time.Instant(0), new org.joda.time.Instant(Long.MAX_VALUE))),
        equalTo(Window.apply(Instant.EPOCH, Instant.ofEpochMilli(Long.MAX_VALUE))));
    assertThat(
        TranslatorUtils.boundedWindowToGearpumpWindow(
            new IntervalWindow(
                new org.joda.time.Instant(Long.MIN_VALUE),
                new org.joda.time.Instant(Long.MAX_VALUE))),
        equalTo(
            Window.apply(
                Instant.ofEpochMilli(Long.MIN_VALUE), Instant.ofEpochMilli(Long.MAX_VALUE))));
    BoundedWindow globalWindow = GlobalWindow.INSTANCE;
    assertThat(
        TranslatorUtils.boundedWindowToGearpumpWindow(globalWindow),
        equalTo(
            Window.apply(
                Instant.ofEpochMilli(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis()),
                Instant.ofEpochMilli(globalWindow.maxTimestamp().getMillis() + 1))));
  }
}
