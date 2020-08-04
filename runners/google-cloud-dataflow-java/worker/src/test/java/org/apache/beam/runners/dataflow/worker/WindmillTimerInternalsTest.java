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
package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.List;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link WindmillTimerInternals}. */
@RunWith(JUnit4.class)
public class WindmillTimerInternalsTest {

  private static final List<KV<Coder<? extends BoundedWindow>, StateNamespace>>
      TEST_NAMESPACES_WITH_CODERS =
          ImmutableList.of(
              KV.of(null, StateNamespaces.global()),
              KV.of(
                  GlobalWindow.Coder.INSTANCE,
                  StateNamespaces.window(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE)),
              KV.of(
                  IntervalWindow.getCoder(),
                  StateNamespaces.window(
                      IntervalWindow.getCoder(),
                      new IntervalWindow(new Instant(13), new Instant(47)))));

  private static final List<Instant> TEST_TIMESTAMPS =
      ImmutableList.of(
          BoundedWindow.TIMESTAMP_MIN_VALUE,
          BoundedWindow.TIMESTAMP_MAX_VALUE,
          GlobalWindow.INSTANCE.maxTimestamp(),
          new Instant(0),
          new Instant(127));

  private static final List<String> TEST_STATE_FAMILIES = ImmutableList.of("", "F24");

  private static final List<String> TEST_TIMER_IDS =
      ImmutableList.of("", "foo", "this one has spaces", "this/one/has/slashes", "/");

  @Test
  public void testTimerDataToFromTimer() {
    for (String stateFamily : TEST_STATE_FAMILIES) {
      for (KV<Coder<? extends BoundedWindow>, StateNamespace> coderAndNamespace :
          TEST_NAMESPACES_WITH_CODERS) {

        @Nullable Coder<? extends BoundedWindow> coder = coderAndNamespace.getKey();
        StateNamespace namespace = coderAndNamespace.getValue();

        for (TimeDomain timeDomain : TimeDomain.values()) {
          for (WindmillNamespacePrefix prefix : WindmillNamespacePrefix.values()) {
            for (Instant timestamp : TEST_TIMESTAMPS) {
              List<TimerData> anonymousTimers =
                  ImmutableList.of(
                      TimerData.of(namespace, timestamp, timestamp, timeDomain),
                      TimerData.of(namespace, timestamp, timestamp.minus(1), timeDomain));
              for (TimerData timer : anonymousTimers) {
                assertThat(
                    WindmillTimerInternals.windmillTimerToTimerData(
                        prefix,
                        WindmillTimerInternals.timerDataToWindmillTimer(stateFamily, prefix, timer),
                        coder),
                    equalTo(timer));
              }

              for (String timerId : TEST_TIMER_IDS) {
                List<TimerData> timers =
                    ImmutableList.of(
                        TimerData.of(timerId, namespace, timestamp, timestamp, timeDomain),
                        TimerData.of(
                            timerId, "family", namespace, timestamp, timestamp, timeDomain),
                        TimerData.of(timerId, namespace, timestamp, timestamp.minus(1), timeDomain),
                        TimerData.of(
                            timerId,
                            "family",
                            namespace,
                            timestamp,
                            timestamp.minus(1),
                            timeDomain));

                for (TimerData timer : timers) {
                  assertThat(
                      WindmillTimerInternals.windmillTimerToTimerData(
                          prefix,
                          WindmillTimerInternals.timerDataToWindmillTimer(
                              stateFamily, prefix, timer),
                          coder),
                      equalTo(timer));
                }
              }
            }
          }
        }
      }
    }
  }
}
