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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaceForTest;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.worker.WindmillNamespacePrefix;
import org.apache.beam.runners.dataflow.worker.util.common.worker.InternedByteString;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WindmillTagEncodingV1Test {
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
          new Instant(127),
          // The encoding of Instant(716000) ends with '+'.
          new Instant(716001));

  private static final List<String> TEST_STATE_FAMILIES = ImmutableList.of("", "F24");

  private static final List<String> TEST_TIMER_IDS =
      ImmutableList.of("", "foo", "this one has spaces", "this/one/has/slashes", "/");

  @Test
  public void testStateTag() {
    StateNamespaceForTest namespace = new StateNamespaceForTest("key");
    StateTag<SetState<Integer>> foo = StateTags.set("foo", VarIntCoder.of());
    InternedByteString bytes = WindmillTagEncodingV1.instance().stateTag(namespace, foo);
    assertEquals("key+ufoo", bytes.byteString().toStringUtf8());
  }

  @Test
  public void testStateTagNested() {
    // Hypothetical case where a namespace/tag encoding depends on a call to encodeKey
    // This tests if thread locals in WindmillStateUtil are not reused with nesting
    StateNamespaceForTest namespace1 = new StateNamespaceForTest("key");
    StateTag<SetState<Integer>> tag1 = StateTags.set("foo", VarIntCoder.of());
    StateTag<SetState<Integer>> tag2 =
        new StateTag<SetState<Integer>>() {
          @Override
          public void appendTo(Appendable sb) throws IOException {
            WindmillTagEncodingV1.instance().stateTag(namespace1, tag1);
            sb.append("tag2");
          }

          @Override
          public String getId() {
            return "";
          }

          @Override
          public StateSpec<SetState<Integer>> getSpec() {
            return null;
          }

          @Override
          public SetState<Integer> bind(StateBinder binder) {
            return null;
          }
        };

    StateNamespace namespace2 =
        new StateNamespaceForTest("key") {
          @Override
          public void appendTo(Appendable sb) throws IOException {
            WindmillTagEncodingV1.instance().stateTag(namespace1, tag1);
            sb.append("namespace2");
          }
        };
    InternedByteString bytes = WindmillTagEncodingV1.instance().stateTag(namespace2, tag2);
    assertEquals("namespace2+tag2", bytes.byteString().toStringUtf8());
  }

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
                      TimerData.of(
                          namespace, timestamp, timestamp.minus(Duration.millis(1)), timeDomain));
              for (TimerData timer : anonymousTimers) {
                Instant expectedTimestamp =
                    timer.getOutputTimestamp().isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)
                        ? BoundedWindow.TIMESTAMP_MIN_VALUE
                        : timer.getOutputTimestamp();
                TimerData computed =
                    WindmillTagEncodingV1.instance()
                        .windmillTimerToTimerData(
                            prefix,
                            WindmillTagEncodingV1.instance()
                                .buildWindmillTimerFromTimerData(
                                    stateFamily, prefix, timer, Timer.newBuilder())
                                .build(),
                            coder,
                            false);
                // The function itself bounds output, so we dont expect the original input as the
                // output, we expect it to be bounded
                TimerData expected =
                    TimerData.of(
                        timer.getNamespace(), timestamp, expectedTimestamp, timer.getDomain());

                assertThat(computed, equalTo(expected));
              }

              for (String timerId : TEST_TIMER_IDS) {
                List<TimerData> timers =
                    ImmutableList.of(
                        TimerData.of(timerId, namespace, timestamp, timestamp, timeDomain),
                        TimerData.of(
                            timerId, "family", namespace, timestamp, timestamp, timeDomain),
                        TimerData.of(
                            timerId,
                            namespace,
                            timestamp,
                            timestamp.minus(Duration.millis(1)),
                            timeDomain),
                        TimerData.of(
                            timerId,
                            "family",
                            namespace,
                            timestamp,
                            timestamp.minus(Duration.millis(1)),
                            timeDomain));

                for (TimerData timer : timers) {
                  Instant expectedTimestamp =
                      timer.getOutputTimestamp().isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)
                          ? BoundedWindow.TIMESTAMP_MIN_VALUE
                          : timer.getOutputTimestamp();

                  TimerData expected =
                      TimerData.of(
                          timer.getTimerId(),
                          timer.getTimerFamilyId(),
                          timer.getNamespace(),
                          timer.getTimestamp(),
                          expectedTimestamp,
                          timer.getDomain());
                  assertThat(
                      WindmillTagEncodingV1.instance()
                          .windmillTimerToTimerData(
                              prefix,
                              WindmillTagEncodingV1.instance()
                                  .buildWindmillTimerFromTimerData(
                                      stateFamily, prefix, timer, Timer.newBuilder())
                                  .build(),
                              coder,
                              false),
                      equalTo(expected));
                }
              }
            }
          }
        }
      }
    }
  }
}
