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
package org.apache.beam.runners.direct;

import java.io.Serializable;
import org.apache.beam.runners.core.ReadyCheckingSideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.UsesTimersInParDo;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

/** Tests for {@link StatefulParDoEvaluatorFactory}. */
@RunWith(JUnit4.class)
public class ParDoTest implements Serializable {
  @Mock private transient EvaluationContext mockEvaluationContext;
  @Mock private transient DirectExecutionContext mockExecutionContext;
  @Mock private transient DirectExecutionContext.DirectStepContext mockStepContext;
  @Mock private transient ReadyCheckingSideInputReader mockSideInputReader;
  @Mock private transient UncommittedBundle<Integer> mockUncommittedBundle;

  private static final String KEY = "any-key";
  private transient StateInternals stateInternals =
      CopyOnAccessInMemoryStateInternals.<Object>withUnderlying(KEY, null);

  private static final BundleFactory BUNDLE_FACTORY = ImmutableListBundleFactory.create();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category({
    ValidatesRunner.class,
    UsesStatefulParDo.class,
    UsesTimersInParDo.class,
    UsesTestStream.class
  })
  public void testValueStateSimple() {
    final String stateId = "foo";
    final String timerId = "bar";
    DoFn<KV<String, Integer>, Integer> fn =
        new DoFn<KV<String, Integer>, Integer>() {

          @TimerId(timerId)
          private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @ProcessElement
          public void processElement(ProcessContext c, @TimerId(timerId) Timer timer) {
            System.out.println("\n\nSTARTED: " + c.timestamp() + "\n\n");
            // c.output(c.element().getValue());

            timer.withOutputTimestamp(new Instant(5)).set(new Instant(8));
          }

          @OnTimer(timerId)
          public void onTimer(OnTimerContext c, BoundedWindow w) {

            System.out.println("timer");
            c.output(100);
          }
        };

    DoFn<Integer, Instant> fn1 =
        new DoFn<Integer, Instant>() {

          @ProcessElement
          public void processElement(ProcessContext c) {
            System.out.println("fn1: " + c.timestamp() + "\n\n\n");
            c.output(c.timestamp());
          }
        };

    Instant base = new Instant(0);

    TestStream<KV<String, Integer>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
            .advanceWatermarkTo(new Instant(0))
            .addElements(KV.of("key", 1))
            .advanceWatermarkTo(new Instant(100))
            .advanceWatermarkToInfinity();

    PCollection<Integer> output =
        pipeline
            .apply(stream)
            .apply(
                Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(10))) // window
                    .withAllowedLateness(Duration.millis(10)) // lateness
                    .discardingFiredPanes())
            .apply("first", ParDo.of(fn));
    // .apply("second", ParDo.of(fn1));

    PAssert.that(output)
        .inWindow(new IntervalWindow(base, base.plus(Duration.millis(10)))) // interval window
        .containsInAnyOrder(100); // result outpit
    pipeline.run();
  }
}
