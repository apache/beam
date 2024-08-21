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
package org.apache.beam.runners.prism;

import static org.junit.Assume.assumeTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrismRunner}. */
@RunWith(JUnit4.class)
public class PrismRunnerTest {

  // See build.gradle for test task configuration.
  private static final String PRISM_BUILD_TARGET_PROPERTY_NAME = "prism.buildTarget";

  @Test
  public void create() {
    Pipeline pipeline = Pipeline.create(options());
    PAssert.that(pipeline.apply(Create.of(1, 2, 3))).containsInAnyOrder(1, 2, 3);
    pipeline.run();
  }

  @Ignore
  @Test
  public void windowing() {
    Pipeline pipeline = Pipeline.create(options());
    PCollection<KV<String, Iterable<Integer>>> got =
        pipeline
            .apply(Create.of(1, 2, 100, 101, 102, 123))
            .apply(WithTimestamps.of(t -> Instant.ofEpochSecond(t)))
            .apply(WithKeys.of("k"))
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
            .apply(GroupByKey.create());

    List<KV<String, Iterable<Integer>>> want =
        Arrays.asList(
            KV.of("k", Arrays.asList(1, 2)),
            KV.of("k", Arrays.asList(100, 101, 102)),
            KV.of("k", Collections.singletonList(123)));

    PAssert.that(got).containsInAnyOrder(want);

    pipeline.run();
  }

  @Ignore("Unable to find inbound timer receiver for instruction")
  @Test
  public void parDoTimersClear() {
    Pipeline pipeline = Pipeline.create(options());
    PAssert.that(
            pipeline
                .apply(Create.of(KV.of("k1", 10), KV.of("k2", 100)))
                .apply(ParDo.of(new TimersDoFn())))
        .satisfies(
            itr -> {
              System.out.println(
                  StreamSupport.stream(itr.spliterator(), false).collect(Collectors.toList()));
              return null;
            });

    pipeline.run();
  }

  private static class TimersDoFn extends DoFn<KV<String, Integer>, Integer> {
    private static final String TIMER_ID = "timer";

    @SuppressWarnings("unused")
    @TimerId(TIMER_ID)
    private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @DoFn.ProcessElement
    public void process(@Element KV<String, Integer> element, @TimerId(TIMER_ID) Timer timer) {
      timer.set(Instant.ofEpochMilli(element.getValue()));
      timer.set(Instant.ofEpochMilli(2L * element.getValue()));
    }

    @OnTimer(TIMER_ID)
    public void onTimer(
        @Timestamp Instant timestamp,
        @TimerId(TIMER_ID) Timer timer,
        OutputReceiver<Integer> receiver) {
      timer.offset(Duration.millis(2L * timestamp.getMillis()));
      timer.clear();
      receiver.output(Long.valueOf(timestamp.getMillis() / 1000L).intValue());
    }
  }

  private static PrismPipelineOptions options() {
    PrismPipelineOptions opts = PipelineOptionsFactory.create().as(PrismPipelineOptions.class);

    opts.setRunner(PrismRunner.class);
    opts.setPrismLocation(getLocalPrismBuildOrIgnoreTest());

    return opts;
  }

  /**
   * Drives ignoring of tests via checking {@link org.junit.Assume#assumeTrue} that the {@link
   * System#getProperty} for {@link #PRISM_BUILD_TARGET_PROPERTY_NAME} is not null or empty.
   */
  static String getLocalPrismBuildOrIgnoreTest() {
    String command = System.getProperty(PRISM_BUILD_TARGET_PROPERTY_NAME);
    assumeTrue(
        "System property: "
            + PRISM_BUILD_TARGET_PROPERTY_NAME
            + " is not set; see build.gradle for test task configuration",
        !Strings.isNullOrEmpty(command));
    return command;
  }
}
