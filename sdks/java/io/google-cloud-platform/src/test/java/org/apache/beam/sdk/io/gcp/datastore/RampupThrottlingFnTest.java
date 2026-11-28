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
package org.apache.beam.sdk.io.gcp.datastore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValueMultiReceiver;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link RampupThrottlingFn}. */
@RunWith(JUnit4.class)
public class RampupThrottlingFnTest {

  @Mock private Counter mockCounter;
  private final Sleeper mockSleeper =
      millis -> {
        mockCounter.inc(millis);
        throw new RampupDelayException();
      };
  private DoFnRunner<String, String> rampupThrottlingFnRunner;

  /**
   * Runs the fn in-process rather than in a {@link TestPipeline}: the test fixes the Joda clock and
   * injects a non-serializable sleeper and counter, and checks throttling between elements of one
   * bundle.
   */
  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    DateTimeUtils.setCurrentMillisFixed(0);

    TestPipeline pipeline = TestPipeline.create();
    PCollectionView<Instant> startTimeView =
        pipeline.apply(Create.of(Instant.now())).apply(View.asSingleton());
    Instant startTime = Instant.now();
    RampupThrottlingFn<String> rampupThrottlingFn = new RampupThrottlingFn<>(1, startTimeView);
    rampupThrottlingFn.setup();
    rampupThrottlingFn.sleeper = mockSleeper;
    rampupThrottlingFn.throttlingMsecs = mockCounter;

    rampupThrottlingFnRunner =
        DoFnRunners.simpleRunner(
            PipelineOptionsFactory.create(),
            rampupThrottlingFn,
            new SideInputReader() {
              @Override
              @SuppressWarnings("unchecked")
              public <T> T get(PCollectionView<T> view, BoundedWindow window) {
                return (T) startTime;
              }

              @Override
              public <T> boolean contains(PCollectionView<T> view) {
                return view.equals(startTimeView);
              }

              @Override
              public boolean isEmpty() {
                return false;
              }
            },
            new WindowedValueMultiReceiver() {
              @Override
              public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {}
            },
            new TupleTag<>(),
            Collections.emptyList(),
            null,
            null,
            Collections.emptyMap(),
            WindowingStrategy.globalDefault(),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    rampupThrottlingFnRunner.startBundle();
  }

  @After
  public void tearDown() {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void testRampupThrottler() throws Exception {
    Map<Duration, Integer> rampupSchedule =
        ImmutableMap.<Duration, Integer>builder()
            .put(Duration.ZERO, 500)
            .put(Duration.millis(1), 0)
            .put(Duration.standardSeconds(1), 500)
            .put(Duration.standardSeconds(1).plus(Duration.millis(1)), 0)
            .put(Duration.standardMinutes(5), 500)
            .put(Duration.standardMinutes(10), 750)
            .put(Duration.standardMinutes(15), 1125)
            .put(Duration.standardMinutes(30), 3796)
            .put(Duration.standardMinutes(60), 43248)
            .build();

    for (Map.Entry<Duration, Integer> entry : rampupSchedule.entrySet()) {
      DateTimeUtils.setCurrentMillisFixed(entry.getKey().getMillis());
      for (int i = 0; i < entry.getValue(); i++) {
        processElement(UUID.randomUUID().toString());
      }
      UserCodeException e = assertThrows(UserCodeException.class, () -> processElement(null));
      assertThat(e.getCause(), instanceOf(RampupDelayException.class));
    }
  }

  private void processElement(String element) {
    rampupThrottlingFnRunner.processElement(WindowedValues.valueInGlobalWindow(element));
  }

  static class RampupDelayException extends InterruptedException {}
}
