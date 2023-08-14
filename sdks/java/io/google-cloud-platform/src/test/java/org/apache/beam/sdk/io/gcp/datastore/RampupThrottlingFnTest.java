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

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.DoFnTester.CloningBehavior;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollectionView;
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
        verify(mockCounter).inc(millis);
        throw new RampupDelayException();
      };
  private DoFnTester<String, String> rampupThrottlingFnTester;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    DateTimeUtils.setCurrentMillisFixed(0);

    TestPipeline pipeline = TestPipeline.create();
    PCollectionView<Instant> startTimeView =
        pipeline.apply(Create.of(Instant.now())).apply(View.asSingleton());
    RampupThrottlingFn<String> rampupThrottlingFn =
        new RampupThrottlingFn<String>(1, startTimeView) {
          @Override
          @Setup
          public void setup() {
            super.setup();
            this.sleeper = mockSleeper;
          }
        };
    rampupThrottlingFnTester = DoFnTester.of(rampupThrottlingFn);
    rampupThrottlingFnTester.setSideInput(startTimeView, GlobalWindow.INSTANCE, Instant.now());
    rampupThrottlingFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
    rampupThrottlingFnTester.startBundle();
    rampupThrottlingFn.throttlingMsecs = mockCounter;
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
        rampupThrottlingFnTester.processElement(UUID.randomUUID().toString());
      }
      assertThrows(RampupDelayException.class, () -> rampupThrottlingFnTester.processElement(null));
    }
  }

  static class RampupDelayException extends InterruptedException {}
}
