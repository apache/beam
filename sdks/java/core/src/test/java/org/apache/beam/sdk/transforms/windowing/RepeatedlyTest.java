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
package org.apache.beam.sdk.transforms.windowing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link Repeatedly}. */
@RunWith(JUnit4.class)
public class RepeatedlyTest {

  @Mock private Trigger mockTrigger;

  public void setUp(WindowFn<Object, IntervalWindow> windowFn) throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  /** Tests that the watermark that guarantees firing is that of the subtrigger. */
  @Test
  public void testFireDeadline() throws Exception {
    setUp(FixedWindows.of(Duration.millis(10)));
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    Instant arbitraryInstant = new Instant(34957849);

    when(mockTrigger.getWatermarkThatGuaranteesFiring(Mockito.<IntervalWindow>any()))
        .thenReturn(arbitraryInstant);

    assertThat(
        Repeatedly.forever(mockTrigger).getWatermarkThatGuaranteesFiring(window),
        equalTo(arbitraryInstant));
  }

  @Test
  public void testContinuation() throws Exception {
    Trigger trigger = AfterProcessingTime.pastFirstElementInPane();
    Trigger repeatedly = Repeatedly.forever(trigger);
    assertEquals(
        Repeatedly.forever(trigger.getContinuationTrigger()), repeatedly.getContinuationTrigger());
    assertEquals(
        Repeatedly.forever(trigger.getContinuationTrigger().getContinuationTrigger()),
        repeatedly.getContinuationTrigger().getContinuationTrigger());
  }

  @Test
  public void testToString() {
    Trigger trigger =
        Repeatedly.forever(
            new StubTrigger() {
              @Override
              public String toString() {
                return "innerTrigger";
              }
            });

    assertEquals("Repeatedly.forever(innerTrigger)", trigger.toString());
  }
}
