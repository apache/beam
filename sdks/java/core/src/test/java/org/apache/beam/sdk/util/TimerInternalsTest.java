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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.TimerInternals.TimerData;
import org.apache.beam.sdk.util.TimerInternals.TimerDataCoder;
import org.apache.beam.sdk.util.state.StateNamespaces;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link TimerInternals}.
 */
@RunWith(JUnit4.class)
public class TimerInternalsTest {

  @Test
  public void testTimerDataCoder() throws Exception {
    CoderProperties.coderDecodeEncodeEqual(
        TimerDataCoder.of(GlobalWindow.Coder.INSTANCE),
        TimerData.of(StateNamespaces.global(), new Instant(0), TimeDomain.EVENT_TIME));

    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();
    CoderProperties.coderDecodeEncodeEqual(
        TimerDataCoder.of(windowCoder),
        TimerData.of(
            StateNamespaces.window(
                windowCoder, new IntervalWindow(new Instant(0), new Instant(100))),
            new Instant(99), TimeDomain.PROCESSING_TIME));
  }
}
