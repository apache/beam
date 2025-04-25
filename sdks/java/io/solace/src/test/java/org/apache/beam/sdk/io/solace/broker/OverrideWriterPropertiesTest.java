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
package org.apache.beam.sdk.io.solace.broker;

import static org.junit.Assert.assertEquals;

import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.beam.sdk.io.solace.MockSessionService;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OverrideWriterPropertiesTest {
  @Test
  public void testOverrideForHigherThroughput() {
    SolaceIO.SubmissionMode mode = SolaceIO.SubmissionMode.HIGHER_THROUGHPUT;
    MockSessionService service = MockSessionService.builder().mode(mode).build();

    JCSMPProperties props = service.initializeWriteSessionProperties(mode);
    assertEquals(false, props.getBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR));
    assertEquals(
        Long.valueOf(255),
        Long.valueOf(props.getIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE)));
  }

  @Test
  public void testOverrideForLowerLatency() {
    SolaceIO.SubmissionMode mode = SolaceIO.SubmissionMode.LOWER_LATENCY;
    MockSessionService service = MockSessionService.builder().mode(mode).build();

    JCSMPProperties props = service.initializeWriteSessionProperties(mode);
    assertEquals(true, props.getBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR));
    assertEquals(
        Long.valueOf(50),
        Long.valueOf(props.getIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE)));
  }

  @Test
  public void testDontOverrideForCustom() {
    SolaceIO.SubmissionMode mode = SolaceIO.SubmissionMode.CUSTOM;
    MockSessionService service = MockSessionService.builder().mode(mode).build();

    JCSMPProperties props = service.initializeWriteSessionProperties(mode);
    assertEquals(
        MockSessionService.callbackOnReactor,
        props.getBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR));
    assertEquals(
        Long.valueOf(MockSessionService.ackWindowSizeForTesting),
        Long.valueOf(props.getIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE)));
  }
}
