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
package org.apache.beam.sdk.io.solace.write.properties;

import static org.junit.Assert.assertEquals;

import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SessionPropertiesProviderTest {

  @Test
  public void testOverrideForHigherThroughput() {
    SubmissionMode mode = SubmissionMode.HIGHER_THROUGHPUT;
    MockSessionPropertiesProvider provider = new MockSessionPropertiesProvider(mode);

    // Test HIGHER_THROUGHPUT mode
    JCSMPProperties props = provider.initializeSessionProperties(mode);
    assertEquals(false, props.getBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR));
    assertEquals(
        Long.valueOf(255),
        Long.valueOf(props.getIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE)));
  }

  @Test
  public void testOverrideForLowerLatency() {
    SubmissionMode mode = SubmissionMode.LOWER_LATENCY;
    MockSessionPropertiesProvider provider = new MockSessionPropertiesProvider(mode);

    // Test HIGHER_THROUGHPUT mode
    JCSMPProperties props = provider.initializeSessionProperties(mode);
    assertEquals(true, props.getBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR));
    assertEquals(
        Long.valueOf(50),
        Long.valueOf(props.getIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE)));
  }
}
