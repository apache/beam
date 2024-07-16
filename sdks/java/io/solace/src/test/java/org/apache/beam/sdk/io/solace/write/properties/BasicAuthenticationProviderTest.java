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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BasicAuthenticationProviderTest {
  private final String username = "Some Username";
  private final String password = "Some Password";
  private final String host = "Some Host";
  private final String vpn = "Some non default VPN";
  private BasicAuthenticationProvider providerNoVpn;
  private BasicAuthenticationProvider providerWithVpn;

  @Before
  public void setUp() throws Exception {
    providerNoVpn =
        BasicAuthenticationProvider.builder()
            .username(username)
            .password(password)
            .host(host)
            .build();
    providerWithVpn =
        BasicAuthenticationProvider.builder()
            .username(username)
            .password(password)
            .host(host)
            .vpnName(vpn)
            .build();
  }

  @Test
  public void testAuthProperties() {
    SubmissionMode mode = SubmissionMode.HIGHER_THROUGHPUT;
    JCSMPProperties props = providerNoVpn.initializeSessionProperties(mode);
    assertEquals(username, props.getStringProperty(JCSMPProperties.USERNAME));
    assertEquals(password, props.getStringProperty(JCSMPProperties.PASSWORD));
    assertEquals(host, props.getStringProperty(JCSMPProperties.HOST));
    assertEquals(
        JCSMPProperties.AUTHENTICATION_SCHEME_BASIC,
        props.getStringProperty(JCSMPProperties.AUTHENTICATION_SCHEME));
  }

  @Test
  public void testVpnNames() {
    SubmissionMode mode = SubmissionMode.LOWER_LATENCY;
    JCSMPProperties propsNoVpn = providerNoVpn.initializeSessionProperties(mode);
    assertEquals("default", propsNoVpn.getStringProperty(JCSMPProperties.VPN_NAME));
    JCSMPProperties propsWithVpn = providerWithVpn.initializeSessionProperties(mode);
    assertEquals(vpn, propsWithVpn.getStringProperty(JCSMPProperties.VPN_NAME));
  }

  @Test
  public void testOverrideWithHigherThroughput() {
    SubmissionMode mode = SubmissionMode.HIGHER_THROUGHPUT;
    JCSMPProperties props = providerNoVpn.initializeSessionProperties(mode);

    assertEquals(false, props.getBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR));
    assertEquals(
        Long.valueOf(255),
        Long.valueOf(props.getIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE)));
  }

  @Test
  public void testOverrideWithLowerLatency() {
    SubmissionMode mode = SubmissionMode.LOWER_LATENCY;
    JCSMPProperties props = providerNoVpn.initializeSessionProperties(mode);
    assertEquals(true, props.getBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR));
    assertEquals(
        Long.valueOf(50),
        Long.valueOf(props.getIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE)));
  }
}
