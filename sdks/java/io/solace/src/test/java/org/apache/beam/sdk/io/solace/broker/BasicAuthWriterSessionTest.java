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

import static org.apache.beam.sdk.io.solace.broker.SessionService.DEFAULT_VPN_NAME;
import static org.junit.Assert.assertEquals;

import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.Queue;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BasicAuthWriterSessionTest {
  private final String username = "Some Username";
  private final String password = "Some Password";
  private final String host = "Some Host";
  private final String vpn = "Some non default VPN";
  SessionService withVpn;
  SessionService withoutVpn;

  @Before
  public void setUp() throws Exception {
    Queue q = JCSMPFactory.onlyInstance().createQueue("test-queue");

    BasicAuthJcsmpSessionServiceFactory factoryWithVpn =
        BasicAuthJcsmpSessionServiceFactory.builder()
            .username(username)
            .password(password)
            .host(host)
            .vpnName(vpn)
            .build();
    factoryWithVpn.setQueue(q);
    withVpn = factoryWithVpn.create();

    BasicAuthJcsmpSessionServiceFactory factoryNoVpn =
        BasicAuthJcsmpSessionServiceFactory.builder()
            .username(username)
            .password(password)
            .host(host)
            .build();
    factoryNoVpn.setQueue(q);
    withoutVpn = factoryNoVpn.create();
  }

  @Test
  public void testAuthProperties() {
    SolaceIO.SubmissionMode mode = SolaceIO.SubmissionMode.HIGHER_THROUGHPUT;
    JCSMPProperties props = withoutVpn.initializeWriteSessionProperties(mode);
    assertEquals(username, props.getStringProperty(JCSMPProperties.USERNAME));
    assertEquals(password, props.getStringProperty(JCSMPProperties.PASSWORD));
    assertEquals(host, props.getStringProperty(JCSMPProperties.HOST));
    assertEquals(
        JCSMPProperties.AUTHENTICATION_SCHEME_BASIC,
        props.getStringProperty(JCSMPProperties.AUTHENTICATION_SCHEME));
  }

  @Test
  public void testVpnNames() {
    SolaceIO.SubmissionMode mode = SolaceIO.SubmissionMode.LOWER_LATENCY;
    JCSMPProperties propsWithoutVpn = withoutVpn.initializeWriteSessionProperties(mode);
    assertEquals(DEFAULT_VPN_NAME, propsWithoutVpn.getStringProperty(JCSMPProperties.VPN_NAME));
    JCSMPProperties propsWithVpn = withVpn.initializeWriteSessionProperties(mode);
    assertEquals(vpn, propsWithVpn.getStringProperty(JCSMPProperties.VPN_NAME));
  }

  @Test
  public void testOverrideWithHigherThroughput() {
    SolaceIO.SubmissionMode mode = SolaceIO.SubmissionMode.HIGHER_THROUGHPUT;
    JCSMPProperties props = withoutVpn.initializeWriteSessionProperties(mode);

    assertEquals(false, props.getBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR));
    assertEquals(
        Long.valueOf(255),
        Long.valueOf(props.getIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE)));
  }

  @Test
  public void testOverrideWithLowerLatency() {
    SolaceIO.SubmissionMode mode = SolaceIO.SubmissionMode.LOWER_LATENCY;
    JCSMPProperties props = withoutVpn.initializeWriteSessionProperties(mode);
    assertEquals(true, props.getBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR));
    assertEquals(
        Long.valueOf(50),
        Long.valueOf(props.getIntegerProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE)));
  }
}
