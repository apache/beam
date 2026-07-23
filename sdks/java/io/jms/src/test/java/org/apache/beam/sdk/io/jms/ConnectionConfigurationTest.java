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
package org.apache.beam.sdk.io.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ConnectionConfiguration}. */
@RunWith(JUnit4.class)
public class ConnectionConfigurationTest {

  public static class CustomTestConnectionFactory implements BeamGenericJmsConnectionFactory {
    @Override
    public ConnectionFactory createConnectionFactory(ConnectionConfiguration config) {
      return new DummyConnectionFactory(
          config.getServerUri(), config.getUsername(), config.getPassword());
    }
  }

  public static class DummyConnectionFactory implements ConnectionFactory {
    private final String serverUri;
    private final String username;
    private final String password;

    public DummyConnectionFactory(
        String serverUri, @Nullable String username, @Nullable String password) {
      this.serverUri = serverUri;
      this.username = username;
      this.password = password;
    }

    public String getServerUri() {
      return serverUri;
    }

    public String getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }

    @Override
    public Connection createConnection() throws JMSException {
      return null;
    }

    @Override
    public Connection createConnection(String username, String password) throws JMSException {
      return null;
    }

    @Override
    public javax.jms.JMSContext createContext() {
      return null;
    }

    @Override
    public javax.jms.JMSContext createContext(int sessionMode) {
      return null;
    }

    @Override
    public javax.jms.JMSContext createContext(String username, String password) {
      return null;
    }

    @Override
    public javax.jms.JMSContext createContext(String username, String password, int sessionMode) {
      return null;
    }
  }

  @Test
  public void testDefaultActiveMQConnectionFactory() {
    ConnectionConfiguration config = ConnectionConfiguration.create("vm://localhost");
    ConnectionFactory cf = config.createConnectionFactory();
    assertNotNull(cf);
    assertTrue(cf instanceof ActiveMQConnectionFactory);
  }

  @Test
  public void testQpidConnectionFactory() {
    ConnectionConfiguration config =
        ConnectionConfiguration.create("amqp://localhost")
            .withConnectionFactoryClassName("org.apache.qpid.jms.JmsConnectionFactory");
    ConnectionFactory cf = config.createConnectionFactory();
    assertNotNull(cf);
    assertTrue(cf instanceof JmsConnectionFactory);
  }

  @Test
  public void testCustomBeamGenericJmsConnectionFactory() {
    ConnectionConfiguration config =
        ConnectionConfiguration.create("custom://localhost")
            .withConnectionFactoryClassName(CustomTestConnectionFactory.class.getName())
            .withUsername("testUser")
            .withPassword("testPass");
    ConnectionFactory cf = config.createConnectionFactory();
    assertNotNull(cf);
    assertTrue(cf instanceof DummyConnectionFactory);
    DummyConnectionFactory dummy = (DummyConnectionFactory) cf;
    assertEquals("custom://localhost", dummy.getServerUri());
    assertEquals("testUser", dummy.getUsername());
    assertEquals("testPass", dummy.getPassword());
  }

  @Test
  public void testUnsupportedConnectionFactoryClass() {
    ConnectionConfiguration config =
        ConnectionConfiguration.create("tcp://localhost")
            .withConnectionFactoryClassName("java.lang.String");
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, config::createConnectionFactory);
    assertTrue(exception.getMessage().contains("BeamGenericJmsConnectionFactory"));
  }
}
