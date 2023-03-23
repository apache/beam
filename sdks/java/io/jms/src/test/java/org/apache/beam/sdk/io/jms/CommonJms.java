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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.amqp.AmqpTransportFactory;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.runners.Parameterized;

public class CommonJms implements Serializable {
  private static final String BROKER_WITHOUT_PREFETCH_PARAM = "?jms.prefetchPolicy.all=0&";

  protected static final String USERNAME = "test_user";
  protected static final String PASSWORD = "test_password";
  protected static final String QUEUE = "test_queue";
  protected static final String TOPIC = "test_topic";

  private final String brokerUrl;
  private final Integer brokerPort;
  private final String forceAsyncAcksParam;
  private transient BrokerService broker;

  protected ConnectionFactory connectionFactory;
  protected final Class<? extends ConnectionFactory> connectionFactoryClass;
  protected ConnectionFactory connectionFactoryWithSyncAcksAndWithoutPrefetch;

  @Parameterized.Parameters(name = "with client class {3}")
  public static Collection<Object[]> connectionFactories() {
    return Arrays.asList(
        new Object[] {
          "vm://localhost", 5672, "jms.sendAcksAsync=false", ActiveMQConnectionFactory.class
        },
        new Object[] {
          "amqp://localhost", 5672, "jms.forceAsyncAcks=false", JmsConnectionFactory.class
        });
  }

  public CommonJms(
      String brokerUrl,
      Integer brokerPort,
      String forceAsyncAcksParam,
      Class<? extends ConnectionFactory> connectionFactoryClass) {
    this.brokerUrl = brokerUrl;
    this.brokerPort = brokerPort;
    this.forceAsyncAcksParam = forceAsyncAcksParam;
    this.connectionFactoryClass = connectionFactoryClass;
  }

  protected void startBroker() throws Exception {
    broker = new BrokerService();
    broker.setUseJmx(false);
    broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
    TransportFactory.registerTransportFactory("amqp", new AmqpTransportFactory());
    if (connectionFactoryClass != ActiveMQConnectionFactory.class) {
      broker.addConnector(String.format("%s:%d?transport.transformer=jms", brokerUrl, brokerPort));
    } else {
      broker.addConnector(brokerUrl);
    }
    broker.setBrokerName("localhost");
    broker.setPopulateJMSXUserID(true);
    broker.setUseAuthenticatedPrincipalForJMSXUserID(true);
    broker.getManagementContext().setCreateConnector(false);

    // enable authentication
    List<AuthenticationUser> users = new ArrayList<>();
    // username and password to use to connect to the broker.
    // This user has users privilege (able to browse, consume, produce, list destinations)
    users.add(new AuthenticationUser(USERNAME, PASSWORD, "users"));
    SimpleAuthenticationPlugin plugin = new SimpleAuthenticationPlugin(users);
    BrokerPlugin[] plugins = new BrokerPlugin[] {plugin};
    broker.setPlugins(plugins);

    broker.start();
    broker.waitUntilStarted();

    // create JMS connection factory
    connectionFactory = connectionFactoryClass.getConstructor(String.class).newInstance(brokerUrl);
    connectionFactoryWithSyncAcksAndWithoutPrefetch =
        connectionFactoryClass
            .getConstructor(String.class)
            .newInstance(brokerUrl + BROKER_WITHOUT_PREFETCH_PARAM + forceAsyncAcksParam);
  }

  protected void stopBroker() throws Exception {
    broker.stop();
    broker.waitUntilStopped();
    broker = null;
  }

  /** A test class that maps a {@link javax.jms.BytesMessage} into a {@link String}. */
  public static class BytesMessageToStringMessageMapper implements JmsIO.MessageMapper<String> {

    @Override
    public String mapMessage(Message message) throws Exception {
      BytesMessage bytesMessage = (BytesMessage) message;

      byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];

      return new String(bytes, StandardCharsets.UTF_8);
    }
  }
}
