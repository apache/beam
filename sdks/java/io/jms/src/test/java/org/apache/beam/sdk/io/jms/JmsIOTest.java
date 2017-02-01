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

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests of {@link JmsIO}.
 */
@RunWith(JUnit4.class)
public class JmsIOTest {

  private static final String BROKER_URL = "vm://localhost";

  private static final String USERNAME = "test_user";
  private static final String PASSWORD = "test_password";
  private static final String QUEUE = "test_queue";

  private BrokerService broker;
  private ConnectionFactory connectionFactory;

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void startBroker() throws Exception {
    broker = new BrokerService();
    broker.setUseJmx(false);
    broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
    broker.addConnector(BROKER_URL);
    broker.setBrokerName("localhost");
    broker.setPopulateJMSXUserID(true);
    broker.setUseAuthenticatedPrincipalForJMSXUserID(true);

    // enable authentication
    List<AuthenticationUser> users = new ArrayList<>();
    // username and password to use to connect to the broker.
    // This user has users privilege (able to browse, consume, produce, list destinations)
    users.add(new AuthenticationUser(USERNAME, PASSWORD, "users"));
    SimpleAuthenticationPlugin plugin = new SimpleAuthenticationPlugin(users);
    BrokerPlugin[] plugins = new BrokerPlugin[]{ plugin };
    broker.setPlugins(plugins);

    broker.start();

    // create JMS connection factory
    connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
  }

  @After
  public void stopBroker() throws Exception {
    broker.stop();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testAuthenticationRequired() {
    expectedException.expect(Exception.class);
    expectedException.expectMessage("User name [null] or password is invalid.");

    pipeline.apply(
        JmsIO.read()
            .withConnectionFactory(connectionFactory)
            .withQueue(QUEUE));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testAuthenticationWithBadPassword() {
    expectedException.expect(Exception.class);
    expectedException.expectMessage("User name [" + USERNAME + "] or password is invalid.");

    pipeline.apply(
        JmsIO.read()
            .withConnectionFactory(connectionFactory)
            .withQueue(QUEUE)
            .withUsername(USERNAME)
            .withPassword("BAD"));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadMessages() throws Exception {

    // produce message
    Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageProducer producer = session.createProducer(session.createQueue(QUEUE));
    TextMessage message = session.createTextMessage("This Is A Test");
    producer.send(message);
    producer.send(message);
    producer.send(message);
    producer.send(message);
    producer.send(message);
    producer.send(message);
    producer.close();
    session.close();
    connection.close();

    // read from the queue
    PCollection<JmsRecord> output = pipeline.apply(
        JmsIO.read()
            .withConnectionFactory(connectionFactory)
            .withQueue(QUEUE)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withMaxNumRecords(5));

    PAssert
        .thatSingleton(output.apply("Count", Count.<JmsRecord>globally()))
        .isEqualTo(new Long(5));
    pipeline.run();

    connection = connectionFactory.createConnection(USERNAME, PASSWORD);
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE));
    Message msg = consumer.receiveNoWait();
    Assert.assertNull(msg);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteMessage() throws Exception {

    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      data.add("Message " + i);
    }
    pipeline.apply(Create.of(data))
        .apply(JmsIO.write()
            .withConnectionFactory(connectionFactory)
            .withQueue(QUEUE)
            .withUsername(USERNAME)
            .withPassword(PASSWORD));

    pipeline.run();

    Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
    connection.start();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE));
    int count = 0;
    while (consumer.receive(1000) != null) {
      count++;
    }
    Assert.assertEquals(100, count);
  }

}
