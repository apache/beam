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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
  private static final String TOPIC = "test_topic";

  private BrokerService broker;
  private ConnectionFactory connectionFactory;

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

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

  private void runPipelineExpectingJmsConnectException(String innerMessage) {
    try {
      pipeline.run();
      fail();
    } catch (Exception e) {
      Throwable cause = e.getCause();
      assertThat(cause, instanceOf(IOException.class));
      assertThat(cause.getMessage(), equalTo("Error connecting to JMS"));
      Throwable innerCause = cause.getCause();
      assertThat(innerCause, instanceOf(JMSException.class));
      assertThat(innerCause.getMessage(), containsString(innerMessage));
    }
  }

  @Test
  public void testAuthenticationRequired() {
    pipeline.apply(
        JmsIO.read()
            .withConnectionFactory(connectionFactory)
            .withQueue(QUEUE));

    runPipelineExpectingJmsConnectException("User name [null] or password is invalid.");
  }

  @Test
  public void testAuthenticationWithBadPassword() {
    pipeline.apply(
        JmsIO.read()
            .withConnectionFactory(connectionFactory)
            .withQueue(QUEUE)
            .withUsername(USERNAME)
            .withPassword("BAD"));

    runPipelineExpectingJmsConnectException(
        "User name [" + USERNAME + "] or password is invalid.");
  }

  @Test
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
    assertNull(msg);
  }

  @Test
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
    assertEquals(100, count);
  }

  @Test
  public void testSplitForQueue() throws Exception {
    JmsIO.Read read = JmsIO.read().withQueue(QUEUE);
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    int desiredNumSplits = 5;
    JmsIO.UnboundedJmsSource initialSource = new JmsIO.UnboundedJmsSource(read);
    List<JmsIO.UnboundedJmsSource> splits = initialSource.split(desiredNumSplits,
        pipelineOptions);
    // in the case of a queue, we have concurrent consumers by default, so the initial number
    // splits is equal to the desired number of splits
    assertEquals(desiredNumSplits, splits.size());
  }

  @Test
  public void testSplitForTopic() throws Exception {
    JmsIO.Read read = JmsIO.read().withTopic(TOPIC);
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    int desiredNumSplits = 5;
    JmsIO.UnboundedJmsSource initialSource = new JmsIO.UnboundedJmsSource(read);
    List<JmsIO.UnboundedJmsSource> splits = initialSource.split(desiredNumSplits,
        pipelineOptions);
    // in the case of a topic, we can have only an unique subscriber on the topic per pipeline
    // else it means we can have duplicate messages (all subscribers on the topic receive every
    // message).
    // So, whatever the desizedNumSplits is, the actual number of splits should be 1.
    assertEquals(1, splits.size());
  }

}
