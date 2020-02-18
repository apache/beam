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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Function;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.util.Callback;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of {@link JmsIO}. */
@RunWith(JUnit4.class)
public class JmsIOTest {

  private static final String BROKER_URL = "vm://localhost";

  private static final String USERNAME = "test_user";
  private static final String PASSWORD = "test_password";
  private static final String QUEUE = "test_queue";
  private static final String TOPIC = "test_topic";

  private BrokerService broker;
  private ConnectionFactory connectionFactory;
  private ConnectionFactory connectionFactoryWithSyncAcksAndWithoutPrefetch;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

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
    BrokerPlugin[] plugins = new BrokerPlugin[] {plugin};
    broker.setPlugins(plugins);

    broker.start();

    // create JMS connection factory
    connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
    connectionFactoryWithSyncAcksAndWithoutPrefetch =
        new ActiveMQConnectionFactory(
            BROKER_URL + "?jms.prefetchPolicy.all=0&jms.sendAcksAsync=false");
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
      assertThat(Throwables.getRootCause(e).getMessage(), containsString(innerMessage));
    }
  }

  @Test
  public void testAuthenticationRequired() {
    pipeline.apply(JmsIO.read().withConnectionFactory(connectionFactory).withQueue(QUEUE));

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

    runPipelineExpectingJmsConnectException("User name [" + USERNAME + "] or password is invalid.");
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
    PCollection<JmsRecord> output =
        pipeline.apply(
            JmsIO.read()
                .withConnectionFactory(connectionFactory)
                .withQueue(QUEUE)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .withMaxNumRecords(5));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(5L);
    pipeline.run();

    connection = connectionFactory.createConnection(USERNAME, PASSWORD);
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE));
    Message msg = consumer.receiveNoWait();
    assertNull(msg);
  }

  @Test
  public void testReadBytesMessages() throws Exception {

    // produce message
    Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageProducer producer = session.createProducer(session.createQueue(QUEUE));
    BytesMessage message = session.createBytesMessage();
    message.writeBytes("This Is A Test".getBytes(StandardCharsets.UTF_8));
    producer.send(message);
    producer.close();
    session.close();
    connection.close();

    // read from the queue
    PCollection<String> output =
        pipeline.apply(
            JmsIO.<String>readMessage()
                .withConnectionFactory(connectionFactory)
                .withQueue(QUEUE)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .withMaxNumRecords(1)
                .withCoder(SerializableCoder.of(String.class))
                .withMessageMapper(new BytesMessageToStringMessageMapper()));

    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(1L);
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
    pipeline
        .apply(Create.of(data))
        .apply(
            JmsIO.write()
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
    List<JmsIO.UnboundedJmsSource> splits = initialSource.split(desiredNumSplits, pipelineOptions);
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
    List<JmsIO.UnboundedJmsSource> splits = initialSource.split(desiredNumSplits, pipelineOptions);
    // in the case of a topic, we can have only a unique subscriber on the topic per pipeline
    // else it means we can have duplicate messages (all subscribers on the topic receive every
    // message).
    // So, whatever the desizedNumSplits is, the actual number of splits should be 1.
    assertEquals(1, splits.size());
  }

  @Test
  public void testCheckpointMark() throws Exception {
    // we are using no prefetch here
    // prefetch is an ActiveMQ feature: to make efficient use of network resources the broker
    // utilizes a 'push' model to dispatch messages to consumers. However, in the case of our
    // test, it means that we can have some latency between the receiveNoWait() method used by
    // the consumer and the prefetch buffer populated by the broker. Using a prefetch to 0 means
    // that the consumer will poll for message, which is exactly what we want for the test.
    // We are also sending message acknowledgements synchronously to ensure that they are
    // processed before any subsequent assertions.
    Connection connection =
        connectionFactoryWithSyncAcksAndWithoutPrefetch.createConnection(USERNAME, PASSWORD);
    connection.start();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageProducer producer = session.createProducer(session.createQueue(QUEUE));
    for (int i = 0; i < 10; i++) {
      producer.send(session.createTextMessage("test " + i));
    }
    producer.close();
    session.close();
    connection.close();

    JmsIO.Read spec =
        JmsIO.read()
            .withConnectionFactory(connectionFactoryWithSyncAcksAndWithoutPrefetch)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withQueue(QUEUE);
    JmsIO.UnboundedJmsSource source = new JmsIO.UnboundedJmsSource(spec);
    JmsIO.UnboundedJmsReader reader = source.createReader(null, null);

    // start the reader and move to the first record
    assertTrue(reader.start());

    // consume 3 messages (NB: start already consumed the first message)
    for (int i = 0; i < 3; i++) {
      assertTrue(reader.advance());
    }

    // the messages are still pending in the queue (no ACK yet)
    assertEquals(10, count(QUEUE));

    // we finalize the checkpoint
    reader.getCheckpointMark().finalizeCheckpoint();

    // the checkpoint finalize ack the messages, and so they are not pending in the queue anymore
    assertEquals(6, count(QUEUE));

    // we read the 6 pending messages
    for (int i = 0; i < 6; i++) {
      assertTrue(reader.advance());
    }

    // still 6 pending messages as we didn't finalize the checkpoint
    assertEquals(6, count(QUEUE));

    // we finalize the checkpoint: no more message in the queue
    reader.getCheckpointMark().finalizeCheckpoint();

    assertEquals(0, count(QUEUE));
  }

  @Test
  public void testCheckpointMarkSafety() throws Exception {

    final int messagesToProcess = 100;

    // we are using no prefetch here
    // prefetch is an ActiveMQ feature: to make efficient use of network resources the broker
    // utilizes a 'push' model to dispatch messages to consumers. However, in the case of our
    // test, it means that we can have some latency between the receiveNoWait() method used by
    // the consumer and the prefetch buffer populated by the broker. Using a prefetch to 0 means
    // that the consumer will poll for message, which is exactly what we want for the test.
    // We are also sending message acknowledgements synchronously to ensure that they are
    // processed before any subsequent assertions.
    Connection connection =
        connectionFactoryWithSyncAcksAndWithoutPrefetch.createConnection(USERNAME, PASSWORD);
    connection.start();
    Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

    // Fill the queue with messages
    MessageProducer producer = session.createProducer(session.createQueue(QUEUE));
    for (int i = 0; i < messagesToProcess; i++) {
      producer.send(session.createTextMessage("test " + i));
    }
    producer.close();
    session.close();
    connection.close();

    // create a JmsIO.Read with a decorated ConnectionFactory which will introduce a delay in
    // sending
    // acknowledgements - this should help uncover threading issues around checkpoint management.
    JmsIO.Read spec =
        JmsIO.read()
            .withConnectionFactory(
                withSlowAcks(connectionFactoryWithSyncAcksAndWithoutPrefetch, 10))
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withQueue(QUEUE);
    JmsIO.UnboundedJmsSource source = new JmsIO.UnboundedJmsSource(spec);
    JmsIO.UnboundedJmsReader reader = source.createReader(null, null);

    // start the reader and move to the first record
    assertTrue(reader.start());

    // consume half the messages (NB: start already consumed the first message)
    for (int i = 0; i < (messagesToProcess / 2) - 1; i++) {
      assertTrue(reader.advance());
    }

    // the messages are still pending in the queue (no ACK yet)
    assertEquals(messagesToProcess, count(QUEUE));

    // we finalize the checkpoint for the already-processed messages while simultaneously consuming
    // the remainder of
    // messages from the queue
    Thread runner =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < messagesToProcess / 2; i++) {
                  assertTrue(reader.advance());
                }
              } catch (IOException ex) {
                throw new RuntimeException(ex);
              }
            });
    runner.start();
    reader.getCheckpointMark().finalizeCheckpoint();

    // Concurrency issues would cause an exception to be thrown before this method exits, failing
    // the test
    runner.join();
  }

  /** Test the checkpoint mark default coder, which is actually AvroCoder. */
  @Test
  public void testCheckpointMarkDefaultCoder() throws Exception {
    JmsCheckpointMark jmsCheckpointMark = new JmsCheckpointMark();
    jmsCheckpointMark.add(new ActiveMQMessage());
    Coder coder = new JmsIO.UnboundedJmsSource(null).getCheckpointMarkCoder();
    CoderProperties.coderSerializable(coder);
    CoderProperties.coderDecodeEncodeEqual(coder, jmsCheckpointMark);
  }

  private int count(String queue) throws Exception {
    Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
    connection.start();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    QueueBrowser browser = session.createBrowser(session.createQueue(queue));
    Enumeration<Message> messages = browser.getEnumeration();
    int count = 0;
    while (messages.hasMoreElements()) {
      messages.nextElement();
      count++;
    }
    return count;
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

  /*
   * A utility method which replaces a ConnectionFactory with one where calling receiveNoWait() -- i.e. pulling a
   * message -- will return a message with its acknowledgement callback decorated to include a sleep for a specified
   * duration. This gives the effect of ensuring messages take at least {@code delay} milliseconds to be processed.
   */
  private ConnectionFactory withSlowAcks(ConnectionFactory factory, long delay) {
    return proxyMethod(
        factory,
        ConnectionFactory.class,
        "createConnection",
        (Connection connection) ->
            proxyMethod(
                connection,
                Connection.class,
                "createSession",
                (Session session) ->
                    proxyMethod(
                        session,
                        Session.class,
                        "createConsumer",
                        (MessageConsumer consumer) ->
                            proxyMethod(
                                consumer,
                                MessageConsumer.class,
                                "receiveNoWait",
                                (ActiveMQMessage message) -> {
                                  final Callback originalCallback =
                                      message.getAcknowledgeCallback();
                                  message.setAcknowledgeCallback(
                                      () -> {
                                        Thread.sleep(delay);
                                        originalCallback.execute();
                                      });
                                  return message;
                                }))));
  }

  /*
   * A utility method which decorates an existing object with a proxy instance adhering to a given interface, with the
   * specified method name having its return value transformed by the provided function.
   */
  private <T, MethodArgT, MethodResultT> T proxyMethod(
      T target,
      Class<? super T> proxyInterface,
      String methodName,
      Function<MethodArgT, MethodResultT> resultTransformer) {
    return (T)
        Proxy.newProxyInstance(
            this.getClass().getClassLoader(),
            new Class[] {proxyInterface},
            (proxy, method, args) -> {
              Object result = method.invoke(target, args);
              if (method.getName().equals(methodName)) {
                result = resultTransformer.apply((MethodArgT) result);
              }
              return result;
            });
  }
}
