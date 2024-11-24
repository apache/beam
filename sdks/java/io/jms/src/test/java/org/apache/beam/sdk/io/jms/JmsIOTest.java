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

import static org.apache.beam.sdk.io.UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
import static org.apache.beam.sdk.io.jms.CommonJms.PASSWORD;
import static org.apache.beam.sdk.io.jms.CommonJms.QUEUE;
import static org.apache.beam.sdk.io.jms.CommonJms.TOPIC;
import static org.apache.beam.sdk.io.jms.CommonJms.USERNAME;
import static org.apache.beam.sdk.io.jms.CommonJms.toSerializableFunction;
import static org.apache.beam.sdk.io.jms.JmsIO.Writer.JMS_IO_PRODUCER_METRIC_NAME;
import static org.apache.beam.sdk.io.jms.JmsIO.Writer.PUBLICATION_RETRIES_METRIC_NAME;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.object.HasToString.hasToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.util.Callback;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.jms.JmsIO.UnboundedJmsReader;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.ExecutorOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.qpid.jms.JmsAcknowledgeCallback;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.message.JmsTextMessage;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests of {@link JmsIO}. */
@RunWith(Parameterized.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class JmsIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(JmsIOTest.class);

  private interface TestMessageCreator<T extends Message> {
    T createMessage(Session session, long messageIndex) throws JMSException;
  }

  private static BytesMessage createBytesMessage(Session session, long messageIndex)
      throws JMSException {
    BytesMessage message = session.createBytesMessage();
    message.writeBytes(
        String.format("This Is A Test %d", messageIndex).getBytes(StandardCharsets.UTF_8));
    return message;
  }

  private static TextMessage createTextMessage(Session session, long messageIndex)
      throws JMSException {
    return session.createTextMessage(String.format("This Is A Test %d", messageIndex));
  }

  private final RetryConfiguration retryConfiguration =
      RetryConfiguration.create(1, Duration.standardSeconds(1), null);
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

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

  private final CommonJms commonJms;
  private final ConnectionFactory connectionFactory;
  private final Class<? extends ConnectionFactory> connectionFactoryClass;
  private final ConnectionFactory connectionFactoryWithSyncAcksAndWithoutPrefetch;

  public JmsIOTest(
      String brokerUrl,
      Integer brokerPort,
      String forceAsyncAcksParam,
      Class<? extends ConnectionFactory> connectionFactoryClass)
      throws InvocationTargetException, NoSuchMethodException, InstantiationException,
          IllegalAccessException {
    this.commonJms =
        new CommonJms(brokerUrl, brokerPort, forceAsyncAcksParam, connectionFactoryClass);
    this.connectionFactoryClass = connectionFactoryClass;
    this.connectionFactory = commonJms.createConnectionFactory();
    this.connectionFactoryWithSyncAcksAndWithoutPrefetch =
        commonJms.createConnectionFactoryWithSyncAcksAndWithoutPrefetch();
  }

  @Before
  public void beforeEach() throws Exception {
    this.commonJms.startBroker();
  }

  @After
  public void tearDown() throws Exception {
    this.commonJms.stopBroker();
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
    String errorMessage =
        this.connectionFactoryClass == ActiveMQConnectionFactory.class
            ? "User name [null] or password is invalid."
            : "Client failed to authenticate using SASL: ANONYMOUS";
    runPipelineExpectingJmsConnectException(errorMessage);
  }

  @Test
  public void testAuthenticationWithBadPassword() {
    pipeline.apply(
        JmsIO.read()
            .withConnectionFactory(connectionFactory)
            .withQueue(QUEUE)
            .withUsername(USERNAME)
            .withPassword("BAD"));

    String errorMessage =
        this.connectionFactoryClass == ActiveMQConnectionFactory.class
            ? "User name [" + USERNAME + "] or password is invalid."
            : "Client failed to authenticate using SASL: PLAIN";
    runPipelineExpectingJmsConnectException(errorMessage);
  }

  @Test
  public void testReadMessages() throws Exception {
    long count = 5;
    produceTestMessages(count, JmsIOTest::createTextMessage);

    // read from the queue
    PCollection<JmsRecord> output =
        pipeline.apply(
            JmsIO.read()
                .withConnectionFactory(connectionFactory)
                .withQueue(QUEUE)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .withMaxNumRecords(count));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(count);
    pipeline.run();

    assertQueueIsEmpty();
  }

  @Test
  public void testPipelineWithNonSerializableCF() {
    SerializableUtils.ensureSerializable(
        JmsIO.read()
            .withConnectionFactoryProviderFn(__ -> new MockNonSerializableConnectionFactory()));
    try {
      SerializableUtils.ensureSerializable(
          JmsIO.read().withConnectionFactory(new MockNonSerializableConnectionFactory()));
      fail();
    } catch (Exception e) {
      assertThat(Throwables.getRootCause(e), isA(NotSerializableException.class));
    }
  }

  @Test
  public void testReadMessagesWithCFProviderFn() throws Exception {
    long count = 5;
    produceTestMessages(count, JmsIOTest::createTextMessage);

    PCollection<JmsRecord> output =
        pipeline.apply(
            JmsIO.read()
                .withConnectionFactoryProviderFn(
                    toSerializableFunction(commonJms::createConnectionFactory))
                .withQueue(QUEUE)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .withMaxNumRecords(count));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(count);
    pipeline.run();

    assertQueueIsEmpty();
  }

  @Test
  public void testReadBytesMessagesWithCFProviderFn() throws Exception {
    long count = 5;
    produceTestMessages(count, JmsIOTest::createBytesMessage);

    PCollection<String> output =
        pipeline.apply(
            JmsIO.<String>readMessage()
                .withConnectionFactoryProviderFn(
                    toSerializableFunction(commonJms::createConnectionFactory))
                .withQueue(QUEUE)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .withMaxNumRecords(count)
                .withMessageMapper(new CommonJms.BytesMessageToStringMessageMapper())
                .withCoder(StringUtf8Coder.of()));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(count);
    pipeline.run();

    assertQueueIsEmpty();
  }

  private void assertQueueContainsMessages(int expectedCount) throws JMSException {
    // we need to disable the prefetch, otherwise the receiveNoWait could still return null even
    // when there are messages waiting on the queue
    Connection connection =
        connectionFactoryWithSyncAcksAndWithoutPrefetch.createConnection(USERNAME, PASSWORD);
    try {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE));
      connection.start();
      int actualCount = 0;
      while (actualCount < expectedCount && consumer.receive(1000L) != null) {
        actualCount += 1;
      }
      assertEquals("queue didn't have expected number of messages", expectedCount, actualCount);
      assertNull(
          String.format("verifying queue is empty after consuming %d messages - ", expectedCount),
          consumer.receiveNoWait());
    } finally {
      connection.close();
    }
  }

  private void assertQueueIsEmpty() throws JMSException {
    assertQueueContainsMessages(0);
  }

  private void produceTestMessages(long count, TestMessageCreator testMessageCreator)
      throws JMSException {
    Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
    try {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(session.createQueue(QUEUE));
      for (long i = 0; i < count; i += 1) {
        Message message = testMessageCreator.createMessage(session, i);
        producer.send(message);
      }
    } finally {
      connection.close();
    }
  }

  @Test
  public void testReadBytesMessages() throws Exception {
    long count = 1L;
    // produce message
    produceTestMessages(count, JmsIOTest::createBytesMessage);
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
                .withMessageMapper(new CommonJms.BytesMessageToStringMessageMapper()));

    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(count);
    pipeline.run();

    assertQueueIsEmpty();
  }

  @Test
  public void testRequiresDedup() throws Exception {
    int count = 10;
    produceTestMessages(count, JmsIOTest::createTextMessage);

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
    Set<ByteBuffer> uniqueIds = new HashSet<>();
    uniqueIds.add(ByteBuffer.wrap(reader.getCurrentRecordId()));

    for (int i = 0; i < count; i++) {
      if (reader.advance()) {
        assertTrue(uniqueIds.add(ByteBuffer.wrap(reader.getCurrentRecordId())));
      }
    }
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
            JmsIO.<String>write()
                .withConnectionFactory(connectionFactory)
                .withValueMapper(new TextMessageMapper())
                .withRetryConfiguration(retryConfiguration)
                .withQueue(QUEUE)
                .withUsername(USERNAME)
                .withPassword(PASSWORD));

    pipeline.run();

    assertQueueContainsMessages(100);
  }

  @Test
  public void testWriteMessageWithCFProviderFn() throws Exception {

    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      data.add("Message " + i);
    }
    pipeline
        .apply(Create.of(data))
        .apply(
            JmsIO.<String>write()
                .withConnectionFactoryProviderFn(
                    toSerializableFunction(commonJms::createConnectionFactory))
                .withValueMapper(new TextMessageMapper())
                .withRetryConfiguration(retryConfiguration)
                .withQueue(QUEUE)
                .withUsername(USERNAME)
                .withPassword(PASSWORD));

    pipeline.run();

    assertQueueContainsMessages(100);
  }

  @Test
  public void testWriteMessageWithError() throws Exception {
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      data.add("Message " + i);
    }

    WriteJmsResult<String> output =
        pipeline
            .apply(Create.of(data))
            .apply(
                JmsIO.<String>write()
                    .withConnectionFactory(connectionFactory)
                    .withValueMapper(new TextMessageMapperWithError())
                    .withRetryConfiguration(retryConfiguration)
                    .withQueue(QUEUE)
                    .withUsername(USERNAME)
                    .withPassword(PASSWORD));

    PAssert.that(output.getFailedMessages()).containsInAnyOrder("Message 1", "Message 2");

    pipeline.run();

    assertQueueContainsMessages(98);
  }

  @Test
  public void testWriteDynamicMessage() throws Exception {
    Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
    connection.start();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageConsumer consumerOne = session.createConsumer(session.createTopic("Topic_One"));
    MessageConsumer consumerTwo = session.createConsumer(session.createTopic("Topic_Two"));
    ArrayList<TestEvent> data = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      data.add(new TestEvent("Topic_One", "Message One " + i));
    }
    for (int i = 0; i < 100; i++) {
      data.add(new TestEvent("Topic_Two", "Message Two " + i));
    }
    pipeline
        .apply(Create.of(data))
        .apply(
            JmsIO.<TestEvent>write()
                .withConnectionFactory(connectionFactory)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .withRetryConfiguration(retryConfiguration)
                .withTopicNameMapper(e -> e.getTopicName())
                .withValueMapper(
                    (e, s) -> {
                      try {
                        TextMessage msg = s.createTextMessage();
                        msg.setText(e.getValue());
                        return msg;
                      } catch (JMSException ex) {
                        throw new JmsIOException("Error writing TextMessage", ex);
                      }
                    }));

    pipeline.run();

    int count = 0;
    while (consumerOne.receive(1000) != null) {
      count++;
    }
    assertEquals(50, count);

    count = 0;
    while (consumerTwo.receive(1000) != null) {
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
    UnboundedJmsReader reader = setupReaderForTest();

    // start the reader and move to the first record
    assertTrue(reader.start());

    // consume 3 messages (NB: start already consumed the first message)
    for (int i = 0; i < 3; i++) {
      assertTrue(String.format("Failed at %d-th message", i), reader.advance());
    }

    // the messages are still pending in the queue (no ACK yet)
    assertEquals(10, count(QUEUE));

    // we finalize the checkpoint
    reader.getCheckpointMark().finalizeCheckpoint();

    // the checkpoint finalize ack the messages, and so they are not pending in the queue anymore
    assertEquals(6, count(QUEUE));

    // we read the 6 pending messages
    for (int i = 0; i < 6; i++) {
      assertTrue(String.format("Failed at %d-th message", i), reader.advance());
    }

    // still 6 pending messages as we didn't finalize the checkpoint
    assertEquals(6, count(QUEUE));

    // we finalize the checkpoint: no more message in the queue
    reader.getCheckpointMark().finalizeCheckpoint();

    assertEquals(0, count(QUEUE));
  }

  @Test
  public void testCheckpointMarkAndFinalizeSeparately() throws Exception {
    UnboundedJmsReader reader = setupReaderForTest();

    // start the reader and move to the first record
    assertTrue(reader.start());

    // consume 2 message (NB: start already consumed the first message)
    assertTrue(reader.advance());
    assertTrue(reader.advance());

    // get checkpoint mark after consumed 4 messages
    CheckpointMark mark = reader.getCheckpointMark();

    // consume two more messages after checkpoint made
    reader.advance();
    reader.advance();

    // the messages are still pending in the queue (no ACK yet)
    assertEquals(10, count(QUEUE));

    // we finalize the checkpoint
    mark.finalizeCheckpoint();

    // the checkpoint finalize ack the messages, and so they are not pending in the queue anymore
    assertEquals(7, count(QUEUE));
  }

  private JmsIO.UnboundedJmsReader setupReaderForTest() throws JMSException {
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
    JmsIO.UnboundedJmsReader reader = source.createReader(PipelineOptionsFactory.create(), null);
    return reader;
  }

  private Function<?, ?> getJmsMessageAck(Class connectorClass) {
    final int delay = 10;
    return connectorClass == JmsConnectionFactory.class
        ? (JmsTextMessage message) -> {
          final JmsAcknowledgeCallback originalCallback = message.getAcknowledgeCallback();
          JmsAcknowledgeCallback jmsAcknowledgeCallbackMock =
              Mockito.mock(JmsAcknowledgeCallback.class);
          try {
            Mockito.doAnswer(
                    invocation -> {
                      Thread.sleep(delay);
                      originalCallback.acknowledge();
                      return null;
                    })
                .when(jmsAcknowledgeCallbackMock)
                .acknowledge();
          } catch (JMSException exception) {
            LOG.error("An exception occurred while adding 10s delay", exception);
          }
          message.setAcknowledgeCallback(jmsAcknowledgeCallbackMock);
          return message;
        }
        : (ActiveMQMessage message) -> {
          final Callback originalCallback = message.getAcknowledgeCallback();
          message.setAcknowledgeCallback(
              () -> {
                Thread.sleep(delay);
                originalCallback.execute();
              });
          return message;
        };
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

    Function jmsMessageAck = getJmsMessageAck(this.connectionFactoryClass);

    // create a JmsIO.Read with a decorated ConnectionFactory which will introduce a delay in
    // sending
    // acknowledgements - this should help uncover threading issues around checkpoint management.
    JmsIO.Read spec =
        JmsIO.read()
            .withConnectionFactory(
                withSlowAcks(connectionFactoryWithSyncAcksAndWithoutPrefetch, jmsMessageAck))
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withQueue(QUEUE);
    JmsIO.UnboundedJmsSource source = new JmsIO.UnboundedJmsSource(spec);
    JmsIO.UnboundedJmsReader reader = source.createReader(PipelineOptionsFactory.create(), null);

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
    JmsCheckpointMark jmsCheckpointMark = JmsCheckpointMark.newPreparer().newCheckpoint(null, null);
    Coder coder = new JmsIO.UnboundedJmsSource(null).getCheckpointMarkCoder();
    CoderProperties.coderSerializable(coder);
    CoderProperties.coderDecodeEncodeEqual(coder, jmsCheckpointMark);
  }

  @Test
  public void testDefaultAutoscaler() throws IOException {
    JmsIO.Read spec =
        JmsIO.read()
            .withConnectionFactory(connectionFactory)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withQueue(QUEUE);
    JmsIO.UnboundedJmsSource source = new JmsIO.UnboundedJmsSource(spec);
    JmsIO.UnboundedJmsReader reader = source.createReader(PipelineOptionsFactory.create(), null);

    // start the reader and check getSplitBacklogBytes and getTotalBacklogBytes values
    reader.start();
    assertEquals(BACKLOG_UNKNOWN, reader.getSplitBacklogBytes());
    assertEquals(BACKLOG_UNKNOWN, reader.getTotalBacklogBytes());
    reader.close();
  }

  @Test
  public void testCustomAutoscaler() throws IOException {
    long excpectedTotalBacklogBytes = 1111L;

    AutoScaler autoScaler = mock(DefaultAutoscaler.class);
    when(autoScaler.getTotalBacklogBytes()).thenReturn(excpectedTotalBacklogBytes);
    JmsIO.Read spec =
        JmsIO.read()
            .withConnectionFactory(connectionFactory)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withQueue(QUEUE)
            .withAutoScaler(autoScaler);

    JmsIO.UnboundedJmsSource source = new JmsIO.UnboundedJmsSource(spec);
    JmsIO.UnboundedJmsReader reader = source.createReader(PipelineOptionsFactory.create(), null);

    // start the reader and check getSplitBacklogBytes and getTotalBacklogBytes values
    reader.start();
    verify(autoScaler, times(1)).start();
    assertEquals(excpectedTotalBacklogBytes, reader.getTotalBacklogBytes());
    verify(autoScaler, times(1)).getTotalBacklogBytes();
    reader.close();
    verify(autoScaler, times(1)).stop();
  }

  @Test
  public void testCloseWithTimeout() throws IOException {
    Duration closeTimeout = Duration.millis(2000L);
    JmsIO.Read spec =
        JmsIO.read()
            .withConnectionFactory(connectionFactory)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withQueue(QUEUE)
            .withCloseTimeout(closeTimeout);

    JmsIO.UnboundedJmsSource source = new JmsIO.UnboundedJmsSource(spec);

    ScheduledExecutorService mockScheduledExecutorService =
        Mockito.mock(ScheduledExecutorService.class);
    ExecutorOptions options = PipelineOptionsFactory.as(ExecutorOptions.class);
    options.setScheduledExecutorService(mockScheduledExecutorService);
    ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
    when(mockScheduledExecutorService.schedule(
            runnableArgumentCaptor.capture(), anyLong(), any(TimeUnit.class)))
        .thenReturn(null /* unused */);

    JmsIO.UnboundedJmsReader reader = source.createReader(options, null);
    reader.start();
    assertFalse(getDiscardedValue(reader));
    reader.close();
    assertFalse(getDiscardedValue(reader));
    verify(mockScheduledExecutorService)
        .schedule(any(Runnable.class), eq(closeTimeout.getMillis()), eq(TimeUnit.MILLISECONDS));
    runnableArgumentCaptor.getValue().run();
    assertTrue(getDiscardedValue(reader));
    verifyNoMoreInteractions(mockScheduledExecutorService);
  }

  private boolean getDiscardedValue(JmsIO.UnboundedJmsReader reader) {
    JmsCheckpointMark.Preparer preparer = reader.checkpointMarkPreparer;
    preparer.lock.readLock().lock();
    try {
      return preparer.discarded;
    } finally {
      preparer.lock.readLock().unlock();
    }
  }

  @Test
  public void testDiscardCheckpointMark() throws Exception {
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
    JmsIO.UnboundedJmsReader reader = source.createReader(PipelineOptionsFactory.create(), null);

    // start the reader and move to the first record
    assertTrue(reader.start());

    // consume 3 more messages (NB: start already consumed the first message)
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

    // But here we discard the pending checkpoint
    reader.checkpointMarkPreparer.discard();
    // we finalize the checkpoint: no messages should be acked
    reader.getCheckpointMark().finalizeCheckpoint();

    assertEquals(6, count(QUEUE));
  }

  @Test
  public void testPublisherWithRetryConfiguration() {
    RetryConfiguration retryPolicy =
        RetryConfiguration.create(5, Duration.standardSeconds(15), null);
    JmsIO.Write<String> publisher =
        JmsIO.<String>write()
            .withConnectionFactory(connectionFactory)
            .withRetryConfiguration(retryPolicy)
            .withQueue(QUEUE)
            .withUsername(USERNAME)
            .withPassword(PASSWORD);
    assertEquals(
        publisher.getRetryConfiguration(),
        RetryConfiguration.create(5, Duration.standardSeconds(15), null));
  }

  @Test
  public void testWriteMessageWithRetryPolicy() throws Exception {
    int waitingSeconds = 5;
    // Margin of the pipeline execution in seconds that should be taken into consideration
    int pipelineDuration = 5;
    Instant now = Instant.now();
    String messageText = now.toString();
    List<String> data = Collections.singletonList(messageText);
    RetryConfiguration retryPolicy =
        RetryConfiguration.create(
            3, Duration.standardSeconds(waitingSeconds), Duration.standardDays(10));

    WriteJmsResult<String> output =
        pipeline
            .apply(Create.of(data))
            .apply(
                JmsIO.<String>write()
                    .withConnectionFactory(connectionFactory)
                    .withValueMapper(new TextMessageMapperWithErrorCounter())
                    .withRetryConfiguration(retryPolicy)
                    .withQueue(QUEUE)
                    .withUsername(USERNAME)
                    .withPassword(PASSWORD));

    PAssert.that(output.getFailedMessages()).empty();
    pipeline.run();

    Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
    connection.start();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE));

    Message message = consumer.receive(1000);
    assertNotNull(message);
    long maximumTimestamp =
        now.plus(java.time.Duration.ofSeconds(waitingSeconds + pipelineDuration)).toEpochMilli();
    assertThat(
        message.getJMSTimestamp(),
        allOf(greaterThanOrEqualTo(now.toEpochMilli()), lessThan(maximumTimestamp)));
    assertNull(consumer.receiveNoWait());
  }

  @Test
  public void testWriteMessageWithRetryPolicyReachesLimit() throws Exception {
    String messageText = "text";
    int maxPublicationAttempts = 2;
    List<String> data = Collections.singletonList(messageText);
    RetryConfiguration retryConfiguration =
        RetryConfiguration.create(maxPublicationAttempts, null, null);

    WriteJmsResult<String> output =
        pipeline
            .apply(Create.of(data))
            .apply(
                JmsIO.<String>write()
                    .withConnectionFactory(connectionFactory)
                    .withValueMapper(
                        (SerializableBiFunction<String, Session, Message>)
                            (s, session) -> {
                              throw new JmsIOException("Error!!");
                            })
                    .withRetryConfiguration(retryConfiguration)
                    .withQueue(QUEUE)
                    .withUsername(USERNAME)
                    .withPassword(PASSWORD));

    PAssert.that(output.getFailedMessages()).containsInAnyOrder(messageText);
    PipelineResult pipelineResult = pipeline.run();

    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            JMS_IO_PRODUCER_METRIC_NAME, PUBLICATION_RETRIES_METRIC_NAME))
                    .build());

    assertThat(
        metrics.getCounters(),
        contains(
            allOf(
                hasProperty("attempted", is((long) maxPublicationAttempts)),
                hasProperty(
                    "key",
                    hasToString(
                        containsString(
                            String.format(
                                "%s:%s",
                                JMS_IO_PRODUCER_METRIC_NAME, PUBLICATION_RETRIES_METRIC_NAME)))))));

    assertQueueIsEmpty();
  }

  @Test
  public void testWriteMessagesWithErrors() throws Exception {
    int maxPublicationAttempts = 2;
    // Message 1 should fail for Published DoFn handled by the republished DoFn and published to the
    // queue
    // Message 2 should fail both DoFn
    // Message 3 & 4 should pass the publish DoFn
    List<String> data = Arrays.asList("Message 1", "Message 2", "Message 3", "Message 4");

    RetryConfiguration retryConfiguration =
        RetryConfiguration.create(maxPublicationAttempts, null, null);

    WriteJmsResult<String> output =
        pipeline
            .apply(Create.of(data))
            .apply(
                JmsIO.<String>write()
                    .withConnectionFactory(connectionFactory)
                    .withValueMapper(new TextMessageMapperWithErrorAndCounter())
                    .withRetryConfiguration(retryConfiguration)
                    .withQueue(QUEUE)
                    .withUsername(USERNAME)
                    .withPassword(PASSWORD));

    PAssert.that(output.getFailedMessages()).containsInAnyOrder("Message 2");
    pipeline.run();

    Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
    connection.start();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE));
    int count = 0;
    while (consumer.receive(1000) != null) {
      count++;
    }
    assertEquals(3, count);
  }

  @Test
  public void testWriteMessageToStaticTopicWithoutRetryPolicy() throws Exception {
    Instant now = Instant.now();
    String messageText = now.toString();
    List<String> data = Collections.singletonList(messageText);

    Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
    connection.start();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageConsumer consumer = session.createConsumer(session.createTopic(TOPIC));

    WriteJmsResult<String> output =
        pipeline
            .apply(Create.of(data))
            .apply(
                JmsIO.<String>write()
                    .withConnectionFactory(connectionFactory)
                    .withValueMapper(new TextMessageMapper())
                    .withTopic(TOPIC)
                    .withUsername(USERNAME)
                    .withPassword(PASSWORD));
    PAssert.that(output.getFailedMessages()).empty();
    pipeline.run();
    Message message = consumer.receive(1000);
    assertNotNull(message);
    assertNull(consumer.receiveNoWait());
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

  /*
   * A utility method which replaces a ConnectionFactory with one where calling receiveNoWait() -- i.e. pulling a
   * message -- will return a message with its acknowledgement callback decorated to include a sleep for a specified
   * duration. This gives the effect of ensuring messages take at least {@code delay} milliseconds to be processed.
   */
  private <T extends Message> ConnectionFactory withSlowAcks(
      ConnectionFactory factory, Function<T, T> resultTransformer) {
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
                                resultTransformer))));
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

  private static class TestEvent implements Serializable {
    private final String topicName;
    private final String value;

    private TestEvent(String topicName, String value) {
      this.topicName = topicName;
      this.value = value;
    }

    private String getTopicName() {
      return this.topicName;
    }

    private String getValue() {
      return this.value;
    }
  }

  private static class TextMessageMapperWithError
      implements SerializableBiFunction<String, Session, Message> {
    @Override
    public Message apply(String value, Session session) {
      try {
        if (value.equals("Message 1") || value.equals("Message 2")) {
          throw new JMSException("Error!!");
        }
        TextMessage msg = session.createTextMessage();
        msg.setText(value);
        return msg;
      } catch (JMSException e) {
        throw new JmsIOException("Error creating TextMessage", e);
      }
    }
  }

  private static class TextMessageMapperWithErrorCounter
      implements SerializableBiFunction<String, Session, Message> {

    private static int errorCounter;

    TextMessageMapperWithErrorCounter() {
      errorCounter = 0;
    }

    @Override
    public Message apply(String value, Session session) {
      try {
        if (errorCounter == 0) {
          errorCounter++;
          throw new JMSException("Error!!");
        }
        TextMessage msg = session.createTextMessage();
        msg.setText(value);
        return msg;
      } catch (JMSException e) {
        throw new JmsIOException("Error creating TextMessage", e);
      }
    }
  }

  private static class TextMessageMapperWithErrorAndCounter
      implements SerializableBiFunction<String, Session, Message> {
    private static int errorCounter = 0;

    @Override
    public Message apply(String value, Session session) {
      try {
        if (value.equals("Message 1") || value.equals("Message 2")) {
          if (errorCounter != 0 && value.equals("Message 1")) {
            TextMessage msg = session.createTextMessage();
            msg.setText(value);
            return msg;
          }
          errorCounter++;
          throw new JMSException("Error!!");
        }
        TextMessage msg = session.createTextMessage();
        msg.setText(value);
        return msg;
      } catch (JMSException e) {
        throw new JmsIOException("Error creating TextMessage", e);
      }
    }
  }
}
