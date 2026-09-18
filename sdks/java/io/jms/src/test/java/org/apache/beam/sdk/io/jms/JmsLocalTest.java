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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.NotSerializableException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ExecutorOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Local unit tests for {@link JmsIO} that do not require an active JMS broker. */
@RunWith(JUnit4.class)
public class JmsLocalTest {

  private static final String QUEUE = "queue";
  private static final String TOPIC = "topic";

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
  public void testSplitForQueue() throws Exception {
    JmsIO.Read<JmsRecord> read = JmsIO.read().withQueue(QUEUE);
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    int desiredNumSplits = 5;
    JmsIO.UnboundedJmsSource<JmsRecord> initialSource = new JmsIO.UnboundedJmsSource<>(read);
    List<JmsIO.UnboundedJmsSource<JmsRecord>> splits =
        initialSource.split(desiredNumSplits, pipelineOptions);
    assertEquals(desiredNumSplits, splits.size());
  }

  @Test
  public void testSplitForTopic() throws Exception {
    JmsIO.Read<JmsRecord> read = JmsIO.read().withTopic(TOPIC);
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    int desiredNumSplits = 5;
    JmsIO.UnboundedJmsSource<JmsRecord> initialSource = new JmsIO.UnboundedJmsSource<>(read);
    List<JmsIO.UnboundedJmsSource<JmsRecord>> splits =
        initialSource.split(desiredNumSplits, pipelineOptions);
    assertEquals(1, splits.size());
  }

  @Test
  public void testPublisherWithRetryConfiguration() {
    RetryConfiguration retryPolicy =
        RetryConfiguration.create(5, Duration.standardSeconds(15), null);
    JmsIO.Write<String> publisher =
        JmsIO.<String>write()
            .withConnectionFactory(new ActiveMQConnectionFactory("vm://localhost"))
            .withRetryConfiguration(retryPolicy)
            .withQueue(QUEUE)
            .withUsername("user")
            .withPassword("password");
    assertEquals(
        publisher.getRetryConfiguration(),
        RetryConfiguration.create(5, Duration.standardSeconds(15), null));
  }

  @Test
  public void testJmsCheckpointMarkIndividualAcknowledgeAllMessages() throws Exception {
    Message msg1 = Mockito.mock(Message.class);
    Message msg2 = Mockito.mock(Message.class);
    Message msg3 = Mockito.mock(Message.class);

    JmsCheckpointMark.Preparer preparer =
        JmsCheckpointMark.newPreparer(JmsIO.AcknowledgeMode.INDIVIDUAL_ACKNOWLEDGE);
    preparer.add(msg1);
    preparer.add(msg2);
    preparer.add(msg3);

    AtomicInteger activeCheckpoints = new AtomicInteger(0);
    JmsCheckpointMark mark =
        preparer.newCheckpoint(
            null, null, JmsIO.AcknowledgeMode.INDIVIDUAL_ACKNOWLEDGE, activeCheckpoints);
    assertNotNull(mark.getMessages());
    assertEquals(3, mark.getMessages().size());
    assertNull(mark.getConsumer());
    assertNull(mark.getSession());
    assertEquals(1, activeCheckpoints.get());

    mark.finalizeCheckpoint();

    Mockito.verify(msg1, Mockito.times(1)).acknowledge();
    Mockito.verify(msg2, Mockito.times(1)).acknowledge();
    Mockito.verify(msg3, Mockito.times(1)).acknowledge();
    assertEquals(0, activeCheckpoints.get());
  }

  @Test
  public void testJmsCheckpointMarkClientAcknowledgeUnsafeNoSessionRecreation() throws Exception {
    Message msg1 = Mockito.mock(Message.class);
    Message msg2 = Mockito.mock(Message.class);

    JmsCheckpointMark.Preparer preparer =
        JmsCheckpointMark.newPreparer(JmsIO.AcknowledgeMode.CLIENT_ACKNOWLEDGE_UNSAFE);
    preparer.add(msg1);
    preparer.add(msg2);

    AtomicInteger activeCheckpoints = new AtomicInteger(0);
    JmsCheckpointMark mark =
        preparer.newCheckpoint(
            null, null, JmsIO.AcknowledgeMode.CLIENT_ACKNOWLEDGE_UNSAFE, activeCheckpoints);
    assertNotNull(mark.getMessages());
    assertEquals(1, mark.getMessages().size());
    assertNull(mark.getConsumer());
    assertNull(mark.getSession());
    assertEquals(1, activeCheckpoints.get());

    mark.finalizeCheckpoint();

    Mockito.verify(msg2, Mockito.times(1)).acknowledge();
    Mockito.verify(msg1, Mockito.never()).acknowledge();
    assertEquals(0, activeCheckpoints.get());
  }

  /** Test the checkpoint mark default coder, which is actually AvroCoder. */
  @Test
  public void testCheckpointMarkDefaultCoder() throws Exception {
    JmsCheckpointMark jmsCheckpointMark =
        JmsCheckpointMark.newPreparer(JmsIO.AcknowledgeMode.CLIENT_ACKNOWLEDGE)
            .newCheckpoint(null, null, JmsIO.AcknowledgeMode.CLIENT_ACKNOWLEDGE, null);
    Coder<JmsCheckpointMark> coder =
        new JmsIO.UnboundedJmsSource<JmsRecord>(null).getCheckpointMarkCoder();
    CoderProperties.coderSerializable(coder);
    CoderProperties.coderDecodeEncodeEqual(coder, jmsCheckpointMark);
  }

  @Test
  public void testCloseWithTimeout() throws IOException, JMSException {
    ConnectionFactory connectionFactory = Mockito.mock(ConnectionFactory.class);
    Connection connection = Mockito.mock(Connection.class);
    Session session = Mockito.mock(Session.class);
    MessageConsumer consumer = Mockito.mock(MessageConsumer.class);
    Queue queue = Mockito.mock(Queue.class);

    Mockito.when(connectionFactory.createConnection(Mockito.any(), Mockito.any()))
        .thenReturn(connection);
    Mockito.when(connection.createSession(Mockito.anyBoolean(), Mockito.anyInt()))
        .thenReturn(session);
    Mockito.when(session.createQueue(Mockito.anyString())).thenReturn(queue);
    Mockito.when(session.createConsumer(Mockito.any())).thenReturn(consumer);

    Duration closeTimeout = Duration.millis(2000L);
    JmsIO.Read<JmsRecord> spec =
        JmsIO.read()
            .withConnectionFactory(connectionFactory)
            .withUsername("user")
            .withPassword("password")
            .withQueue(QUEUE)
            .withCloseTimeout(closeTimeout);

    JmsIO.UnboundedJmsSource<JmsRecord> source = new JmsIO.UnboundedJmsSource<>(spec);

    ScheduledExecutorService mockScheduledExecutorService =
        Mockito.mock(ScheduledExecutorService.class);
    ExecutorOptions options = PipelineOptionsFactory.as(ExecutorOptions.class);
    options.setScheduledExecutorService(mockScheduledExecutorService);
    ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
    Mockito.when(
            mockScheduledExecutorService.schedule(
                runnableArgumentCaptor.capture(), Mockito.anyLong(), Mockito.any(TimeUnit.class)))
        .thenReturn(null /* unused */);

    JmsIO.UnboundedJmsReader<JmsRecord> reader = source.createReader(options, null);
    reader.start();
    assertFalse(getDiscardedValue(reader));
    reader.checkpointMarkPreparer.add(Mockito.mock(Message.class));
    org.apache.beam.sdk.io.UnboundedSource.CheckpointMark mark = reader.getCheckpointMark();
    reader.close();
    assertTrue(getDiscardedValue(reader));
    Mockito.verify(mockScheduledExecutorService)
        .schedule(Mockito.any(Runnable.class), Mockito.eq(1L), Mockito.eq(TimeUnit.SECONDS));
    mark.finalizeCheckpoint();
    runnableArgumentCaptor.getValue().run();
    assertTrue(getDiscardedValue(reader));
    Mockito.verifyNoMoreInteractions(mockScheduledExecutorService);
  }

  private boolean getDiscardedValue(JmsIO.UnboundedJmsReader<JmsRecord> reader) {
    JmsCheckpointMark.Preparer preparer = reader.checkpointMarkPreparer;
    preparer.lock.readLock().lock();
    try {
      return preparer.discarded;
    } finally {
      preparer.lock.readLock().unlock();
    }
  }
}
