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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;

import com.google.common.annotations.VisibleForTesting;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * An unbounded source for JMS destinations (queues or topics).
 *
 * <h3>Reading from a JMS destination</h3>
 *
 * <p>JmsIO source returns unbounded collection of JMS records as {@code PCollection<JmsRecord<T>>}.
 * A {@link JmsRecord} includes JMS headers and properties, along with the JMS message payload.</p>
 *
 * <p>To configure a JMS source, you have to provide a {@link javax.jms.ConnectionFactory}
 * and the destination (queue or topic) where to consume. The following example
 * illustrates various options for configuring the source:</p>
 *
 * <pre>{@code
 *
 * pipeline.apply(JmsIO.read()
 *    .withConnectionFactory(myConnectionFactory)
 *    .withQueue("my-queue")
 *    // above two are required configuration, returns PCollection<JmsRecord<byte[]>>
 *
 *    // rest of the settings are optional
 *
 * }</pre>
 *
 * <h3>Writing to a JMS destination</h3>
 *
 * JmsIO sink supports writing text messages to a JMS destination on a broker.
 * To configure a JMS sink, you must specify a {@link javax.jms.ConnectionFactory} and a
 * {@link javax.jms.Destination} name.
 * For instance:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...) // returns PCollection<String>
 *    .apply(JmsIO.write()
 *        .withConnectionFactory(myConnectionFactory)
 *        .withQueue("my-queue")
 *
 * }</pre>
 */
public class JmsIO {

  private static final Logger LOG = LoggerFactory.getLogger(JmsIO.class);

  public static Read read() {
    return new Read();
  }

  public static Write write() {
    return new Write();
  }

  /**
   * A {@link PTransform} to read from a JMS destination. See {@link JmsIO} for more
   * information on usage and configuration.
   */
  public static class Read extends PTransform<PBegin, PCollection<JmsRecord>> {

    public Read withConnectionFactory(ConnectionFactory connectionFactory) {
      return new Read(connectionFactory, queue, topic, maxNumRecords, maxReadTime);
    }

    public Read withQueue(String queue) {
      return new Read(connectionFactory, queue, topic, maxNumRecords, maxReadTime);
    }

    public Read withTopic(String topic) {
      return new Read(connectionFactory, queue, topic, maxNumRecords, maxReadTime);
    }

    public Read withMaxNumRecords(long maxNumRecords) {
      return new Read(connectionFactory, queue, topic, maxNumRecords, maxReadTime);
    }

    public Read withMaxReadTime(Duration maxReadTime) {
      return new Read(connectionFactory, queue, topic, maxNumRecords, maxReadTime);
    }

    @Override
    public PCollection<JmsRecord> apply(PBegin input) {
      // handles unbounded source to bounded conversion if maxNumRecords is set.
      Unbounded<JmsRecord> unbounded = org.apache.beam.sdk.io.Read.from(createSource());

      PTransform<PInput, PCollection<JmsRecord>> transform = unbounded;

      if (maxNumRecords != Long.MAX_VALUE) {
        transform = unbounded.withMaxNumRecords(maxNumRecords);
      } else if (maxReadTime != null) {
        transform = unbounded.withMaxReadTime(maxReadTime);
      }

      return input.getPipeline().apply(transform);
    }

    @Override
    public void validate(PBegin input) {
      checkNotNull(connectionFactory, "ConnectionFactory not specified");
      checkArgument((queue != null || topic != null), "Either queue or topic not specified");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.addIfNotNull(DisplayData.item("queue", queue));
      builder.addIfNotNull(DisplayData.item("topic", topic));

    }

    ///////////////////////////////////////////////////////////////////////////////////////

    /**
     * NB: According to http://docs.oracle.com/javaee/1.4/api/javax/jms/ConnectionFactory.html
     * "It is expected that JMS providers will provide the tools an administrator needs to create
     * and configure administered objects in a JNDI namespace. JMS provider implementations of
     * administered objects should be both javax.jndi.Referenceable and java.io.Serializable so
     * that they can be stored in all JNDI naming contexts. In addition, it is recommended that
     * these implementations follow the JavaBeansTM design patterns."
     *
     * So, a {@link ConnectionFactory} implementation is serializable.
     */
    protected ConnectionFactory connectionFactory;
    @Nullable
    protected String queue;
    @Nullable
    protected String topic;
    protected long maxNumRecords;
    protected Duration maxReadTime;

    private Read() {}

    private Read(
        ConnectionFactory connectionFactory,
        String queue,
        String topic,
        long maxNumRecords,
        Duration maxReadTime) {
      super("JmsIO.Read");

      this.connectionFactory = connectionFactory;
      this.queue = queue;
      this.topic = topic;
      this.maxNumRecords = maxNumRecords;
      this.maxReadTime = maxReadTime;
    }

    /**
     * Creates an {@link UnboundedSource<JmsRecord, ?>} with the configuration in
     * {@link Read}. Primary use case is unit tests, should not be used in an
     * application.
     */
    @VisibleForTesting
    UnboundedSource<JmsRecord, JmsCheckpointMark> createSource() {
      return new UnboundedJmsSource(
          connectionFactory,
          queue,
          topic);
    }

  }

  private JmsIO() {}

  private static class UnboundedJmsSource extends UnboundedSource<JmsRecord, JmsCheckpointMark> {

    private final ConnectionFactory connectionFactory;
    private final String queue;
    private final String topic;

    public UnboundedJmsSource(
        ConnectionFactory connectionFactory,
        String queue,
        String topic) {
      this.connectionFactory = connectionFactory;
      this.queue = queue;
      this.topic = topic;
    }

    @Override
    public List<UnboundedJmsSource> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      List<UnboundedJmsSource> sources = new ArrayList<>();
      for (int i = 0; i < desiredNumSplits; i++) {
        sources.add(new UnboundedJmsSource(connectionFactory, queue, topic));
      }
      return sources;
    }

    @Override
    public UnboundedJmsReader createReader(PipelineOptions options,
                                           JmsCheckpointMark checkpointMark) {
      return new UnboundedJmsReader(this, checkpointMark);
    }

    @Override
    public void validate() {
      checkNotNull(connectionFactory, "ConnectionFactory is not defined");
      checkArgument((queue != null || topic != null), "Either queue or topic is not defined");
    }

    @Override
    public Coder getCheckpointMarkCoder() {
      return AvroCoder.of(JmsCheckpointMark.class);
    }

    @Override
    public Coder<JmsRecord> getDefaultOutputCoder() {
      return SerializableCoder.of(JmsRecord.class);
    }

  }

  private static class UnboundedJmsReader extends UnboundedReader<JmsRecord> {

    private UnboundedJmsSource source;
    private JmsCheckpointMark checkpointMark;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;

    private JmsRecord currentRecord;
    private Instant currentTimestamp;

    public UnboundedJmsReader(
        UnboundedJmsSource source,
        JmsCheckpointMark checkpointMark) {
      this.source = source;
      if (checkpointMark != null) {
        this.checkpointMark = checkpointMark;
      } else {
        this.checkpointMark = new JmsCheckpointMark();
      }
      this.currentRecord = null;
    }

    @Override
    public boolean start() throws IOException {
      ConnectionFactory connectionFactory = source.connectionFactory;
      try {
        this.connection = connectionFactory.createConnection();
        this.connection.start();
        this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        if (source.topic != null) {
          this.consumer = this.session.createConsumer(this.session.createTopic(source.topic));
        } else {
          this.consumer = this.session.createConsumer(this.session.createQueue(source.queue));
        }

        return advance();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean advance() throws IOException {
      try {
        TextMessage message = (TextMessage) this.consumer.receiveNoWait();

        if (message == null) {
          currentRecord = null;
          return false;
        }

        Map<String, Object> properties = new HashMap<>();
        Enumeration propertyNames = message.getPropertyNames();
        while (propertyNames.hasMoreElements()) {
          String propertyName = (String) propertyNames.nextElement();
          properties.put(propertyName, message.getObjectProperty(propertyName));
        }

        JmsRecord jmsRecord = new JmsRecord(
            message.getJMSMessageID(),
            message.getJMSTimestamp(),
            message.getJMSCorrelationID(),
            message.getJMSReplyTo(),
            message.getJMSDestination(),
            message.getJMSDeliveryMode(),
            message.getJMSRedelivered(),
            message.getJMSType(),
            message.getJMSExpiration(),
            message.getJMSPriority(),
            properties,
            message.getText());

        checkpointMark.addMessage(message);

        currentRecord = jmsRecord;
        currentTimestamp = new Instant(message.getJMSTimestamp());

        return true;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public JmsRecord getCurrent() throws NoSuchElementException {
      if (currentRecord == null) {
        throw new NoSuchElementException();
      }
      return currentRecord;
    }

    @Override
    public Instant getWatermark() {
      return checkpointMark.getOldestPendingTimestamp();
    }

    @Override
    public Instant getCurrentTimestamp() {
      if (currentRecord == null) {
        throw new NoSuchElementException();
      }
      return currentTimestamp;
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return checkpointMark;
    }

    @Override
    public UnboundedSource<JmsRecord, ?> getCurrentSource() {
      return source;
    }

    @Override
    public void close() throws IOException {
      try {
        if (consumer != null) {
          consumer.close();
          consumer = null;
        }
        if (session != null) {
          session.close();
          session = null;
        }
        if (connection != null) {
          connection.stop();
          connection.close();
          connection = null;
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

  }

  /**
   * A {@link PTransform} to write to a JMS queue. See {@link JmsIO} for
   * more information on usage and configuration.
   */
  public static class Write extends PTransform<PCollection<String>, PDone> {

    protected ConnectionFactory connectionFactory;
    protected String queue;
    protected String topic;

    public Write withConnectionFactory(ConnectionFactory connectionFactory) {
      return new Write(connectionFactory, queue, topic);
    }

    public Write withQueue(String queue) {
      return new Write(connectionFactory, queue, topic);
    }

    public Write withTopic(String topic) {
      return new Write(connectionFactory, queue, topic);
    }

    private Write() {}

    private Write(ConnectionFactory connectionFactory, String queue, String topic) {
      this.connectionFactory = connectionFactory;
      this.queue = queue;
      this.topic = topic;
    }

    @Override
    public PDone apply(PCollection<String> input) {
      input.apply(ParDo.of(new JmsWriter(connectionFactory, queue, topic)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<String> input) {
      checkNotNull(connectionFactory, "ConnectionFactory is not defined");
      checkArgument((queue != null || topic != null), "Either queue or topic is required");
    }

    private static class JmsWriter extends OldDoFn<String, Void> {

      private ConnectionFactory connectionFactory;
      private String queue;
      private String topic;

      private Connection connection;
      private Session session;
      private MessageProducer producer;

      public JmsWriter(ConnectionFactory connectionFactory, String queue, String topic) {
        this.connectionFactory = connectionFactory;
        this.queue = queue;
        this.topic = topic;
      }

      @Override
      public void startBundle(Context c) throws Exception {
        if (producer == null) {
          this.connection = connectionFactory.createConnection();
          this.connection.start();
          // false means we don't use JMS transaction.
          this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
          Destination destination;
          if (queue != null) {
            destination = session.createQueue(queue);
          } else {
            destination = session.createTopic(topic);
          }
          this.producer = this.session.createProducer(destination);
        }
      }

      @Override
      public void processElement(ProcessContext ctx) throws Exception {
        String value = ctx.element();

        try {
          TextMessage message = session.createTextMessage(value);
          producer.send(message);
        } catch (Exception t) {
          finishBundle(null);
          throw t;
        }
      }

      @Override
      public void finishBundle(Context c) throws Exception {
        producer.close();
        producer = null;
        session.close();
        session = null;
        connection.stop();
        connection.close();
        connection = null;
      }
    }

  }

}
