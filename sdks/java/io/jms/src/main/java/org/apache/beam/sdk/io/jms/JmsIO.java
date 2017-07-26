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
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <p>JmsIO sink supports writing text messages to a JMS destination on a broker.
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
@Experimental(Experimental.Kind.SOURCE_SINK)
public class JmsIO {

  private static final Logger LOG = LoggerFactory.getLogger(JmsIO.class);

  public static Read read() {
    return new AutoValue_JmsIO_Read.Builder().setMaxNumRecords(Long.MAX_VALUE).build();
  }

  public static Write write() {
    return new AutoValue_JmsIO_Write.Builder().build();
  }

  /**
   * A {@link PTransform} to read from a JMS destination. See {@link JmsIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<JmsRecord>> {

    /**
     * NB: According to http://docs.oracle.com/javaee/1.4/api/javax/jms/ConnectionFactory.html
     * "It is expected that JMS providers will provide the tools an administrator needs to create
     * and configure administered objects in a JNDI namespace. JMS provider implementations of
     * administered objects should be both javax.jndi.Referenceable and java.io.Serializable so
     * that they can be stored in all JNDI naming contexts. In addition, it is recommended that
     * these implementations follow the JavaBeansTM design patterns."
     *
     * <p>So, a {@link ConnectionFactory} implementation is serializable.
     */
    @Nullable abstract ConnectionFactory getConnectionFactory();
    @Nullable abstract String getQueue();
    @Nullable abstract String getTopic();
    @Nullable abstract String getUsername();
    @Nullable abstract String getPassword();
    abstract long getMaxNumRecords();
    @Nullable abstract Duration getMaxReadTime();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionFactory(ConnectionFactory connectionFactory);
      abstract Builder setQueue(String queue);
      abstract Builder setTopic(String topic);
      abstract Builder setUsername(String username);
      abstract Builder setPassword(String password);
      abstract Builder setMaxNumRecords(long maxNumRecords);
      abstract Builder setMaxReadTime(Duration maxReadTime);
      abstract Read build();
    }

    /**
     * Specify the JMS connection factory to connect to the JMS broker.
     *
     * <p>For instance:
     *
     * <pre>
     *   {@code
     *    pipeline.apply(JmsIO.read().withConnectionFactory(myConnectionFactory)
     *   }
     * </pre>
     *
     * @param connectionFactory The JMS {@link ConnectionFactory}.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Read withConnectionFactory(ConnectionFactory connectionFactory) {
      checkArgument(connectionFactory != null, "withConnectionFactory(connectionFactory) called"
          + " with null connectionFactory");
      return builder().setConnectionFactory(connectionFactory).build();
    }

    /**
     * Specify the JMS queue destination name where to read messages from. The
     * {@link JmsIO.Read} acts as a consumer on the queue.
     *
     * <p>This method is exclusive with {@link JmsIO.Read#withTopic(String)}. The user has to
     * specify a destination: queue or topic.
     *
     * <p>For instance:
     *
     * <pre>
     *   {@code
     *    pipeline.apply(JmsIO.read().withQueue("my-queue")
     *   }
     * </pre>
     *
     * @param queue The JMS queue name where to read messages from.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Read withQueue(String queue) {
      checkArgument(queue != null, "withQueue(queue) called with null queue");
      return builder().setQueue(queue).build();
    }

    /**
     * Specify the JMS topic destination name where to receive messages from. The
     * {@link JmsIO.Read} acts as a subscriber on the topic.
     *
     * <p>This method is exclusive with {@link JmsIO.Read#withQueue(String)}. The user has to
     * specify a destination: queue or topic.
     *
     * <p>For instance:
     *
     * <pre>
     *   {@code
     *    pipeline.apply(JmsIO.read().withTopic("my-topic")
     *   }
     * </pre>
     *
     * @param topic The JMS topic name.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Read withTopic(String topic) {
      checkArgument(topic != null, "withTopic(topic) called with null topic");
      return builder().setTopic(topic).build();
    }

    /**
     * Define the username to connect to the JMS broker (authenticated).
     */
    public Read withUsername(String username) {
      checkArgument(username != null, "JmsIO.read().withUsername(username) called with null "
          + "username");
      return builder().setUsername(username).build();
    }

    /**
     * Define the password to connect to the JMS broker (authenticated).
     */
    public Read withPassword(String password) {
      checkArgument(password != null, "JmsIO.read().withPassword(password) called with null "
          + "password");
      return builder().setPassword(password).build();
    }

    /**
     * Define the max number of records that the source will read. Using a max number of records
     * different from {@code Long.MAX_VALUE} means the source will be {@code Bounded}, and will
     * stop once the max number of records read is reached.
     *
     * <p>For instance:
     *
     * <pre>
     *   {@code
     *    pipeline.apply(JmsIO.read().withNumRecords(1000)
     *   }
     * </pre>
     *
     * @param maxNumRecords The max number of records to read from the JMS destination.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      checkArgument(maxNumRecords >= 0, "withMaxNumRecords(maxNumRecords) called with invalid "
          + "maxNumRecords");
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time that the source will read. Using a non null max read time
     * duration means the source will be {@code Bounded}, and will stop once the max read time is
     * reached.
     *
     * <p>For instance:
     *
     * <pre>
     *   {@code
     *    pipeline.apply(JmsIO.read().withMaxReadTime(Duration.minutes(10))
     *   }
     * </pre>
     *
     * @param maxReadTime The max read time duration.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      checkArgument(maxReadTime != null, "withMaxReadTime(maxReadTime) called with null "
          + "maxReadTime");
      return builder().setMaxReadTime(maxReadTime).build();
    }

    @Override
    public PCollection<JmsRecord> expand(PBegin input) {
      // handles unbounded source to bounded conversion if maxNumRecords is set.
      Unbounded<JmsRecord> unbounded = org.apache.beam.sdk.io.Read.from(createSource());

      PTransform<PBegin, PCollection<JmsRecord>> transform = unbounded;

      if (getMaxNumRecords() != Long.MAX_VALUE) {
        transform = unbounded.withMaxNumRecords(getMaxNumRecords());
      } else if (getMaxReadTime() != null) {
        transform = unbounded.withMaxReadTime(getMaxReadTime());
      }

      return input.getPipeline().apply(transform);
    }

    @Override
    public void validate(PipelineOptions options) {
      checkState(getConnectionFactory() != null, "JmsIO.read() requires a JMS connection "
          + "factory to be set via withConnectionFactory(connectionFactory)");
      checkState((getQueue() != null || getTopic() != null), "JmsIO.read() requires a JMS "
          + "destination (queue or topic) to be set via withQueue(queueName) or withTopic"
          + "(topicName)");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("queue", getQueue()));
      builder.addIfNotNull(DisplayData.item("topic", getTopic()));

    }

    ///////////////////////////////////////////////////////////////////////////////////////

    /**
     * Creates an {@link UnboundedSource UnboundedSource&lt;JmsRecord, ?&gt;} with the configuration
     * in {@link Read}. Primary use case is unit tests, should not be used in an
     * application.
     */
    @VisibleForTesting
    UnboundedSource<JmsRecord, JmsCheckpointMark> createSource() {
      return new UnboundedJmsSource(this);
    }

  }

  private JmsIO() {}

  /**
   * An unbounded JMS source.
   */
  @VisibleForTesting
  protected static class UnboundedJmsSource extends UnboundedSource<JmsRecord, JmsCheckpointMark> {

    private final Read spec;

    public UnboundedJmsSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public List<UnboundedJmsSource> split(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      List<UnboundedJmsSource> sources = new ArrayList<>();
      if (spec.getTopic() != null) {
        // in the case of a topic, we create a single source, so an unique subscriber, to avoid
        // element duplication
        sources.add(new UnboundedJmsSource(spec));
      } else {
        // in the case of a queue, we allow concurrent consumers
        for (int i = 0; i < desiredNumSplits; i++) {
          sources.add(new UnboundedJmsSource(spec));
        }
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
      spec.validate(null);
    }

    @Override
    public Coder<JmsCheckpointMark> getCheckpointMarkCoder() {
      return AvroCoder.of(JmsCheckpointMark.class);
    }

    @Override
    public Coder<JmsRecord> getOutputCoder() {
      return SerializableCoder.of(JmsRecord.class);
    }

  }

  @VisibleForTesting
  static class UnboundedJmsReader extends UnboundedReader<JmsRecord> {

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
      Read spec = source.spec;
      ConnectionFactory connectionFactory = spec.getConnectionFactory();
      try {
        Connection connection;
        if (spec.getUsername() != null) {
          connection =
              connectionFactory.createConnection(spec.getUsername(), spec.getPassword());
        } else {
          connection = connectionFactory.createConnection();
        }
        connection.start();
        this.connection = connection;
      } catch (Exception e) {
        throw new IOException("Error connecting to JMS", e);
      }

      try {
        this.session = this.connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      } catch (Exception e) {
        throw new IOException("Error creating JMS session", e);
      }

      try {
        if (spec.getTopic() != null) {
          this.consumer =
              this.session.createConsumer(this.session.createTopic(spec.getTopic()));
        } else {
          this.consumer =
              this.session.createConsumer(this.session.createQueue(spec.getQueue()));
        }
      } catch (Exception e) {
        throw new IOException("Error creating JMS consumer", e);
      }

      return advance();
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
        @SuppressWarnings("rawtypes")
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
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {

    @Nullable abstract ConnectionFactory getConnectionFactory();
    @Nullable abstract String getQueue();
    @Nullable abstract String getTopic();
    @Nullable abstract String getUsername();
    @Nullable abstract String getPassword();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionFactory(ConnectionFactory connectionFactory);
      abstract Builder setQueue(String queue);
      abstract Builder setTopic(String topic);
      abstract Builder setUsername(String username);
      abstract Builder setPassword(String password);
      abstract Write build();
    }

    /**
     * Specify the JMS connection factory to connect to the JMS broker.
     *
     * <p>For instance:
     *
     * <pre>
     *   {@code
     *    .apply(JmsIO.write().withConnectionFactory(myConnectionFactory)
     *   }
     * </pre>
     *
     * @param connectionFactory The JMS {@link ConnectionFactory}.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Write withConnectionFactory(ConnectionFactory connectionFactory) {
      checkArgument(connectionFactory != null, "withConnectionFactory(connectionFactory) called"
          + " with null connectionFactory");
      return builder().setConnectionFactory(connectionFactory).build();
    }

    /**
     * Specify the JMS queue destination name where to send messages to. The
     * {@link JmsIO.Write} acts as a producer on the queue.
     *
     * <p>This method is exclusive with {@link JmsIO.Write#withTopic(String)}. The user has to
     * specify a destination: queue or topic.
     *
     * <p>For instance:
     *
     * <pre>
     *   {@code
     *    .apply(JmsIO.write().withQueue("my-queue")
     *   }
     * </pre>
     *
     * @param queue The JMS queue name where to send messages to.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Write withQueue(String queue) {
      checkArgument(queue != null, "withQueue(queue) called with null queue");
      return builder().setQueue(queue).build();
    }

    /**
     * Specify the JMS topic destination name where to send messages to. The
     * {@link JmsIO.Read} acts as a publisher on the topic.
     *
     * <p>This method is exclusive with {@link JmsIO.Write#withQueue(String)}. The user has to
     * specify a destination: queue or topic.
     *
     * <p>For instance:
     *
     * <pre>
     *   {@code
     *    .apply(JmsIO.write().withTopic("my-topic")
     *   }
     * </pre>
     *
     * @param topic The JMS topic name.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Write withTopic(String topic) {
      checkArgument(topic != null, "withTopic(topic) called with null topic");
      return builder().setTopic(topic).build();
    }

    /**
     * Define the username to connect to the JMS broker (authenticated).
     */
    public Write withUsername(String username) {
      checkArgument(username != null,  "JmsIO.write().withUsername(username) called with null "
          + "username");
      return builder().setUsername(username).build();
    }

    /**
     * Define the password to connect to the JMS broker (authenticated).
     */
    public Write withPassword(String password) {
      checkArgument(password != null, "JmsIO.write().withPassword(password) called with null "
          + "password");
      return builder().setPassword(password).build();
    }

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(ParDo.of(new WriterFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PipelineOptions options) {
      checkState(getConnectionFactory() != null, "JmsIO.write() requires a JMS connection "
          + "factory to be set via withConnectionFactory(connectionFactory)");
      checkState((getQueue() != null || getTopic() != null), "JmsIO.write() requires a JMS "
          + "destination (queue or topic) to be set via withQueue(queue) or withTopic(topic)");
    }

    private static class WriterFn extends DoFn<String, Void> {

      private Write spec;

      private Connection connection;
      private Session session;
      private MessageProducer producer;

      public WriterFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        if (producer == null) {
          if (spec.getUsername() != null) {
            this.connection =
                spec.getConnectionFactory()
                    .createConnection(spec.getUsername(), spec.getPassword());
          } else {
            this.connection = spec.getConnectionFactory().createConnection();
          }
          this.connection.start();
          // false means we don't use JMS transaction.
          this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
          Destination destination;
          if (spec.getQueue() != null) {
            destination = session.createQueue(spec.getQueue());
          } else {
            destination = session.createTopic(spec.getTopic());
          }
          this.producer = this.session.createProducer(destination);
        }
      }

      @ProcessElement
      public void processElement(ProcessContext ctx) throws Exception {
        String value = ctx.element();
        TextMessage message = session.createTextMessage(value);
        producer.send(message);
      }

      @Teardown
      public void teardown() throws Exception {
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
