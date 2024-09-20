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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ExecutorOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded source for JMS destinations (queues or topics).
 *
 * <h3>Reading from a JMS destination</h3>
 *
 * <p>JmsIO source returns unbounded collection of JMS records as {@code PCollection<JmsRecord>}. A
 * {@link JmsRecord} includes JMS headers and properties, along with the JMS {@link
 * javax.jms.TextMessage} payload.
 *
 * <p>To configure a JMS source, you have to provide a {@link javax.jms.ConnectionFactory} and the
 * destination (queue or topic) where to consume. The following example illustrates various options
 * for configuring the source:
 *
 * <pre>{@code
 * pipeline.apply(JmsIO.read()
 *    .withConnectionFactory(myConnectionFactory)
 *    .withQueue("my-queue")
 *    // above two are required configuration, returns PCollection<JmsRecord>
 *
 *    // rest of the settings are optional
 *
 * }</pre>
 *
 * <p>It is possible to read any type of JMS {@link javax.jms.Message} into a custom POJO using the
 * following configuration:
 *
 * <pre>{@code
 * pipeline.apply(JmsIO.<T>readMessage()
 *    .withConnectionFactory(myConnectionFactory)
 *    .withQueue("my-queue")
 *    .withMessageMapper((MessageMapper<T>) message -> {
 *      // code that maps message to T
 *    })
 *    .withCoder(
 *      // a coder for T
 *    )
 *
 * }</pre>
 *
 * <h3>Writing to a JMS destination</h3>
 *
 * <p>JmsIO sink supports writing text messages to a JMS destination on a broker. To configure a JMS
 * sink, you must specify a {@link javax.jms.ConnectionFactory} and a {@link javax.jms.Destination}
 * name. For instance:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...) // returns PCollection<String>
 *   .apply(JmsIO.write()
 *       .withConnectionFactory(myConnectionFactory)
 *       .withQueue("my-queue")
 * }</pre>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class JmsIO {

  private static final Logger LOG = LoggerFactory.getLogger(JmsIO.class);
  private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.millis(60000L);

  public static Read<JmsRecord> read() {
    return new AutoValue_JmsIO_Read.Builder<JmsRecord>()
        .setMaxNumRecords(Long.MAX_VALUE)
        .setCoder(SerializableCoder.of(JmsRecord.class))
        .setCloseTimeout(DEFAULT_CLOSE_TIMEOUT)
        .setRequiresDeduping(false)
        .setMessageMapper(
            new MessageMapper<JmsRecord>() {
              @Override
              public JmsRecord mapMessage(Message message) throws Exception {
                TextMessage textMessage = (TextMessage) message;
                Map<String, Object> properties = new HashMap<>();
                @SuppressWarnings("rawtypes")
                Enumeration propertyNames = textMessage.getPropertyNames();
                while (propertyNames.hasMoreElements()) {
                  String propertyName = (String) propertyNames.nextElement();
                  properties.put(propertyName, textMessage.getObjectProperty(propertyName));
                }

                return new JmsRecord(
                    textMessage.getJMSMessageID(),
                    textMessage.getJMSTimestamp(),
                    textMessage.getJMSCorrelationID(),
                    textMessage.getJMSReplyTo(),
                    textMessage.getJMSDestination(),
                    textMessage.getJMSDeliveryMode(),
                    textMessage.getJMSRedelivered(),
                    textMessage.getJMSType(),
                    textMessage.getJMSExpiration(),
                    textMessage.getJMSPriority(),
                    properties,
                    textMessage.getText());
              }
            })
        .build();
  }

  public static <T> Read<T> readMessage() {
    return new AutoValue_JmsIO_Read.Builder<T>()
        .setMaxNumRecords(Long.MAX_VALUE)
        .setCloseTimeout(DEFAULT_CLOSE_TIMEOUT)
        .setRequiresDeduping(false)
        .build();
  }

  public static <EventT> Write<EventT> write() {
    return new AutoValue_JmsIO_Write.Builder<EventT>().build();
  }

  public interface ConnectionFactoryContainer<T extends ConnectionFactoryContainer<T>> {

    T withConnectionFactory(ConnectionFactory connectionFactory);

    T withConnectionFactoryProviderFn(
        SerializableFunction<Void, ? extends ConnectionFactory> connectionFactoryProviderFn);
  }

  /**
   * A {@link PTransform} to read from a JMS destination. See {@link JmsIO} for more information on
   * usage and configuration.
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>>
      implements ConnectionFactoryContainer<Read<T>> {

    private @Nullable transient ConnectionFactory connectionFactory;

    /**
     * NB: According to http://docs.oracle.com/javaee/1.4/api/javax/jms/ConnectionFactory.html "It
     * is expected that JMS providers will provide the tools an administrator needs to create and
     * configure administered objects in a JNDI namespace. JMS provider implementations of
     * administered objects should be both javax.jndi.Referenceable and java.io.Serializable so that
     * they can be stored in all JNDI naming contexts. In addition, it is recommended that these
     * implementations follow the JavaBeansTM design patterns."
     *
     * <p>So, a {@link ConnectionFactory} implementation should be serializable.
     */
    @Nullable
    ConnectionFactory getConnectionFactory() {
      if (connectionFactory == null) {
        connectionFactory =
            Optional.ofNullable(getConnectionFactoryProviderFn())
                .map(provider -> provider.apply(null))
                .orElse(null);
      }
      return connectionFactory;
    }

    /**
     * In case the {@link ConnectionFactory} is not serializable it can be constructed separately on
     * each worker from scratch by providing a {@link SerializableFunction} instead of a ready-made
     * object.
     */
    @Nullable
    abstract SerializableFunction<Void, ? extends ConnectionFactory>
        getConnectionFactoryProviderFn();

    abstract @Nullable String getQueue();

    abstract @Nullable String getTopic();

    abstract @Nullable String getUsername();

    abstract @Nullable String getPassword();

    abstract long getMaxNumRecords();

    abstract @Nullable Duration getMaxReadTime();

    abstract @Nullable MessageMapper<T> getMessageMapper();

    abstract @Nullable Coder<T> getCoder();

    abstract @Nullable AutoScaler getAutoScaler();

    abstract Duration getCloseTimeout();

    abstract @Nullable Duration getReceiveTimeout();

    abstract boolean isRequiresDeduping();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConnectionFactoryProviderFn(
          SerializableFunction<Void, ? extends ConnectionFactory> connectionFactoryProviderFn);

      abstract Builder<T> setQueue(String queue);

      abstract Builder<T> setTopic(String topic);

      abstract Builder<T> setUsername(String username);

      abstract Builder<T> setPassword(String password);

      abstract Builder<T> setMaxNumRecords(long maxNumRecords);

      abstract Builder<T> setMaxReadTime(Duration maxReadTime);

      abstract Builder<T> setMessageMapper(MessageMapper<T> mesageMapper);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setAutoScaler(AutoScaler autoScaler);

      abstract Builder<T> setCloseTimeout(Duration closeTimeout);

      abstract Builder<T> setReceiveTimeout(Duration receiveTimeout);

      abstract Builder<T> setRequiresDeduping(boolean requiresDeduping);

      abstract Read<T> build();
    }

    /**
     * Specify the JMS connection factory to connect to the JMS broker.
     *
     * <p>The {@link ConnectionFactory} object has to be serializable, if it is not consider using
     * the {@link #withConnectionFactoryProviderFn}
     *
     * <p>For instance:
     *
     * <pre>{@code
     * pipeline.apply(JmsIO.read().withConnectionFactory(myConnectionFactory)
     *
     * }</pre>
     *
     * @param connectionFactory The JMS {@link ConnectionFactory}.
     * @return The corresponding {@link JmsIO.Read}.
     */
    @Override
    public Read<T> withConnectionFactory(ConnectionFactory connectionFactory) {
      checkArgument(connectionFactory != null, "connectionFactory can not be null");
      return builder().setConnectionFactoryProviderFn(__ -> connectionFactory).build();
    }

    /**
     * Specify a JMS connection factory provider function to connect to the JMS broker. Use this
     * method in case your {@link ConnectionFactory} objects are themselves not serializable, but
     * you can recreate them as needed with a {@link SerializableFunction} provider
     *
     * <p>For instance:
     *
     * <pre>
     * {@code pipeline.apply(JmsIO.read().withConnectionFactoryProviderFn(() -> new MyJmsConnectionFactory());}
     * </pre>
     *
     * @param connectionFactoryProviderFn a {@link SerializableFunction} that creates a {@link
     *     ConnectionFactory}
     * @return The corresponding {@link JmsIO.Read}
     */
    @Override
    public Read<T> withConnectionFactoryProviderFn(
        SerializableFunction<Void, ? extends ConnectionFactory> connectionFactoryProviderFn) {
      checkArgument(
          connectionFactoryProviderFn != null, "connectionFactoryProviderFn cannot be null");
      return builder().setConnectionFactoryProviderFn(connectionFactoryProviderFn).build();
    }

    /**
     * Specify the JMS queue destination name where to read messages from. The {@link JmsIO.Read}
     * acts as a consumer on the queue.
     *
     * <p>This method is exclusive with {@link JmsIO.Read#withTopic(String)}. The user has to
     * specify a destination: queue or topic.
     *
     * <p>For instance:
     *
     * <pre>{@code
     * pipeline.apply(JmsIO.read().withQueue("my-queue")
     *
     * }</pre>
     *
     * @param queue The JMS queue name where to read messages from.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Read<T> withQueue(String queue) {
      checkArgument(queue != null, "queue can not be null");
      return builder().setQueue(queue).build();
    }

    /**
     * Specify the JMS topic destination name where to receive messages from. The {@link JmsIO.Read}
     * acts as a subscriber on the topic.
     *
     * <p>This method is exclusive with {@link JmsIO.Read#withQueue(String)}. The user has to
     * specify a destination: queue or topic.
     *
     * <p>For instance:
     *
     * <pre>{@code
     * pipeline.apply(JmsIO.read().withTopic("my-topic")
     *
     * }</pre>
     *
     * @param topic The JMS topic name.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Read<T> withTopic(String topic) {
      checkArgument(topic != null, "topic can not be null");
      return builder().setTopic(topic).build();
    }

    /** Define the username to connect to the JMS broker (authenticated). */
    public Read<T> withUsername(String username) {
      checkArgument(username != null, "username can not be null");
      return builder().setUsername(username).build();
    }

    /** Define the password to connect to the JMS broker (authenticated). */
    public Read<T> withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return builder().setPassword(password).build();
    }

    /**
     * Define the max number of records that the source will read. Using a max number of records
     * different from {@code Long.MAX_VALUE} means the source will be {@code Bounded}, and will stop
     * once the max number of records read is reached.
     *
     * <p>For instance:
     *
     * <pre>{@code
     * pipeline.apply(JmsIO.read().withNumRecords(1000)
     *
     * }</pre>
     *
     * @param maxNumRecords The max number of records to read from the JMS destination.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Read<T> withMaxNumRecords(long maxNumRecords) {
      checkArgument(maxNumRecords >= 0, "maxNumRecords must be > 0, but was: %s", maxNumRecords);
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time that the source will read. Using a non null max read time duration
     * means the source will be {@code Bounded}, and will stop once the max read time is reached.
     *
     * <p>For instance:
     *
     * <pre>{@code
     * pipeline.apply(JmsIO.read().withMaxReadTime(Duration.minutes(10))
     *
     * }</pre>
     *
     * @param maxReadTime The max read time duration.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Read<T> withMaxReadTime(Duration maxReadTime) {
      checkArgument(maxReadTime != null, "maxReadTime can not be null");
      return builder().setMaxReadTime(maxReadTime).build();
    }

    public Read<T> withMessageMapper(MessageMapper<T> messageMapper) {
      checkArgument(messageMapper != null, "messageMapper can not be null");
      return builder().setMessageMapper(messageMapper).build();
    }

    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    /**
     * Sets the {@link AutoScaler} to use for reporting backlog during the execution of this source.
     */
    public Read<T> withAutoScaler(AutoScaler autoScaler) {
      checkArgument(autoScaler != null, "autoScaler can not be null");
      return builder().setAutoScaler(autoScaler).build();
    }

    /**
     * Sets the amount of time to wait for callbacks from the runner stating that the output has
     * been durably persisted before closing the connection to the JMS broker. Any callbacks that do
     * not occur will cause unacknowledged messages to be returned to the JMS broker and redelivered
     * to other clients.
     */
    public Read<T> withCloseTimeout(Duration closeTimeout) {
      checkArgument(closeTimeout != null, "closeTimeout can not be null");
      checkArgument(closeTimeout.getMillis() >= 0, "Close timeout must be non-negative.");
      return builder().setCloseTimeout(closeTimeout).build();
    }

    /**
     * If set, block for the Duration of timeout for each poll to new JMS record if the previous
     * poll returns no new record.
     *
     * <p>Use this option if the requirement for read latency is not a concern or excess client
     * polling has resulted network issues.
     */
    public Read<T> withReceiveTimeout(Duration receiveTimeout) {
      return builder().setReceiveTimeout(receiveTimeout).build();
    }

    /**
     * If set, requires runner deduplication for the messages. Each message is identified by its
     * {@code JMSMessageID}.
     */
    public Read<T> withRequiresDeduping() {
      return builder().setRequiresDeduping(true).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument(
          getConnectionFactoryProviderFn() != null,
          "Either withConnectionFactory() or withConnectionFactoryProviderFn() is required");
      checkArgument(
          getQueue() != null || getTopic() != null,
          "Either withQueue() or withTopic() is required");
      checkArgument(
          getQueue() == null || getTopic() == null, "withQueue() and withTopic() are exclusive");
      checkArgument(getMessageMapper() != null, "withMessageMapper() is required");
      checkArgument(getCoder() != null, "withCoder() is required");

      // handles unbounded source to bounded conversion if maxNumRecords is set.
      Unbounded<T> unbounded = org.apache.beam.sdk.io.Read.from(createSource());

      PTransform<PBegin, PCollection<T>> transform = unbounded;

      if (getMaxNumRecords() < Long.MAX_VALUE || getMaxReadTime() != null) {
        transform =
            unbounded.withMaxReadTime(getMaxReadTime()).withMaxNumRecords(getMaxNumRecords());
      }

      return input.getPipeline().apply(transform);
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
     * in {@link Read}. Primary use case is unit tests, should not be used in an application.
     */
    UnboundedSource<T, JmsCheckpointMark> createSource() {
      return new UnboundedJmsSource<T>(this);
    }
  }

  private JmsIO() {}

  /**
   * An interface used by {@link JmsIO.Read} for converting each jms {@link Message} into an element
   * of the resulting {@link PCollection}.
   */
  @FunctionalInterface
  public interface MessageMapper<T> extends Serializable {
    T mapMessage(Message message) throws Exception;
  }

  /** An unbounded JMS source. */
  static class UnboundedJmsSource<T> extends UnboundedSource<T, JmsCheckpointMark> {

    private final Read<T> spec;

    public UnboundedJmsSource(Read<T> spec) {
      this.spec = spec;
    }

    @Override
    public List<UnboundedJmsSource<T>> split(int desiredNumSplits, PipelineOptions options)
        throws Exception {
      List<UnboundedJmsSource<T>> sources = new ArrayList<>();
      if (spec.getTopic() != null) {
        // in the case of a topic, we create a single source, so a unique subscriber, to avoid
        // element duplication
        sources.add(new UnboundedJmsSource<T>(spec));
      } else {
        // in the case of a queue, we allow concurrent consumers
        for (int i = 0; i < desiredNumSplits; i++) {
          sources.add(new UnboundedJmsSource<T>(spec));
        }
      }
      return sources;
    }

    @Override
    public UnboundedJmsReader<T> createReader(
        PipelineOptions options, JmsCheckpointMark checkpointMark) {
      return new UnboundedJmsReader<T>(this, options);
    }

    @Override
    public Coder<JmsCheckpointMark> getCheckpointMarkCoder() {
      return SerializableCoder.of(JmsCheckpointMark.class);
    }

    @Override
    public Coder<T> getOutputCoder() {
      return this.spec.getCoder();
    }

    @Override
    public boolean requiresDeduping() {
      return this.spec.isRequiresDeduping();
    }
  }

  static class UnboundedJmsReader<T> extends UnboundedReader<T> {
    private static final byte[] EMPTY = new byte[0];
    private UnboundedJmsSource<T> source;
    @VisibleForTesting JmsCheckpointMark.Preparer checkpointMarkPreparer;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private AutoScaler autoScaler;

    private T currentMessage;
    private Instant currentTimestamp;
    private byte[] currentID;
    private long receiveTimeoutMillis;
    private PipelineOptions options;

    public UnboundedJmsReader(UnboundedJmsSource<T> source, PipelineOptions options) {
      this.source = source;
      this.checkpointMarkPreparer = JmsCheckpointMark.newPreparer();
      this.currentMessage = null;
      this.currentID = EMPTY;
      this.options = options;
    }

    /** recreate session and consumer. */
    private synchronized void recreateSession() throws IOException {
      try {
        this.session = this.connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      } catch (Exception e) {
        throw new IOException("Error creating JMS session", e);
      }

      Read<T> spec = source.spec;
      Duration receiveTimeout =
          MoreObjects.firstNonNull(source.spec.getReceiveTimeout(), Duration.ZERO);
      receiveTimeoutMillis = receiveTimeout.getMillis();

      try {
        if (source.spec.getTopic() != null) {
          consumer = session.createConsumer(session.createTopic(spec.getTopic()));
        } else {
          consumer = session.createConsumer(session.createQueue(spec.getQueue()));
        }
      } catch (Exception e) {
        throw new IOException("Error creating JMS consumer", e);
      }
    }

    @Override
    public boolean start() throws IOException {
      Read<T> spec = source.spec;
      ConnectionFactory connectionFactory = spec.getConnectionFactory();
      try {
        Connection connection;
        if (spec.getUsername() != null) {
          connection = connectionFactory.createConnection(spec.getUsername(), spec.getPassword());
        } else {
          connection = connectionFactory.createConnection();
        }
        connection.start();
        this.connection = connection;
        if (spec.getAutoScaler() == null) {
          this.autoScaler = new DefaultAutoscaler();
        } else {
          this.autoScaler = spec.getAutoScaler();
        }
        this.autoScaler.start();
      } catch (Exception e) {
        throw new IOException("Error connecting to JMS", e);
      }

      recreateSession();

      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      try {
        Message message;
        synchronized (this) {
          if (receiveTimeoutMillis == 0L) {
            message = this.consumer.receiveNoWait();
          } else {
            message = this.consumer.receive(receiveTimeoutMillis);
          }
          // put add in synchronized to make sure all messages in preparer are in same session
          if (message != null) {
            checkpointMarkPreparer.add(message);
          }
        }
        if (message == null) {
          currentMessage = null;
          return false;
        }

        currentMessage = this.source.spec.getMessageMapper().mapMessage(message);
        currentTimestamp = new Instant(message.getJMSTimestamp());

        String messageID = message.getJMSMessageID();
        if (messageID != null) {
          if (this.source.spec.isRequiresDeduping()) {
            // per JMS specification, message ID has prefix "id:". The runner use it to dedup
            // message. Empty or non-exist message id (possible for optimization configuration set)
            // will cause data loss.
            if (messageID.length() <= 3) {
              throw new RuntimeException(
                  String.format(
                      "Invalid JMSMessageID %s while requiresDeduping is set. Data loss possible.",
                      messageID));
            }
          }
          currentID = messageID.getBytes(StandardCharsets.UTF_8);
        } else {
          currentID = EMPTY;
        }

        return true;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (currentMessage == null) {
        throw new NoSuchElementException();
      }
      return currentMessage;
    }

    @Override
    public Instant getWatermark() {
      return checkpointMarkPreparer.getOldestMessageTimestamp();
    }

    @Override
    public Instant getCurrentTimestamp() {
      if (currentMessage == null) {
        throw new NoSuchElementException();
      }
      return currentTimestamp;
    }

    @Override
    public byte[] getCurrentRecordId() {
      if (currentMessage == null) {
        throw new NoSuchElementException();
      } else if (currentID == EMPTY && this.source.spec.isRequiresDeduping()) {
        LOG.warn(
            "Empty JMSRecordID received when requiresDeduping enabled, runner deduplication will"
                + " not be effective");
        // Return a random UUID to ensure it won't get dedup
        currentID = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
      }
      return currentID;
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      if (checkpointMarkPreparer.isEmpty()) {
        return checkpointMarkPreparer.emptyCheckpoint();
      }

      MessageConsumer consumerToClose;
      Session sessionTofinalize;
      synchronized (this) {
        consumerToClose = consumer;
        sessionTofinalize = session;
      }
      try {
        recreateSession();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return checkpointMarkPreparer.newCheckpoint(consumerToClose, sessionTofinalize);
    }

    @Override
    public long getTotalBacklogBytes() {
      return this.autoScaler.getTotalBacklogBytes();
    }

    @Override
    public UnboundedSource<T, ?> getCurrentSource() {
      return source;
    }

    @Override
    public void close() {
      doClose();
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private void doClose() {
      try {
        closeAutoscaler();
        closeConsumer();
        ScheduledExecutorService executorService =
            options.as(ExecutorOptions.class).getScheduledExecutorService();
        executorService.schedule(
            () -> {
              LOG.debug("Closing connection after delay {}", source.spec.getCloseTimeout());
              // Discard the checkpoints and set the reader as inactive
              checkpointMarkPreparer.discard();
              closeSession();
              closeConnection();
            },
            source.spec.getCloseTimeout().getMillis(),
            TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        LOG.error("Error closing reader", e);
      }
    }

    private void closeConnection() {
      try {
        if (connection != null) {
          connection.stop();
          connection.close();
          connection = null;
        }
      } catch (Exception e) {
        LOG.error("Error closing connection", e);
      }
    }

    private synchronized void closeSession() {
      try {
        if (session != null) {
          session.close();
          session = null;
        }
      } catch (Exception e) {
        LOG.error("Error closing session" + e.getMessage(), e);
      }
    }

    private synchronized void closeConsumer() {
      try {
        if (consumer != null) {
          consumer.close();
          consumer = null;
        }
      } catch (Exception e) {
        LOG.error("Error closing consumer", e);
      }
    }

    private void closeAutoscaler() {
      try {
        if (autoScaler != null) {
          autoScaler.stop();
          autoScaler = null;
        }
      } catch (Exception e) {
        LOG.error("Error closing autoscaler", e);
      }
    }

    @Override
    protected void finalize() {
      doClose();
    }
  }

  /**
   * A {@link PTransform} to write to a JMS queue. See {@link JmsIO} for more information on usage
   * and configuration.
   */
  @AutoValue
  public abstract static class Write<EventT>
      extends PTransform<PCollection<EventT>, WriteJmsResult<EventT>>
      implements ConnectionFactoryContainer<Write<EventT>> {

    private @Nullable transient ConnectionFactory connectionFactory;

    @Nullable
    ConnectionFactory getConnectionFactory() {
      if (connectionFactory == null) {
        connectionFactory =
            Optional.ofNullable(getConnectionFactoryProviderFn())
                .map(provider -> provider.apply(null))
                .orElse(null);
      }

      return connectionFactory;
    }

    abstract @Nullable SerializableFunction<Void, ? extends ConnectionFactory>
        getConnectionFactoryProviderFn();

    abstract @Nullable String getQueue();

    abstract @Nullable String getTopic();

    abstract @Nullable String getUsername();

    abstract @Nullable String getPassword();

    abstract @Nullable SerializableBiFunction<EventT, Session, Message> getValueMapper();

    abstract @Nullable SerializableFunction<EventT, String> getTopicNameMapper();

    abstract @Nullable RetryConfiguration getRetryConfiguration();

    abstract Builder<EventT> builder();

    @AutoValue.Builder
    abstract static class Builder<EventT> {
      abstract Builder<EventT> setConnectionFactoryProviderFn(
          SerializableFunction<Void, ? extends ConnectionFactory> connectionFactoryProviderFn);

      abstract Builder<EventT> setQueue(String queue);

      abstract Builder<EventT> setTopic(String topic);

      abstract Builder<EventT> setUsername(String username);

      abstract Builder<EventT> setPassword(String password);

      abstract Builder<EventT> setValueMapper(
          SerializableBiFunction<EventT, Session, Message> valueMapper);

      abstract Builder<EventT> setTopicNameMapper(
          SerializableFunction<EventT, String> topicNameMapper);

      abstract Builder<EventT> setRetryConfiguration(RetryConfiguration retryConfiguration);

      abstract Write<EventT> build();
    }

    /**
     * Specify the JMS connection factory to connect to the JMS broker.
     *
     * <p>The {@link ConnectionFactory} object has to be serializable, if it is not consider using
     * the {@link #withConnectionFactoryProviderFn}
     *
     * <p>For instance:
     *
     * <pre>{@code
     * .apply(JmsIO.write().withConnectionFactory(myConnectionFactory)
     *
     * }</pre>
     *
     * @param connectionFactory The JMS {@link ConnectionFactory}.
     * @return The corresponding {@link JmsIO.Read}.
     */
    @Override
    public Write<EventT> withConnectionFactory(ConnectionFactory connectionFactory) {
      checkArgument(connectionFactory != null, "connectionFactory can not be null");
      return builder().setConnectionFactoryProviderFn(__ -> connectionFactory).build();
    }

    /**
     * Specify a JMS connection factory provider function to connect to the JMS broker. Use this
     * method in case your {@link ConnectionFactory} objects are themselves not serializable, but
     * you can recreate them as needed with a {@link SerializableFunction} provider
     *
     * <p>For instance:
     *
     * <pre>
     * {@code pipeline.apply(JmsIO.write().withConnectionFactoryProviderFn(() -> new MyJmsConnectionFactory());}
     * </pre>
     *
     * @param connectionFactoryProviderFn a {@link SerializableFunction} that creates a {@link
     *     ConnectionFactory}
     * @return The corresponding {@link JmsIO.Write}
     */
    @Override
    public Write<EventT> withConnectionFactoryProviderFn(
        SerializableFunction<Void, ? extends ConnectionFactory> connectionFactoryProviderFn) {
      checkArgument(
          connectionFactoryProviderFn != null, "connectionFactoryProviderFn can not be null");
      return builder().setConnectionFactoryProviderFn(connectionFactoryProviderFn).build();
    }

    /**
     * Specify the JMS queue destination name where to send messages to. The {@link JmsIO.Write}
     * acts as a producer on the queue.
     *
     * <p>This method is exclusive with {@link JmsIO.Write#withTopic(String)}. The user has to
     * specify a destination: queue, topic, or topicNameMapper.
     *
     * <p>For instance:
     *
     * <pre>{@code
     * .apply(JmsIO.write().withQueue("my-queue")
     *
     * }</pre>
     *
     * @param queue The JMS queue name where to send messages to.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Write<EventT> withQueue(String queue) {
      checkArgument(queue != null, "queue can not be null");
      return builder().setQueue(queue).build();
    }

    /**
     * Specify the JMS topic destination name where to send messages to. The {@link JmsIO.Read} acts
     * as a publisher on the topic.
     *
     * <p>This method is exclusive with {@link JmsIO.Write#withQueue(String)}. The user has to
     * specify a destination: queue, topic, or topicNameMapper.
     *
     * <p>For instance:
     *
     * <pre>{@code
     * .apply(JmsIO.write().withTopic("my-topic")
     *
     * }</pre>
     *
     * @param topic The JMS topic name.
     * @return The corresponding {@link JmsIO.Read}.
     */
    public Write<EventT> withTopic(String topic) {
      checkArgument(topic != null, "topic can not be null");
      return builder().setTopic(topic).build();
    }

    /** Define the username to connect to the JMS broker (authenticated). */
    public Write<EventT> withUsername(String username) {
      checkArgument(username != null, "username can not be null");
      return builder().setUsername(username).build();
    }

    /** Define the password to connect to the JMS broker (authenticated). */
    public Write<EventT> withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return builder().setPassword(password).build();
    }

    /**
     * Specify the JMS topic destination name where to send messages to dynamically. The {@link
     * JmsIO.Write} acts as a publisher on the topic.
     *
     * <p>This method is exclusive with {@link JmsIO.Write#withQueue(String)} and {@link
     * JmsIO.Write#withTopic(String)}. The user has to specify a {@link SerializableFunction} that
     * takes {@code EventT} object as a parameter, and returns the topic name depending of the
     * content of the event object.
     *
     * <p>For example:
     *
     * <pre>{@code
     * SerializableFunction<CompanyEvent, String> topicNameMapper =
     *   (event ->
     *    String.format(
     *    "company/%s/employee/%s",
     *    event.getCompanyName(),
     *    event.getEmployeeId()));
     * }</pre>
     *
     * <pre>{@code
     * .apply(JmsIO.write().withTopicNameMapper(topicNameNapper)
     * }</pre>
     *
     * @param topicNameMapper The function returning the dynamic topic name.
     * @return The corresponding {@link JmsIO.Write}.
     */
    public Write<EventT> withTopicNameMapper(SerializableFunction<EventT, String> topicNameMapper) {
      checkArgument(topicNameMapper != null, "topicNameMapper can not be null");
      return builder().setTopicNameMapper(topicNameMapper).build();
    }

    /**
     * Map the {@code EventT} object to a {@link javax.jms.Message}.
     *
     * <p>For instance:
     *
     * <pre>{@code
     * SerializableBiFunction<SomeEventObject, Session, Message> valueMapper = (e, s) -> {
     *
     *       try {
     *         TextMessage msg = s.createTextMessage();
     *         msg.setText(Mapper.MAPPER.toJson(e));
     *         return msg;
     *       } catch (JMSException ex) {
     *         throw new JmsIOException("Error!!", ex);
     *       }
     *     };
     *
     * }</pre>
     *
     * <pre>{@code
     * .apply(JmsIO.write().withValueMapper(valueNapper)
     * }</pre>
     *
     * @param valueMapper The function returning the {@link javax.jms.Message}
     * @return The corresponding {@link JmsIO.Write}.
     */
    public Write<EventT> withValueMapper(
        SerializableBiFunction<EventT, Session, Message> valueMapper) {
      checkArgument(valueMapper != null, "valueMapper can not be null");
      return builder().setValueMapper(valueMapper).build();
    }

    /**
     * Specify the JMS retry configuration. The {@link JmsIO.Write} acts as a publisher on the
     * topic.
     *
     * <p>Allows a retry for failed published messages, the user should specify the maximum number
     * of retries, a duration for retrying and a maximum cumulative retries. By default, the
     * duration for retrying used is 15s and the maximum cumulative is 1000 days {@link
     * RetryConfiguration}
     *
     * <p>For example:
     *
     * <pre>{@code
     * RetryConfiguration retryConfiguration = RetryConfiguration.create(5);
     * }</pre>
     *
     * or
     *
     * <pre>{@code
     * RetryConfiguration retryConfiguration =
     *   RetryConfiguration.create(5, Duration.standardSeconds(30), null);
     * }</pre>
     *
     * or
     *
     * <pre>{@code
     * RetryConfiguration retryConfiguration =
     *   RetryConfiguration.create(5, Duration.standardSeconds(30), Duration.standardDays(15));
     * }</pre>
     *
     * <pre>{@code
     * .apply(JmsIO.write().withPublicationRetryPolicy(publicationRetryPolicy)
     * }</pre>
     *
     * @param retryConfiguration The retry configuration that should be used in case of failed
     *     publications.
     * @return The corresponding {@link JmsIO.Write}.
     */
    public Write<EventT> withRetryConfiguration(RetryConfiguration retryConfiguration) {
      checkArgument(retryConfiguration != null, "retryConfiguration can not be null");
      return builder().setRetryConfiguration(retryConfiguration).build();
    }

    @Override
    public WriteJmsResult<EventT> expand(PCollection<EventT> input) {
      checkArgument(
          getConnectionFactoryProviderFn() != null,
          "Either withConnectionFactory() or withConnectionFactoryProviderFn() is required");
      checkArgument(
          getTopicNameMapper() != null || getQueue() != null || getTopic() != null,
          "Either withTopicNameMapper(topicNameMapper), withQueue(queue), or withTopic(topic) is"
              + " required");
      boolean exclusiveTopicQueue = isExclusiveTopicQueue();
      checkArgument(
          exclusiveTopicQueue,
          "Only one of withQueue(queue), withTopic(topic), or withTopicNameMapper(function) must be"
              + " set.");
      checkArgument(getValueMapper() != null, "withValueMapper() is required");

      return input.apply(new Writer<>(this));
    }

    private boolean isExclusiveTopicQueue() {
      boolean exclusiveTopicQueue =
          Stream.of(getQueue() != null, getTopic() != null, getTopicNameMapper() != null)
                  .filter(b -> b)
                  .count()
              == 1;
      return exclusiveTopicQueue;
    }
  }

  static class Writer<T> extends PTransform<PCollection<T>, WriteJmsResult<T>> {

    public static final String CONNECTION_ERRORS_METRIC_NAME = "connectionErrors";
    public static final String PUBLICATION_RETRIES_METRIC_NAME = "publicationRetries";
    public static final String JMS_IO_PRODUCER_METRIC_NAME = Writer.class.getCanonicalName();

    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);
    private static final String PUBLISH_TO_JMS_STEP_NAME = "Publish to JMS";

    private final JmsIO.Write<T> spec;
    private final TupleTag<T> messagesTag;
    private final TupleTag<T> failedMessagesTag;

    Writer(JmsIO.Write<T> spec) {
      this.spec = spec;
      this.messagesTag = new TupleTag<>();
      this.failedMessagesTag = new TupleTag<>();
    }

    @Override
    public WriteJmsResult<T> expand(PCollection<T> input) {
      PCollectionTuple failedPublishedMessagesTuple =
          input.apply(
              PUBLISH_TO_JMS_STEP_NAME,
              ParDo.of(new JmsIOProducerFn<>(spec, failedMessagesTag))
                  .withOutputTags(messagesTag, TupleTagList.of(failedMessagesTag)));
      PCollection<T> failedPublishedMessages =
          failedPublishedMessagesTuple.get(failedMessagesTag).setCoder(input.getCoder());
      failedPublishedMessagesTuple.get(messagesTag).setCoder(input.getCoder());

      return WriteJmsResult.in(input.getPipeline(), failedMessagesTag, failedPublishedMessages);
    }

    private static class JmsConnection<T> implements Serializable {

      private static final long serialVersionUID = 1L;

      private transient @Initialized Session session;
      private transient @Initialized Connection connection;
      private transient @Initialized Destination destination;
      private transient @Initialized MessageProducer producer;

      private final JmsIO.Write<T> spec;
      private final Counter connectionErrors =
          Metrics.counter(JMS_IO_PRODUCER_METRIC_NAME, CONNECTION_ERRORS_METRIC_NAME);

      JmsConnection(Write<T> spec) {
        this.spec = spec;
      }

      void connect() throws JMSException {
        if (this.producer == null) {
          ConnectionFactory connectionFactory = spec.getConnectionFactory();
          if (spec.getUsername() != null) {
            this.connection =
                connectionFactory.createConnection(spec.getUsername(), spec.getPassword());
          } else {
            this.connection = connectionFactory.createConnection();
          }
          this.connection.setExceptionListener(
              exception -> {
                this.connectionErrors.inc();
              });
          this.connection.start();
          // false means we don't use JMS transaction.
          this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

          if (spec.getQueue() != null) {
            this.destination = session.createQueue(spec.getQueue());
          } else if (spec.getTopic() != null) {
            this.destination = session.createTopic(spec.getTopic());
          }
          // Create producer with null destination. Destination will be set with producer.send().
          startProducer();
        }
      }

      void publishMessage(T input) throws JMSException, JmsIOException {
        Destination destinationToSendTo = destination;
        try {
          Message message = spec.getValueMapper().apply(input, session);
          if (spec.getTopicNameMapper() != null) {
            destinationToSendTo = session.createTopic(spec.getTopicNameMapper().apply(input));
          }
          producer.send(destinationToSendTo, message);
        } catch (JMSException | JmsIOException | NullPointerException exception) {
          // Handle NPE in case of getValueMapper or getTopicNameMapper returns NPE
          if (exception instanceof NullPointerException) {
            throw new JmsIOException("An error occurred", exception);
          }
          throw exception;
        }
      }

      void startProducer() throws JMSException {
        this.producer = this.session.createProducer(null);
      }

      void closeProducer() throws JMSException {
        if (producer != null) {
          producer.close();
          producer = null;
        }
      }

      void close() {
        try {
          closeProducer();
          if (session != null) {
            session.close();
          }
          if (connection != null) {
            connection.close();
          }
        } catch (JMSException exception) {
          LOG.warn("The connection couldn't be closed", exception);
        } finally {
          session = null;
          connection = null;
        }
      }
    }

    static class JmsIOProducerFn<T> extends DoFn<T, T> {

      private transient @Initialized FluentBackoff retryBackOff;

      private final JmsIO.Write<T> spec;
      private final TupleTag<T> failedMessagesTags;
      private final @Initialized JmsConnection<T> jmsConnection;
      private final Counter publicationRetries =
          Metrics.counter(JMS_IO_PRODUCER_METRIC_NAME, PUBLICATION_RETRIES_METRIC_NAME);

      JmsIOProducerFn(JmsIO.Write<T> spec, TupleTag<T> failedMessagesTags) {
        this.spec = spec;
        this.failedMessagesTags = failedMessagesTags;
        this.jmsConnection = new JmsConnection<>(spec);
      }

      @Setup
      public void setup() throws JMSException {
        this.jmsConnection.connect();
        RetryConfiguration retryConfiguration =
            MoreObjects.firstNonNull(spec.getRetryConfiguration(), RetryConfiguration.create());
        retryBackOff =
            FluentBackoff.DEFAULT
                .withInitialBackoff(checkStateNotNull(retryConfiguration.getInitialDuration()))
                .withMaxCumulativeBackoff(checkStateNotNull(retryConfiguration.getMaxDuration()))
                .withMaxRetries(retryConfiguration.getMaxAttempts());
      }

      @StartBundle
      public void startBundle() throws JMSException {
        this.jmsConnection.startProducer();
      }

      @ProcessElement
      public void processElement(@Element T input, ProcessContext context) {
        try {
          publishMessage(input);
        } catch (JMSException | JmsIOException | IOException | InterruptedException exception) {
          LOG.error("Error while publishing the message", exception);
          context.output(this.failedMessagesTags, input);
          if (exception instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
        }
      }

      private void publishMessage(T input)
          throws JMSException, JmsIOException, IOException, InterruptedException {
        Sleeper sleeper = Sleeper.DEFAULT;
        BackOff backoff = checkStateNotNull(retryBackOff).backoff();
        while (true) {
          try {
            this.jmsConnection.publishMessage(input);
            break;
          } catch (JMSException | JmsIOException exception) {
            if (!BackOffUtils.next(sleeper, backoff)) {
              throw exception;
            } else {
              publicationRetries.inc();
            }
          }
        }
      }

      @FinishBundle
      public void finishBundle() throws JMSException {
        this.jmsConnection.closeProducer();
      }

      @Teardown
      public void tearDown() {
        this.jmsConnection.close();
      }
    }
  }
}
