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
package org.apache.beam.sdk.io.mqtt;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded source for MQTT broker.
 *
 * <h3>Reading from a MQTT broker</h3>
 *
 * <p>MqttIO source returns an unbounded {@link PCollection} containing MQTT message payloads (as
 * {@code byte[]}).
 *
 * <p>To configure a MQTT source, you have to provide a MQTT connection configuration including
 * {@code ClientId}, a {@code ServerURI}, a {@code Topic} pattern, and optionally {@code username}
 * and {@code password} to connect to the MQTT broker. The following example illustrates various
 * options for configuring the source:
 *
 * <pre>{@code
 * pipeline.apply(
 *   MqttIO.read()
 *    .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
 *      "tcp://host:11883",
 *      "my_topic"))
 *
 * }</pre>
 *
 * <h3>Reading with Metadata from a MQTT broker</h3>
 *
 * <p>The {@code readWithMetadata} method extends the functionality of the basic {@code read} method
 * by returning a {@link PCollection} of metadata that includes both the topic name and the payload.
 * The metadata is encapsulated in a container class {@link MqttRecord} that includes the topic name
 * and payload. This allows you to implement business logic that can differ depending on the topic
 * from which the message was received.
 *
 * <pre>{@code
 * pipeline.apply(
 *   MqttIO.readWithMetadata()
 *    .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
 *      "tcp://host:11883",
 *      "my_topic_pattern"))
 *
 * }</pre>
 *
 * <p>By using the topic information, you can apply different processing logic depending on the
 * source topic, enhancing the flexibility of message processing.
 *
 * <h4>Example</h4>
 *
 * <pre>{@code
 * pipeline
 *   .apply(MqttIO.readWithMetadata()
 *     .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
 *       "tcp://localhost:1883", "my_topic_pattern")))
 *   .apply(ParDo.of(new DoFn<MqttRecord, Void>() {
 *     @ProcessElement
 *     public void processElement(ProcessContext c) {
 *       MqttRecord record = c.element();
 *       String topic = record.getTopic();
 *       byte[] payload = record.getPayload();
 *       // Apply business logic based on the topic
 *       if (topic.equals("important_topic")) {
 *         // Special processing for important_topic
 *       }
 *     }
 *   }));
 *
 * }</pre>
 *
 * <h3>Writing to a MQTT broker</h3>
 *
 * <p>MqttIO sink supports writing {@code byte[]} to a topic on a MQTT broker.
 *
 * <p>To configure a MQTT sink, as for the read, you have to specify a MQTT connection configuration
 * with {@code ServerURI}, {@code Topic}, ...
 *
 * <p>The MqttIO only fully supports QoS 1 (at least once). It's the only QoS level guaranteed due
 * to potential retries on bundles.
 *
 * <p>For instance:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...) // provide PCollection<byte[]>
 *   .MqttIO.write()
 *     .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
 *       "tcp://host:11883",
 *       "my_topic"))
 *
 * }</pre>
 *
 * <h3>Dynamic Writing to a MQTT Broker</h3>
 *
 * <p>MqttIO also supports dynamic writing to multiple topics based on the data. You can specify a
 * function to determine the target topic for each message. The following example demonstrates how
 * to configure dynamic topic writing:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...)  // Provide PCollection<InputT>
 *   .apply(
 *     MqttIO.<InputT>dynamicWrite()
 *       .withConnectionConfiguration(
 *         MqttIO.ConnectionConfiguration.create("tcp://host:11883"))
 *       .withTopicFn(<Function to determine the topic dynamically>)
 *       .withPayloadFn(<Function to extract the payload>));
 * }</pre>
 *
 * <p>This dynamic writing capability allows for more flexible MQTT message routing based on the
 * message content, enabling scenarios where messages are directed to different topics.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class MqttIO {

  private static final Logger LOG = LoggerFactory.getLogger(MqttIO.class);
  private static final int MQTT_3_1_MAX_CLIENT_ID_LENGTH = 23;

  public static Read<byte[]> read() {
    return new AutoValue_MqttIO_Read.Builder<byte[]>()
        .setMaxReadTime(null)
        .setWithMetadata(false)
        .setMaxNumRecords(Long.MAX_VALUE)
        .build();
  }

  public static Read<MqttRecord> readWithMetadata() {
    return new AutoValue_MqttIO_Read.Builder<MqttRecord>()
        .setMaxReadTime(null)
        .setWithMetadata(true)
        .setMaxNumRecords(Long.MAX_VALUE)
        .build();
  }

  public static Write<byte[]> write() {
    return new AutoValue_MqttIO_Write.Builder<byte[]>()
        .setRetained(false)
        .setPayloadFn(SerializableFunctions.identity())
        .setDynamic(false)
        .build();
  }

  public static <InputT> Write<InputT> dynamicWrite() {
    return new AutoValue_MqttIO_Write.Builder<InputT>().setRetained(false).setDynamic(true).build();
  }

  private MqttIO() {}

  /** A POJO describing a MQTT connection. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    abstract String getServerUri();

    abstract @Nullable String getTopic();

    abstract @Nullable String getClientId();

    abstract @Nullable String getUsername();

    abstract @Nullable String getPassword();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setServerUri(String serverUri);

      abstract Builder setTopic(String topic);

      abstract Builder setClientId(String clientId);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract ConnectionConfiguration build();
    }

    /**
     * Describe a connection configuration to the MQTT broker. This method creates a unique random
     * MQTT client ID.
     *
     * @param serverUri The MQTT broker URI.
     * @param topic The MQTT getTopic pattern.
     * @return A connection configuration to the MQTT broker.
     */
    public static ConnectionConfiguration create(String serverUri, String topic) {
      checkArgument(serverUri != null, "serverUri can not be null");
      checkArgument(topic != null, "topic can not be null");
      return new AutoValue_MqttIO_ConnectionConfiguration.Builder()
          .setServerUri(serverUri)
          .setTopic(topic)
          .build();
    }

    public static ConnectionConfiguration create(String serverUri) {
      checkArgument(serverUri != null, "serverUri can not be null");
      return new AutoValue_MqttIO_ConnectionConfiguration.Builder().setServerUri(serverUri).build();
    }

    /** Set up the MQTT broker URI. */
    public ConnectionConfiguration withServerUri(String serverUri) {
      checkArgument(serverUri != null, "serverUri can not be null");
      return builder().setServerUri(serverUri).build();
    }

    /** Set up the MQTT getTopic pattern. */
    public ConnectionConfiguration withTopic(String topic) {
      checkArgument(topic != null, "topic can not be null");
      return builder().setTopic(topic).build();
    }

    /** Set up the client ID prefix, which is used to construct a unique client ID. */
    public ConnectionConfiguration withClientId(String clientId) {
      checkArgument(clientId != null, "clientId can not be null");
      return builder().setClientId(clientId).build();
    }

    public ConnectionConfiguration withUsername(String username) {
      checkArgument(username != null, "username can not be null");
      return builder().setUsername(username).build();
    }

    public ConnectionConfiguration withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return builder().setPassword(password).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("serverUri", getServerUri()));
      builder.addIfNotNull(DisplayData.item("topic", getTopic()));
      builder.addIfNotNull(DisplayData.item("clientId", getClientId()));
      builder.addIfNotNull(DisplayData.item("username", getUsername()));
    }

    private MQTT createClient() throws Exception {
      LOG.debug("Creating MQTT client to {}", getServerUri());
      MQTT client = new MQTT();
      client.setHost(getServerUri());
      if (getUsername() != null) {
        LOG.debug("MQTT client uses username {}", getUsername());
        client.setUserName(getUsername());
        client.setPassword(getPassword());
      }
      if (getClientId() != null) {
        String clientId = getClientId() + "-" + UUID.randomUUID().toString();
        clientId =
            clientId.substring(0, Math.min(clientId.length(), MQTT_3_1_MAX_CLIENT_ID_LENGTH));
        LOG.debug("MQTT client id set to {}", clientId);
        client.setClientId(clientId);
      } else {
        String clientId = UUID.randomUUID().toString();
        clientId =
            clientId.substring(0, Math.min(clientId.length(), MQTT_3_1_MAX_CLIENT_ID_LENGTH));
        LOG.debug("MQTT client id set to random value {}", clientId);
        client.setClientId(clientId);
      }
      return client;
    }
  }

  /** A {@link PTransform} to read from a MQTT broker. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract @Nullable ConnectionConfiguration connectionConfiguration();

    abstract long maxNumRecords();

    abstract @Nullable Duration maxReadTime();

    abstract Builder<T> builder();

    abstract boolean withMetadata();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConnectionConfiguration(ConnectionConfiguration config);

      abstract Builder<T> setMaxNumRecords(long maxNumRecords);

      abstract Builder<T> setMaxReadTime(Duration maxReadTime);

      abstract Builder<T> setWithMetadata(boolean withMetadata);

      abstract Read<T> build();
    }

    /** Define the MQTT connection configuration used to connect to the MQTT broker. */
    public Read<T> withConnectionConfiguration(ConnectionConfiguration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return builder().setConnectionConfiguration(configuration).build();
    }

    /**
     * Define the max number of records received by the {@link Read}. When this max number of
     * records is lower than {@code Long.MAX_VALUE}, the {@link Read} will provide a bounded {@link
     * PCollection}.
     */
    public Read<T> withMaxNumRecords(long maxNumRecords) {
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time (duration) while the {@link Read} will receive messages. When this
     * max read time is not null, the {@link Read} will provide a bounded {@link PCollection}.
     */
    public Read<T> withMaxReadTime(Duration maxReadTime) {
      return builder().setMaxReadTime(maxReadTime).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument(connectionConfiguration() != null, "connectionConfiguration can not be null");
      checkArgument(connectionConfiguration().getTopic() != null, "topic can not be null");

      org.apache.beam.sdk.io.Read.Unbounded<T> unbounded =
          org.apache.beam.sdk.io.Read.from(new UnboundedMqttSource<>(this));

      PTransform<PBegin, PCollection<T>> transform = unbounded;

      if (maxNumRecords() < Long.MAX_VALUE || maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime()).withMaxNumRecords(maxNumRecords());
      }

      return input.getPipeline().apply(transform);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      connectionConfiguration().populateDisplayData(builder);
      if (maxNumRecords() != Long.MAX_VALUE) {
        builder.add(DisplayData.item("maxNumRecords", maxNumRecords()));
      }
      builder.addIfNotNull(DisplayData.item("maxReadTime", maxReadTime()));
    }
  }

  /**
   * Checkpoint for an unbounded MQTT source. Consists of the MQTT messages waiting to be
   * acknowledged and oldest pending message timestamp.
   */
  @VisibleForTesting
  static class MqttCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

    @VisibleForTesting String clientId;
    @VisibleForTesting Instant oldestMessageTimestamp = Instant.now();
    @VisibleForTesting transient List<Message> messages = new ArrayList<>();

    public MqttCheckpointMark() {}

    public MqttCheckpointMark(String id) {
      clientId = id;
    }

    public void add(Message message, Instant timestamp) {
      if (timestamp.isBefore(oldestMessageTimestamp)) {
        oldestMessageTimestamp = timestamp;
      }
      messages.add(message);
    }

    @Override
    public void finalizeCheckpoint() {
      LOG.debug("Finalizing checkpoint acknowledging pending messages for client ID {}", clientId);
      for (Message message : messages) {
        try {
          message.ack();
        } catch (Exception e) {
          LOG.warn("Can't ack message for client ID {}", clientId, e);
        }
      }
      oldestMessageTimestamp = Instant.now();
      messages.clear();
    }

    // set an empty list to messages when deserialize
    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
      stream.defaultReadObject();
      messages = new ArrayList<>();
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other instanceof MqttCheckpointMark) {
        MqttCheckpointMark that = (MqttCheckpointMark) other;
        return Objects.equals(this.clientId, that.clientId)
            && Objects.equals(this.oldestMessageTimestamp, that.oldestMessageTimestamp)
            && Objects.deepEquals(this.messages, that.messages);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(clientId, oldestMessageTimestamp, messages);
    }
  }

  @VisibleForTesting
  static class UnboundedMqttSource<T> extends UnboundedSource<T, MqttCheckpointMark> {

    private final Read<T> spec;

    public UnboundedMqttSource(Read<T> spec) {
      this.spec = spec;
    }

    @Override
    @SuppressWarnings("unchecked")
    public UnboundedReader<T> createReader(
        PipelineOptions options, MqttCheckpointMark checkpointMark) {
      final UnboundedMqttReader<T> unboundedMqttReader;
      if (spec.withMetadata()) {
        unboundedMqttReader =
            new UnboundedMqttReader<>(
                this,
                checkpointMark,
                message -> (T) new MqttRecord(message.getTopic(), message.getPayload()));
      } else {
        unboundedMqttReader = new UnboundedMqttReader<>(this, checkpointMark);
      }

      return unboundedMqttReader;
    }

    @Override
    public List<UnboundedMqttSource<T>> split(int desiredNumSplits, PipelineOptions options) {
      // MQTT is based on a pub/sub pattern
      // so, if we create several subscribers on the same topic, they all will receive the same
      // message, resulting to duplicate messages in the PCollection.
      // So, for MQTT, we limit to number of split ot 1 (unique source).
      return Collections.singletonList(new UnboundedMqttSource<>(spec));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
    }

    @Override
    public Coder<MqttCheckpointMark> getCheckpointMarkCoder() {
      return SerializableCoder.of(MqttCheckpointMark.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Coder<T> getOutputCoder() {
      if (spec.withMetadata()) {
        return (Coder<T>) MqttRecordCoder.of();
      } else {
        return (Coder<T>) ByteArrayCoder.of();
      }
    }
  }

  @VisibleForTesting
  static class UnboundedMqttReader<T> extends UnboundedSource.UnboundedReader<T> {

    private final UnboundedMqttSource<T> source;

    private MQTT client;
    private BlockingConnection connection;
    private T current;
    private Instant currentTimestamp;
    private MqttCheckpointMark checkpointMark;
    private SerializableFunction<Message, T> extractFn;

    public UnboundedMqttReader(UnboundedMqttSource<T> source, MqttCheckpointMark checkpointMark) {
      this.source = source;
      this.current = null;
      if (checkpointMark != null) {
        this.checkpointMark = checkpointMark;
      } else {
        this.checkpointMark = new MqttCheckpointMark();
      }
      this.extractFn = message -> (T) message.getPayload();
    }

    public UnboundedMqttReader(
        UnboundedMqttSource<T> source,
        MqttCheckpointMark checkpointMark,
        SerializableFunction<Message, T> extractFn) {
      this(source, checkpointMark);
      this.extractFn = extractFn;
    }

    @Override
    public boolean start() throws IOException {
      LOG.debug("Starting MQTT reader ...");
      Read<T> spec = source.spec;
      try {
        client = spec.connectionConfiguration().createClient();
        LOG.debug("Reader client ID is {}", client.getClientId());
        checkpointMark.clientId = client.getClientId().toString();
        connection = createConnection(client);
        connection.subscribe(
            new Topic[] {new Topic(spec.connectionConfiguration().getTopic(), QoS.AT_LEAST_ONCE)});
        return advance();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean advance() throws IOException {
      try {
        LOG.trace("MQTT reader (client ID {}) waiting message ...", client.getClientId());
        Message message = connection.receive(1, TimeUnit.SECONDS);
        if (message == null) {
          return false;
        }
        current = this.extractFn.apply(message);
        currentTimestamp = Instant.now();
        checkpointMark.add(message, currentTimestamp);
      } catch (Exception e) {
        throw new IOException(e);
      }
      return true;
    }

    @Override
    public void close() throws IOException {
      LOG.debug("Closing MQTT reader (client ID {})", client.getClientId());
      try {
        if (connection != null) {
          connection.disconnect();
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public Instant getWatermark() {
      return checkpointMark.oldestMessageTimestamp;
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return checkpointMark;
    }

    @Override
    public T getCurrent() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return currentTimestamp;
    }

    @Override
    public UnboundedMqttSource<T> getCurrentSource() {
      return source;
    }
  }

  /** A {@link PTransform} to write and send a message to a MQTT server. */
  @AutoValue
  public abstract static class Write<InputT> extends PTransform<PCollection<InputT>, PDone> {
    abstract @Nullable ConnectionConfiguration connectionConfiguration();

    abstract @Nullable SerializableFunction<InputT, String> topicFn();

    abstract @Nullable SerializableFunction<InputT, byte[]> payloadFn();

    abstract boolean dynamic();

    abstract boolean retained();

    abstract Builder<InputT> builder();

    @AutoValue.Builder
    abstract static class Builder<InputT> {
      abstract Builder<InputT> setConnectionConfiguration(ConnectionConfiguration configuration);

      abstract Builder<InputT> setRetained(boolean retained);

      abstract Builder<InputT> setTopicFn(SerializableFunction<InputT, String> topicFn);

      abstract Builder<InputT> setPayloadFn(SerializableFunction<InputT, byte[]> payloadFn);

      abstract Builder<InputT> setDynamic(boolean dynamic);

      abstract Write<InputT> build();
    }

    /** Define MQTT connection configuration used to connect to the MQTT broker. */
    public Write<InputT> withConnectionConfiguration(ConnectionConfiguration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return builder().setConnectionConfiguration(configuration).build();
    }

    public Write<InputT> withTopicFn(SerializableFunction<InputT, String> topicFn) {
      checkArgument(dynamic(), "withTopicFn can not use in non-dynamic write");
      return builder().setTopicFn(topicFn).build();
    }

    public Write<InputT> withPayloadFn(SerializableFunction<InputT, byte[]> payloadFn) {
      checkArgument(dynamic(), "withPayloadFn can not use in non-dynamic write");
      return builder().setPayloadFn(payloadFn).build();
    }

    /**
     * Whether or not the publish message should be retained by the messaging engine. Sending a
     * message with the retained set to {@code false} will clear the retained message from the
     * server. The default value is {@code false}. When a subscriber connects, it gets the latest
     * retained message (else it doesn't get any existing message, it will have to wait a new
     * incoming message).
     *
     * @param retained Whether or not the messaging engine should retain the message.
     * @return The {@link Write} {@link PTransform} with the corresponding retained configuration.
     */
    public Write<InputT> withRetained(boolean retained) {
      return builder().setRetained(retained).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      connectionConfiguration().populateDisplayData(builder);
      builder.add(DisplayData.item("retained", retained()));
    }

    @Override
    public PDone expand(PCollection<InputT> input) {
      checkArgument(connectionConfiguration() != null, "connectionConfiguration can not be null");
      if (dynamic()) {
        checkArgument(
            connectionConfiguration().getTopic() == null, "DynamicWrite can not have static topic");
        checkArgument(topicFn() != null, "topicFn can not be null");
      } else {
        checkArgument(connectionConfiguration().getTopic() != null, "topic can not be null");
      }
      checkArgument(payloadFn() != null, "payloadFn can not be null");

      input.apply(ParDo.of(new WriteFn<>(this)));
      return PDone.in(input.getPipeline());
    }

    private static class WriteFn<InputT> extends DoFn<InputT, Void> {

      private final Write<InputT> spec;
      private final SerializableFunction<InputT, String> topicFn;
      private final SerializableFunction<InputT, byte[]> payloadFn;
      private final boolean retained;

      private transient MQTT client;
      private transient BlockingConnection connection;

      public WriteFn(Write<InputT> spec) {
        this.spec = spec;
        if (spec.dynamic()) {
          this.topicFn = spec.topicFn();
        } else {
          String topic = spec.connectionConfiguration().getTopic();
          this.topicFn = ignore -> topic;
        }
        this.payloadFn = spec.payloadFn();
        this.retained = spec.retained();
      }

      @Setup
      public void createMqttClient() throws Exception {
        LOG.debug("Starting MQTT writer");
        this.client = this.spec.connectionConfiguration().createClient();
        LOG.debug("MQTT writer client ID is {}", client.getClientId());
        this.connection = createConnection(client);
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        InputT element = context.element();
        byte[] payload = this.payloadFn.apply(element);
        String topic = this.topicFn.apply(element);
        LOG.debug("Sending message {}", new String(payload, StandardCharsets.UTF_8));
        this.connection.publish(topic, payload, QoS.AT_LEAST_ONCE, this.retained);
      }

      @Teardown
      public void closeMqttClient() throws Exception {
        if (this.connection != null) {
          LOG.debug("Disconnecting MQTT connection (client ID {})", client.getClientId());
          this.connection.disconnect();
        }
      }
    }
  }

  /** Create a connected MQTT BlockingConnection from given client, aware of connection timeout. */
  static BlockingConnection createConnection(MQTT client) throws Exception {
    FutureConnection futureConnection = client.futureConnection();
    org.fusesource.mqtt.client.Future<Void> connecting = futureConnection.connect();
    while (true) {
      try {
        connecting.await(1, TimeUnit.MINUTES);
      } catch (TimeoutException e) {
        LOG.warn("Connection to {} pending after waiting for 1 minute", client.getHost());
        continue;
      }
      break;
    }
    return new BlockingConnection(futureConnection);
  }
}
