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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.fusesource.mqtt.client.BlockingConnection;
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
 */
@Experimental(Kind.SOURCE_SINK)
public class MqttIO {

  private static final Logger LOG = LoggerFactory.getLogger(MqttIO.class);
  private static final int MQTT_3_1_MAX_CLIENT_ID_LENGTH = 23;

  public static Read read() {
    return new AutoValue_MqttIO_Read.Builder()
        .setMaxReadTime(null)
        .setMaxNumRecords(Long.MAX_VALUE)
        .build();
  }

  public static Write write() {
    return new AutoValue_MqttIO_Write.Builder().setRetained(false).build();
  }

  private MqttIO() {}

  /** A POJO describing a MQTT connection. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    abstract String getServerUri();

    abstract String getTopic();

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

    /**
     * Describe a connection configuration to the MQTT broker.
     *
     * @param serverUri The MQTT broker URI.
     * @param topic The MQTT getTopic pattern.
     * @param clientId A client ID prefix, used to construct a unique client ID.
     * @return A connection configuration to the MQTT broker.
     * @deprecated This constructor will be removed in a future version of Beam, please use
     *     #create(String, String)} and {@link #withClientId(String)} instead.
     */
    @Deprecated
    public static ConnectionConfiguration create(String serverUri, String topic, String clientId) {
      checkArgument(clientId != null, "clientId can not be null");
      return create(serverUri, topic).withClientId(clientId);
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
      builder.add(DisplayData.item("topic", getTopic()));
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
  public abstract static class Read extends PTransform<PBegin, PCollection<byte[]>> {

    abstract @Nullable ConnectionConfiguration connectionConfiguration();

    abstract long maxNumRecords();

    abstract @Nullable Duration maxReadTime();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration config);

      abstract Builder setMaxNumRecords(long maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Read build();
    }

    /** Define the MQTT connection configuration used to connect to the MQTT broker. */
    public Read withConnectionConfiguration(ConnectionConfiguration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return builder().setConnectionConfiguration(configuration).build();
    }

    /**
     * Define the max number of records received by the {@link Read}. When this max number of
     * records is lower than {@code Long.MAX_VALUE}, the {@link Read} will provide a bounded {@link
     * PCollection}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time (duration) while the {@link Read} will receive messages. When this
     * max read time is not null, the {@link Read} will provide a bounded {@link PCollection}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      return builder().setMaxReadTime(maxReadTime).build();
    }

    @Override
    public PCollection<byte[]> expand(PBegin input) {
      org.apache.beam.sdk.io.Read.Unbounded<byte[]> unbounded =
          org.apache.beam.sdk.io.Read.from(new UnboundedMqttSource(this));

      PTransform<PBegin, PCollection<byte[]>> transform = unbounded;

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
  static class UnboundedMqttSource extends UnboundedSource<byte[], MqttCheckpointMark> {

    private final Read spec;

    public UnboundedMqttSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public UnboundedReader<byte[]> createReader(
        PipelineOptions options, MqttCheckpointMark checkpointMark) {
      return new UnboundedMqttReader(this, checkpointMark);
    }

    @Override
    public List<UnboundedMqttSource> split(int desiredNumSplits, PipelineOptions options) {
      // MQTT is based on a pub/sub pattern
      // so, if we create several subscribers on the same topic, they all will receive the same
      // message, resulting to duplicate messages in the PCollection.
      // So, for MQTT, we limit to number of split ot 1 (unique source).
      return Collections.singletonList(new UnboundedMqttSource(spec));
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
    public Coder<byte[]> getOutputCoder() {
      return ByteArrayCoder.of();
    }
  }

  @VisibleForTesting
  static class UnboundedMqttReader extends UnboundedSource.UnboundedReader<byte[]> {

    private final UnboundedMqttSource source;

    private MQTT client;
    private BlockingConnection connection;
    private byte[] current;
    private Instant currentTimestamp;
    private MqttCheckpointMark checkpointMark;

    public UnboundedMqttReader(UnboundedMqttSource source, MqttCheckpointMark checkpointMark) {
      this.source = source;
      this.current = null;
      if (checkpointMark != null) {
        this.checkpointMark = checkpointMark;
      } else {
        this.checkpointMark = new MqttCheckpointMark();
      }
    }

    @Override
    public boolean start() throws IOException {
      LOG.debug("Starting MQTT reader ...");
      Read spec = source.spec;
      try {
        client = spec.connectionConfiguration().createClient();
        LOG.debug("Reader client ID is {}", client.getClientId());
        checkpointMark.clientId = client.getClientId().toString();
        connection = client.blockingConnection();
        connection.connect();
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
        current = message.getPayload();
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
    public byte[] getCurrent() {
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
    public UnboundedMqttSource getCurrentSource() {
      return source;
    }
  }

  /** A {@link PTransform} to write and send a message to a MQTT server. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<byte[]>, PDone> {

    abstract @Nullable ConnectionConfiguration connectionConfiguration();

    abstract boolean retained();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration configuration);

      abstract Builder setRetained(boolean retained);

      abstract Write build();
    }

    /** Define MQTT connection configuration used to connect to the MQTT broker. */
    public Write withConnectionConfiguration(ConnectionConfiguration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return builder().setConnectionConfiguration(configuration).build();
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
    public Write withRetained(boolean retained) {
      return builder().setRetained(retained).build();
    }

    @Override
    public PDone expand(PCollection<byte[]> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      connectionConfiguration().populateDisplayData(builder);
      builder.add(DisplayData.item("retained", retained()));
    }

    private static class WriteFn extends DoFn<byte[], Void> {

      private final Write spec;

      private transient MQTT client;
      private transient BlockingConnection connection;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void createMqttClient() throws Exception {
        LOG.debug("Starting MQTT writer");
        client = spec.connectionConfiguration().createClient();
        LOG.debug("MQTT writer client ID is {}", client.getClientId());
        connection = client.blockingConnection();
        connection.connect();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        byte[] payload = context.element();
        LOG.debug("Sending message {}", new String(payload, StandardCharsets.UTF_8));
        connection.publish(
            spec.connectionConfiguration().getTopic(), payload, QoS.AT_LEAST_ONCE, false);
      }

      @Teardown
      public void closeMqttClient() throws Exception {
        if (connection != null) {
          LOG.debug("Disconnecting MQTT connection (client ID {})", client.getClientId());
          connection.disconnect();
        }
      }
    }
  }
}
