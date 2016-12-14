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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded source for MQTT broker.
 *
 * <h3>Reading from a MQTT broker</h3>
 *
 * <p>MqttIO source returns an unbounded {@link PCollection} containing MQTT message
 * payloads (as {@code byte[]}).
 *
 * <p>To configure a MQTT source, you have to provide a MQTT connection configuration including
 * {@code ClientId}, a {@code ServerURI}, and a {@code Topic} pattern. The following
 * example illustrates various options for configuring the source:
 *
 * <pre>{@code
 *
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
 * <p>To configure a MQTT sink, as for the read, you have to specify a MQTT connection
 * configuration with {@code ClientId}, {@code ServerURI}, {@code Topic}.
 *
 * <p>The MqttIO only fully supports QoS 1 (at least once). It's the only QoS level guaranteed
 * due to potential retries on bundles.
 *
 * <p>For instance:
 *
 * <pre>{@code
 *
 * pipeline
 *   .apply(...) // provide PCollection<byte[]>
 *   .MqttIO.write()
 *     .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
 *       "tcp://host:11883",
 *       "my_topic"))
 *
 * }</pre>
 */
public class MqttIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(MqttIO.class);

  public static Read read() {
    return new AutoValue_MqttIO_Read.Builder()
        .setMaxReadTime(null).setMaxNumRecords(Long.MAX_VALUE).build();
  }

  public static Write write() {
    return new AutoValue_MqttIO_Write.Builder()
        .setRetained(false)
        .build();
  }

  private MqttIO() {
  }

  /**
   * A POJO describing a MQTT connection.
   */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    @Nullable abstract String serverUri();
    @Nullable abstract String topic();
    @Nullable abstract String clientId();

    public static ConnectionConfiguration create(String serverUri, String topic) {
      checkArgument(serverUri != null,
          "MqttIO.ConnectionConfiguration.create(serverUri, topic) called with null "
              + "serverUri");
      checkArgument(topic != null,
          "MqttIO.ConnectionConfiguration.create(serverUri, topic) called with null "
              + "topic");
      return new AutoValue_MqttIO_ConnectionConfiguration(serverUri, topic,
          MqttClient.generateClientId());
    }

    public static ConnectionConfiguration create(String serverUri, String topic, String clientId) {
      checkArgument(serverUri != null,
          "MqttIO.ConnectionConfiguration.create(serverUri, topic) called with null "
              + "serverUri");
      checkArgument(topic != null,
          "MqttIO.ConnectionConfiguration.create(serverUri, topic) called with null "
              + "topic");
      checkArgument(clientId != null, "MqttIO.ConnectionConfiguration.create(serverUri, topic, "
          + "clientId) called with null clientId");
      return new AutoValue_MqttIO_ConnectionConfiguration(serverUri, topic, clientId);
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("serverUri", serverUri()));
      builder.add(DisplayData.item("topic", topic()));
      builder.add(DisplayData.item("clientId", clientId()));
    }

    private MqttClient getClient(boolean random) throws MqttException {
      String id = clientId();
      if (random) {
        id = clientId() + "-" + MqttClient.generateClientId();
      }
      MqttClient client = new MqttClient(serverUri(), id);
      client.connect();
      return client;
    }

    private MqttClient getClient() throws MqttException {
      return getClient(false);
    }

  }

  /**
   * A {@link PTransform} to read from a MQTT broker.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<byte[]>> {

    @Nullable abstract ConnectionConfiguration connectionConfiguration();
    abstract long maxNumRecords();
    @Nullable abstract Duration maxReadTime();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration config);
      abstract Builder setMaxNumRecords(long maxNumRecords);
      abstract Builder setMaxReadTime(Duration maxReadTime);
      abstract Read build();
    }

    /**
     * Define the MQTT connection configuration used to connect to the MQTT broker.
     */
    public Read withConnectionConfiguration(ConnectionConfiguration configuration) {
      checkArgument(configuration != null,
          "MqttIO.read().withConnectionConfiguration(configuration) called with null "
              + "configuration or not called at all");
      return builder().setConnectionConfiguration(configuration).build();
    }

    /**
     * Define the max number of records received by the {@link Read}.
     * When this max number of records is lower than {@code Long.MAX_VALUE}, the {@link Read}
     * will provide a bounded {@link PCollection}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      checkArgument(maxReadTime() == null,
          "maxNumRecord and maxReadTime are exclusive");
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time (duration) while the {@link Read} will receive messages.
     * When this max read time is not null, the {@link Read} will provide a bounded
     * {@link PCollection}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      checkArgument(maxNumRecords() == Long.MAX_VALUE,
          "maxNumRecord and maxReadTime are exclusive");
      return builder().setMaxReadTime(maxReadTime).build();
    }

    @Override
    public PCollection<byte[]> expand(PBegin input) {

      org.apache.beam.sdk.io.Read.Unbounded<byte[]> unbounded =
          org.apache.beam.sdk.io.Read.from(new UnboundedMqttSource(this));

      PTransform<PBegin, PCollection<byte[]>> transform = unbounded;

      if (maxNumRecords() != Long.MAX_VALUE) {
        transform = unbounded.withMaxNumRecords(maxNumRecords());
      } else if (maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime());
      }

      return input.getPipeline().apply(transform);
    }

    @Override
    public void validate(PBegin input) {
      // validation is performed in the ConnectionConfiguration create()
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
  private static class MqttCheckpointMark implements UnboundedSource.CheckpointMark {

    private transient MqttClient client;
    private Instant oldestMessageTimestamp = Instant.now();

    private static class MessageIdWithQos {

      private int id;
      private int qos;

      MessageIdWithQos(int id, int qos) {
        this.id = id;
        this.qos = qos;
      }

    }

    private final List<MessageIdWithQos> messages = new ArrayList<>();

    public MqttCheckpointMark() {
    }

    public void setClient(MqttClient client) {
      this.client = client;
    }

    public void add(int id, int qos, Instant timestamp) {
      if (timestamp.isBefore(oldestMessageTimestamp)) {
        oldestMessageTimestamp = timestamp;
      }
      MessageIdWithQos message = new MessageIdWithQos(id, qos);
      messages.add(message);
    }

    @Override
    public void finalizeCheckpoint() {
      for (MessageIdWithQos message : messages) {
        try {
          client.messageArrivedComplete(message.id, message.qos);
        } catch (Exception e) {
          LOGGER.warn("Can't ack message {} with QoS {}", message.id, message.qos);
        }
      }
      oldestMessageTimestamp = Instant.now();
      messages.clear();
    }

  }

  private static class UnboundedMqttSource
      extends UnboundedSource<byte[], MqttCheckpointMark> {

    private final Read spec;

    public UnboundedMqttSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public UnboundedReader<byte[]> createReader(PipelineOptions options,
                                        MqttCheckpointMark checkpointMark) {
      return new UnboundedMqttReader(this, checkpointMark);
    }

    @Override
    public List<UnboundedMqttSource> generateInitialSplits(int desiredNumSplits,
                                                           PipelineOptions options) {
      // MQTT is based on a pub/sub pattern
      // so, if we create several subscribers on the same topic, they all will receive the same
      // message, resulting to duplicate messages in the PCollection.
      // So, for MQTT, we limit to number of split ot 1 (unique source).
      return Collections.singletonList(new UnboundedMqttSource(spec));
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
    }

    @Override
    public Coder<MqttCheckpointMark> getCheckpointMarkCoder() {
      return AvroCoder.of(MqttCheckpointMark.class);
    }

    @Override
    public Coder<byte[]> getDefaultOutputCoder() {
      return ByteArrayCoder.of();
    }
  }

  /**
   * POJO used to store MQTT message and its timestamp.
   */
  private static class MqttMessageWithTimestamp {

    private final MqttMessage message;
    private final Instant timestamp;

    public MqttMessageWithTimestamp(MqttMessage message, Instant timestamp) {
      this.message = message;
      this.timestamp = timestamp;
    }

    public MqttMessage getMessage() {
      return message;
    }

    public Instant getTimestamp() {
      return timestamp;
    }
  }

  private static class UnboundedMqttReader extends UnboundedSource.UnboundedReader<byte[]> {

    private final UnboundedMqttSource source;

    private MqttClient client;
    private byte[] current;
    private Instant currentTimestamp;
    private MqttCheckpointMark checkpointMark;
    private BlockingQueue<MqttMessageWithTimestamp> queue;

    public UnboundedMqttReader(UnboundedMqttSource source, MqttCheckpointMark checkpointMark) {
      this.source = source;
      this.current = null;
      if (checkpointMark != null) {
        this.checkpointMark = checkpointMark;
      } else {
        this.checkpointMark = new MqttCheckpointMark();
      }
      this.queue = new LinkedBlockingQueue<>();
    }

    @Override
    public boolean start() throws IOException {
      LOGGER.debug("Starting MQTT reader");
      Read spec = source.spec;
      try {
        client = spec.connectionConfiguration().getClient();
        client.subscribe(spec.connectionConfiguration().topic());
        client.setManualAcks(true);
        client.setCallback(new MqttCallback() {
          @Override
          public void connectionLost(Throwable cause) {
            LOGGER.warn("MQTT connection lost", cause);
            cause.printStackTrace();
          }

          @Override
          public void messageArrived(String topic, MqttMessage message) throws Exception {
            LOGGER.trace("Message arrived");
            queue.put(new MqttMessageWithTimestamp(message, Instant.now()));
          }

          @Override
          public void deliveryComplete(IMqttDeliveryToken token) {
            // nothing to do
          }
        });
        checkpointMark.setClient(client);
        return advance();
      } catch (MqttException e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean advance() throws IOException {
      LOGGER.debug("Taking from the pending queue ({})", queue.size());
      MqttMessageWithTimestamp message = null;
      try {
        message = queue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      if (message == null) {
        current = null;
        currentTimestamp = null;
        return false;
      }

      checkpointMark.add(message.getMessage().getId(),
          message.getMessage().getQos(),
          message.timestamp);

      current = message.message.getPayload();
      currentTimestamp = message.timestamp;

      return true;
    }

    @Override
    public void close() throws IOException {
      LOGGER.debug("Closing MQTT reader");
      try {
        if (client != null) {
          try {
            client.disconnect();
          } finally {
            client.close();
          }
        }
      } catch (MqttException mqttException) {
        throw new IOException(mqttException);
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

  /**
   * A {@link PTransform} to write and send a message to a MQTT server.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<byte[]>, PDone> {

    @Nullable abstract ConnectionConfiguration connectionConfiguration();
    @Nullable abstract boolean retained();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration configuration);
      abstract Builder setRetained(boolean retained);
      abstract Write build();
    }

    /**
     * Define MQTT connection configuration used to connect to the MQTT broker.
     */
    public Write withConnectionConfiguration(ConnectionConfiguration configuration) {
      checkArgument(configuration != null,
          "MqttIO.write().withConnectionConfiguration(configuration) called with null "
              + "configuration or not called at all");
      return builder().setConnectionConfiguration(configuration).build();
    }

    /**
     * Whether or not the publish message should be retained by the messaging engine.
     * Sending a message with the retained set to {@code false} will clear the
     * retained message from the server. The default value is {@code false}.
     * When a subscriber connects, it gets the latest retained message (else it doesn't get any
     * existing message, it will have to wait a new incoming message).
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
    public void validate(PCollection<byte[]> input) {
      // validate is done in connection configuration
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      connectionConfiguration().populateDisplayData(builder);
      builder.add(DisplayData.item("retained", retained()));
    }

    private static class WriteFn extends DoFn<byte[], Void> {

      private final Write spec;

      private transient MqttClient client;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void createMqttClient() throws Exception {
        client = spec.connectionConfiguration().getClient(true);
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        byte[] payload = context.element();
        MqttMessage message = new MqttMessage();
        message.setQos(1);
        message.setRetained(spec.retained());
        message.setPayload(payload);
        client.publish(spec.connectionConfiguration().topic(), message);
      }

      @Teardown
      public void closeMqttClient() throws Exception {
        if (client != null) {
          try {
            client.disconnect();
          } finally {
            client.close();
          }
        }
      }

    }

  }

}
