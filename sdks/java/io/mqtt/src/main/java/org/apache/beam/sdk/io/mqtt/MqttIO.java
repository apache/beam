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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VoidCoder;
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
 * <p>MqttIO source returns an unbounded collection of {@code byte[]} as
 * {@code PCollection<byte[]>}, where {@code byte[]} is the MQTT message payload.</p>
 *
 * <p>To configure a MQTT source, you have to provide a MQTT connection configuration including
 * {@code ClientId}, a {@code ServerURI}, and eventually a {@code Topic} pattern. The following
 * example illustrates various options for configuring the source:</p>
 *
 * <pre>{@code
 *
 * pipeline.apply(
 *   MqttIO.read()
 *    .withMqttConnectionConfiguration(MqttIO.MqttConnectionConfiguration.create(
 *      "tcp://host:11883",
 *      "my_client_id",
 *      "my_topic"))
 *
 * }</pre>
 *
 * <h3>Writing to a MQTT broker</h3>
 *
 * <p>MqttIO sink supports writing {@code byte[]} to a topic on a MQTT broker.</p>
 *
 * <p>To configure a MQTT sink, as for the read, you have to specify a MQTT connection
 * configuration with {@code ClientId}, {@code ServerURI}, {@code Topic}.</p>
 *
 * <p>Eventually, you can also specify the {@code Retained} and {@code QoS} of the MQTT
 * message.</p>
 *
 * <p>For instance:</p>
 *
 * <pre>{@code
 *
 * pipeline
 *   .apply(...) // provide PCollection<byte[]>
 *   .MqttIO.write()
 *     .withMqttConnectionConfiguration(MqttIO.MqttConnectionConfiguration.create(
 *       "tcp://host:11883",
 *       "my_client_id",
 *       "my_topic"))
 *     .withRetained(true)
 *     .withQoS(2)
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
        .setQos(0)
        .build();
  }

  private MqttIO() {
  }

  /**
   * A POJO describing a MQTT connection.
   */
  @AutoValue
  abstract static class MqttConnectionConfiguration implements Serializable {

    @Nullable abstract String serverUri();
    @Nullable abstract String clientId();
    @Nullable abstract String topic();

    public static MqttConnectionConfiguration create(String serverUri, String clientId,
                                                     String topic) {
      checkNotNull(serverUri, "serverUri");
      checkNotNull(clientId, "clientId");
      checkNotNull(topic, "topic");
      return new AutoValue_MqttIO_MqttConnectionConfiguration(serverUri, clientId, topic);
    }

    private void validate() {
      checkNotNull(serverUri());
      checkNotNull(clientId());
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("serverUri", serverUri()));
      builder.add(DisplayData.item("clientId", clientId()));
      builder.add(DisplayData.item("topic", topic()));
    }

    MqttClient getClient() throws Exception {
      MqttClient client = new MqttClient(serverUri(), clientId());
      client.connect();
      return client;
    }

  }

  /**
   * A {@link PTransform} to read from a MQTT broker.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<byte[]>> {

    @Nullable abstract MqttConnectionConfiguration mqttConnectionConfiguration();
    @Nullable abstract long maxNumRecords();
    @Nullable abstract Duration maxReadTime();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMqttConnectionConfiguration(MqttConnectionConfiguration config);
      abstract Builder setMaxNumRecords(long maxNumRecords);
      abstract Builder setMaxReadTime(Duration maxReadTime);
      abstract Read build();
    }

    public Read withMqttConnectionConfiguration(MqttConnectionConfiguration configuration) {
      checkNotNull(configuration, "MqttConnectionConfiguration");
      return toBuilder().setMqttConnectionConfiguration(configuration).build();
    }

    public Read withMaxNumRecords(long maxNumRecords) {
      return toBuilder().setMaxNumRecords(maxNumRecords).build();
    }

    public Read withMaxReadTime(Duration maxReadTime) {
      return toBuilder().setMaxReadTime(maxReadTime).build();
    }

    @Override
    public PCollection<byte[]> apply(PBegin input) {

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
      mqttConnectionConfiguration().validate();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      mqttConnectionConfiguration().populateDisplayData(builder);
      builder.add(DisplayData.item("maxNumRecords", maxNumRecords()));
      builder.addIfNotNull(DisplayData.item("maxReadTime", maxReadTime()));
    }

  }

  private static class UnboundedMqttSource
      extends UnboundedSource<byte[], UnboundedSource.CheckpointMark> {

    private Read spec;

    public UnboundedMqttSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public UnboundedReader createReader(PipelineOptions options,
                                        CheckpointMark checkpointMark) {
      return new UnboundedMqttReader(this);
    }

    @Override
    public List<UnboundedMqttSource> generateInitialSplits(int desiredNumSplits,
                                                           PipelineOptions options) {
      List<UnboundedMqttSource> sources = new ArrayList<>();
      for (int i = 0; i < desiredNumSplits; i++) {
        // NB: it's important that user understand the impact of MQTT message QoS
        sources.add(new UnboundedMqttSource(spec));
      }
      return sources;
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
    public Coder getCheckpointMarkCoder() {
      return VoidCoder.of();
    }

    @Override
    public Coder getDefaultOutputCoder() {
      return SerializableCoder.of(byte[].class);
    }
  }

  /**
   * POJO used to store MQTT message and its timestamp.
   */
  private static class MqttMessageWithTimestamp {

    private MqttMessage message;
    private Instant timestamp;

    public MqttMessageWithTimestamp(MqttMessage message, Instant timestamp) {
      this.message = message;
      this.timestamp = timestamp;
    }

    public MqttMessage getMessage() {
      return message;
    }

    public void setMessage(MqttMessage message) {
      this.message = message;
    }

    public Instant getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
      this.timestamp = timestamp;
    }
  }

  private static class UnboundedMqttReader extends UnboundedSource.UnboundedReader<byte[]> {

    private final UnboundedMqttSource source;

    private MqttClient client;
    private byte[] current;
    private Instant currentTimestamp;
    private BlockingQueue<MqttMessageWithTimestamp> queue;

    private UnboundedMqttReader(UnboundedMqttSource source) {
      this.source = source;
      this.current = null;
      this.queue = new LinkedBlockingQueue<>();
    }

    @Override
    public boolean start() throws IOException {
      LOGGER.debug("Starting MQTT reader");
      Read spec = source.spec;
      try {
        client = spec.mqttConnectionConfiguration().getClient();
        client.subscribe(spec.mqttConnectionConfiguration().topic());
        client.setCallback(new MqttCallback() {
          @Override
          public void connectionLost(Throwable cause) {
            LOGGER.warn("MQTT connection lost", cause);
            try {
              close();
            } catch (Exception e) {
              // nothing to do
            }
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
        return advance();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean advance() throws IOException {
      LOGGER.debug("Taking from the pending queue ({})", queue.size());
      try {
        MqttMessageWithTimestamp message = queue.take();
        current = message.getMessage().getPayload();
        currentTimestamp = message.getTimestamp();
        return true;
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
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
      // TODO use custom or better watermark
      return Instant.now();
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return new UnboundedSource.CheckpointMark() {
        @Override
        public void finalizeCheckpoint() throws IOException {
          // nothing to do
        }
      };
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

    @Nullable abstract MqttConnectionConfiguration mqttConnectionConfiguration();
    @Nullable abstract int qos();
    @Nullable abstract boolean retained();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMqttConnectionConfiguration(MqttConnectionConfiguration configuration);
      abstract Builder setQos(int qos);
      abstract Builder setRetained(boolean retained);
      abstract Write build();
    }

    public Write withMqttConnectionConfiguration(MqttConnectionConfiguration configuration) {
      return toBuilder().setMqttConnectionConfiguration(configuration).build();
    }

    public Write withQoS(int qos) {
      return toBuilder().setQos(qos).build();
    }

    public Write withRetained(boolean retained) {
      return toBuilder().setRetained(retained).build();
    }

    @Override
    public PDone apply(PCollection<byte[]> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<byte[]> input) {
      mqttConnectionConfiguration().validate();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      mqttConnectionConfiguration().populateDisplayData(builder);
      builder.add(DisplayData.item("qos", qos()));
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
        client = spec.mqttConnectionConfiguration().getClient();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        byte[] payload = context.element();
        MqttMessage message = new MqttMessage();
        message.setQos(spec.qos());
        message.setRetained(spec.retained());
        message.setPayload(payload);
        client.publish(spec.mqttConnectionConfiguration().topic(), message);
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
