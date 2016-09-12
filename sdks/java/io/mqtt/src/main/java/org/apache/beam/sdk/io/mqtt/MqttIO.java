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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

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

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
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
 * {@code PCollection<byte[]>}, where {@code byte[]} is the message payload.</p>
 *
 * <p>To configure a MQTT source, you have to provide a {@code ClientId}, a {@code ServerURI},
 * and eventually a {@code Topic} pattern. The following example illustrates various options
 * for configuring the source:</p>
 *
 * <pre>{@code
 *
 * pipeline.apply(MqttIO.read().withClientId("my_client").withServerUri("tcp://host:11883")
 *   .withTopic("topic")
 *
 * }</pre>
 *
 * <h3>Writing to a MQTT broker</h3>
 *
 * <p>MqttIO sink supports writing {@code byte[]} to a topic on a MQTT broker.</p>
 *
 * <p>To configure a MQTT sink, you must specify the {@code ClientId}, {@code ServerURI},
 * {@code Topic}. Eventually, you can also specify the {@code Retained} and {@code QoS} of the
 * message.</p>
 *
 * <p>For instance:</p>
 *
 * <pre>{@code
 *
 * pipeline
 *   .apply(...) // provide PCollection<byte[]>
 *   .MqttIO.write()
 *     .withClientId("my-client")
 *     .withServerUri("tcp://host:11883")
 *     .withTopic("my-topic")
 *     .withRetained(true)
 *     .withQoS(2)
 *
 * }</pre>
 */
public class MqttIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(MqttIO.class);

  public static Read read() {
    return new Read(new UnboundedMqttSource(null, null, null,
        Long.MAX_VALUE, null));
  }

  public static Write write() {
    return new Write(new Write.MqttWriter(null, null, null, 1, false));
  }

  private MqttIO() {
  }

  /**
   * A {@link PTransform<PBegin, PCollection<byte[]>>} to read from a MQTT broker.
   */
  public static class Read extends PTransform<PBegin, PCollection<byte[]>> {

    public Read withServerUri(String serverUri) {
      return new Read(source.withServerUri(serverUri));
    }

    public Read withClientId(String clientId) {
      return new Read(source.withClientId(clientId));
    }

    public Read withTopic(String topic) {
      return new Read(source.withTopic(topic));
    }

    public Read withMaxNumRecords(long maxNumRecords) {
      return new Read(source.withMaxNumRecords(maxNumRecords));
    }

    public Read withMaxReadTime(Duration maxReadTime) {
      return new Read(source.withMaxReadTime(maxReadTime));
    }

    private final UnboundedMqttSource source;

    private Read(UnboundedMqttSource source) {
      this.source = source;
    }

    @Override
    public PCollection<byte[]> apply(PBegin input) {

      org.apache.beam.sdk.io.Read.Unbounded<byte[]> unbounded =
          org.apache.beam.sdk.io.Read.from(getSource());

      PTransform<PBegin, PCollection<byte[]>> transform = unbounded;

      if (source.maxNumRecords != Long.MAX_VALUE) {
        transform = unbounded.withMaxNumRecords(source.maxNumRecords);
      } else if (source.maxReadTime != null) {
        transform = unbounded.withMaxReadTime(source.maxReadTime);
      }

      return input.getPipeline().apply(transform);
    }

    @VisibleForTesting
    public UnboundedMqttSource getSource() {
      return source;
    }

    @Override
    public void validate(PBegin input) {
      source.validate();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      source.populateDisplayData(builder);
    }

  }

  private static class UnboundedMqttSource extends UnboundedSource<byte[], MqttCheckpointMark> {

    public UnboundedMqttSource withServerUri(String serverUri) {
      return new UnboundedMqttSource(serverUri, clientId, topic, maxNumRecords, maxReadTime);
    }

    public UnboundedMqttSource withClientId(String clientId) {
      return new UnboundedMqttSource(serverUri, clientId, topic, maxNumRecords, maxReadTime);
    }

    public UnboundedMqttSource withTopic(String topic) {
      return new UnboundedMqttSource(serverUri, clientId, topic, maxNumRecords, maxReadTime);
    }

    public UnboundedMqttSource withMaxNumRecords(long maxNumRecords) {
      return new UnboundedMqttSource(serverUri, clientId, topic, maxNumRecords, maxReadTime);
    }

    public UnboundedMqttSource withMaxReadTime(Duration maxReadTime) {
      return new UnboundedMqttSource(serverUri, clientId, topic, maxNumRecords, maxReadTime);
    }

    private final String serverUri;
    private final String clientId;
    @Nullable
    private final String topic;

    private final long maxNumRecords;
    private final Duration maxReadTime;

    public UnboundedMqttSource(String serverUri, String clientId, String topic,
                               long maxNumRecords, Duration maxReadTime) {
      this.serverUri = serverUri;
      this.clientId = clientId;
      this.topic = topic;
      this.maxNumRecords = maxNumRecords;
      this.maxReadTime = maxReadTime;
    }

    @Override
    public UnboundedReader createReader(PipelineOptions options,
                                        MqttCheckpointMark checkpointMark) {
      return new UnboundedMqttReader(this, checkpointMark);
    }

    @Override
    public List<UnboundedMqttSource> generateInitialSplits(int desiredNumSplits,
                                                           PipelineOptions options) {
      List<UnboundedMqttSource> sources = new ArrayList<>();
      for (int i = 0; i < desiredNumSplits; i++) {
        // NB: it's important that user understand the impact of MQTT message QoS
        sources.add(new UnboundedMqttSource(serverUri, clientId + "-" + i, topic, maxNumRecords,
            maxReadTime));
      }
      return sources;
    }

    @Override
    public void validate() {
      Preconditions.checkNotNull(serverUri, "serverUri");
      Preconditions.checkNotNull(clientId, "clientId");
    }

    @Override
    public Coder getCheckpointMarkCoder() {
      return SerializableCoder.of(MqttCheckpointMark.class);
    }

    @Override
    public Coder getDefaultOutputCoder() {
      return SerializableCoder.of(byte[].class);
    }

  }

  private static class UnboundedMqttReader extends UnboundedSource.UnboundedReader
      implements MqttCallback {

    private UnboundedMqttSource source;

    private MqttCheckpointMark checkpointMark;

    private MqttClient client;
    private byte[] current;
    private Instant currentTimestamp;

    private UnboundedMqttReader(UnboundedMqttSource source,
                                MqttCheckpointMark checkpointMark) {
      this.source = source;
      if (checkpointMark != null) {
        this.checkpointMark = checkpointMark;
      } else {
        this.checkpointMark = new MqttCheckpointMark();
      }
      this.current = null;
    }

    @Override
    public boolean start() throws IOException {
      try {
        client = new MqttClient(source.serverUri, source.clientId);
        client.connect();
        if (source.topic != null) {
          client.subscribe(source.topic);
        }
        client.setCallback(this);
        return advance();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean advance() throws IOException {
      if (current == null) {
        return false;
      } else {
        return true;
      }
    }

    @Override
    public void close() throws IOException {
      try {
        if (client != null) {
          client.disconnect();
          client.close();
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public Instant getWatermark() {
      return checkpointMark.getOldestTimestamp();
    }

    @Override
    public MqttCheckpointMark getCheckpointMark() {
      return checkpointMark;
    }

    @Override
    public byte[] getCurrent() {
      return current;
    }

    @Override
    public UnboundedMqttSource getCurrentSource() {
      return source;
    }

    @Override
    public Instant getCurrentTimestamp() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return currentTimestamp;
    }

    @Override
    public void connectionLost(Throwable t) {
      LOGGER.warn("Lost connection to MQTT server", t);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
      this.current = message.getPayload();
      this.currentTimestamp = checkpointMark.addMessage(message);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

  }

  /**
   * A {@link PTransform} to write and send a message to a MQTT server.
   */
  public static class Write extends PTransform<PCollection<byte[]>, PDone> {

    public Write withServerUri(String serverUri) {
      return new Write(writer.withServerUri(serverUri));
    }

    public Write withClientId(String clientId) {
      return new Write(writer.withClientId(clientId));
    }

    public Write withTopic(String topic) {
      return new Write(writer.withTopic(topic));
    }

    public Write withQoS(int qos) {
      return new Write(writer.withQoS(qos));
    }

    public Write withRetained(boolean retained) {
      return new Write(writer.withRetained(retained));
    }

    private final MqttWriter writer;

    private Write(MqttWriter writer) {
      this.writer = writer;
    }

    @Override
    public PDone apply(PCollection<byte[]> input) {
      input.apply(ParDo.of(writer));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<byte[]> input) {
      writer.validate();
    }

    /**
     * MQTT writer.
     */
    private static class MqttWriter extends DoFn<byte[], Void> {

      private final String serverUri;
      private final String clientId;
      private final String topic;
      private final int qos;
      private final boolean retained;

      private MqttClient client;

      public MqttWriter(String serverUri, String clientId, String topic, int qos,
                        boolean retained) {
        this.serverUri = serverUri;
        this.clientId = clientId;
        this.topic = topic;
        this.qos = qos;
        this.retained = retained;
      }

      public MqttWriter withServerUri(String serverUri) {
        return new MqttWriter(serverUri, clientId, topic, qos, retained);
      }

      public MqttWriter withClientId(String clientId) {
        return new MqttWriter(serverUri, clientId, topic, qos, retained);
      }

      public MqttWriter withTopic(String topic) {
        return new MqttWriter(serverUri, clientId, topic, qos, retained);
      }

      public MqttWriter withQoS(int qos) {
        return new MqttWriter(serverUri, clientId, topic, qos, retained);
      }

      public MqttWriter withRetained(boolean retained) {
        return new MqttWriter(serverUri, clientId, topic, qos, retained);
      }

      public void validate() {
        Preconditions.checkNotNull(serverUri, "serverUri");
        Preconditions.checkNotNull(clientId, "clientId");
      }

      @Setup
      public void createMqttClient() throws Exception {
        client = new MqttClient(serverUri, clientId);
        client.connect();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        byte[] payload = context.element();
        MqttMessage message = new MqttMessage();
        message.setQos(qos);
        message.setRetained(retained);
        message.setPayload(payload);
        client.publish(topic, message);
      }

      @Teardown
      public void closeMqttClient() throws Exception {
        if (client != null) {
          client.disconnect();
          client.close();
        }
      }

    }

  }

}
