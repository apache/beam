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
package org.apache.beam.runners.flink;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Properties;
import java.util.TimeZone;
import java.util.zip.Deflater;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.flink.LyftFlinkStreamingPortableTranslations.LyftBase64ZlibJsonSchema;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link LyftFlinkStreamingPortableTranslations}. */
public class LyftFlinkStreamingPortableTranslationsTest {

  @Mock
  private FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext streamingContext;

  @Mock private StreamExecutionEnvironment streamingEnvironment;

  @Mock private DataStream dataStream;

  @Mock private SingleOutputStreamOperator outputStreamOperator;

  @Mock private DataStreamSink streamSink;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    when(streamingContext.getExecutionEnvironment()).thenReturn(streamingEnvironment);
  }

  @Test
  public void testBeamKinesisSchema() throws IOException {
    // [{"event_id": 1, "occurred_at": "2018-10-27 00:20:02.900"}]"
    byte[] message =
        Base64.getDecoder()
            .decode(
                "eJyLrlZKLUvNK4nPTFGyUjDUUVDKT04uLSpKTYlPLAGKKBkZ"
                    + "GFroGhroGpkrGBhYGRlYGRjpWRoYKNXGAgARiA/1");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(1540599602000L, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaLongTimestamp() throws IOException {
    // [{"event_id": 1, "occurred_at": "2018-10-27 00:20:02.900"}]"
    byte[] message =
        Base64.getDecoder()
            .decode(
                "eJyLrlZKLUvNK4nPTFGyUjDUUVDKT04uL" + "SpKTYlPLAGJmJqYGBhbGlsYmhlZ1MYCAGYeDek=");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(1544039381628L, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaNoTimestamp() throws IOException {
    byte[] message = encode("[{\"event_id\": 1}]");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(Long.MIN_VALUE, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaMultipleRecords() throws IOException {
    // [{"event_id": 1, "occurred_at": "2018-10-27 00:20:02.900"},
    //  {"event_id": 2, "occurred_at": "2018-10-27 00:38:13.005"}]
    byte[] message =
        Base64.getDecoder()
            .decode(
                "eJyLrlZKLUvNK4nPTFGyUjDUUVDKT04uLSpKTYlPLAGKKBkZGFroGhroGpkr"
                    + "GBhYGRlYGRjpWRoYKNXqKKBoNSKk1djCytBYz8DAVKk2FgC35B+F");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    // we should output the oldest timestamp in the bundle
    Assert.assertEquals(1540599602000L, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaFutureOccurredAtTimestamp() throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

    long loggedAtMillis = sdf.parse("2018-10-27 00:10:02.000000").getTime();
    String events =
        "[{\"event_id\": 1, \"occurred_at\": \"2018-10-27 00:20:02.900\", \"logged_at\": "
            + loggedAtMillis / 1000
            + "}]";
    byte[] message = encode(events);
    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(loggedAtMillis, value.getTimestamp().getMillis());
  }

  private static byte[] encode(String data) throws IOException {
    Deflater deflater = new Deflater();
    deflater.setInput(data.getBytes(Charset.defaultCharset()));
    deflater.finish();
    byte[] buf = new byte[4096];
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length())) {
      while (!deflater.finished()) {
        int count = deflater.deflate(buf);
        bos.write(buf, 0, count);
      }
      return bos.toByteArray();
    }
  }

  @Test
  public void testKafkaInputWithDevKafkaBroker() throws JsonProcessingException {

    String id = "1";
    String topicName = "kinesis_to_kafka";
    String bootstrapServer = "localhost:9093";

    byte[] payload = createPayload(topicName, bootstrapServer, true, false);
    runAndAssertKafkaInput(id, topicName, payload);
  }

  @Test
  public void testKafkaInputWithNonDevKafkaBroker() throws JsonProcessingException {

    String id = "1";
    String topicName = "kinesis_to_kafka";
    String bootstrapServer = "staging-hdd.lyft.net";

    byte[] payload = createPayload(topicName, bootstrapServer, true, true);
    runAndAssertKafkaInput(id, topicName, payload);
  }

  private void runAndAssertKafkaInput(String id, String topicName, byte[] payload) {

    RunnerApi.Pipeline pipeline = createPipeline(id, payload);

    // run
    new LyftFlinkStreamingPortableTranslations()
        .translateKafkaInput(id, pipeline, streamingContext);

    // assert
    ArgumentCaptor<FlinkKafkaConsumer011> kafkaSourceCaptor =
        ArgumentCaptor.forClass(FlinkKafkaConsumer011.class);
    ArgumentCaptor<String> kafkaSourceNameCaptor = ArgumentCaptor.forClass(String.class);
    verify(streamingEnvironment)
        .addSource(kafkaSourceCaptor.capture(), kafkaSourceNameCaptor.capture());
    Assert.assertEquals(
        WindowedValue.class, kafkaSourceCaptor.getValue().getProducedType().getTypeClass());
    Assert.assertTrue(kafkaSourceNameCaptor.getValue().contains(topicName));
  }

  @Test
  public void shouldFailForMissingGroupIdToKafkaInput() throws JsonProcessingException {

    LyftFlinkStreamingPortableTranslations portableTranslations =
        new LyftFlinkStreamingPortableTranslations();
    String id = "1";
    String topicName = "kinesis_to_kafka";
    String bootstrapServer = "test-hdd.lyft.com";

    byte[] payload = createPayload(topicName, bootstrapServer, false, false);
    RunnerApi.Pipeline pipeline = createPipeline(id, payload);

    NullPointerException npe =
        assertThrows(
            NullPointerException.class,
            () -> portableTranslations.translateKafkaInput(id, pipeline, streamingContext));
    Assert.assertTrue(npe.getMessage().contains("group.id is a required property"));
  }

  @Test
  public void testKafkaSinkForNonDevBroker() throws JsonProcessingException {

    String id = "1";
    String topicName = "kinesis_to_kafka";
    String bootstrapServers = "test-hdd.lyft.com,test-hdd1.lyft.com:9093";
    byte[] payload = createPayload(topicName, bootstrapServers, false, true);

    runAndAssertKafkaSink(id, topicName, payload);
  }

  @Test
  public void testKafkaSinkForDevBroker() throws JsonProcessingException {

    String id = "1";
    String topicName = "kinesis_to_kafka";
    String bootstrapServer = "kafka-server.devbox.lyft.net";
    byte[] payload = createPayload(topicName, bootstrapServer, false, false);

    runAndAssertKafkaSink(id, topicName, payload);
  }

  private void runAndAssertKafkaSink(String id, String topicName, byte[] payload) {

    RunnerApi.Pipeline pipeline = createPipeline(id, payload);
    LyftFlinkStreamingPortableTranslations portableTranslations =
        new LyftFlinkStreamingPortableTranslations();
    when(outputStreamOperator.addSink(any())).thenReturn(streamSink);
    when(dataStream.transform(anyString(), any(), any(OneInputStreamOperator.class)))
        .thenReturn(outputStreamOperator);
    when(streamingContext.getDataStreamOrThrow("fake_pcollection_id")).thenReturn(dataStream);

    // run
    portableTranslations.translateKafkaSink(id, pipeline, streamingContext);

    // assert
    verify(streamingContext).getDataStreamOrThrow("fake_pcollection_id");
    verify(dataStream).transform(anyString(), any(), any(OneInputStreamOperator.class));
    ArgumentCaptor<FlinkKafkaProducer011> kafkaSinkCaptor =
        ArgumentCaptor.forClass(FlinkKafkaProducer011.class);
    ArgumentCaptor<String> kafkaSinkNameCaptor = ArgumentCaptor.forClass(String.class);
    verify(streamSink).name(kafkaSinkNameCaptor.capture());
    verify(outputStreamOperator).addSink(kafkaSinkCaptor.capture());

    Assert.assertTrue(kafkaSinkNameCaptor.getValue().contains(topicName));
    Assert.assertEquals(FlinkKafkaProducer011.class, kafkaSinkCaptor.getValue().getClass());
  }

  /**
   * utility method to create payload for tests.
   *
   * @param topicName name of the topic
   * @param bootstrapServers bootstrap server
   * @param withGroupId if {@code true}, include group.id to properties
   * @param withCredentials if {@code true}, include username/password to properties.
   * @return byte[]
   * @throws JsonProcessingException
   */
  private byte[] createPayload(
      String topicName, String bootstrapServers, boolean withGroupId, boolean withCredentials)
      throws JsonProcessingException {

    Properties properties = new Properties();
    properties.put("bootstrap.servers", bootstrapServers);
    if (withGroupId) {
      properties.put("group.id", String.format("%s_%s", topicName, System.currentTimeMillis()));
    }

    ImmutableMap.Builder<String, Object> builder =
        ImmutableMap.<String, Object>builder()
            .put("topic", topicName)
            .put("properties", properties);

    if (withCredentials) {
      builder.put("username", "kinesis_to_kafka").put("password", "abcde1234");
    }

    return new ObjectMapper().writeValueAsBytes(builder.build());
  }

  /**
   * Creates a new {@link RunnerApi.Pipeline} with payload.
   *
   * @param id
   * @param payload
   * @return
   */
  private RunnerApi.Pipeline createPipeline(String id, byte[] payload) {

    RunnerApi.PTransform pTransform =
        RunnerApi.PTransform.newBuilder()
            .putOutputs("fake_output_name", "fake_pcollection_id")
            .putInputs("fake_input_name", "fake_pcollection_id")
            .setSpec(RunnerApi.FunctionSpec.newBuilder().setPayload(ByteString.copyFrom(payload)))
            .build();

    RunnerApi.Pipeline pipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(RunnerApi.Components.newBuilder().putTransforms(id, pTransform).build())
            .build();

    return pipeline;
  }
}
