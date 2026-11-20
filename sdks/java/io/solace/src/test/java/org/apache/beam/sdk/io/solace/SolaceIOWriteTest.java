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
package org.apache.beam.sdk.io.solace;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.solace.MockSessionServiceFactory.SessionServiceType;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.SolaceIO.WriterType;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.io.solace.data.SolaceDataUtils;
import org.apache.beam.sdk.io.solace.write.SolaceOutput;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandlingTestUtils.ErrorSinkTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SolaceIOWriteTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private final List<String> keys = ImmutableList.of("450", "451", "452");
  private final List<String> payloads = ImmutableList.of("payload0", "payload1", "payload2");

  private PCollection<Record> getRecords(Pipeline p) {
    TestStream.Builder<KV<String, String>> kvBuilder =
        TestStream.create(KvCoder.of(AvroCoder.of(String.class), AvroCoder.of(String.class)))
            .advanceWatermarkTo(Instant.EPOCH);

    assert keys.size() == payloads.size();

    for (int k = 0; k < keys.size(); k++) {
      kvBuilder =
          kvBuilder
              .addElements(KV.of(keys.get(k), payloads.get(k)))
              .advanceProcessingTime(Duration.standardSeconds(60));
    }

    TestStream<KV<String, String>> testStream = kvBuilder.advanceWatermarkToInfinity();
    PCollection<KV<String, String>> kvs = p.apply("Test stream", testStream);

    return kvs.apply(
        "To Record",
        MapElements.into(TypeDescriptor.of(Record.class))
            .via(kv -> SolaceDataUtils.getSolaceRecord(kv.getValue(), kv.getKey())));
  }

  private PCollection<Record> getRecordsForEachPayloadTypes(Pipeline p) {
    TestStream.Builder<Record.PayloadType> kvBuilder =
        TestStream.create(AvroCoder.of(Record.PayloadType.class)).advanceWatermarkTo(Instant.EPOCH);

    for (var payloadType : Record.PayloadType.values()) {
      kvBuilder =
          kvBuilder.addElements(payloadType).advanceProcessingTime(Duration.standardSeconds(60));
    }

    TestStream<Record.PayloadType> testStream = kvBuilder.advanceWatermarkToInfinity();

    return p.apply("Test stream ", testStream)
        .apply(
            "To Record",
            MapElements.into(TypeDescriptor.of(Record.class))
                .via(
                    payloadType ->
                        Solace.Record.builder()
                            .setMessageId(payloadType.name().toLowerCase())
                            .setPayloadType(payloadType)
                            .setPayload(
                                ("payload-" + payloadType.name()).getBytes(StandardCharsets.UTF_8))
                            .build()));
  }

  private SolaceOutput getWriteTransform(
      SubmissionMode mode,
      WriterType writerType,
      Pipeline p,
      ErrorHandler<BadRecord, ?> errorHandler) {
    return getWriteTransform(
        mode, writerType, p, errorHandler, SessionServiceType.WITH_SUCCEEDING_PRODUCER);
  }

  private SolaceOutput getWriteTransform(
      SubmissionMode mode,
      WriterType writerType,
      Pipeline p,
      ErrorHandler<BadRecord, ?> errorHandler,
      SessionServiceType sessionServiceType) {
    SessionServiceFactory fakeSessionServiceFactory =
        MockSessionServiceFactory.builder()
            .mode(mode)
            .sessionServiceType(sessionServiceType)
            .build();

    PCollection<Record> records = getRecords(p);
    return records.apply(
        "Write to Solace",
        SolaceIO.write()
            .to(Solace.Queue.fromName("queue"))
            .withSubmissionMode(mode)
            .withWriterType(writerType)
            .withDeliveryMode(DeliveryMode.PERSISTENT)
            .withSessionServiceFactory(fakeSessionServiceFactory)
            .withErrorHandler(errorHandler));
  }

  private static PCollection<String> getIdsPCollection(SolaceOutput output) {
    return output
        .getSuccessfulPublish()
        .apply(
            "Get message ids", MapElements.into(strings()).via(Solace.PublishResult::getMessageId));
  }

  @Test
  public void testWriteLatencyStreaming() throws Exception {
    SubmissionMode mode = SubmissionMode.LOWER_LATENCY;
    WriterType writerType = WriterType.STREAMING;

    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());
    SolaceOutput output = getWriteTransform(mode, writerType, pipeline, errorHandler);
    PCollection<String> ids = getIdsPCollection(output);

    PAssert.that(ids).containsInAnyOrder(keys);
    errorHandler.close();
    PAssert.that(errorHandler.getOutput()).empty();

    pipeline.run();
  }

  @Test
  public void testWriteThroughputStreaming() throws Exception {
    SubmissionMode mode = SubmissionMode.HIGHER_THROUGHPUT;
    WriterType writerType = WriterType.STREAMING;
    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());
    SolaceOutput output = getWriteTransform(mode, writerType, pipeline, errorHandler);
    PCollection<String> ids = getIdsPCollection(output);

    PAssert.that(ids).containsInAnyOrder(keys);
    errorHandler.close();
    PAssert.that(errorHandler.getOutput()).empty();

    pipeline.run();
  }

  @Test
  public void testWriteLatencyBatched() throws Exception {
    SubmissionMode mode = SubmissionMode.LOWER_LATENCY;
    WriterType writerType = WriterType.BATCHED;
    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());
    SolaceOutput output = getWriteTransform(mode, writerType, pipeline, errorHandler);
    PCollection<String> ids = getIdsPCollection(output);

    PAssert.that(ids).containsInAnyOrder(keys);
    errorHandler.close();
    PAssert.that(errorHandler.getOutput()).empty();
    pipeline.run();
  }

  @Test
  public void testWriteThroughputBatched() throws Exception {
    SubmissionMode mode = SubmissionMode.HIGHER_THROUGHPUT;
    WriterType writerType = WriterType.BATCHED;
    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());
    SolaceOutput output = getWriteTransform(mode, writerType, pipeline, errorHandler);
    PCollection<String> ids = getIdsPCollection(output);

    PAssert.that(ids).containsInAnyOrder(keys);
    errorHandler.close();
    PAssert.that(errorHandler.getOutput()).empty();
    pipeline.run();
  }

  @Test
  public void testWriteMixedPayloadTypesStreaming() throws Exception {
    PCollection<Record> records = getRecordsForEachPayloadTypes(pipeline);

    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());

    SolaceOutput output =
        records.apply(
            "Write mixed records",
            SolaceIO.write()
                .to(Solace.Queue.fromName("queue"))
                .withSubmissionMode(SubmissionMode.LOWER_LATENCY)
                .withWriterType(WriterType.STREAMING)
                .withDeliveryMode(DeliveryMode.PERSISTENT)
                .withSessionServiceFactory(MockSessionServiceFactory.builder().build())
                .withErrorHandler(errorHandler));

    var expectedIds =
        Stream.of(Record.PayloadType.values())
            .map(payloadType -> payloadType.name().toLowerCase())
            .collect(Collectors.toList());
    PAssert.that(getIdsPCollection(output)).containsInAnyOrder(expectedIds);
    errorHandler.close();
    PAssert.that(errorHandler.getOutput()).empty();
    pipeline.run();
  }

  @Test
  public void testWriteMixedPayloadTypesBatched() throws Exception {
    PCollection<Record> records = getRecordsForEachPayloadTypes(pipeline);

    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());
    SolaceOutput output =
        records.apply(
            "Write mixed records",
            SolaceIO.write()
                .to(Solace.Queue.fromName("queue"))
                .withSubmissionMode(SubmissionMode.HIGHER_THROUGHPUT)
                .withWriterType(WriterType.BATCHED)
                .withDeliveryMode(DeliveryMode.PERSISTENT)
                .withSessionServiceFactory(MockSessionServiceFactory.builder().build())
                .withErrorHandler(errorHandler));

    var expectedIds =
        Stream.of(Record.PayloadType.values())
            .map(payloadType -> payloadType.name().toLowerCase())
            .collect(Collectors.toList());
    PAssert.that(getIdsPCollection(output)).containsInAnyOrder(expectedIds);
    errorHandler.close();
    PAssert.that(errorHandler.getOutput()).empty();
    pipeline.run();
  }

  @Test
  public void testWriteWithFailedRecords() throws Exception {
    SubmissionMode mode = SubmissionMode.HIGHER_THROUGHPUT;
    WriterType writerType = WriterType.BATCHED;
    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());

    SessionServiceFactory fakeSessionServiceFactory =
        MockSessionServiceFactory.builder()
            .mode(mode)
            .sessionServiceType(SessionServiceType.WITH_FAILING_PRODUCER)
            .build();

    PCollection<Record> records = getRecords(pipeline);
    SolaceOutput output =
        records.apply(
            "Write to Solace",
            SolaceIO.write()
                .to(Solace.Queue.fromName("queue"))
                .withSubmissionMode(mode)
                .withWriterType(writerType)
                .withDeliveryMode(DeliveryMode.PERSISTENT)
                .withSessionServiceFactory(fakeSessionServiceFactory)
                .withErrorHandler(errorHandler));

    PCollection<String> ids = getIdsPCollection(output);

    PAssert.that(ids).empty();
    errorHandler.close();
    PAssert.thatSingleton(Objects.requireNonNull(errorHandler.getOutput()))
        .isEqualTo((long) payloads.size());
    pipeline.run();
  }

  @Test
  public void testWriteLatencyStreamingWithDelayedAck() throws Exception {
    SubmissionMode mode = SubmissionMode.LOWER_LATENCY;
    WriterType writerType = WriterType.STREAMING;

    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());
    SolaceOutput output =
        getWriteTransform(
            mode, writerType, pipeline, errorHandler, SessionServiceType.WITH_DELAYED_PRODUCER);
    PCollection<String> ids = getIdsPCollection(output);

    PAssert.that(ids).containsInAnyOrder(keys);
    errorHandler.close();
    PAssert.that(errorHandler.getOutput()).empty();

    pipeline.run();
  }

  @Test
  public void testWriteLatencyBatchedWithDelayedAck() throws Exception {
    SubmissionMode mode = SubmissionMode.LOWER_LATENCY;
    WriterType writerType = WriterType.BATCHED;

    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());
    SolaceOutput output =
        getWriteTransform(
            mode, writerType, pipeline, errorHandler, SessionServiceType.WITH_DELAYED_PRODUCER);
    PCollection<String> ids = getIdsPCollection(output);

    PAssert.that(ids).containsInAnyOrder(keys);
    errorHandler.close();
    PAssert.that(errorHandler.getOutput()).empty();

    pipeline.run();
  }

  @Test
  public void testWriteWithExceptionRecords() throws Exception {
    SubmissionMode mode = SubmissionMode.HIGHER_THROUGHPUT;
    WriterType writerType = WriterType.BATCHED;
    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());

    SessionServiceFactory fakeSessionServiceFactory =
        MockSessionServiceFactory.builder()
            .mode(mode)
            .sessionServiceType(SessionServiceType.WITH_EXCEPTION_PRODUCER)
            .build();

    PCollection<Record> records = getRecords(pipeline);
    SolaceOutput output =
        records.apply(
            "Write to Solace",
            SolaceIO.write()
                .to(Solace.Queue.fromName("queue"))
                .withSubmissionMode(mode)
                .withWriterType(writerType)
                .withDeliveryMode(DeliveryMode.PERSISTENT)
                .withSessionServiceFactory(fakeSessionServiceFactory)
                .withErrorHandler(errorHandler));

    PCollection<String> ids = getIdsPCollection(output);

    PAssert.that(ids).empty();
    errorHandler.close();
    PAssert.thatSingleton(Objects.requireNonNull(errorHandler.getOutput()))
        .isEqualTo((long) payloads.size());
    pipeline.run();
  }

  @Test
  public void testWithEnableOpenTelemetryTracingConfiguration() {
    SolaceIO.Write<Record> defaultWrite = SolaceIO.write();
    assertFalse(defaultWrite.getEnableOpenTelemetryTracing());

    SolaceIO.Write<Record> writeWithTracing = SolaceIO.write().withEnableOpenTelemetryTracing();
    assertTrue(writeWithTracing.getEnableOpenTelemetryTracing());
  }

  @Test
  public void testWriteWithOpenTelemetryTracingStreaming() throws Exception {
    runWriteWithOpenTelemetryTracingTest(WriterType.STREAMING);
  }

  @Test
  public void testWriteWithOpenTelemetryTracingBatched() throws Exception {
    runWriteWithOpenTelemetryTracingTest(WriterType.BATCHED);
  }

  private void runWriteWithOpenTelemetryTracingTest(WriterType writerType) throws Exception {
    InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .setSampler(Sampler.alwaysOn())
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
    GlobalOpenTelemetry.resetForTest();
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).buildAndRegisterGlobal();
    pipeline.getOptions().as(SdkHarnessOptions.class).setOpenTelemetry(openTelemetry);
    MockProducer.clearPublishedRecords();

    try {
      ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
          pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());

      PCollection<Record> records = getRecords(pipeline);
      SolaceOutput output =
          records.apply(
              "Write to Solace",
              SolaceIO.write()
                  .to(Solace.Queue.fromName("queue"))
                  .withSubmissionMode(SubmissionMode.LOWER_LATENCY)
                  .withWriterType(writerType)
                  .withDeliveryMode(DeliveryMode.PERSISTENT)
                  .withSessionServiceFactory(
                      MockSessionServiceFactory.builder()
                          .mode(SubmissionMode.LOWER_LATENCY)
                          .sessionServiceType(SessionServiceType.WITH_SUCCEEDING_PRODUCER)
                          .build())
                  .withErrorHandler(errorHandler)
                  .withEnableOpenTelemetryTracing());

      PCollection<String> ids = getIdsPCollection(output);
      PAssert.that(ids).containsInAnyOrder(keys);
      errorHandler.close();
      PAssert.that(errorHandler.getOutput()).empty();
      pipeline.run();

      List<SpanData> spans = spanExporter.getFinishedSpanItems();
      assertEquals(keys.size(), spans.size());
      for (SpanData span : spans) {
        assertEquals("SolaceIO.Write", span.getName());
        assertEquals(SpanKind.PRODUCER, span.getKind());
      }

      Set<String> expectedTraceparents =
          spans.stream()
              .map(
                  s ->
                      String.format(
                          "00-%s-%s-%s",
                          s.getTraceId(),
                          s.getSpanId(),
                          s.getSpanContext().getTraceFlags().asHex()))
              .collect(Collectors.toSet());

      List<Record> publishedRecords = MockProducer.getPublishedRecords();
      assertEquals(keys.size(), publishedRecords.size());
      for (Record publishedRecord : publishedRecords) {
        Solace.UserPropertyValue traceparentProp =
            publishedRecord.getUserProperties().get("traceparent");
        assertNotNull(traceparentProp);
        String traceparent = traceparentProp.getString();
        assertNotNull(traceparent);
        assertTrue(
            "Unexpected traceparent: " + traceparent + ", expected one of: " + expectedTraceparents,
            expectedTraceparents.contains(traceparent));

        BytesXMLMessage jcsmpMsg = Solace.SolaceRecordMapper.toMessage(publishedRecord);
        assertNotNull(jcsmpMsg.getProperties());
        assertEquals(traceparent, jcsmpMsg.getProperties().getString("traceparent"));
      }
    } finally {
      MockProducer.clearPublishedRecords();
      tracerProvider.close();
      GlobalOpenTelemetry.resetForTest();
    }
  }

  @Test
  public void testOpenTelemetryTraceContextPropagationConsumerToProducer() {
    String traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
    String upstreamSpanId = "00f067aa0ba902b7";
    String incomingTraceparent = "00-" + traceId + "-" + upstreamSpanId + "-01";
    String incomingTracestate = "congo=t61rcWkgMzE";

    InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .setSampler(Sampler.alwaysOn())
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
    GlobalOpenTelemetry.resetForTest();
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).buildAndRegisterGlobal();
    pipeline.getOptions().as(SdkHarnessOptions.class).setOpenTelemetry(openTelemetry);

    try {
      Record inputRecord =
          Record.builder()
              .setMessageId("id0")
              .setPayload("payload_test0".getBytes(StandardCharsets.UTF_8))
              .setUserProperties(
                  ImmutableMap.of(
                      "traceparent", Solace.UserPropertyValue.of(incomingTraceparent),
                      "tracestate", Solace.UserPropertyValue.of(incomingTracestate),
                      "customKey", Solace.UserPropertyValue.of("customVal")))
              .build();

      PCollection<Record> propagated =
          pipeline
              .apply(Create.of(inputRecord))
              .apply(
                  "Extract OpenTelemetry context",
                  ParDo.of(new SolaceIO.OpenTelemetryHeaderConsumer<>()))
              .apply(
                  "Propagate OpenTelemetry context",
                  ParDo.of(new SolaceIO.OpenTelemetryHeaderPropagator()));

      PCollection<Map<String, String>> outputProps =
          propagated.apply(
              MapElements.into(new TypeDescriptor<Map<String, String>>() {})
                  .via(
                      r ->
                          ImmutableMap.of(
                              "traceparent",
                              Objects.requireNonNull(
                                  r.getUserProperties().get("traceparent").getString()),
                              "tracestate",
                              Objects.requireNonNull(
                                  r.getUserProperties().get("tracestate").getString()),
                              "customKey",
                              Objects.requireNonNull(
                                  r.getUserProperties().get("customKey").getString()))));

      PAssert.thatSingleton(outputProps)
          .satisfies(
              props -> {
                assertEquals("customVal", props.get("customKey"));
                assertEquals(incomingTracestate, props.get("tracestate"));
                assertTrue(props.get("traceparent").startsWith("00-" + traceId + "-"));
                return null;
              });

      pipeline.run();

      List<SpanData> spans = spanExporter.getFinishedSpanItems();
      assertEquals(2, spans.size());

      SpanData readSpan =
          spans.stream()
              .filter(s -> "SolaceIO.Read".equals(s.getName()))
              .findFirst()
              .orElseThrow(AssertionError::new);
      SpanData writeSpan =
          spans.stream()
              .filter(s -> "SolaceIO.Write".equals(s.getName()))
              .findFirst()
              .orElseThrow(AssertionError::new);

      assertEquals(SpanKind.CONSUMER, readSpan.getKind());
      assertEquals(traceId, readSpan.getTraceId());
      assertEquals(upstreamSpanId, readSpan.getParentSpanId());

      assertEquals(SpanKind.PRODUCER, writeSpan.getKind());
      assertEquals(traceId, writeSpan.getTraceId());
      assertEquals(readSpan.getSpanId(), writeSpan.getParentSpanId());
    } finally {
      tracerProvider.close();
      GlobalOpenTelemetry.resetForTest();
    }
  }
}
