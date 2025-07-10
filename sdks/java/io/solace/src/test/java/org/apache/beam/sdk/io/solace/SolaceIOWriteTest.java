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

import com.solacesystems.jcsmp.DeliveryMode;
import java.util.List;
import java.util.Objects;
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
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandlingTestUtils.ErrorSinkTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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

  private SolaceOutput getWriteTransform(
      SubmissionMode mode,
      WriterType writerType,
      Pipeline p,
      ErrorHandler<BadRecord, ?> errorHandler) {
    SessionServiceFactory fakeSessionServiceFactory =
        MockSessionServiceFactory.builder().mode(mode).build();

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
}
