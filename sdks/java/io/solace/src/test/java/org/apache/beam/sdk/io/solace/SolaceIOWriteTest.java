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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.io.solace.data.SolaceDataUtils;
import org.apache.beam.sdk.io.solace.write.SolaceOutput;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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
        TestStream.create(KvCoder.of(AvroCoder.of(String.class), AvroCoder.of(String.class))).advanceWatermarkTo(Instant.EPOCH);

    assert keys.size() == payloads.size();

    for (int k = 0; k < keys.size(); k++) {
      kvBuilder = kvBuilder.addElements(KV.of(keys.get(k), payloads.get(k)));
    }

    TestStream<KV<String, String>> testStream = kvBuilder.advanceWatermarkToInfinity();
    PCollection<KV<String, String>> kvs = p.apply("Test stream", testStream);

    return kvs.apply(
        "To Record",
        MapElements.into(TypeDescriptor.of(Record.class))
            .via(kv -> SolaceDataUtils.getSolaceRecord(kv.getValue(), kv.getKey())));
  }

  private SolaceOutput getWriteTransform(
      SolaceIO.SubmissionMode mode, SolaceIO.WriterType writerType, Pipeline p) {
    MockSessionService service = new MockSessionService(mode);
    SessionServiceFactory fakeSessionServiceFactory = new MockSessionServiceFactory(service);

    PCollection<Record> records = getRecords(p);
    return records.apply(
        "Write to Solace",
        SolaceIO.write()
            .to(Solace.Queue.fromName("queue"))
            .withSubmissionMode(mode)
            .withWriterType(writerType)
            .withDeliveryMode(DeliveryMode.PERSISTENT)
            .withSessionServiceFactory(fakeSessionServiceFactory));
  }

  private static PCollection<String> getIdsPCollection(SolaceOutput output) {
    return output
        .getSuccessfulPublish()
        .apply(
            "Get message ids", MapElements.into(strings()).via(Solace.PublishResult::getMessageId));
  }

  @Test
  public void testWriteLatencyStreaming() {
    SolaceIO.SubmissionMode mode = SolaceIO.SubmissionMode.LOWER_LATENCY;
    SolaceIO.WriterType writerType = SolaceIO.WriterType.STREAMING;

    SolaceOutput output = getWriteTransform(mode, writerType, pipeline);
    PCollection<String> ids = getIdsPCollection(output);

    PAssert.that(ids).containsInAnyOrder(keys);
    PAssert.that(output.getFailedPublish()).empty();

    pipeline.run();
  }

  @Test
  public void testWriteThroughputStreaming() {
    SolaceIO.SubmissionMode mode = SolaceIO.SubmissionMode.HIGHER_THROUGHPUT;
    SolaceIO.WriterType writerType = SolaceIO.WriterType.STREAMING;

    SolaceOutput output = getWriteTransform(mode, writerType, pipeline);
    PCollection<String> ids = getIdsPCollection(output);

    PAssert.that(ids).containsInAnyOrder(keys);
    PAssert.that(output.getFailedPublish()).empty();

    pipeline.run();
  }

  @Test
  public void testWriteLatencyBatched() {
    SolaceIO.SubmissionMode mode = SolaceIO.SubmissionMode.LOWER_LATENCY;
    SolaceIO.WriterType writerType = SolaceIO.WriterType.BATCHED;

    SolaceOutput output = getWriteTransform(mode, writerType, pipeline);
    PCollection<String> ids = getIdsPCollection(output);

    PAssert.that(ids).containsInAnyOrder(keys);
    PAssert.that(output.getFailedPublish()).empty();

    pipeline.run();
  }

  @Test
  public void testWriteThroughputBatched() {
    SolaceIO.SubmissionMode mode = SolaceIO.SubmissionMode.HIGHER_THROUGHPUT;
    SolaceIO.WriterType writerType = SolaceIO.WriterType.BATCHED;

    SolaceOutput output = getWriteTransform(mode, writerType, pipeline);
    PCollection<String> ids = getIdsPCollection(output);

    PAssert.that(ids).containsInAnyOrder(keys);
    PAssert.that(output.getFailedPublish()).empty();

    pipeline.run();
  }
}
