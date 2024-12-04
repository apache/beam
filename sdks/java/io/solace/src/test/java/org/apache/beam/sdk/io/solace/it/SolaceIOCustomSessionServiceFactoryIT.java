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
package org.apache.beam.sdk.io.solace.it;

import static org.apache.beam.sdk.io.solace.it.SolaceContainerManager.TOPIC_NAME;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.junit.Assert.assertEquals;

import com.solacesystems.jcsmp.DeliveryMode;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.apache.beam.sdk.io.solace.SolaceIO.WriterType;
import org.apache.beam.sdk.io.solace.broker.BasicAuthJcsmpSessionServiceFactory;
import org.apache.beam.sdk.io.solace.broker.BasicAuthSempClientFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.Queue;
import org.apache.beam.sdk.io.solace.data.SolaceDataUtils;
import org.apache.beam.sdk.io.solace.write.SolaceOutput;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class SolaceIOCustomSessionServiceFactoryIT {
  private static final String NAMESPACE = SolaceIOCustomSessionServiceFactoryIT.class.getName();
  private static final String READ_COUNT = "read_count";
  private static final String QUEUE_NAME = "test_queue";
  private static final long PUBLISH_MESSAGE_COUNT = 20;
  private static final TestPipelineOptions pipelineOptions;
  private static SolaceContainerManager solaceContainerManager;

  static {
    pipelineOptions = PipelineOptionsFactory.create().as(TestPipelineOptions.class);
    pipelineOptions.as(StreamingOptions.class).setStreaming(true);
    // For the read connector tests, we need to make sure that p.run() does not block
    pipelineOptions.setBlockOnRun(false);
    pipelineOptions.as(TestPipelineOptions.class).setBlockOnRun(false);
  }

  @Rule public final TestPipeline pipeline = TestPipeline.fromOptions(pipelineOptions);

  @BeforeClass
  public static void setup() throws IOException {
    solaceContainerManager = new SolaceContainerManager();
    solaceContainerManager.start();
    solaceContainerManager.createQueueWithSubscriptionTopic(QUEUE_NAME);
  }

  @AfterClass
  public static void afterClass() {
    if (solaceContainerManager != null) {
      solaceContainerManager.stop();
    }
  }

  /**
   * This test verifies ability to create a custom {@link
   * org.apache.beam.sdk.io.solace.broker.SessionServiceFactory} and presents an example of doing
   * that with the custom {@link FixedCredentialsBasicAuthJcsmpSessionServiceFactory}.
   */
  @Test
  public void test01writeAndReadWithCustomSessionServiceFactory() {
    Pipeline writerPipeline = createWriterPipeline(WriterType.BATCHED);
    writerPipeline
        .apply(
            "Read from Solace",
            SolaceIO.read()
                .from(Queue.fromName(QUEUE_NAME))
                .withMaxNumConnections(1)
                .withDeduplicateRecords(true)
                .withSempClientFactory(
                    BasicAuthSempClientFactory.builder()
                        .host("http://localhost:" + solaceContainerManager.sempPortMapped)
                        .username("admin")
                        .password("admin")
                        .vpnName(SolaceContainerManager.VPN_NAME)
                        .build())
                .withSessionServiceFactory(
                    new FixedCredentialsBasicAuthJcsmpSessionServiceFactory(
                        "localhost:" + solaceContainerManager.jcsmpPortMapped)))
        .apply("Count", ParDo.of(new CountingFn<>(NAMESPACE, READ_COUNT)));

    PipelineResult pipelineResult = writerPipeline.run();
    // We need enough time for Beam to pull all messages from the queue, but we need a timeout too,
    // as the Read connector will keep attempting to read forever.
    pipelineResult.waitUntilFinish(Duration.standardSeconds(15));

    MetricsReader metricsReader = new MetricsReader(pipelineResult, NAMESPACE);
    long actualRecordsCount = metricsReader.getCounterMetric(READ_COUNT);
    assertEquals(PUBLISH_MESSAGE_COUNT, actualRecordsCount);
  }

  private Pipeline createWriterPipeline(WriterType writerType) {
    TestStream.Builder<KV<String, String>> kvBuilder =
        TestStream.create(KvCoder.of(AvroCoder.of(String.class), AvroCoder.of(String.class)))
            .advanceWatermarkTo(Instant.EPOCH);

    for (int i = 0; i < PUBLISH_MESSAGE_COUNT; i++) {
      String key = "Solace-Message-ID:m" + i;
      String payload = String.format("{\"field_str\":\"value\",\"field_int\":123%d}", i);
      kvBuilder =
          kvBuilder
              .addElements(KV.of(key, payload))
              .advanceProcessingTime(Duration.standardSeconds(60));
    }

    TestStream<KV<String, String>> testStream = kvBuilder.advanceWatermarkToInfinity();

    PCollection<KV<String, String>> kvs =
        pipeline.apply(String.format("Test stream %s", writerType), testStream);

    PCollection<Solace.Record> records =
        kvs.apply(
            String.format("To Record %s", writerType),
            MapElements.into(TypeDescriptor.of(Solace.Record.class))
                .via(kv -> SolaceDataUtils.getSolaceRecord(kv.getValue(), kv.getKey())));

    SolaceOutput result =
        records.apply(
            String.format("Write to Solace %s", writerType),
            SolaceIO.write()
                .to(Solace.Topic.fromName(TOPIC_NAME))
                .withSubmissionMode(SolaceIO.SubmissionMode.TESTING)
                .withWriterType(writerType)
                .withDeliveryMode(DeliveryMode.PERSISTENT)
                .withNumberOfClientsPerWorker(1)
                .withNumShards(1)
                .withSessionServiceFactory(
                    BasicAuthJcsmpSessionServiceFactory.builder()
                        .host("localhost:" + solaceContainerManager.jcsmpPortMapped)
                        .username(SolaceContainerManager.USERNAME)
                        .password(SolaceContainerManager.PASSWORD)
                        .vpnName(SolaceContainerManager.VPN_NAME)
                        .build()));
    result
        .getSuccessfulPublish()
        .apply(
            String.format("Get ids %s", writerType),
            MapElements.into(strings()).via(Solace.PublishResult::getMessageId));

    return pipeline;
  }

  private static class CountingFn<T> extends DoFn<T, T> {

    private final Counter elementCounter;

    CountingFn(String namespace, String name) {
      elementCounter = Metrics.counter(namespace, name);
    }

    @ProcessElement
    public void processElement(@Element T record, OutputReceiver<T> c) {
      elementCounter.inc(1L);
      c.output(record);
    }
  }
}
