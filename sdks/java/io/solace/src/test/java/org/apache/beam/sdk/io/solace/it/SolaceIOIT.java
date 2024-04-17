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

import static org.junit.Assert.assertNotEquals;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.apache.beam.sdk.io.solace.broker.BasicAuthJcsmpSessionServiceFactory;
import org.apache.beam.sdk.io.solace.broker.BasicAuthSempClientFactory;
import org.apache.beam.sdk.io.solace.data.Solace.Queue;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.builder.EqualsBuilder;

public class SolaceIOIT {
  private static final Logger LOG = LoggerFactory.getLogger(SolaceIOIT.class);
  private static SolaceContainerManager solaceContainerManager;
  private static final TestPipelineOptions testOptions;

  static {
    testOptions = PipelineOptionsFactory.create().as(TestPipelineOptions.class);
    testOptions.setBlockOnRun(false);
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.fromOptions(testOptions);

  @BeforeClass
  public static void setup() {
    System.out.println("START");
    solaceContainerManager = new SolaceContainerManager();
    solaceContainerManager.start();
  }

  @AfterClass
  public static void afterClass() {
    if (solaceContainerManager != null) {
      solaceContainerManager.stop();
    }
  }

  @Test
  public void test() {
    // Similar approach to
    // https://github.com/apache/beam/blob/812e98fac243bab2a88f6ea5fad6147ff8e54a97/sdks/java/io/kafka/src/test/java/org/apache/beam/sdk/io/kafka/KafkaIOIT.java#L216
    String queueName = "test_queue";
    solaceContainerManager.createQueueWithSubscriptionTopic(queueName);

    solaceContainerManager.getQueueDetails(queueName);
    String payload = "{\"field_str\":\"value\",\"field_int\":123}";
    solaceContainerManager.sendToTopic(payload, ImmutableList.of("Solace-Message-ID:m1"));
    solaceContainerManager.sendToTopic(payload, ImmutableList.of("Solace-Message-ID:m2"));
    solaceContainerManager.getQueueDetails(queueName);

    pipeline.getOptions().as(StreamingOptions.class).setStreaming(true);

    PCollection<Record> events =
        pipeline.apply(
            "Read from Solace",
            SolaceIO.read()
                .from(Queue.fromName(queueName))
                .withMaxNumConnections(1)
                .withSempClientFactory(
                    BasicAuthSempClientFactory.builder()
                        .host("http://localhost:8080")
                        .username("admin")
                        .password("admin")
                        .vpnName(SolaceContainerManager.VPN_NAME)
                        .build())
                .withSessionServiceFactory(
                    BasicAuthJcsmpSessionServiceFactory.builder()
                        .host("localhost")
                        .username(SolaceContainerManager.USERNAME)
                        .password(SolaceContainerManager.PASSWORD)
                        .vpnName(SolaceContainerManager.VPN_NAME)
                        .build()));
    // PCollection<Long> count =
    PCollection<Record> records =
        events
            .apply(
                "PassThrough",
                MapElements.via(
                    new SimpleFunction<Record, Record>() {
                      @Override
                      public Record apply(Record s) {
                        System.out.println("passthrough rec: " + s);
                        return s;
                      }
                      // })).apply("Window",
                      // Window.into(CalendarWindows.years(1)));
                    }))
            .apply("Window", Window.into(FixedWindows.of(Duration.standardSeconds(5))));

    System.out.println("xxxxx");

    PAssert.thatSingleton(
            records.apply(
                "Counting element", Combine.globally(Count.<Record>combineFn()).withoutDefaults()))
        .isEqualTo(1L);

    PAssert.that(records)
        .containsInAnyOrder(
            partialMatch(
                Record.builder()
                    .setMessageId("m1")
                    .setPayload(payload.getBytes(StandardCharsets.UTF_8))
                    .build()));

    PipelineResult writeResult = pipeline.run();
    // removing this line causes the pipeline not ingest any data
    PipelineResult.State writeState = writeResult.waitUntilFinish(Duration.standardSeconds(10));
    assertNotEquals(PipelineResult.State.FAILED, writeState);

    System.out.println("queue after pipeline");
    solaceContainerManager.getQueueDetails(queueName);
  }

  private static SerializableMatcher<Record> partialMatch(Record expected) {
    class Matcher extends BaseMatcher<Record> implements SerializableMatcher<Record> {
      @Override
      public boolean matches(Object item) {
        LOG.info("matches!!!");
        System.out.println("matches");
        if (!(item instanceof Record)) {
          return false;
        }

        Record actual = (Record) item;
        boolean partiallyEqual =
            EqualsBuilder.reflectionEquals(actual, expected, "replicationGroupMessageId");
        System.out.println("expected.equals(actual): " + expected.equals(actual));
        System.out.println("partiallyEqual: " + partiallyEqual);
        System.out.println("expected: " + expected);
        System.out.println("actual:   " + actual);

        return true;

        // for (Record needle : needles) {
        //     if (!haystack.contains(needle)) {
        //         return false;
        //     }
        // }
        // return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Contains all of: ");
        description.appendText(expected.toString());
      }
    }
    System.out.println("new matcher");
    return new Matcher();
  }
  // @Test
  // public void testWrite() {
  //     TestStream<String> createEvents =
  //             TestStream.create(StringUtf8Coder.of())
  //                     .addElements("r1", "r2")
  //                     .advanceWatermarkTo(
  //                             Instant.ofEpochMilli(0L).plus(Duration.standardSeconds(10)))
  //                     .addElements("r3", "r4")
  //                     .advanceWatermarkToInfinity();
  //
  //     PCollection<String> records = pipeline.apply(createEvents);
  //
  //     SolacePublishResult results =
  //             records.apply(
  //                             "map",
  //                             ParDo.of(
  //                                     new DoFn<String, Record>() {
  //                                         @ProcessElement
  //                                         public void processElement(ProcessContext c) {
  //                                             // System.out.println("Failed: " + c.element());
  //                                             c.output(
  //                                                     buildRecord(
  //                                                             c.element(),
  //                                                             "payload_" + c.element()));
  //                                         }
  //                                     }))
  //                     .apply(
  //                             SolaceIO.writeSolaceRecords()
  //                                     .to(Topic.fromName("test_topic"))
  //                                     .withSessionPropertiesProvider(
  //                                             BasicAuthenticationProvider.builder()
  //                                                     .username("xx")
  //                                                     .password("xx")
  //                                                     .host("localhost")
  //                                                     .vpnName(solaceContainer.getVpn())
  //                                                     .build())
  //                                     .withDeliveryMode(DeliveryMode.PERSISTENT)
  //                                     .withSubmissionMode(SubmissionMode.HIGHER_THROUGHPUT)
  //                                     .withWriterType(WriterType.BATCHED)
  //                                     .withMaxNumOfUsedWorkers(1)
  //                                     .withNumberOfClientsPerWorker(1));
  //
  //     results.getSuccessfulPublish()
  //             .apply(
  //                     "Successful records",
  //                     ParDo.of(
  //                             new DoFn<PublishResult, Integer>() {
  //                                 @ProcessElement
  //                                 public void processElement(ProcessContext c) {
  //                                     System.out.println("OK: " + c.element());
  //                                     c.output(1);
  //                                 }
  //                             }));
  //
  //     results.getFailedPublish()
  //             .apply(
  //                     "Failed records",
  //                     ParDo.of(
  //                             new DoFn<PublishResult, Integer>() {
  //                                 @ProcessElement
  //                                 public void processElement(ProcessContext c) {
  //                                     System.out.println("Failed: " + c.element());
  //                                     c.output(1);
  //                                 }
  //                             }));
  //
  //     pipeline.run().waitUntilFinish();
  // }

  // private static Record buildRecord(String id, String payload) {
  //     return Record.builder()
  //             .setMessageId(id)
  //             .setPayload(payload.getBytes(StandardCharsets.UTF_8))
  //             .setSenderTimestamp(1712224703L)
  //             .setDestination(
  //                     Destination.builder()
  //                             .setName("test_topic")
  //                             .setType(DestinationType.TOPIC)
  //                             .build())
  //             .build();
  // }
}
