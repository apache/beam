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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.apache.beam.sdk.io.solace.broker.BasicAuthJcsmpSessionServiceFactory;
import org.apache.beam.sdk.io.solace.broker.BasicAuthSempClientFactory;
import org.apache.beam.sdk.io.solace.data.Solace.Queue;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class SolaceIOIT {
  private static final String NAMESPACE = SolaceIOIT.class.getName();
  private static final String READ_COUNT = "read_count";
  private static SolaceContainerManager solaceContainerManager;
  private static final TestPipelineOptions readPipelineOptions;

  static {
    readPipelineOptions = PipelineOptionsFactory.create().as(TestPipelineOptions.class);
    readPipelineOptions.setBlockOnRun(false);
    readPipelineOptions.as(TestPipelineOptions.class).setBlockOnRun(false);
    readPipelineOptions.as(StreamingOptions.class).setStreaming(false);
  }

  @Rule public final TestPipeline readPipeline = TestPipeline.fromOptions(readPipelineOptions);

  @BeforeClass
  public static void setup() throws IOException {
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
  public void testRead() {
    String queueName = "test_queue";
    solaceContainerManager.createQueueWithSubscriptionTopic(queueName);

    // todo this is very slow, needs to be replaced with the SolaceIO.write connector.
    int publishMessagesCount = 20;
    for (int i = 0; i < publishMessagesCount; i++) {
      solaceContainerManager.sendToTopic(
          "{\"field_str\":\"value\",\"field_int\":123}",
          ImmutableList.of("Solace-Message-ID:m" + i));
    }

    readPipeline
        .apply(
            "Read from Solace",
            SolaceIO.read()
                .from(Queue.fromName(queueName))
                .withDeduplicateRecords(true)
                .withMaxNumConnections(1)
                .withSempClientFactory(
                    BasicAuthSempClientFactory.builder()
                        .host("http://localhost:" + solaceContainerManager.sempPortMapped)
                        .username("admin")
                        .password("admin")
                        .vpnName(SolaceContainerManager.VPN_NAME)
                        .build())
                .withSessionServiceFactory(
                    BasicAuthJcsmpSessionServiceFactory.builder()
                        .host("localhost:" + solaceContainerManager.jcsmpPortMapped)
                        .username(SolaceContainerManager.USERNAME)
                        .password(SolaceContainerManager.PASSWORD)
                        .vpnName(SolaceContainerManager.VPN_NAME)
                        .build()))
        .apply("Count", ParDo.of(new CountingFn<>(NAMESPACE, READ_COUNT)));

    PipelineResult pipelineResult = readPipeline.run();
    pipelineResult.waitUntilFinish(Duration.standardSeconds(15));

    MetricsReader metricsReader = new MetricsReader(pipelineResult, NAMESPACE);
    long actualRecordsCount = metricsReader.getCounterMetric(READ_COUNT);
    assertEquals(publishMessagesCount, actualRecordsCount);
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
