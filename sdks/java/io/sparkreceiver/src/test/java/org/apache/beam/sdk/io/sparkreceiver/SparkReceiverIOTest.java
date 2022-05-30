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
package org.apache.beam.sdk.io.sparkreceiver;

import io.cdap.plugin.hubspot.common.BaseHubspotConfig;
import io.cdap.plugin.hubspot.source.streaming.HubspotStreamingSourceConfig;
import io.cdap.plugin.hubspot.source.streaming.PullFrequency;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.receiver.Receiver;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test class for {@link SparkReceiverIO}. */
@RunWith(JUnit4.class)
public class SparkReceiverIOTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static final String HUBSPOT_CONTACTS_OUTPUT_TXT =
      "src/test/resources/hubspot-contacts-output.txt";

  private static final ImmutableMap<String, Object> TEST_HUBSPOT_PARAMS_MAP =
      ImmutableMap.<String, Object>builder()
          .put("apiServerUrl", BaseHubspotConfig.DEFAULT_API_SERVER_URL)
          .put("objectType", "Contacts")
          .put("referenceName", "Contacts")
          .put("apiKey", System.getenv("HUBSPOT_TOKEN"))
          .put("pullFrequency", PullFrequency.MINUTES_15.getName())
          .build();

  private static class CustomSparkConsumer<V> implements SparkConsumer<V> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomSparkConsumer.class);

    private static final Queue<Object> queue = new ConcurrentLinkedQueue<>();
    private Receiver<V> sparkReceiver;

    @Override
    public V poll() {
      return (V) queue.poll();
    }

    @Override
    public void start(Receiver<V> sparkReceiver) {
      try {
        this.sparkReceiver = sparkReceiver;
        new WrappedSupervisor<>(
            sparkReceiver,
            new SparkConf(),
            objects -> {
              queue.offer(objects[0]);
              return null;
            });
        sparkReceiver.supervisor().startReceiver();
      } catch (Exception e) {
        LOG.error("Can not init Spark Receiver!", e);
      }
    }

    @Override
    public void stop() {
      queue.clear();
      sparkReceiver.stop("Stopped");
    }

    @Override
    public boolean hasRecords() {
      return !queue.isEmpty();
    }
  }

  @Test
  public void testReadFromHubspot() throws Exception {

    HubspotStreamingSourceConfig pluginConfig =
        new ConfigWrapper<>(HubspotStreamingSourceConfig.class)
            .withParams(TEST_HUBSPOT_PARAMS_MAP)
            .build();

    ProxyReceiverBuilder<String, HubspotCustomReceiver> receiverBuilder =
        new ProxyReceiverBuilder<>(HubspotCustomReceiver.class).withConstructorArgs(pluginConfig);
    SparkReceiverIO.Read<String> reader =
        SparkReceiverIO.<String>read()
            .withValueClass(String.class)
            .withValueCoder(StringUtf8Coder.of())
            .withGetOffsetFn(SparkReceiverUtils::getOffsetByHubspotRecord)
            .withSparkConsumer(new CustomSparkConsumer<>())
            .withSparkReceiverBuilder(receiverBuilder);

    PCollection<String> input = p.apply(reader).setCoder(StringUtf8Coder.of());

    input
        .apply(
            "globalwindow",
            Window.<String>into(new GlobalWindows())
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(30))))
                .discardingFiredPanes())
        .apply(
            "Write to file", TextIO.write().withWindowedWrites().to(HUBSPOT_CONTACTS_OUTPUT_TXT));

    p.run().waitUntilFinish(Duration.standardSeconds(30));
  }
}
