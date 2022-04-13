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

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.common.BaseHubspotConfig;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.source.streaming.HubspotReceiver;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.source.streaming.HubspotStreamingSourceConfig;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.source.streaming.PullFrequency;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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

  @Test
  public void testReadFromHubspot() {

    HubspotStreamingSourceConfig pluginConfig =
        new ConfigWrapper<>(HubspotStreamingSourceConfig.class)
            .withParams(TEST_HUBSPOT_PARAMS_MAP)
            .build();

    SparkReceiverIO.Read<String> reader =
        SparkReceiverIO.<String>read()
            .withPluginConfig(pluginConfig)
            .withValueClass(String.class)
            .withSparkReceiverClass(HubspotReceiver.class);

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
