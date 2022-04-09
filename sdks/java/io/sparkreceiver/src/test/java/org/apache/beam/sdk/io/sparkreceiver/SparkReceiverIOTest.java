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

import static org.junit.Assert.*;

import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.common.BaseHubspotConfig;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.source.streaming.HubspotReceiver;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.source.streaming.HubspotStreamingSourceConfig;
import org.apache.beam.sdk.io.sparkreceiver.hubspot.source.streaming.PullFrequency;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

/** Test class for {@link SparkReceiverIO}. */
@RunWith(JUnit4.class)
public class SparkReceiverIOTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

    Duration WINDOW_TIME = Duration.standardSeconds(5);

    Duration ALLOWED_LATENESS = Duration.standardSeconds(5);

    private static final String HUBSPOT_CONTACTS_OUTPUT_TXT = "src/test/resources/hubspot-contacts-output.txt";

  private static final long NUM_OF_TEST_HUBSPOT_CONTACTS =
      Long.parseLong(System.getenv("HUBSPOT_CONTACTS_NUM"));
  private static final ImmutableMap<String, Object> TEST_HUBSPOT_PARAMS_MAP =
      ImmutableMap.<String, Object>builder()
          .put("apiServerUrl", BaseHubspotConfig.DEFAULT_API_SERVER_URL)
          .put("objectType", "Contacts")
          .put("referenceName", "Contacts")
          .put("apiKey", System.getenv("HUBSPOT_TOKEN"))
          .put("pullFrequency", PullFrequency.MINUTES_15.getName())
          .build();

  @Test
  public void testReadFromHubspot() throws IOException {

    HubspotStreamingSourceConfig pluginConfig =
        new ConfigWrapper<>(HubspotStreamingSourceConfig.class)
            .withParams(TEST_HUBSPOT_PARAMS_MAP)
            .build();

    SparkReceiverIO.Read<String> reader =
        SparkReceiverIO.<String>read()
            .withPluginConfig(pluginConfig)
            .withValueClass(String.class)
            .withSparkReceiverClass(HubspotReceiver.class);

    PCollection<String> input = p.apply(reader).setCoder(StringUtf8Coder.of())
            .apply("apply window", Window.<String>into(FixedWindows.of(WINDOW_TIME)));

    input.apply(new WriteOneFilePerWindow(HUBSPOT_CONTACTS_OUTPUT_TXT, 1));

    PAssert.that(input)
        .satisfies(
            (map) -> {
              long numOfCorrectRecords = 0;
              for (String record : map) {
                assertFalse(StringUtils.isEmpty(record));
                numOfCorrectRecords++;
              }
              assertEquals(NUM_OF_TEST_HUBSPOT_CONTACTS, numOfCorrectRecords);
              return null;
            });

      p.run().waitUntilFinish(Duration.millis(5000));

  }
}
