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
package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PreparePubsubWriteTest implements Serializable {

  private static final String pathInMessage =
      PubsubClient.topicPathFromName("test-project", "testTopicInMessage").getPath();
  private static final String pathInTopicProvider =
      PubsubClient.topicPathFromName("test-project", "testTopicInTopicProvider").getPath();
  private static final ValueProvider<PubsubIO.PubsubTopic> topicValueProvider =
      StaticValueProvider.of(PubsubIO.PubsubTopic.fromPath(pathInTopicProvider));
  private static final PubsubMessage messageAccordingToPubsubMessage =
      new PubsubMessage(
          "testPayload".getBytes(StandardCharsets.UTF_8), ImmutableMap.of(), null, pathInMessage);
  private static final List<PubsubMessage> pubsubMessages =
      ImmutableList.of(messageAccordingToPubsubMessage);

  @Rule public transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void topicInMessageOverrideTopicProvider() {
    PCollection<PubsubMessage> results =
        p.apply(Create.of(pubsubMessages).withCoder(PubsubMessageCoder.of()))
            .apply(new PreparePubsubWrite<>(topicValueProvider, x -> x));
    PAssert.that(results).containsInAnyOrder(messageAccordingToPubsubMessage);
    p.run().waitUntilFinish();
  }

  @Test
  public void topicInMessageNoTopicProvider() {
    PCollection<PubsubMessage> results =
        p.apply(Create.of(pubsubMessages).withCoder(PubsubMessageCoder.of()))
            .apply(new PreparePubsubWrite<>(null, x -> x));
    PAssert.that(results).containsInAnyOrder(messageAccordingToPubsubMessage);
    p.run().waitUntilFinish();
  }

  @Test
  public void topicOnlyInTopicProvider() {
    List<PubsubMessage> data = new ArrayList<>();
    data.add(
        new PubsubMessage(
            "testTopicProvider".getBytes(StandardCharsets.UTF_8), ImmutableMap.of(), null, null));
    PCollection<PubsubMessage> results =
        p.apply(Create.of(data).withCoder(PubsubMessageCoder.of()))
            .apply(new PreparePubsubWrite<>(topicValueProvider, x -> x));
    PAssert.that(results)
        .containsInAnyOrder(
            new PubsubMessage(
                "testTopicProvider".getBytes(StandardCharsets.UTF_8),
                ImmutableMap.of(),
                null,
                pathInTopicProvider));
    p.run().waitUntilFinish();
  }

  @Test
  public void pubsubMessageWithoutFormatFunction() {
    PCollection<PubsubMessage> results =
        p.apply(Create.of(pubsubMessages).withCoder(PubsubMessageCoder.of()))
            .apply(new PreparePubsubWrite<>(topicValueProvider, null));
    PAssert.that(results).containsInAnyOrder(messageAccordingToPubsubMessage);
    p.run().waitUntilFinish();
  }

  @Test
  public void stringWithoutFormatFunctionThrowError() {
    thrown.expectMessage("element should parse to pubsub message");
    List<String> data = new ArrayList<>();
    data.add("testUnparseablePayload");
    p.apply(Create.of(data).withCoder(StringUtf8Coder.of()))
        .apply(new PreparePubsubWrite<>(topicValueProvider, null));
    p.run().waitUntilFinish();
  }

  @Test
  public void pubsubMessageWithFormatFunction() {
    List<String> data = new ArrayList<>();
    data.add("testStringPayload");
    PCollection<PubsubMessage> results =
        p.apply(Create.of(data).withCoder(StringUtf8Coder.of()))
            .apply(
                new PreparePubsubWrite<>(
                    topicValueProvider,
                    x ->
                        new PubsubMessage(
                            x.getBytes(StandardCharsets.UTF_8),
                            ImmutableMap.of(),
                            null,
                            pathInMessage)));
    PAssert.that(results)
        .containsInAnyOrder(
            new PubsubMessage(
                "testStringPayload".getBytes(StandardCharsets.UTF_8),
                ImmutableMap.of(),
                null,
                pathInMessage));
    p.run().waitUntilFinish();
  }
}
