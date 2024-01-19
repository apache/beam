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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link PubsubIO.Write} operations. */
@RunWith(JUnit4.class)
public class PubsubWriteIT {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private PubsubClient pubsubClient;

  private TopicPath testTopic;

  @Before
  public void setup() throws IOException {
    PubsubOptions options = TestPipeline.testingPipelineOptions().as(PubsubOptions.class);
    String project = options.getProject();
    pubsubClient = PubsubGrpcClient.FACTORY.newClient(null, null, options);
    testTopic =
        PubsubClient.topicPathFromName(project, "pubsub-write-" + Instant.now().getMillis());
    pubsubClient.createTopic(testTopic);
  }

  @After
  public void tearDown() throws IOException {
    pubsubClient.deleteTopic(testTopic);
    pubsubClient.close();
  }

  @Test
  public void testBoundedWriteSmallMessage() {
    String smallMessage = RandomStringUtils.randomAscii(100);
    pipeline.apply(Create.of(smallMessage)).apply(PubsubIO.writeStrings().to(testTopic.getPath()));
    pipeline.run();
  }

  @Test
  public void testBoundedWriteSequence() {
    pipeline
        .apply(GenerateSequence.from(0L).to(1000L))
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(sequence -> Objects.requireNonNull(sequence).toString()))
        .apply(PubsubIO.writeStrings().to(testTopic.getPath()));
    pipeline.run();
  }

  @Test
  public void testBoundedWriteLargeMessage() {
    String largeMessage = RandomStringUtils.randomAscii(1_000_000);
    pipeline.apply(Create.of(largeMessage)).apply(PubsubIO.writeStrings().to(testTopic.getPath()));
    pipeline.run();
  }

  @Test
  public void testBoundedWriteMessageWithAttributes() {
    byte[] payload = RandomStringUtils.randomAscii(1_000_000).getBytes(StandardCharsets.UTF_8);
    Map<String, String> attributes =
        ImmutableMap.<String, String>builder()
            .put("id", "1")
            .put("description", RandomStringUtils.randomAscii(100))
            .build();

    pipeline
        .apply(Create.of(new PubsubMessage(payload, attributes)))
        .apply(PubsubIO.writeMessages().to(testTopic.getPath()));
    pipeline.run();
  }
}
