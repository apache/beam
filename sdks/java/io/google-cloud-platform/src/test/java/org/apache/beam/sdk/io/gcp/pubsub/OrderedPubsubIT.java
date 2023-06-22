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

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration tests for {@link Schema} related {@link PubsubClient} operations. */
@RunWith(JUnit4.class)
public class OrderedPubsubIT {

  private static final Logger LOG = LoggerFactory.getLogger(OrderedPubsubIT.class);

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  private static PubsubClient pubsubClient;

  public static final String TOPIC_NAME = "ordered-delivery-test-topic";
  private static TopicPath topic;
  private static PubsubClient.SubscriptionPath subscription;

  @BeforeClass
  public static void setup() throws IOException {
    PubsubOptions options = TestPipeline.testingPipelineOptions().as(PubsubOptions.class);
    String project = options.getProject();
    String postFix = "-" + Instant.now().getMillis();
    pubsubClient = PubsubGrpcClient.FACTORY.newClient(null, null, options);
    topic = PubsubClient.topicPathFromName(project, TOPIC_NAME + postFix);

    pubsubClient.createTopic(topic);
    subscription =
        PubsubClient.subscriptionPathFromName(
            project, "ordered-delivery-test-subscription" + postFix);
    pubsubClient.createOrderedSubscription(topic, subscription, 30);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    pubsubClient.deleteTopic(topic);
    pubsubClient.close();
  }

  private void publishWithRetry(int i) {
    while (true) {
      try {
        pubsubClient.publish(
            topic,
            Lists.newArrayList(
                PubsubClient.OutgoingMessage.of(
                    PubsubMessage.newBuilder()
                        .setData(ByteString.copyFrom(String.format("%d", i).getBytes("utf8")))
                        .setOrderingKey(String.format("%d", i % 7))
                        .build(),
                    12345,
                    "key" + i,
                    topic.getFullPath())));
        return;
      } catch (Exception e) {
        LOG.error("problems with pubsub publish", e);
      }
    }
  }

  private void publishMessagesToPubsub() throws IOException, InterruptedException {
    for (int i = 0; i < 5000; i++) {
      publishWithRetry(i);
      if (i % 100 == 0) {
        System.out.println("Published " + i + " messages");
        Thread.sleep(1000);
      }
    }
  }

  @Test
  public void testReadOrderedMessages() throws IOException, InterruptedException {
    publishMessagesToPubsub();

    pipeline
        .apply(new ReadOrderedMessages(subscription.getName(), "apache-beam-testing", 10))
        .apply(ParDo.of(new VerifyOrderFn()));
    pipeline.runWithAdditionalOptionArgs(Collections.singletonList("--streaming"));
  }

  private static class VerifyOrderFn extends DoFn<KV<byte[], byte[]>, KV<byte[], byte[]>> {

    @SuppressWarnings("unused")
    @StateId("lastKey")
    private final StateSpec<ValueState<Integer>> lastKey = StateSpecs.value();

    @ProcessElement
    public void process(
        @Element KV<byte[], byte[]> elm, @StateId("lastKey") ValueState<Integer> lastKey)
        throws UnsupportedEncodingException {
      Integer lastVal = lastKey.read();
      Integer nextVal =
          Integer.parseInt(
              new String(Objects.requireNonNull(elm.getValue()), StandardCharsets.UTF_8));
      if (lastVal != null && nextVal < lastVal) {
        throw new RuntimeException(
            "Out of order message for key "
                + new String(Objects.requireNonNull(elm.getKey()), StandardCharsets.UTF_8)
                + " lastVal "
                + lastVal
                + " nextVal "
                + nextVal);
      }
      lastKey.write(nextVal);
    }
  }
}
