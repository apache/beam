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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Read;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesUnboundedPCollections;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for PubsubIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
public class PubsubIOTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testPubsubIOGetName() {
    assertEquals("PubsubIO.Read",
        PubsubIO.readStrings().fromTopic("projects/myproject/topics/mytopic").getName());
    assertEquals("PubsubIO.Write",
        PubsubIO.writeStrings().to("projects/myproject/topics/mytopic").getName());
  }

  @Test
  public void testTopicValidationSuccess() throws Exception {
    PubsubIO.readStrings().fromTopic("projects/my-project/topics/abc");
    PubsubIO.readStrings().fromTopic("projects/my-project/topics/ABC");
    PubsubIO.readStrings().fromTopic("projects/my-project/topics/AbC-DeF");
    PubsubIO.readStrings().fromTopic("projects/my-project/topics/AbC-1234");
    PubsubIO.readStrings().fromTopic("projects/my-project/topics/AbC-1234-_.~%+-_.~%+-_.~%+-abc");
    PubsubIO.readStrings().fromTopic(new StringBuilder()
        .append("projects/my-project/topics/A-really-long-one-")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("11111111111111111111111111111111111111111111111111111111111111111111111111")
        .toString());
  }

  @Test
  public void testTopicValidationBadCharacter() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    PubsubIO.readStrings().fromTopic("projects/my-project/topics/abc-*-abc");
  }

  @Test
  public void testTopicValidationTooLong() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    PubsubIO.readStrings().fromTopic(new StringBuilder().append
        ("projects/my-project/topics/A-really-long-one-")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("1111111111111111111111111111111111111111111111111111111111111111111111111111")
        .toString());
  }

  @Test
  public void testReadTopicDisplayData() {
    String topic = "projects/project/topics/topic";
    PubsubIO.Read<String> read = PubsubIO.readStrings()
        .fromTopic(StaticValueProvider.of(topic))
        .withTimestampAttribute("myTimestamp")
        .withIdAttribute("myId");

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("topic", topic));
    assertThat(displayData, hasDisplayItem("timestampAttribute", "myTimestamp"));
    assertThat(displayData, hasDisplayItem("idAttribute", "myId"));
  }

  @Test
  public void testReadSubscriptionDisplayData() {
    String subscription = "projects/project/subscriptions/subscription";
    PubsubIO.Read<String> read = PubsubIO.readStrings()
        .fromSubscription(StaticValueProvider.of(subscription))
        .withTimestampAttribute("myTimestamp")
        .withIdAttribute("myId");

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("subscription", subscription));
    assertThat(displayData, hasDisplayItem("timestampAttribute", "myTimestamp"));
    assertThat(displayData, hasDisplayItem("idAttribute", "myId"));
  }

  @Test
  public void testNullTopic() {
    String subscription = "projects/project/subscriptions/subscription";
    PubsubIO.Read<String> read = PubsubIO.readStrings()
        .fromSubscription(StaticValueProvider.of(subscription));
    assertNull(read.getTopicProvider());
    assertNotNull(read.getSubscriptionProvider());
    assertNotNull(DisplayData.from(read));
  }

  @Test
  public void testNullSubscription() {
    String topic = "projects/project/topics/topic";
    PubsubIO.Read<String> read = PubsubIO.readStrings()
        .fromTopic(StaticValueProvider.of(topic));
    assertNotNull(read.getTopicProvider());
    assertNull(read.getSubscriptionProvider());
    assertNotNull(DisplayData.from(read));
  }

  @Test
  public void testValueProviderSubscription() {
    StaticValueProvider<String> provider =
        StaticValueProvider.of("projects/project/subscriptions/subscription");
    Read<String> pubsubRead =
        PubsubIO.readStrings()
            .fromSubscription(provider);
    Pipeline.create().apply(pubsubRead);
    assertThat(pubsubRead.getSubscriptionProvider(), not(nullValue()));
    assertThat(pubsubRead.getSubscriptionProvider().isAccessible(), is(true));
    assertThat(pubsubRead.getSubscriptionProvider().get().asPath(), equalTo(provider.get()));
  }

  @Test
  public void testRuntimeValueProviderSubscription() {
    TestPipeline pipeline = TestPipeline.create();
    ValueProvider<String> subscription =
        pipeline.newProvider("projects/project/subscriptions/subscription");
    Read<String> pubsubRead = PubsubIO.readStrings().fromSubscription(subscription);
    pipeline.apply(pubsubRead);
    assertThat(pubsubRead.getSubscriptionProvider(), not(nullValue()));
    assertThat(pubsubRead.getSubscriptionProvider().isAccessible(), is(false));
  }

  @Test
  public void testValueProviderTopic() {
    StaticValueProvider<String> provider = StaticValueProvider.of("projects/project/topics/topic");
    Read<String> pubsubRead =
        PubsubIO.readStrings().fromTopic(provider);
    Pipeline.create().apply(pubsubRead);
    assertThat(pubsubRead.getTopicProvider(), not(nullValue()));
    assertThat(pubsubRead.getTopicProvider().isAccessible(), is(true));
    assertThat(
        pubsubRead.getTopicProvider().get().asPath(),
        equalTo(provider.get()));
  }

  @Test
  public void testRuntimeValueProviderTopic() {
    TestPipeline pipeline = TestPipeline.create();
    ValueProvider<String> topic = pipeline.newProvider("projects/project/topics/topic");
    Read<String> pubsubRead = PubsubIO.readStrings().fromTopic(topic);
    pipeline.apply(pubsubRead);
    assertThat(pubsubRead.getTopicProvider(), not(nullValue()));
    assertThat(pubsubRead.getTopicProvider().isAccessible(), is(false));
  }

  @Test
  @Category({ValidatesRunner.class, UsesUnboundedPCollections.class})
  public void testPrimitiveReadDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    Set<DisplayData> displayData;
    PubsubIO.Read<String> baseRead = PubsubIO.readStrings();

    // Reading from a subscription.
    PubsubIO.Read<String> read =
        baseRead.fromSubscription("projects/project/subscriptions/subscription");
    displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat(
        "PubsubIO.Read should include the subscription in its primitive display data",
        displayData,
        hasItem(hasDisplayItem("subscription")));

    // Reading from a topic.
    read = baseRead.fromTopic("projects/project/topics/topic");
    displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat("PubsubIO.Read should include the topic in its primitive display data",
        displayData, hasItem(hasDisplayItem("topic")));
  }

  @Test
  public void testWriteDisplayData() {
    String topic = "projects/project/topics/topic";
    PubsubIO.Write<?> write = PubsubIO.writeStrings()
        .to(topic)
        .withTimestampAttribute("myTimestamp")
        .withIdAttribute("myId");

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("topic", topic));
    assertThat(displayData, hasDisplayItem("timestampAttribute", "myTimestamp"));
    assertThat(displayData, hasDisplayItem("idAttribute", "myId"));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testPrimitiveWriteDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    PubsubIO.Write<?> write = PubsubIO.writeStrings().to("projects/project/topics/topic");

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("PubsubIO.Write should include the topic in its primitive display data",
        displayData, hasItem(hasDisplayItem("topic")));
  }
}
