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
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.Set;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.UsesUnboundedPCollections;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.joda.time.Duration;
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
        PubsubIO.<String>read().topic("projects/myproject/topics/mytopic").getName());
    assertEquals("PubsubIO.Write",
        PubsubIO.<String>write().topic("projects/myproject/topics/mytopic").getName());
  }

  @Test
  public void testTopicValidationSuccess() throws Exception {
    PubsubIO.<String>read().topic("projects/my-project/topics/abc");
    PubsubIO.<String>read().topic("projects/my-project/topics/ABC");
    PubsubIO.<String>read().topic("projects/my-project/topics/AbC-DeF");
    PubsubIO.<String>read().topic("projects/my-project/topics/AbC-1234");
    PubsubIO.<String>read().topic("projects/my-project/topics/AbC-1234-_.~%+-_.~%+-_.~%+-abc");
    PubsubIO.<String>read().topic(new StringBuilder()
        .append("projects/my-project/topics/A-really-long-one-")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("11111111111111111111111111111111111111111111111111111111111111111111111111")
        .toString());
  }

  @Test
  public void testTopicValidationBadCharacter() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    PubsubIO.<String>read().topic("projects/my-project/topics/abc-*-abc");
  }

  @Test
  public void testTopicValidationTooLong() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    PubsubIO.<String>read().topic(new StringBuilder().append
        ("projects/my-project/topics/A-really-long-one-")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("1111111111111111111111111111111111111111111111111111111111111111111111111111")
        .toString());
  }

  @Test
  public void testReadTopicDisplayData() {
    String topic = "projects/project/topics/topic";
    String subscription = "projects/project/subscriptions/subscription";
    Duration maxReadTime = Duration.standardMinutes(5);
    PubsubIO.Read<String> read = PubsubIO.<String>read()
        .topic(StaticValueProvider.of(topic))
        .timestampLabel("myTimestamp")
        .idLabel("myId")
        .maxNumRecords(1234)
        .maxReadTime(maxReadTime);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("topic", topic));
    assertThat(displayData, hasDisplayItem("timestampLabel", "myTimestamp"));
    assertThat(displayData, hasDisplayItem("idLabel", "myId"));
    assertThat(displayData, hasDisplayItem("maxNumRecords", 1234));
    assertThat(displayData, hasDisplayItem("maxReadTime", maxReadTime));
  }

  @Test
  public void testReadSubscriptionDisplayData() {
    String topic = "projects/project/topics/topic";
    String subscription = "projects/project/subscriptions/subscription";
    Duration maxReadTime = Duration.standardMinutes(5);
    PubsubIO.Read<String> read = PubsubIO.<String>read()
        .subscription(StaticValueProvider.of(subscription))
        .timestampLabel("myTimestamp")
        .idLabel("myId")
        .maxNumRecords(1234)
        .maxReadTime(maxReadTime);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("subscription", subscription));
    assertThat(displayData, hasDisplayItem("timestampLabel", "myTimestamp"));
    assertThat(displayData, hasDisplayItem("idLabel", "myId"));
    assertThat(displayData, hasDisplayItem("maxNumRecords", 1234));
    assertThat(displayData, hasDisplayItem("maxReadTime", maxReadTime));
  }

  @Test
  public void testNullTopic() {
    String subscription = "projects/project/subscriptions/subscription";
    PubsubIO.Read<String> read = PubsubIO.<String>read()
        .subscription(StaticValueProvider.of(subscription));
    assertNull(read.getTopic());
    assertNotNull(read.getSubscription());
    assertNotNull(DisplayData.from(read));
  }

  @Test
  public void testNullSubscription() {
    String topic = "projects/project/topics/topic";
    PubsubIO.Read<String> read = PubsubIO.<String>read()
        .topic(StaticValueProvider.of(topic));
    assertNotNull(read.getTopic());
    assertNull(read.getSubscription());
    assertNotNull(DisplayData.from(read));
  }

  @Test
  @Category({ValidatesRunner.class, UsesUnboundedPCollections.class})
  public void testPrimitiveReadDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    Set<DisplayData> displayData;
    PubsubIO.Read<String> read = PubsubIO.<String>read().withCoder(StringUtf8Coder.of());

    // Reading from a subscription.
    read = read.subscription("projects/project/subscriptions/subscription");
    displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat("PubsubIO.Read should include the subscription in its primitive display data",
        displayData, hasItem(hasDisplayItem("subscription")));

    // Reading from a topic.
    read = read.topic("projects/project/topics/topic");
    displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat("PubsubIO.Read should include the topic in its primitive display data",
        displayData, hasItem(hasDisplayItem("topic")));
  }

  @Test
  public void testWriteDisplayData() {
    String topic = "projects/project/topics/topic";
    PubsubIO.Write<?> write = PubsubIO.<String>write()
        .topic(topic)
        .timestampLabel("myTimestamp")
        .idLabel("myId");

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("topic", topic));
    assertThat(displayData, hasDisplayItem("timestampLabel", "myTimestamp"));
    assertThat(displayData, hasDisplayItem("idLabel", "myId"));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testPrimitiveWriteDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    PubsubIO.Write<?> write = PubsubIO.<String>write().topic("projects/project/topics/topic");

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("PubsubIO.Write should include the topic in its primitive display data",
        displayData, hasItem(hasDisplayItem("topic")));
  }
}
