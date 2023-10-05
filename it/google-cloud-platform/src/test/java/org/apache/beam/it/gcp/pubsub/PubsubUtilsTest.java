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
package org.apache.beam.it.gcp.pubsub;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.pubsub.PubsubUtils.createTestId;
import static org.apache.beam.it.gcp.pubsub.PubsubUtils.toSubscriptionName;
import static org.apache.beam.it.gcp.pubsub.PubsubUtils.toTopicName;
import static org.junit.Assert.assertThrows;

import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import org.junit.Test;

/** Unit tests for {@link PubsubUtils}. */
public class PubsubUtilsTest {

  @Test
  public void testToTopicNameValid() {
    TopicName topicName =
        toTopicName(Topic.newBuilder().setName("projects/project-a/topics/topic-x").build());
    assertThat(topicName.getProject()).isEqualTo("project-a");
    assertThat(topicName.getTopic()).isEqualTo("topic-x");
  }

  @Test
  public void testToTopicNameInvalidShouldFail() {
    assertThrows(
        IllegalArgumentException.class,
        () -> toTopicName(Topic.newBuilder().setName("project-a.topic-x").build()));
  }

  @Test
  public void testToSubscriptionNameValid() {
    SubscriptionName subscriptionName =
        toSubscriptionName(
            Subscription.newBuilder()
                .setName("projects/project-a/subscriptions/topic-x-sub0")
                .build());
    assertThat(subscriptionName.getProject()).isEqualTo("project-a");
    assertThat(subscriptionName.getSubscription()).isEqualTo("topic-x-sub0");
  }

  @Test
  public void testToSubscriptionNameInvalidShouldFail() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            toSubscriptionName(
                Subscription.newBuilder().setName("project-a.topic-x-sub0").build()));
  }

  @Test
  public void testCreateTopicName() {
    String name = "create-topic-name";
    assertThat(createTestId(name)).matches(name + "-\\d{17}");
  }

  @Test
  public void testCreateTopicNameWithUppercase() {
    assertThat(createTestId("testWithUpperCase")).matches("test-with-upper-case-\\d{17}");
  }
}
