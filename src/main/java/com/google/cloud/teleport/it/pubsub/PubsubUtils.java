/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.pubsub;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.CaseFormat;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** Utilities to make working with Pubsub easier. */
final class PubsubUtils {
  private PubsubUtils() {}

  /**
   * Regular expression to validate/extract a topic name string, following
   * projects/{project_id}/subscription/{topic_name}.
   */
  private static final Pattern TOPICS_PATTERN =
      Pattern.compile("^projects/(?P<project_id>[^/]+)/topics/(?P<topic_name>[^/]+)$");

  /**
   * Regular expression to validate/extract a subscription name string, following
   * projects/{project_id}/subscription/{subscription_name}.
   */
  private static final Pattern SUBSCRIPTIONS_PATTERN =
      Pattern.compile(
          "^projects/(?P<project_id>[^/]+)/subscriptions/(?P<subscription_name>[^/]+)$");

  /**
   * Convert the instance of {@link Topic} to a {@link TopicName}.
   *
   * <p>The idea of this project is to abstract the conversion/parse of the format
   * <strong>projects/{project_id}/topics/{topic_name}</strong>
   *
   * @param topic Base topic.
   * @return The reference to the topic.
   */
  static TopicName toTopicName(Topic topic) {
    Matcher matcher = TOPICS_PATTERN.matcher(topic.getName());
    checkArgument(
        matcher.find(),
        "Topic name must be in the format 'projects/{project_id}/topics/{topic_name}.");

    return TopicName.of(matcher.group("project_id"), matcher.group("topic_name"));
  }

  /**
   * Convert the instance of {@link Subscription} to a {@link SubscriptionName}.
   *
   * <p>The idea of this project is to abstract the conversion/parse of the format
   * <strong>projects/{project_id}/subscription/{subscription_name}</strong>
   *
   * @param subscription Base subscription.
   * @return The reference to the subscription.
   */
  static SubscriptionName toSubscriptionName(Subscription subscription) {
    Matcher matcher = SUBSCRIPTIONS_PATTERN.matcher(subscription.getName());
    checkArgument(
        matcher.find(),
        "Subscription name must be in the format"
            + " 'projects/{project_id}/subscriptions/{subscription_name}.");

    return SubscriptionName.of(matcher.group("project_id"), matcher.group("subscription_name"));
  }

  /**
   * Creates a topic name. Based on {@link
   * com.google.cloud.teleport.it.dataflow.DataflowUtils#createJobName(String)}
   *
   * <p>If there are uppercase characters in {@code prefix}, then this will convert them into a dash
   * followed by the lowercase equivalent of that letter.
   *
   * <p>The topic name will normally be unique, but this is not guaranteed if multiple topics with
   * the same prefix are created in a short period of time.
   *
   * @param prefix a prefix for the topic
   * @return the prefix plus some way of identifying it separate from other topics with the same
   *     prefix
   */
  static String createTestId(String prefix) {
    String convertedPrefix =
        CaseFormat.UPPER_CAMEL.converterTo(CaseFormat.LOWER_HYPHEN).convert(prefix);
    String formattedTimestamp =
        DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")
            .withZone(ZoneId.of("UTC"))
            .format(Instant.now());
    return String.format("%s-%s", convertedPrefix, formattedTimestamp);
  }
}
