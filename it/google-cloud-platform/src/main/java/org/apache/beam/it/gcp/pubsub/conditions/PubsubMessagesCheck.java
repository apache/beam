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
package org.apache.beam.it.gcp.pubsub.conditions;

import com.google.auto.value.AutoValue;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriptionName;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** ConditionCheck to validate if Pub/Sub has received a certain amount of messages. */
@AutoValue
public abstract class PubsubMessagesCheck extends ConditionCheck {

  abstract PubsubResourceManager resourceManager();

  abstract SubscriptionName subscription();

  abstract Integer minMessages();

  abstract @Nullable Integer maxMessages();

  private final List<ReceivedMessage> receivedMessageList = new ArrayList<>();

  @Override
  public String getDescription() {
    if (maxMessages() != null) {
      return String.format(
          "Pub/Sub check if subscription %s received between %d and %d messages",
          subscription(), minMessages(), maxMessages());
    }
    return String.format(
        "Pub/Sub check if subscription %s received %d messages", subscription(), minMessages());
  }

  @Override
  @SuppressWarnings("unboxing.of.nullable")
  public CheckResult check() {
    PullResponse pullResponse =
        resourceManager()
            .pull(subscription(), MoreObjects.firstNonNull(maxMessages(), minMessages()) + 1);
    receivedMessageList.addAll(pullResponse.getReceivedMessagesList());

    long totalRows = receivedMessageList.size();
    if (totalRows < minMessages()) {
      return new CheckResult(
          false, String.format("Expected %d messages but has only %d", minMessages(), totalRows));
    }
    if (maxMessages() != null && totalRows > maxMessages()) {
      return new CheckResult(
          false,
          String.format("Expected up to %d but found %d messages", maxMessages(), totalRows));
    }

    if (maxMessages() != null) {
      return new CheckResult(
          true,
          String.format(
              "Expected between %d and %d messages and found %d",
              minMessages(), maxMessages(), totalRows));
    }

    return new CheckResult(
        true,
        String.format("Expected at least %d messages and found %d", minMessages(), totalRows));
  }

  public static Builder builder(
      PubsubResourceManager resourceManager, SubscriptionName subscription) {
    return new AutoValue_PubsubMessagesCheck.Builder()
        .setResourceManager(resourceManager)
        .setSubscription(subscription);
  }

  public List<ReceivedMessage> getReceivedMessageList() {
    return receivedMessageList;
  }

  /** Builder for {@link PubsubMessagesCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setResourceManager(PubsubResourceManager resourceManager);

    public abstract Builder setSubscription(SubscriptionName subscription);

    public abstract Builder setMinMessages(Integer minMessages);

    public abstract Builder setMaxMessages(Integer maxMessages);

    abstract PubsubMessagesCheck autoBuild();

    public PubsubMessagesCheck build() {
      return autoBuild();
    }
  }
}
