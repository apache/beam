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
package org.apache.beam.sdk.io.pulsar;

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.pulsar.client.api.Message;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Class representing a Pulsar Message record. Each PulsarMessage contains a single message basic
 * message data and Message record to access directly.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class PulsarMessage {
  abstract @Nullable String getTopic();

  abstract long getPublishTimestamp();

  abstract @Nullable String getKey();

  @SuppressWarnings("mutable")
  abstract byte[] getValue();

  abstract @Nullable Map<String, String> getProperties();

  @SuppressWarnings("mutable")
  abstract byte[] getMessageId();

  public static PulsarMessage create(
      @Nullable String topicName,
      long publishTimestamp,
      @Nullable String key,
      byte[] value,
      @Nullable Map<String, String> properties,
      byte[] messageId) {
    return new AutoValue_PulsarMessage(
        topicName, publishTimestamp, key, value, properties, messageId);
  }

  public static PulsarMessage create(Message<byte[]> message) {
    return create(
        message.getTopicName(),
        message.getPublishTime(),
        message.getKey(),
        message.getValue(),
        message.getProperties(),
        message.getMessageId().toByteArray());
  }
}
