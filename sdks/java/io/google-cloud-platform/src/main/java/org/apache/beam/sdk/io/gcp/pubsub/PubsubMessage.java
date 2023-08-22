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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Class representing a Pub/Sub message. Each message contains a single message payload, a map of
 * attached attributes, a message id and an ordering key.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PubsubMessage {
  @AutoValue
  abstract static class Impl {
    abstract @Nullable String getTopic();

    @SuppressWarnings("mutable")
    abstract byte[] getPayload();

    abstract @Nullable Map<String, String> getAttributeMap();

    abstract @Nullable String getMessageId();

    abstract @Nullable String getOrderingKey();

    static Impl create(
        @Nullable String topic,
        byte[] payload,
        @Nullable Map<String, String> attributes,
        @Nullable String messageId,
        @Nullable String orderingKey) {
      return new AutoValue_PubsubMessage_Impl(topic, payload, attributes, messageId, orderingKey);
    }
  }

  private Impl impl;

  public PubsubMessage(byte[] payload, @Nullable Map<String, String> attributes) {
    this(payload, attributes, null, null);
  }

  public PubsubMessage(
      byte[] payload, @Nullable Map<String, String> attributes, @Nullable String messageId) {
    impl = Impl.create(null, payload, attributes, messageId, null);
  }

  public PubsubMessage(
      byte[] payload,
      @Nullable Map<String, String> attributes,
      @Nullable String messageId,
      @Nullable String orderingKey) {
    impl = Impl.create(null, payload, attributes, messageId, orderingKey);
  }

  private PubsubMessage(Impl impl) {
    this.impl = impl;
  }

  public PubsubMessage withTopic(String topic) {
    return new PubsubMessage(
        Impl.create(
            topic,
            impl.getPayload(),
            impl.getAttributeMap(),
            impl.getMessageId(),
            impl.getOrderingKey()));
  }

  public @Nullable String getTopic() {
    return impl.getTopic();
  }

  /** Returns the main PubSub message. */
  public byte[] getPayload() {
    return impl.getPayload();
  }

  /** Returns the given attribute value. If not such attribute exists, returns null. */
  public @Nullable String getAttribute(String attribute) {
    checkNotNull(attribute, "attribute");
    return impl.getAttributeMap().get(attribute);
  }

  /** Returns the full map of attributes. This is an unmodifiable map. */
  public @Nullable Map<String, String> getAttributeMap() {
    return impl.getAttributeMap();
  }

  /** Returns the messageId of the message populated by Cloud Pub/Sub. */
  public @Nullable String getMessageId() {
    return impl.getMessageId();
  }

  /** Returns the ordering key of the message. */
  public @Nullable String getOrderingKey() {
    return impl.getOrderingKey();
  }

  @Override
  public String toString() {
    return impl.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PubsubMessage)) {
      return false;
    }
    return impl.equals(((PubsubMessage) other).impl);
  }

  @Override
  public int hashCode() {
    return impl.hashCode();
  }
}
