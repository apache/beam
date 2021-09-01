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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Class representing a Pub/Sub message. Each message contains a single message payload, a map of
 * attached attributes, and a message id.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class PubsubMessage {
  @AutoValue
  abstract static class Impl {
    @SuppressWarnings("mutable")
    abstract byte[] getPayload();

    abstract @Nullable Map<String, String> getAttributeMap();

    abstract @Nullable String getMessageId();

    static Impl create(
        byte[] payload, @Nullable Map<String, String> attributes, @Nullable String messageId) {
      return new AutoValue_PubsubMessage_Impl(payload, attributes, messageId);
    }
  }

  private Impl impl;

  public PubsubMessage(byte[] payload, @Nullable Map<String, String> attributes) {
    this(payload, attributes, null);
  }

  public PubsubMessage(
      byte[] payload, @Nullable Map<String, String> attributes, @Nullable String messageId) {
    impl = Impl.create(payload, attributes, messageId);
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
