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

import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Class representing a Pub/Sub message. Each message contains a single message payload, a map of
 * attached attributes, and a message id.
 */
public class PubsubMessage {

  private byte[] message;
  private @Nullable Map<String, String> attributes;
  private @Nullable String messageId;

  public PubsubMessage(byte[] payload, @Nullable Map<String, String> attributes) {
    this.message = payload;
    this.attributes = attributes;
    this.messageId = null;
  }

  public PubsubMessage(
      byte[] payload, @Nullable Map<String, String> attributes, @Nullable String messageId) {
    this.message = payload;
    this.attributes = attributes;
    this.messageId = messageId;
  }

  /** Returns the main PubSub message. */
  public byte[] getPayload() {
    return message;
  }

  /** Returns the given attribute value. If not such attribute exists, returns null. */
  public @Nullable String getAttribute(String attribute) {
    checkNotNull(attribute, "attribute");
    return attributes.get(attribute);
  }

  /** Returns the full map of attributes. This is an unmodifiable map. */
  public @Nullable Map<String, String> getAttributeMap() {
    return attributes;
  }

  /** Returns the messageId of the message populated by Cloud Pub/Sub. */
  public @Nullable String getMessageId() {
    return messageId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("message", message)
        .add("attributes", attributes)
        .add("messageId", messageId)
        .toString();
  }
}
