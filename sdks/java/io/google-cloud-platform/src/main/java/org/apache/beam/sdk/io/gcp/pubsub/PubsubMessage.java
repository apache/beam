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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.sdk.util.StringUtils.getStringByteSize;
import static org.apache.beam.sdk.util.StringUtils.getTotalSizeInBytes;

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Class representing a Pub/Sub message. Each message contains a single message payload and a map of
 * attached attributes.
 */
public class PubsubMessage {

  private byte[] message;
  private Map<String, String> attributes;

  public PubsubMessage(byte[] payload, Map<String, String> attributes) {
    this.message = payload;
    this.attributes = attributes;
    PubsubValidator.validate(this);
  }

  /** Returns the main PubSub message. */
  public byte[] getPayload() {
    return message;
  }

  /** Returns the given attribute value. If not such attribute exists, returns null. */
  @Nullable
  public String getAttribute(String attribute) {
    checkNotNull(attribute, "attribute");
    return attributes.get(attribute);
  }

  /** Returns the full map of attributes. This is an unmodifiable map. */
  public Map<String, String> getAttributeMap() {
    return attributes;
  }

  public int getMessageByteSize() {
    return message.length + getAttributeMapByteSize();
  }

  int getAttributeMapByteSize() {
    return getTotalSizeInBytes(attributes.values()) + getTotalSizeInBytes(attributes.keySet());
  }

  static class PubsubValidator {
    public static final int MAX_PUBLISH_BYTE_SIZE = 10485760;
    private static final String MAX_MSG_SIZE_ERROR_MESSAGE =
      String.format("Total size of PubsubMessage must be <= %d bytes", MAX_PUBLISH_BYTE_SIZE);

    protected static final int MAX_ATTRIBUTE_KEY_SIZE = 256;
    protected static final int MAX_ATTRIBUTE_VALUE_SIZE = 1000;
    private static final String ATTR_ERROR_MESSAGE =
      String.format("PubsubMessage contains an attribute that exceeds the PubSub quota. " +
        "Keys must be <= %d bytes, Values must be <= %d bytes.",
        MAX_ATTRIBUTE_KEY_SIZE,
        MAX_ATTRIBUTE_VALUE_SIZE);

    protected static final int MAX_MSG_ATTRIBUTES = 100;
    private static final String MAX_MSG_ATTRIBUTES_ERROR_MESSAGE =
      String.format("PubsubMessage contains too many attributes, " +
        "maximum allowed is %d messages.",
        MAX_MSG_ATTRIBUTES);

    static void validate(PubsubMessage message) {
      Preconditions.checkState(message.message.length <= MAX_PUBLISH_BYTE_SIZE,
              MAX_MSG_SIZE_ERROR_MESSAGE +
        String.format(", got %d bytes", message.message.length));

      Preconditions.checkState(message.attributes.size() <= MAX_MSG_ATTRIBUTES,
        MAX_MSG_ATTRIBUTES_ERROR_MESSAGE);

      Preconditions.checkState(hasValidAttributes(message.attributes), ATTR_ERROR_MESSAGE);
    }

    static boolean isInvalidAttribute(Map.Entry<String, String> entry) {
      return getStringByteSize(entry.getKey()) > MAX_ATTRIBUTE_KEY_SIZE ||
        getStringByteSize(entry.getValue()) > MAX_ATTRIBUTE_VALUE_SIZE;
    }
    static boolean hasValidAttributes(Map<String, String> attributes) {
      for (Map.Entry<String, String> entry : attributes.entrySet()) {
        if (isInvalidAttribute(entry)) {
          return false;
        }
      }
      return true;
    }
  }
}
