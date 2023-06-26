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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.naming.SizeLimitExceededException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

public class PreparePubsubWriteDoFn<InputT> extends DoFn<InputT, PubsubMessage> {
  // See https://cloud.google.com/pubsub/quotas#resource_limits.
  private static final int PUBSUB_MESSAGE_DATA_MAX_BYTES = 10 << 20;
  private static final int PUBSUB_MESSAGE_MAX_ATTRIBUTES = 100;
  private static final int PUBSUB_MESSAGE_ATTRIBUTE_MAX_KEY_BYTES = 256;
  private static final int PUBSUB_MESSAGE_ATTRIBUTE_MAX_VALUE_BYTES = 1024;
  // The amount of bytes that each attribute entry adds up to the request
  private static final int PUBSUB_MESSAGE_ATTRIBUTE_ENCODE_ADDITIONAL_BYTES = 6;
  private int maxPublishBatchSize;

  private SerializableFunction<ValueInSingleWindow<InputT>, PubsubMessage> formatFunction;
  @Nullable SerializableFunction<ValueInSingleWindow<InputT>, PubsubIO.PubsubTopic> topicFunction;

  static int validatePubsubMessageSize(PubsubMessage message, int maxPublishBatchSize)
      throws SizeLimitExceededException {
    int payloadSize = message.getPayload().length;
    if (payloadSize > PUBSUB_MESSAGE_DATA_MAX_BYTES) {
      throw new SizeLimitExceededException(
          "Pubsub message data field of length "
              + payloadSize
              + " exceeds maximum of "
              + PUBSUB_MESSAGE_DATA_MAX_BYTES
              + " bytes. See https://cloud.google.com/pubsub/quotas#resource_limits");
    }
    int totalSize = payloadSize;

    @Nullable Map<String, String> attributes = message.getAttributeMap();
    if (attributes != null) {
      if (attributes.size() > PUBSUB_MESSAGE_MAX_ATTRIBUTES) {
        throw new SizeLimitExceededException(
            "Pubsub message contains "
                + attributes.size()
                + " attributes which exceeds the maximum of "
                + PUBSUB_MESSAGE_MAX_ATTRIBUTES
                + ". See https://cloud.google.com/pubsub/quotas#resource_limits");
      }

      // Consider attribute encoding overhead, so it doesn't go over the request limits
      totalSize += attributes.size() * PUBSUB_MESSAGE_ATTRIBUTE_ENCODE_ADDITIONAL_BYTES;

      for (Map.Entry<String, String> attribute : attributes.entrySet()) {
        String key = attribute.getKey();
        int keySize = key.getBytes(StandardCharsets.UTF_8).length;
        if (keySize > PUBSUB_MESSAGE_ATTRIBUTE_MAX_KEY_BYTES) {
          throw new SizeLimitExceededException(
              "Pubsub message attribute key '"
                  + key
                  + "' exceeds the maximum of "
                  + PUBSUB_MESSAGE_ATTRIBUTE_MAX_KEY_BYTES
                  + " bytes. See https://cloud.google.com/pubsub/quotas#resource_limits");
        }
        totalSize += keySize;

        String value = attribute.getValue();
        int valueSize = value.getBytes(StandardCharsets.UTF_8).length;
        if (valueSize > PUBSUB_MESSAGE_ATTRIBUTE_MAX_VALUE_BYTES) {
          throw new SizeLimitExceededException(
              "Pubsub message attribute value for key '"
                  + key
                  + "' starting with '"
                  + value.substring(0, Math.min(256, value.length()))
                  + "' exceeds the maximum of "
                  + PUBSUB_MESSAGE_ATTRIBUTE_MAX_VALUE_BYTES
                  + " bytes. See https://cloud.google.com/pubsub/quotas#resource_limits");
        }
        totalSize += valueSize;
      }
    }

    if (totalSize > maxPublishBatchSize) {
      throw new SizeLimitExceededException(
          "Pubsub message of length "
              + totalSize
              + " exceeds maximum of "
              + maxPublishBatchSize
              + " bytes, when considering the payload and attributes. "
              + "See https://cloud.google.com/pubsub/quotas#resource_limits");
    }
    return totalSize;
  }

  PreparePubsubWriteDoFn(
      SerializableFunction<ValueInSingleWindow<InputT>, PubsubMessage> formatFunction,
      @Nullable
          SerializableFunction<ValueInSingleWindow<InputT>, PubsubIO.PubsubTopic> topicFunction,
      int maxPublishBatchSize) {
    this.formatFunction = formatFunction;
    this.topicFunction = topicFunction;
    this.maxPublishBatchSize = maxPublishBatchSize;
  }

  @ProcessElement
  public void process(
      @Element InputT element,
      @Timestamp Instant ts,
      BoundedWindow window,
      PaneInfo paneInfo,
      OutputReceiver<PubsubMessage> o) {
    ValueInSingleWindow<InputT> valueInSingleWindow =
        ValueInSingleWindow.of(element, ts, window, paneInfo);
    PubsubMessage message = formatFunction.apply(valueInSingleWindow);
    if (topicFunction != null) {
      message = message.withTopic(topicFunction.apply(valueInSingleWindow).asPath());
    }
    try {
      validatePubsubMessageSize(message, maxPublishBatchSize);
    } catch (SizeLimitExceededException e) {
      throw new IllegalArgumentException(e);
    }
    o.output(message);
  }
}
