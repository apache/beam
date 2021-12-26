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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import javax.naming.SizeLimitExceededException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PreparePubsubWrite<InputT>
    extends PTransform<PCollection<InputT>, PCollection<PubsubMessage>> {
  private SerializableFunction<InputT, PubsubMessage> formatFunction;
  private ValueProvider<PubsubIO.PubsubTopic> topicValueProvider;

  // See https://cloud.google.com/pubsub/quotas#resource_limits.
  private static final int PUBSUB_MESSAGE_DATA_MAX_LENGTH = 10 << 20;
  private static final int PUBSUB_MESSAGE_MAX_ATTRIBUTES = 100;
  private static final int PUBSUB_MESSAGE_ATTRIBUTE_MAX_KEY_LENGTH = 256;
  private static final int PUBSUB_MESSAGE_ATTRIBUTE_MAX_VALUE_LENGTH = 1024;

  public PreparePubsubWrite(
      ValueProvider<PubsubIO.PubsubTopic> topicValueProvider,
      SerializableFunction<InputT, PubsubMessage> formatFunction) {
    this.formatFunction = formatFunction;
    this.topicValueProvider = topicValueProvider;
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<InputT> input) {
    return input.apply(ParDo.of(new PreparePubsubWriteDoFn())).setCoder(PubsubMessageCoder.of());
  }

  public class PreparePubsubWriteDoFn extends DoFn<InputT, PubsubMessage> {
    @ProcessElement
    public void processElement(ProcessContext context, @Element InputT element) {
      PubsubMessage outputValue = null;
      if (formatFunction != null) {
        outputValue = formatFunction.apply(element);
        checkArgument(
            outputValue != null,
            "formatFunction may not return null, but %s returned null on element %s",
            formatFunction,
            element);

        if (outputValue.getTopicPath() == null && topicValueProvider.isAccessible()) {
          outputValue =
              new PubsubMessage(
                  outputValue.getPayload(),
                  outputValue.getAttributeMap(),
                  outputValue.getMessageId(),
                  topicValueProvider.get().asPath());
        }

      } else if (element.getClass().equals(PubsubMessage.class)) {
        outputValue = (PubsubMessage) element;
      }

      try {
        validatePubsubMessage(outputValue);
      } catch (SizeLimitExceededException e) {
        throw new IllegalArgumentException(e);
      }
      context.output(outputValue);
    }
  }

  public static void validatePubsubMessage(PubsubMessage message)
      throws SizeLimitExceededException {
    if (message.getPayload().length > PUBSUB_MESSAGE_DATA_MAX_LENGTH) {
      throw new SizeLimitExceededException(
          "Pubsub message data field of length "
              + message.getPayload().length
              + " exceeds maximum of "
              + PUBSUB_MESSAGE_DATA_MAX_LENGTH
              + ". See https://cloud.google.com/pubsub/quotas#resource_limits");
    }
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
      for (Map.Entry<String, String> attribute : attributes.entrySet()) {
        if (attribute.getKey().length() > PUBSUB_MESSAGE_ATTRIBUTE_MAX_KEY_LENGTH) {
          throw new SizeLimitExceededException(
              "Pubsub message attribute key "
                  + attribute.getKey()
                  + " exceeds the maximum of "
                  + PUBSUB_MESSAGE_ATTRIBUTE_MAX_KEY_LENGTH
                  + ". See https://cloud.google.com/pubsub/quotas#resource_limits");
        }
        String value = attribute.getValue();
        if (value.length() > PUBSUB_MESSAGE_ATTRIBUTE_MAX_VALUE_LENGTH) {
          throw new SizeLimitExceededException(
              "Pubsub message attribute value starting with "
                  + value.substring(0, Math.min(256, value.length()))
                  + " exceeds the maximum of "
                  + PUBSUB_MESSAGE_ATTRIBUTE_MAX_VALUE_LENGTH
                  + ". See https://cloud.google.com/pubsub/quotas#resource_limits");
        }
      }
    }
  }
}
