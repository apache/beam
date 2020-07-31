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

import com.google.auto.service.AutoService;
import com.google.protobuf.ByteString;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.PubsubSubscription;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.PubsubTopic;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Exposes {@link PubsubIO.Read} as an external transform for cross-language usage. */
@Experimental
@AutoService(ExternalTransformRegistrar.class)
public final class ExternalRead implements ExternalTransformRegistrar {
  public ExternalRead() {}

  public static final String URN = "beam:external:java:pubsub:read:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
    return ImmutableMap.of(URN, ReadBuilder.class);
  }

  /** Parameters class to expose the transform to an external SDK. */
  public static class Configuration {
    private @Nullable String topic;
    private @Nullable String subscription;
    private @Nullable String idAttribute;
    private @Nullable String timestampAttribute;
    private boolean needsAttributes;

    public void setTopic(@Nullable String topic) {
      this.topic = topic;
    }

    public void setSubscription(@Nullable String subscription) {
      this.subscription = subscription;
    }

    public void setIdLabel(@Nullable String idAttribute) {
      this.idAttribute = idAttribute;
    }

    public void setTimestampAttribute(@Nullable String timestampAttribute) {
      this.timestampAttribute = timestampAttribute;
    }

    public void setWithAttributes(Boolean needsAttributes) {
      // we must use Boolean instead of boolean because the external payload system
      // inspects the native type of each coder urn, and BooleanCoder wants Boolean.
      this.needsAttributes = needsAttributes;
    }
  }

  public static class ReadBuilder
      implements ExternalTransformBuilder<Configuration, PBegin, PCollection<byte[]>> {
    public ReadBuilder() {}

    @Override
    public PTransform<PBegin, PCollection<byte[]>> buildExternal(Configuration config) {
      PubsubIO.Read.Builder<byte[]> readBuilder;
      if (config.needsAttributes) {
        readBuilder = PubsubIO.Read.newBuilder(new ParsePayloadAsPubsubMessageProto());
        readBuilder.setNeedsAttributes(true);
      } else {
        readBuilder = PubsubIO.Read.newBuilder(PubsubMessage::getPayload);
      }
      readBuilder.setCoder(ByteArrayCoder.of());
      if (config.topic != null) {
        StaticValueProvider<String> topic = StaticValueProvider.of(config.topic);
        readBuilder.setTopicProvider(NestedValueProvider.of(topic, PubsubTopic::fromPath));
      }
      if (config.subscription != null) {
        StaticValueProvider<String> subscription = StaticValueProvider.of(config.subscription);
        readBuilder.setSubscriptionProvider(
            NestedValueProvider.of(subscription, PubsubSubscription::fromPath));
      }
      if (config.idAttribute != null) {
        readBuilder.setIdAttribute(config.idAttribute);
      }
      if (config.timestampAttribute != null) {
        readBuilder.setTimestampAttribute(config.timestampAttribute);
      }
      return readBuilder.build();
    }
  }

  // Convert the PubsubMessage to a PubsubMessage proto, then return its serialized representation.
  private static class ParsePayloadAsPubsubMessageProto
      implements SerializableFunction<PubsubMessage, byte[]> {
    @Override
    public byte[] apply(PubsubMessage input) {
      Map<String, String> attributes = input.getAttributeMap();
      com.google.pubsub.v1.PubsubMessage.Builder message =
          com.google.pubsub.v1.PubsubMessage.newBuilder()
              .setData(ByteString.copyFrom(input.getPayload()));
      // TODO(BEAM-8085) this should not be null
      if (attributes != null) {
        message.putAllAttributes(attributes);
      }
      return message.build().toByteArray();
    }
  }
}
