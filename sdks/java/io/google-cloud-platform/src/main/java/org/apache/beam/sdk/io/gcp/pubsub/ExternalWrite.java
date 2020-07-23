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
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.PubsubTopic;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Exposes {@link PubsubIO.Write} as an external transform for cross-language usage. */
@Experimental
@AutoService(ExternalTransformRegistrar.class)
public final class ExternalWrite implements ExternalTransformRegistrar {
  public ExternalWrite() {}

  public static final String URN = "beam:external:java:pubsub:write:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders() {
    return ImmutableMap.of(URN, WriteBuilder.class);
  }

  /** Parameters class to expose the transform to an external SDK. */
  public static class Configuration {
    private String topic;
    private @Nullable String idAttribute;
    private @Nullable String timestampAttribute;

    public void setTopic(String topic) {
      this.topic = topic;
    }

    public void setIdLabel(@Nullable String idAttribute) {
      this.idAttribute = idAttribute;
    }

    public void setTimestampAttribute(@Nullable String timestampAttribute) {
      this.timestampAttribute = timestampAttribute;
    }
  }

  public static class WriteBuilder
      implements ExternalTransformBuilder<Configuration, PCollection<byte[]>, PDone> {
    public WriteBuilder() {}

    @Override
    public PTransform<PCollection<byte[]>, PDone> buildExternal(Configuration config) {
      PubsubIO.Write.Builder<byte[]> writeBuilder = PubsubIO.Write.newBuilder(new FormatFn());
      if (config.topic != null) {
        StaticValueProvider<String> topic = StaticValueProvider.of(config.topic);
        writeBuilder.setTopicProvider(NestedValueProvider.of(topic, PubsubTopic::fromPath));
      }
      if (config.idAttribute != null) {
        writeBuilder.setIdAttribute(config.idAttribute);
      }
      if (config.timestampAttribute != null) {
        writeBuilder.setTimestampAttribute(config.timestampAttribute);
      }
      return writeBuilder.build();
    }
  }

  private static class FormatFn implements SerializableFunction<byte[], PubsubMessage> {
    @Override
    public PubsubMessage apply(byte[] input) {
      try {
        com.google.pubsub.v1.PubsubMessage message =
            com.google.pubsub.v1.PubsubMessage.parseFrom(input);
        return new PubsubMessage(message.getData().toByteArray(), message.getAttributesMap());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Could not decode Pubsub message", e);
      }
    }
  }
}
