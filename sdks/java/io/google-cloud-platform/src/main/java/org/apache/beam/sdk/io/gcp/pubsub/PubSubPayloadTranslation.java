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

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.ENABLE_CUSTOM_PUBSUB_SINK;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.ENABLE_CUSTOM_PUBSUB_SOURCE;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PubSubReadPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PubSubWritePayload;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource.PubsubSource;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
/**
 * Utility methods for translating a {@link Unbounded} which reads from {@link
 * PubsubUnboundedSource} to {@link RunnerApi} representations.
 */
public class PubSubPayloadTranslation {
  static class PubSubReadPayloadTranslator
      implements TransformPayloadTranslator<Read.Unbounded<?>> {

    @Override
    public String getUrn() {
      return PTransformTranslation.PUBSUB_READ;
    }

    @Override
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, Unbounded<?>> transform, SdkComponents components) {
      if (ExperimentalOptions.hasExperiment(
          transform.getPipeline().getOptions(), ENABLE_CUSTOM_PUBSUB_SOURCE)) {
        return null;
      }
      if (!(transform.getTransform().getSource() instanceof PubsubUnboundedSource.PubsubSource)) {
        return null;
      }
      PubSubReadPayload.Builder payloadBuilder = PubSubReadPayload.newBuilder();
      PubsubUnboundedSource pubsubUnboundedSource =
          ((PubsubSource) transform.getTransform().getSource()).outer;
      ValueProvider<TopicPath> topicProvider = pubsubUnboundedSource.getTopicProvider();
      if (topicProvider != null) {
        if (topicProvider.isAccessible()) {
          payloadBuilder.setTopic(topicProvider.get().getFullPath());
        } else {
          payloadBuilder.setTopicRuntimeOverridden(
              ((NestedValueProvider) topicProvider).propertyName());
        }
      }
      ValueProvider<SubscriptionPath> subscriptionProvider =
          pubsubUnboundedSource.getSubscriptionProvider();
      if (subscriptionProvider != null) {
        if (subscriptionProvider.isAccessible()) {
          payloadBuilder.setSubscription(subscriptionProvider.get().getFullPath());
        } else {
          payloadBuilder.setSubscriptionRuntimeOverridden(
              ((NestedValueProvider) subscriptionProvider).propertyName());
        }
      }

      if (pubsubUnboundedSource.getTimestampAttribute() != null) {
        payloadBuilder.setTimestampAttribute(pubsubUnboundedSource.getTimestampAttribute());
      }
      if (pubsubUnboundedSource.getIdAttribute() != null) {
        payloadBuilder.setIdAttribute(pubsubUnboundedSource.getIdAttribute());
      }
      payloadBuilder.setWithAttributes(
          pubsubUnboundedSource.getNeedsAttributes() || pubsubUnboundedSource.getNeedsMessageId());
      return FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(payloadBuilder.build().toByteString())
          .build();
    }
  }

  static class PubSubWritePayloadTranslator
      implements TransformPayloadTranslator<PubsubUnboundedSink.PubsubSink> {
    @Override
    public String getUrn() {
      return PTransformTranslation.PUBSUB_WRITE;
    }

    @Override
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, PubsubUnboundedSink.PubsubSink> transform,
        SdkComponents components) {
      if (ExperimentalOptions.hasExperiment(
          transform.getPipeline().getOptions(), ENABLE_CUSTOM_PUBSUB_SINK)) {
        return null;
      }
      PubSubWritePayload.Builder payloadBuilder = PubSubWritePayload.newBuilder();
      ValueProvider<TopicPath> topicProvider =
          Preconditions.checkStateNotNull(transform.getTransform().outer.getTopicProvider());
      if (topicProvider.isAccessible()) {
        payloadBuilder.setTopic(topicProvider.get().getFullPath());
      } else {
        payloadBuilder.setTopicRuntimeOverridden(
            ((NestedValueProvider) topicProvider).propertyName());
      }
      if (transform.getTransform().outer.getTimestampAttribute() != null) {
        payloadBuilder.setTimestampAttribute(
            transform.getTransform().outer.getTimestampAttribute());
      }
      if (transform.getTransform().outer.getIdAttribute() != null) {
        payloadBuilder.setIdAttribute(transform.getTransform().outer.getIdAttribute());
      }
      return FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(payloadBuilder.build().toByteString())
          .build();
    }
  }

  static class PubSubDynamicWritePayloadTranslator
      implements TransformPayloadTranslator<PubsubUnboundedSink.PubsubDynamicSink> {
    @Override
    public String getUrn() {
      return PTransformTranslation.PUBSUB_WRITE_DYNAMIC;
    }

    @Override
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, PubsubUnboundedSink.PubsubDynamicSink> transform,
        SdkComponents components) {
      if (ExperimentalOptions.hasExperiment(
          transform.getPipeline().getOptions(), ENABLE_CUSTOM_PUBSUB_SINK)) {
        return null;
      }
      PubSubWritePayload.Builder payloadBuilder = PubSubWritePayload.newBuilder();
      if (transform.getTransform().outer.getTimestampAttribute() != null) {
        payloadBuilder.setTimestampAttribute(
            transform.getTransform().outer.getTimestampAttribute());
      }
      if (transform.getTransform().outer.getIdAttribute() != null) {
        payloadBuilder.setIdAttribute(transform.getTransform().outer.getIdAttribute());
      }
      return FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(payloadBuilder.build().toByteString())
          .build();
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class WriteRegistrar implements TransformPayloadTranslatorRegistrar {

    @Override
    @SuppressWarnings("rawtypes")
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.of(
          PubsubUnboundedSink.PubsubSink.class,
          new PubSubWritePayloadTranslator(),
          PubsubUnboundedSink.PubsubDynamicSink.class,
          new PubSubDynamicWritePayloadTranslator());
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class ReadRegistrar implements TransformPayloadTranslatorRegistrar {

    @Override
    @SuppressWarnings("rawtypes")
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(Read.Unbounded.class, new PubSubReadPayloadTranslator());
    }
  }
}
