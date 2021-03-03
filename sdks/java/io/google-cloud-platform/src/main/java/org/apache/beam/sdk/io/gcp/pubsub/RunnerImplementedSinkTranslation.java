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
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PubSubWritePayload;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;

@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
/**
 * Utility methods for translating a {@link RunnerImplementedSink} to {@link RunnerApi}
 * representations.
 */
public class RunnerImplementedSinkTranslation {
  static class RunnerImplementedSinkTranslator
      implements TransformPayloadTranslator<RunnerImplementedSink> {

    @Override
    public String getUrn(RunnerImplementedSink transform) {
      return PTransformTranslation.PUBSUB_WRITE;
    }

    @Override
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, RunnerImplementedSink> transform, SdkComponents components) {
      PubSubWritePayload.Builder payloadBuilder = PubSubWritePayload.newBuilder();
      ValueProvider<TopicPath> topicProvider = transform.getTransform().getTopicProvider();
      if (topicProvider.isAccessible()) {
        payloadBuilder.setTopic(topicProvider.get().getFullPath());
      } else {
        payloadBuilder.setTopicRuntimeOverridden(
            ((NestedValueProvider) topicProvider).propertyName());
      }
      if (transform.getTransform().getTimestampAttribute() != null) {
        payloadBuilder.setTimestampAttribute(transform.getTransform().getTimestampAttribute());
      }
      if (transform.getTransform().getIdAttribute() != null) {
        payloadBuilder.setIdAttribute(transform.getTransform().getIdAttribute());
      }
      return FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(payloadBuilder.build().toByteString())
          .build();
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {

    @Override
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(
          RunnerImplementedSink.class, new RunnerImplementedSinkTranslator());
    }
  }
}
