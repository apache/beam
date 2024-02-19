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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PubSubWritePayload;
import org.apache.beam.sdk.io.gcp.pubsub.PubSubPayloadTranslation.PubSubWritePayloadTranslator;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSink.PubsubSink;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValues;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test RunnerImplementedSinkTranslator. */
@RunWith(JUnit4.class)
public class PubSubWritePayloadTranslationTest {
  private static final String TIMESTAMP_ATTRIBUTE = "timestamp";
  private static final String ID_ATTRIBUTE = "id";
  private static final TopicPath TOPIC = PubsubClient.topicPathFromName("testProject", "testTopic");
  private final PubSubPayloadTranslation.PubSubWritePayloadTranslator sinkTranslator =
      new PubSubWritePayloadTranslator();
  private final PubSubPayloadTranslation.PubSubDynamicWritePayloadTranslator dynamicSinkTranslator =
      new PubSubPayloadTranslation.PubSubDynamicWritePayloadTranslator();

  @Rule public TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testTranslateSinkWithTopic() throws Exception {
    PubsubUnboundedSink pubsubUnboundedSink =
        new PubsubUnboundedSink(
            null,
            StaticValueProvider.of(TOPIC),
            TIMESTAMP_ATTRIBUTE,
            ID_ATTRIBUTE,
            0,
            0,
            0,
            Duration.ZERO,
            null,
            null);
    PubsubUnboundedSink.PubsubSink pubsubSink = new PubsubSink(pubsubUnboundedSink);
    PCollection<byte[]> input = pipeline.apply(Create.of(new byte[0]));
    PDone output = input.apply(pubsubSink);
    AppliedPTransform<?, ?, PubsubSink> appliedPTransform =
        AppliedPTransform.of(
            "sink",
            PValues.expandInput(input),
            PValues.expandOutput(output),
            pubsubSink,
            ResourceHints.create(),
            pipeline);
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.FunctionSpec spec = sinkTranslator.translate(appliedPTransform, components);

    assertEquals(PTransformTranslation.PUBSUB_WRITE, spec.getUrn());
    PubSubWritePayload payload = PubSubWritePayload.parseFrom(spec.getPayload());
    assertEquals(TOPIC.getFullPath(), payload.getTopic());
    assertTrue(payload.getTopicRuntimeOverridden().isEmpty());
    assertEquals(TIMESTAMP_ATTRIBUTE, payload.getTimestampAttribute());
    assertEquals(ID_ATTRIBUTE, payload.getIdAttribute());
  }

  @Test
  public void testTranslateDynamicSink() throws Exception {
    PubsubUnboundedSink pubsubUnboundedSink =
        new PubsubUnboundedSink(
            null,
            StaticValueProvider.of(TOPIC),
            TIMESTAMP_ATTRIBUTE,
            ID_ATTRIBUTE,
            0,
            0,
            0,
            Duration.ZERO,
            null,
            null);
    PubsubUnboundedSink.PubsubDynamicSink pubsubSink =
        new PubsubUnboundedSink.PubsubDynamicSink(pubsubUnboundedSink);
    PCollection<KV<String, byte[]>> input = pipeline.apply(Create.of(KV.of("foo", new byte[0])));
    PDone output = input.apply(pubsubSink);
    AppliedPTransform<?, ?, PubsubUnboundedSink.PubsubDynamicSink> appliedPTransform =
        AppliedPTransform.of(
            "sink",
            PValues.expandInput(input),
            PValues.expandOutput(output),
            pubsubSink,
            ResourceHints.create(),
            pipeline);
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.FunctionSpec spec = dynamicSinkTranslator.translate(appliedPTransform, components);

    assertEquals(PTransformTranslation.PUBSUB_WRITE_DYNAMIC, spec.getUrn());
    PubSubWritePayload payload = PubSubWritePayload.parseFrom(spec.getPayload());
    assertEquals("", payload.getTopic());
    assertTrue(payload.getTopicRuntimeOverridden().isEmpty());
    assertEquals(TIMESTAMP_ATTRIBUTE, payload.getTimestampAttribute());
    assertEquals(ID_ATTRIBUTE, payload.getIdAttribute());
  }

  @Test
  public void testTranslateSinkWithTopicOverridden() throws Exception {
    ValueProvider<TopicPath> runtimeProvider = pipeline.newProvider(TOPIC);
    PubsubUnboundedSink pubsubUnboundedSinkSink =
        new PubsubUnboundedSink(
            null,
            runtimeProvider,
            TIMESTAMP_ATTRIBUTE,
            ID_ATTRIBUTE,
            0,
            0,
            0,
            Duration.ZERO,
            null,
            null);
    PubsubSink pubsubSink = new PubsubSink(pubsubUnboundedSinkSink);
    PCollection<byte[]> input = pipeline.apply(Create.of(new byte[0]));
    PDone output = input.apply(pubsubSink);
    AppliedPTransform<?, ?, PubsubSink> appliedPTransform =
        AppliedPTransform.of(
            "sink",
            PValues.expandInput(input),
            PValues.expandOutput(output),
            pubsubSink,
            ResourceHints.create(),
            pipeline);
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.FunctionSpec spec = sinkTranslator.translate(appliedPTransform, components);

    assertEquals(PTransformTranslation.PUBSUB_WRITE, spec.getUrn());
    PubSubWritePayload payload = PubSubWritePayload.parseFrom(spec.getPayload());
    assertEquals(
        ((NestedValueProvider) runtimeProvider).propertyName(),
        payload.getTopicRuntimeOverridden());
    assertTrue(payload.getTopic().isEmpty());
    assertEquals(TIMESTAMP_ATTRIBUTE, payload.getTimestampAttribute());
    assertEquals(ID_ATTRIBUTE, payload.getIdAttribute());
  }
}
