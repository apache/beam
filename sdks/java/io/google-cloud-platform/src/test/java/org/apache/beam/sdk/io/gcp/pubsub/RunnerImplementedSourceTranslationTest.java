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

import java.util.Arrays;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PubSubReadPayload;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.RunnerImplementedSourceTranslation.RunnerImplementedSourceTranslator;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValues;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Test RunnerImplementedSourceTranslator. */
@RunWith(Parameterized.class)
public class RunnerImplementedSourceTranslationTest {
  private static final String TIMESTAMP_ATTRIBUTE = "timestamp";
  private static final String ID_ATTRIBUTE = "id";
  private static final String PROJECT = "project";
  private static final TopicPath TOPIC = PubsubClient.topicPathFromName(PROJECT, "testTopic");
  private static final SubscriptionPath SUBSCRIPTION =
      PubsubClient.subscriptionPathFromName(PROJECT, "testSubscription");
  private final RunnerImplementedSourceTranslator sourceTranslator =
      new RunnerImplementedSourceTranslator();

  public static TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
  private static final ValueProvider<TopicPath> TOPIC_PROVIDER = pipeline.newProvider(TOPIC);
  private static final ValueProvider<SubscriptionPath> SUBSCRIPTION_PROVIDER =
      pipeline.newProvider(SUBSCRIPTION);

  @Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {
            // Read payload only from TOPIC.
            new RunnerImplementedSource(
                new PubsubUnboundedSource(
                    PubsubTestClient.createFactoryForCreateSubscription(),
                    StaticValueProvider.of(PubsubClient.projectPathFromId(PROJECT)),
                    StaticValueProvider.of(TOPIC),
                    null /* subscription */,
                    null /* timestampLabel */,
                    null /* idLabel */,
                    false /* needsAttributes */,
                    false /* needsMessageId*/)),
            PubSubReadPayload.newBuilder()
                .setTopic(TOPIC.getFullPath())
                .setWithAttributes(false)
                .build()
          },
          {
            // Read with attributes and message id from TOPIC.
            new RunnerImplementedSource(
                new PubsubUnboundedSource(
                    PubsubTestClient.createFactoryForCreateSubscription(),
                    StaticValueProvider.of(PubsubClient.projectPathFromId(PROJECT)),
                    StaticValueProvider.of(TOPIC),
                    null /* subscription */,
                    TIMESTAMP_ATTRIBUTE /* timestampLabel */,
                    ID_ATTRIBUTE /* idLabel */,
                    true /* needsAttributes */,
                    true /* needsMessageId */)),
            PubSubReadPayload.newBuilder()
                .setTopic(TOPIC.getFullPath())
                .setIdAttribute(ID_ATTRIBUTE)
                .setTimestampAttribute(TIMESTAMP_ATTRIBUTE)
                .setWithAttributes(true)
                .build()
          },
          {
            // Read payload from runtime provided topic.
            new RunnerImplementedSource(
                new PubsubUnboundedSource(
                    PubsubTestClient.createFactoryForCreateSubscription(),
                    StaticValueProvider.of(PubsubClient.projectPathFromId(PROJECT)),
                    TOPIC_PROVIDER,
                    null /* subscription */,
                    null /* timestampLabel */,
                    null /* idLabel */,
                    false /* needsAttributes */,
                    false /* needsMessageId */)),
            PubSubReadPayload.newBuilder()
                .setTopicRuntimeOverridden(((NestedValueProvider) TOPIC_PROVIDER).propertyName())
                .setWithAttributes(false)
                .build()
          },
          {
            // Read payload with attributes and message id from runtime provided topic.
            new RunnerImplementedSource(
                new PubsubUnboundedSource(
                    PubsubTestClient.createFactoryForCreateSubscription(),
                    StaticValueProvider.of(PubsubClient.projectPathFromId(PROJECT)),
                    TOPIC_PROVIDER,
                    null /* subscription */,
                    TIMESTAMP_ATTRIBUTE /* timestampLabel */,
                    ID_ATTRIBUTE /* idLabel */,
                    true /* needsAttributes */,
                    true /* needsMessageId */)),
            PubSubReadPayload.newBuilder()
                .setTopicRuntimeOverridden(((NestedValueProvider) TOPIC_PROVIDER).propertyName())
                .setIdAttribute(ID_ATTRIBUTE)
                .setTimestampAttribute(TIMESTAMP_ATTRIBUTE)
                .setWithAttributes(true)
                .build()
          },
          {
            // Read payload only from SUBSCRIPTION.
            new RunnerImplementedSource(
                new PubsubUnboundedSource(
                    PubsubTestClient.createFactoryForCreateSubscription(),
                    StaticValueProvider.of(PubsubClient.projectPathFromId(PROJECT)),
                    null /* topic */,
                    StaticValueProvider.of(SUBSCRIPTION),
                    null /* timestampLabel */,
                    null /* idLabel */,
                    false /* needsAttributes */,
                    false /* needsMessageId */)),
            PubSubReadPayload.newBuilder()
                .setSubscription(SUBSCRIPTION.getFullPath())
                .setWithAttributes(false)
                .build()
          },
          {
            // Read payload with attributes and message id from SUBSCRIPTION.
            new RunnerImplementedSource(
                new PubsubUnboundedSource(
                    PubsubTestClient.createFactoryForCreateSubscription(),
                    StaticValueProvider.of(PubsubClient.projectPathFromId(PROJECT)),
                    null /* topic */,
                    StaticValueProvider.of(SUBSCRIPTION),
                    TIMESTAMP_ATTRIBUTE /* timestampLabel */,
                    ID_ATTRIBUTE /* idLabel */,
                    true /* needsAttributes */,
                    true /* needsMessageId */)),
            PubSubReadPayload.newBuilder()
                .setSubscription(SUBSCRIPTION.getFullPath())
                .setIdAttribute(ID_ATTRIBUTE)
                .setTimestampAttribute(TIMESTAMP_ATTRIBUTE)
                .setWithAttributes(true)
                .build()
          },
          {
            // Read payload only from runtime provided subscription.
            new RunnerImplementedSource(
                new PubsubUnboundedSource(
                    PubsubTestClient.createFactoryForCreateSubscription(),
                    StaticValueProvider.of(PubsubClient.projectPathFromId(PROJECT)),
                    null /* topic */,
                    SUBSCRIPTION_PROVIDER,
                    null /* timestampLabel */,
                    null /* idLabel */,
                    false /* needsAttributes */,
                    false /* needsMessageId */)),
            PubSubReadPayload.newBuilder()
                .setSubscriptionRuntimeOverridden(
                    ((NestedValueProvider) SUBSCRIPTION_PROVIDER).propertyName())
                .setWithAttributes(false)
                .build()
          },
          {
            // Read payload with attributes and message id from runtime provided subscription.
            new RunnerImplementedSource(
                new PubsubUnboundedSource(
                    PubsubTestClient.createFactoryForCreateSubscription(),
                    StaticValueProvider.of(PubsubClient.projectPathFromId(PROJECT)),
                    null /* topic */,
                    SUBSCRIPTION_PROVIDER,
                    TIMESTAMP_ATTRIBUTE /* timestampLabel */,
                    ID_ATTRIBUTE /* idLabel */,
                    true /* needsAttributes */,
                    true /* needsMessageId */)),
            PubSubReadPayload.newBuilder()
                .setSubscriptionRuntimeOverridden(
                    ((NestedValueProvider) SUBSCRIPTION_PROVIDER).propertyName())
                .setIdAttribute(ID_ATTRIBUTE)
                .setTimestampAttribute(TIMESTAMP_ATTRIBUTE)
                .setWithAttributes(true)
                .build()
          },
        });
  }

  @Parameter(0)
  public RunnerImplementedSource runnerImplementedSource;

  @Parameter(1)
  public PubSubReadPayload pubsubReadPayload;

  @Test
  public void testTranslateSourceToFunctionSpec() throws Exception {
    PCollection<byte[]> output = pipeline.apply(runnerImplementedSource);
    AppliedPTransform<?, ?, RunnerImplementedSource> appliedPTransform =
        AppliedPTransform.of(
            "sink",
            PValues.expandInput(pipeline.begin()),
            PValues.expandOutput(output),
            runnerImplementedSource,
            pipeline);
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.FunctionSpec spec = sourceTranslator.translate(appliedPTransform, components);
    assertEquals(PTransformTranslation.PUBSUB_READ, spec.getUrn());
    PubSubReadPayload result = PubSubReadPayload.parseFrom(spec.getPayload());
    assertEquals(pubsubReadPayload, result);
  }
}
