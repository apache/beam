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
package org.apache.beam.examples.complete.kafkatopubsub;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;

import com.google.auth.Credentials;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.beam.examples.complete.kafkatopubsub.options.KafkaToPubsubOptions;
import org.apache.beam.examples.complete.kafkatopubsub.transforms.FormatTransform.FORMAT;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsub;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

/** E2E test for {@link KafkaToPubsub} pipeline. */
public class KafkaToPubsubE2ETest {
  private static final String PUBSUB_EMULATOR_IMAGE =
      "gcr.io/google.com/cloudsdktool/cloud-sdk:316.0.0-emulators";
  private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:5.4.3";
  private static final String PUBSUB_MESSAGE = "test pubsub message";
  private static final String KAFKA_TOPIC_NAME = "messages-topic";
  private static final String PROJECT_ID = "try-kafka-pubsub";
  private static final PipelineOptions OPTIONS = TestPipeline.testingPipelineOptions();

  @ClassRule
  public static final PubSubEmulatorContainer PUB_SUB_EMULATOR_CONTAINER =
      new PubSubEmulatorContainer(DockerImageName.parse(PUBSUB_EMULATOR_IMAGE));

  @ClassRule
  public static final KafkaContainer KAFKA_CONTAINER =
      new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME));

  @Rule public final transient TestPipeline pipeline = TestPipeline.fromOptions(OPTIONS);
  @Rule public final transient TestPubsub testPubsub = TestPubsub.fromOptions(OPTIONS);

  @BeforeClass
  public static void beforeClass() throws Exception {
    Credentials credentials = NoopCredentialFactory.fromOptions(OPTIONS).getCredential();
    OPTIONS.as(DirectOptions.class).setBlockOnRun(false);
    OPTIONS.as(GcpOptions.class).setGcpCredential(credentials);
    OPTIONS.as(GcpOptions.class).setProject(PROJECT_ID);
    OPTIONS
        .as(PubsubOptions.class)
        .setPubsubRootUrl("http://" + PUB_SUB_EMULATOR_CONTAINER.getEmulatorEndpoint());
    OPTIONS.as(KafkaToPubsubOptions.class).setOutputFormat(FORMAT.PUBSUB);
    OPTIONS
        .as(KafkaToPubsubOptions.class)
        .setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers());
    OPTIONS.as(KafkaToPubsubOptions.class).setInputTopics(KAFKA_TOPIC_NAME);
    OPTIONS
        .as(KafkaToPubsubOptions.class)
        .setKafkaConsumerConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "=earliest");
  }

  @Before
  public void setUp() {
    OPTIONS.as(KafkaToPubsubOptions.class).setOutputTopic(testPubsub.topicPath().getPath());
  }

  @Test
  public void testKafkaToPubsubE2E() throws Exception {
    PipelineResult job = KafkaToPubsub.run(pipeline, OPTIONS.as(KafkaToPubsubOptions.class));

    sendKafkaMessage();
    testPubsub
        .assertThatTopicEventuallyReceives(
            hasProperty("payload", equalTo(PUBSUB_MESSAGE.getBytes(StandardCharsets.UTF_8))))
        .waitForUpTo(Duration.standardMinutes(1));
    try {
      job.cancel();
    } catch (UnsupportedOperationException e) {
      throw new AssertionError("Could not stop pipeline.", e);
    }
  }

  private void sendKafkaMessage() {
    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(
            ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers(),
                ProducerConfig.CLIENT_ID_CONFIG,
                UUID.randomUUID().toString()),
            new StringSerializer(),
            new StringSerializer())) {
      producer.send(new ProducerRecord<>(KAFKA_TOPIC_NAME, "testcontainers", PUBSUB_MESSAGE)).get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Something went wrong in kafka producer", e);
    }
  }
}
