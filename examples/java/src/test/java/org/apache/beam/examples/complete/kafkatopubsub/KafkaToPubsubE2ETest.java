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

import static org.apache.beam.examples.complete.kafkatopubsub.transforms.FormatTransform.readFromKafka;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;

import com.google.auth.Credentials;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsub;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
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

  @ClassRule public static final PubSubEmulatorContainer pubSubEmulatorContainer = new PubSubEmulatorContainer(DockerImageName.parse(PUBSUB_EMULATOR_IMAGE));
  @ClassRule public static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME));
  @Rule public final transient TestPipeline pipeline = TestPipeline.fromOptions(OPTIONS);
  @Rule public final transient TestPubsub testPubsub = TestPubsub.fromOptions(OPTIONS);

  @BeforeClass
  public static void beforeClass() throws Exception {
    Credentials credentials = NoopCredentialFactory.fromOptions(OPTIONS).getCredential();
    OPTIONS.as(GcpOptions.class).setGcpCredential(credentials);
    OPTIONS.as(GcpOptions.class).setProject(PROJECT_ID);
    OPTIONS.as(PubsubOptions.class).setPubsubRootUrl("http://" + pubSubEmulatorContainer.getEmulatorEndpoint());
  }

  @Test
  public void testKafkaToPubsubE2E() throws IOException, InterruptedException {
    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);
    String pubsubTopicPath = testPubsub.topicPath().getPath();
    Map<String, Object> kafkaConfig = ImmutableMap.of(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        "earliest"
    );

    PCollection<KV<String, String>> readStrings =
        pipeline.apply(
            "readFromKafka",
            readFromKafka(kafkaContainer.getBootstrapServers(), Collections.singletonList(KAFKA_TOPIC_NAME), kafkaConfig, new HashMap<>())
        );
    readStrings
        .apply(Values.create())
        .apply("writeToPubSub", PubsubIO.writeStrings().to(pubsubTopicPath));

    PipelineResult job = pipeline.run();

    sendKafkaMessage();
    testPubsub.assertThatTopicEventuallyReceives(
        hasProperty("payload", equalTo(PUBSUB_MESSAGE.getBytes(StandardCharsets.UTF_8)))
    ).waitForUpTo(Duration.standardSeconds(25));
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
                    kafkaContainer.getBootstrapServers(),
                    ProducerConfig.CLIENT_ID_CONFIG,
                    UUID.randomUUID().toString()),
                new StringSerializer(),
                new StringSerializer()
            )
    ) {
      producer.send(new ProducerRecord<>(KAFKA_TOPIC_NAME, "testcontainers", PUBSUB_MESSAGE)).get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Something went wrong in kafka producer", e);
    }
  }
}
