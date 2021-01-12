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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.examples.complete.kafkatopubsub.utils.RunKafkaContainer;
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
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

/** E2E test for {@link KafkaToPubsub} pipeline. */
public class KafkaToPubsubE2ETest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.fromOptions(OPTIONS);
  @Rule public final transient TestPubsub testPubsub = TestPubsub.fromOptions(OPTIONS);

  private static final String PUBSUB_EMULATOR_IMAGE =
      "gcr.io/google.com/cloudsdktool/cloud-sdk:316.0.0-emulators";
  private static final String PUBSUB_MESSAGE = "test pubsub message";
  private static final String PROJECT_ID = "try-kafka-pubsub";
  private static final PipelineOptions OPTIONS = TestPipeline.testingPipelineOptions();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Credentials credentials = NoopCredentialFactory.fromOptions(OPTIONS).getCredential();
    OPTIONS.as(GcpOptions.class).setGcpCredential(credentials);
    OPTIONS.as(GcpOptions.class).setProject(PROJECT_ID);
    setupPubsubContainer(OPTIONS.as(PubsubOptions.class));
  }

  @Test
  public void testKafkaToPubsubE2E() throws IOException, InterruptedException {
    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    RunKafkaContainer rkc = new RunKafkaContainer(PUBSUB_MESSAGE);
    String bootstrapServer = rkc.getBootstrapServer();
    String[] kafkaTopicsList = new String[]{rkc.getTopicName()};

    String pubsubTopicPath = testPubsub.topicPath().getPath();

    Map<String, Object> kafkaConfig = new HashMap<>();
    Map<String, String> sslConfig = new HashMap<>();

    PCollection<KV<String, String>> readStrings =
        pipeline.apply(
            "readFromKafka",
            readFromKafka(bootstrapServer, Arrays.asList(kafkaTopicsList), kafkaConfig, sslConfig));

    readStrings
        .apply(Values.create())
        .apply("writeToPubSub", PubsubIO.writeStrings().to(pubsubTopicPath));

    PipelineResult job = pipeline.run();
    testPubsub.assertThatTopicEventuallyReceives(
        hasProperty("payload", equalTo(PUBSUB_MESSAGE.getBytes(StandardCharsets.UTF_8)))
    ).waitForUpTo(Duration.standardMinutes(2));
    try {
      job.cancel();
    } catch (UnsupportedOperationException e) {
      throw new AssertionError("Could not stop pipeline.", e);
    }
  }

  private static void setupPubsubContainer(PubsubOptions options) {
    PubSubEmulatorContainer emulator =
        new PubSubEmulatorContainer(DockerImageName.parse(PUBSUB_EMULATOR_IMAGE));
    emulator.start();
    String pubsubUrl = emulator.getEmulatorEndpoint();
    options.setPubsubRootUrl("http://" + pubsubUrl);
  }
}
