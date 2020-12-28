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

import static org.apache.beam.examples.complete.kafkatopubsub.KafkaPubsubConstants.PASSWORD;
import static org.apache.beam.examples.complete.kafkatopubsub.KafkaPubsubConstants.USERNAME;
import static org.apache.beam.examples.complete.kafkatopubsub.kafka.consumer.Utils.getKafkaCredentialsFromVault;
import static org.apache.beam.examples.complete.kafkatopubsub.transforms.FormatTransform.readFromKafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.examples.complete.kafkatopubsub.kafka.consumer.Utils;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsubSignal;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test of KafkaToPubsub. */
@RunWith(JUnit4.class)
public class KafkaToPubsubTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule
  public transient TestPubsubSignal signal = TestPubsubSignal.create();
  @Rule
  public static final String pubsubMessage = "rulezzz";

  private static class TestMessage implements SerializableFunction<Set<String>, Boolean> {

    @Override
    public Boolean apply(Set<String> input) {
      for (String message : input) {
        if (!Objects.equals(message, pubsubMessage)) {
          return false;
        }
      }
      return true;
    }
  }

  @Before
  public void setUp() throws Exception {
//    SetupPubsubContainer psc = new SetupPubsubContainer(pipeline);
  }

  @Test
  public void testKafkaToPubsubE2E() throws IOException {
    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    RunKafkaContainer rkc = new RunKafkaContainer(pubsubMessage);
    String bootstrapServer = rkc.getBootstrapServer();
    String[] kafkaTopicsList = new String[]{rkc.getTopicName()};

    String pubsubTopicPath = SetupPubsubContainer.getTopicPath();

    Map<String, Object> kafkaConfig = new HashMap<>();
    Map<String, String> sslConfig = null;

    PCollection<KV<String, String>> readStrings = pipeline
        .apply("readFromKafka",
            readFromKafka(bootstrapServer, Arrays.asList(kafkaTopicsList), kafkaConfig, sslConfig));

    PCollection<String> readFromPubsub = readStrings.apply(Values.create())
        .apply("writeToPubSub", PubsubIO.writeStrings().to(pubsubTopicPath)).getPipeline()
        .apply("readFromPubsub",
            PubsubIO.readStrings().fromTopic(pubsubTopicPath));

    readFromPubsub.apply(
        "waitForTestMessage",
        signal.signalSuccessWhen(readFromPubsub.getCoder(), new TestMessage()));

    Supplier<Void> start = signal.waitForStart(Duration.standardSeconds(10));
    pipeline.apply(signal.signalStart());
    PipelineResult job = pipeline.run();
    start.get();
    signal.waitForSuccess(Duration.standardMinutes(2));
    try {
      job.cancel();
    } catch (IOException | UnsupportedOperationException e) {
      System.out.println("Something went wrong");
    }
  }

  /** Tests configureKafka() with a null input properties. */
  @Test
  public void testConfigureKafkaNullProps() {
    Map<String, Object> config = Utils.configureKafka(null);
    Assert.assertEquals(new HashMap<>(), config);
  }

  /** Tests configureKafka() without a Password in input properties. */
  @Test
  public void testConfigureKafkaNoPassword() {
    Map<String, String> props = new HashMap<>();
    props.put(USERNAME, "username");
    Map<String, Object> config = Utils.configureKafka(props);
    Assert.assertEquals(new HashMap<>(), config);
  }

  /** Tests configureKafka() without a Username in input properties. */
  @Test
  public void testConfigureKafkaNoUsername() {
    Map<String, String> props = new HashMap<>();
    props.put(PASSWORD, "password");
    Map<String, Object> config = Utils.configureKafka(props);
    Assert.assertEquals(new HashMap<>(), config);
  }

  /** Tests configureKafka() with an appropriate input properties. */
  @Test
  public void testConfigureKafka() {
    Map<String, String> props = new HashMap<>();
    props.put(USERNAME, "username");
    props.put(PASSWORD, "password");

    Map<String, Object> expectedConfig = new HashMap<>();
    expectedConfig.put(SaslConfigs.SASL_MECHANISM, ScramMechanism.SCRAM_SHA_512.mechanismName());
    expectedConfig.put(
        SaslConfigs.SASL_JAAS_CONFIG,
        String.format(
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"%s\" password=\"%s\";",
            props.get(USERNAME), props.get(PASSWORD)));

    Map<String, Object> config = Utils.configureKafka(props);
    Assert.assertEquals(expectedConfig, config);
  }

  /** Tests getKafkaCredentialsFromVault() with an invalid url. */
  @Test
  public void testGetKafkaCredentialsFromVaultInvalidUrl() {
    Map<String, Map<String, String>> credentials =
        getKafkaCredentialsFromVault("some-url", "some-token");
    Assert.assertEquals(new HashMap<>(), credentials);
  }
}
