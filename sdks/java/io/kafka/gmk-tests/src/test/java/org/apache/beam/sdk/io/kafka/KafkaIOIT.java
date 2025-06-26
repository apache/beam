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
package org.apache.beam.sdk.io.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class KafkaIOIT {
  private static Options options;

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  private static KafkaContainer kafkaContainer;

  @BeforeClass
  public static void setup() throws IOException {
    // check kafka version first
    @Nullable String targetVer = System.getProperty("beam.target.kafka.version");
    if (!Strings.isNullOrEmpty(targetVer)) {
      String actualVer = AppInfoParser.getVersion();
      assertEquals(targetVer, actualVer);
    }

    options = IOITHelper.readIOTestPipelineOptions(Options.class);
    setupKafkaContainer();
  }

  @AfterClass
  public static void afterClass() {
    if (kafkaContainer != null) {
      kafkaContainer.stop();
    }
  }

  @Test
  public void testReadAvroGenericRecordsWithSchemaRegistry() {
    // Define test resources. The Schema Registry and BootstrapURL should be static and remain
    // unchanged.

    String topicName = "TestManagedIOWithSchemaRegistry";
    String schemaRegistryUrl =
        "https://managedkafka.googleapis.com/v1/projects/apache-beam-testing/locations/us-central1/schemaRegistries/managed_io_with_schema_registry_integration_test";
    String bootstrapServer =
        "bootstrap.kafkaio-testing.us-central1.managedkafka.apache-beam-testing.cloud.goog:9092";
    String schemaRegistrySubject = topicName + "-value";
    final Schema kafkaTopicSchema =
        Schema.builder().addStringField("name").addInt32Field("age").build();
    String schemaString =
        "{\n"
            + "  \"type\":\"record\",\n"
            + "  \"name\": \"Person\",\n"
            + "  \"fields\": [\n"
            + "    {\"name\":\"name\",\"type\":\"string\"},\n"
            + "    {\"name\":\"age\",\"type\":\"int\"}\n"
            + "  ]\n"
            + "}\n";

    // This test is required to run on Dataflow to invoke the latest version of the transform.
    // Set up the dataflow pipeline options to match the other test options.
    DataflowPipelineOptions pReadOptions =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);

    List<String> experiments = new ArrayList<>();
    experiments.add("use_sdf_read");
    experiments.add("beam_fn_api");

    pReadOptions.setAppName("KafkaIOIT-testReadAvroGenericRecordsWithSchemaRegistry");
    pReadOptions.setExperiments(experiments);
    pReadOptions.setRunner(DataflowRunner.class);
    pReadOptions.setProject("apache-beam-testing");
    pReadOptions.setRegion("us-central1");
    pReadOptions.setJobName("testManagedIOWithSchemaRegistry" + UUID.randomUUID());
    pReadOptions.setGcpTempLocation("gs://temp-storage-for-end-to-end-tests");

    Pipeline pRead = Pipeline.create(pReadOptions);

    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema avroSchema = parser.parse(schemaString);

    GenericRecord record1 = new GenericData.Record(avroSchema);
    GenericRecord record2 = new GenericData.Record(avroSchema);
    GenericRecord record3 = new GenericData.Record(avroSchema);

    record1.put("name", "Alice Wonderland");
    record1.put("age", 25);

    record2.put("name", "Bob The Builder");
    record2.put("age", 30);

    record3.put("name", "Charlie Chaplin");
    record3.put("age", 35);

    ArrayList<KV<String, GenericRecord>> sampleData = new ArrayList<>();
    sampleData.add(KV.of("1", record1));
    sampleData.add(KV.of("2", record2));
    sampleData.add(KV.of("3", record3));

    AdminClient client =
        AdminClient.create(
            ImmutableMap.of(
                "bootstrap.servers",
                bootstrapServer,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                "SASL_SSL",
                SaslConfigs.SASL_MECHANISM,
                "OAUTHBEARER",
                SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler",
                SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"));

    try {
      client.createTopics(ImmutableSet.of(new NewTopic(topicName, 1, (short) 1)));

      ImmutableMap<String, Object> producerConfigUpdates =
          ImmutableMap.<String, Object>builder()
              .put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
              .put(KafkaAvroSerializerConfig.BEARER_AUTH_CREDENTIALS_SOURCE, "CUSTOM")
              .put("auto.register.schemas", true)
              .put(
                  "bearer.auth.custom.provider.class",
                  "com.google.cloud.hosted.kafka.auth.GcpBearerAuthCredentialProvider")
              .put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
              .put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER")
              .put(
                  SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                  "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
              .put(
                  SaslConfigs.SASL_JAAS_CONFIG,
                  "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;")
              .build();

      PCollection<KV<String, GenericRecord>> inputRecords =
          writePipeline.apply(
              "CreateSampleData",
              Create.of(sampleData)
                  .withCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(avroSchema))));

      inputRecords.apply(
          "Write to Kafka",
          KafkaIO.<String, GenericRecord>write()
              .withBootstrapServers(bootstrapServer)
              .withTopic(topicName)
              .withKeySerializer(StringSerializer.class)
              .withGCPApplicationDefaultCredentials()
              .withProducerConfigUpdates(producerConfigUpdates)
              .withValueSerializer((Class) KafkaAvroSerializer.class));

      ImmutableMap<String, Object> config =
          ImmutableMap.<String, Object>builder()
              .put(
                  "bootstrap_servers",
                  "bootstrap.fozzie-test-cluster.us-central1.managedkafka.dataflow-testing-311516.cloud.goog:9092")
              .put(
                  "consumer_config_updates",
                  ImmutableMap.of(
                      "auto.offset.reset",
                      "earliest",
                      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                      "SASL_SSL",
                      SaslConfigs.SASL_MECHANISM,
                      "OAUTHBEARER",
                      SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                      "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler",
                      SaslConfigs.SASL_JAAS_CONFIG,
                      "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"))
              .put("topic", topicName)
              .put("confluent_schema_registry_url", schemaRegistryUrl)
              .put("confluent_schema_registry_subject", schemaRegistrySubject)
              .put("max_read_time_seconds", 20)
              .put("format", "AVRO")
              .build();

      PCollection<Row> output =
          pRead
              .apply(Managed.read(Managed.KAFKA).withConfig(config))
              .get("output")
              .setCoder(RowCoder.of(kafkaTopicSchema));

      PAssert.that(output)
          .containsInAnyOrder(
              ImmutableList.of(
                  Row.withSchema(kafkaTopicSchema)
                      .withFieldValue("name", "Alice Wonderland")
                      .withFieldValue("age", 25)
                      .build(),
                  Row.withSchema(kafkaTopicSchema)
                      .withFieldValue("name", "Bob The Builder")
                      .withFieldValue("age", 30)
                      .build(),
                  Row.withSchema(kafkaTopicSchema)
                      .withFieldValue("name", "Charlie Chaplin")
                      .withFieldValue("age", 35)
                      .build()));

      writePipeline.run().waitUntilFinish();
      PipelineResult readResult = pRead.run();

      // Pipeline should only take about 5 minutes to execute, so this should be enough of a buffer
      // for timeouts.
      PipelineResult.State readState = readResult.waitUntilFinish(Duration.standardMinutes(10));

      // Fail the test if pipeline failed.
      assertNotEquals(PipelineResult.State.FAILED, readState);
    } finally {
      // client.deleteTopics(ImmutableSet.of(topicName));
      client.close();
    }
  }

  public interface Options extends IOTestPipelineOptions, StreamingOptions {

    @Description("Options for synthetic source.")
    @Validation.Required
    String getSourceOptions();

    void setSourceOptions(String sourceOptions);

    @Description("Kafka bootstrap server addresses")
    @Default.String("localhost:9092")
    String getKafkaBootstrapServerAddresses();

    void setKafkaBootstrapServerAddresses(String address);

    @Description("Kafka topic")
    @Validation.Required
    String getKafkaTopic();

    void setKafkaTopic(String topic);

    @Description("Time to wait for the events to be processed by the read pipeline (in seconds)")
    @Validation.Required
    Integer getReadTimeout();

    void setReadTimeout(Integer readTimeout);

    @Description("Whether to use testcontainers")
    @Default.Boolean(false)
    Boolean isWithTestcontainers();

    void setWithTestcontainers(Boolean withTestcontainers);

    @Description("Kafka container version in format 'X.Y.Z'. Use when useTestcontainers is true")
    @Nullable
    String getKafkaContainerVersion();

    void setKafkaContainerVersion(String kafkaContainerVersion);
  }

  private static void setupKafkaContainer() {
    kafkaContainer =
        new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka")
                .withTag(options.getKafkaContainerVersion()));
    // Adding startup attempts to try and deflake
    kafkaContainer.withStartupAttempts(3);
    kafkaContainer.start();
    options.setKafkaBootstrapServerAddresses(kafkaContainer.getBootstrapServers());
  }
}
