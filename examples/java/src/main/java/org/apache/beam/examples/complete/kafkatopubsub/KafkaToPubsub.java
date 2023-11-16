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

import static org.apache.beam.examples.complete.kafkatopubsub.kafka.consumer.Utils.configureKafka;
import static org.apache.beam.examples.complete.kafkatopubsub.kafka.consumer.Utils.configureSsl;
import static org.apache.beam.examples.complete.kafkatopubsub.kafka.consumer.Utils.getKafkaCredentialsFromVault;
import static org.apache.beam.examples.complete.kafkatopubsub.kafka.consumer.Utils.isSslSpecified;
import static org.apache.beam.examples.complete.kafkatopubsub.kafka.consumer.Utils.parseKafkaConsumerConfig;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.examples.complete.kafkatopubsub.avro.AvroDataClass;
import org.apache.beam.examples.complete.kafkatopubsub.avro.AvroDataClassKafkaAvroDeserializer;
import org.apache.beam.examples.complete.kafkatopubsub.options.KafkaToPubsubOptions;
import org.apache.beam.examples.complete.kafkatopubsub.transforms.FormatTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link KafkaToPubsub} pipeline is a streaming pipeline which ingests data in JSON format from
 * Kafka, and outputs the resulting records to PubSub. Input topics, output topic, Bootstrap servers
 * are specified by the user as template parameters. <br>
 * Kafka may be configured with SASL/SCRAM security mechanism, in this case a Vault secret storage
 * with credentials should be provided. URL to credentials and Vault token are specified by the user
 * as template parameters.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>Kafka Bootstrap Server(s).
 *   <li>Kafka Topic(s) exists.
 *   <li>The PubSub output topic exists.
 *   <li>(Optional) An existing HashiCorp Vault secret storage
 *   <li>(Optional) A configured secure SSL connection for Kafka
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Gradle preparation
 *
 * To run this example your {@code build.gradle} file should contain the following task
 * to execute the pipeline:
 * {@code
 * task execute (type:JavaExec) {
 *     mainClass = System.getProperty("mainClass")
 *     classpath = sourceSets.main.runtimeClasspath
 *     systemProperties System.getProperties()
 *     args System.getProperty("exec.args", "").split()
 * }
 * }
 *
 * This task allows to run the pipeline via the following command:
 * {@code
 * gradle clean execute -DmainClass=org.apache.beam.examples.complete.kafkatopubsub.KafkaToPubsub \
 *      -Dexec.args="--<argument>=<value> --<argument>=<value>"
 * }
 *
 * # Running the pipeline
 * To execute this pipeline, specify the parameters:
 *
 * - Kafka Bootstrap servers
 * - Kafka input topics
 * - Pub/Sub output topic
 * - Output format
 *
 * in the following format:
 * {@code
 * --bootstrapServers=host:port \
 * --inputTopics=your-input-topic \
 * --outputTopic=projects/your-project-id/topics/your-topic-pame \
 * --outputFormat=AVRO|PUBSUB
 * }
 *
 * Optionally, to retrieve Kafka credentials for SASL/SCRAM,
 * specify a URL to the credentials in HashiCorp Vault and the vault access token:
 * {@code
 * --secretStoreUrl=http(s)://host:port/path/to/credentials
 * --vaultToken=your-token
 * }
 *
 * Optionally, to configure secure SSL connection between the Beam pipeline and Kafka,
 * specify the parameters:
 * - A path to a truststore file (it can be a local path or a GCS path, which should start with `gs://`)
 * - A path to a keystore file (it can be a local path or a GCS path, which should start with `gs://`)
 * - Truststore password
 * - Keystore password
 * - Key password
 * {@code
 * --truststorePath=path/to/kafka.truststore.jks
 * --keystorePath=path/to/kafka.keystore.jks
 * --truststorePassword=your-truststore-password
 * --keystorePassword=your-keystore-password
 * --keyPassword=your-key-password
 * }
 * By default this will run the pipeline locally with the DirectRunner. To change the runner, specify:
 * {@code
 * --runner=YOUR_SELECTED_RUNNER
 * }
 * </pre>
 *
 * <p><b>Example Avro usage</b>
 *
 * <pre>
 * This template contains an example Class to deserialize AVRO from Kafka and serialize it to AVRO in Pub/Sub.
 *
 * To use this example in the specific case, follow the few steps:
 * <ul>
 * <li> Create your own class to describe AVRO schema. As an example use {@link AvroDataClass}. Just define necessary fields.
 * <li> Create your own Avro Deserializer class. As an example use {@link AvroDataClassKafkaAvroDeserializer}. Just rename it, and put your own Schema class as the necessary types.
 * <li> Modify the {@link FormatTransform}. Put your Schema class and Deserializer to the related parameter.
 * <li> Modify write step in the {@link KafkaToPubsub} by put your Schema class to "writeAvrosToPubSub" step.
 * </ul>
 * </pre>
 */
public class KafkaToPubsub {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToPubsub.class);

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    KafkaToPubsubOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToPubsubOptions.class);

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    run(pipeline, options);
  }

  /**
   * Runs a pipeline which reads message from Kafka and writes it to GCS.
   *
   * @param options arguments to the pipeline
   */
  public static PipelineResult run(Pipeline pipeline, KafkaToPubsubOptions options) {
    // Configure Kafka consumer properties
    Map<String, Object> kafkaConfig = new HashMap<>();
    kafkaConfig.putAll(parseKafkaConsumerConfig(options.getKafkaConsumerConfig()));
    Map<String, String> sslConfig = new HashMap<>();
    if (options.getSecretStoreUrl() != null && options.getVaultToken() != null) {
      Map<String, Map<String, String>> credentials =
          getKafkaCredentialsFromVault(options.getSecretStoreUrl(), options.getVaultToken());
      kafkaConfig = configureKafka(credentials.get(KafkaPubsubConstants.KAFKA_CREDENTIALS));
    } else {
      LOG.warn(
          "No information to retrieve Kafka credentials was provided. "
              + "Trying to initiate an unauthorized connection.");
    }

    if (isSslSpecified(options)) {
      sslConfig.putAll(configureSsl(options));
    } else {
      LOG.info(
          "No information to retrieve SSL certificate was provided by parameters."
              + "Trying to initiate a plain text connection.");
    }

    List<String> topicsList = new ArrayList<>(Arrays.asList(options.getInputTopics().split(",")));

    checkArgument(
        topicsList.size() > 0 && topicsList.get(0).length() > 0,
        "inputTopics cannot be an empty string.");

    List<String> bootstrapServersList =
        new ArrayList<>(Arrays.asList(options.getBootstrapServers().split(",")));

    checkArgument(
        bootstrapServersList.size() > 0 && topicsList.get(0).length() > 0,
        "bootstrapServers cannot be an empty string.");

    LOG.info(
        "Starting Kafka-To-PubSub pipeline with parameters bootstrap servers:"
            + options.getBootstrapServers()
            + " input topics: "
            + options.getInputTopics()
            + " output pubsub topic: "
            + options.getOutputTopic());

    /*
     * Steps:
     *  1) Read messages in from Kafka
     *  2) Extract values only
     *  3) Write successful records to PubSub
     */

    if (options.getOutputFormat() == FormatTransform.FORMAT.AVRO) {
      pipeline
          .apply(
              "readAvrosFromKafka",
              FormatTransform.readAvrosFromKafka(
                  options.getBootstrapServers(), topicsList, kafkaConfig, sslConfig))
          .apply("createValues", Values.create())
          .apply("writeAvrosToPubSub", PubsubIO.writeAvros(AvroDataClass.class));

    } else {
      pipeline
          .apply(
              "readFromKafka",
              FormatTransform.readFromKafka(
                  options.getBootstrapServers(), topicsList, kafkaConfig, sslConfig))
          .apply("createValues", Values.create())
          .apply("writeToPubSub", new FormatTransform.FormatOutput(options));
    }

    return pipeline.run();
  }
}
