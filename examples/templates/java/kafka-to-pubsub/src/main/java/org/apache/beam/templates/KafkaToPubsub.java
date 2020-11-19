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
package org.apache.beam.templates;

import static org.apache.beam.templates.kafka.consumer.Utils.*;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.templates.avro.TaxiRide;
import org.apache.beam.templates.options.KafkaToPubsubOptions;
import org.apache.beam.templates.transforms.FormatTransform;
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
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT=id-of-my-project
 * BUCKET_NAME=my-bucket
 *
 * # Set containerization vars
 * IMAGE_NAME=my-image-name
 * TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
 * BASE_CONTAINER_IMAGE=my-base-container-image
 * TEMPLATE_PATH="gs://${BUCKET_NAME}/templates/kafka-pubsub.json"
 *
 * # Create bucket in the cloud storage
 * gsutil mb gs://${BUCKET_NAME}
 *
 * # Go to the beam folder
 * cd /path/to/beam
 *
 * <b>FLEX TEMPLATE</b>
 * # Assemble uber-jar
 * ./gradlew -p templates/kafka-to-pubsub clean shadowJar
 *
 * # Go to the template folder
 * cd /path/to/beam/templates/kafka-to-pubsub
 *
 * # Build the flex template
 * gcloud dataflow flex-template build ${TEMPLATE_PATH} \
 *       --image-gcr-path "${TARGET_GCR_IMAGE}" \
 *       --sdk-language "JAVA" \
 *       --flex-template-base-image ${BASE_CONTAINER_IMAGE} \
 *       --metadata-file "src/main/resources/kafka_to_pubsub_metadata.json" \
 *       --jar "build/libs/beam-templates-kafka-to-pubsub-2.25.0-SNAPSHOT-all.jar" \
 *       --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.apache.beam.templates.KafkaToPubsub"
 *
 * # Execute template:
 *    API_ROOT_URL="https://dataflow.googleapis.com"
 *    TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/locations/${REGION}/flexTemplates:launch"
 *    JOB_NAME="kafka-to-pubsub-`date +%Y%m%d-%H%M%S-%N`"
 *
 *    time curl -X POST -H "Content-Type: application/json" \
 *            -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 *            -d '
 *             {
 *                 "launch_parameter": {
 *                     "jobName": "'$JOB_NAME'",
 *                     "containerSpecGcsPath": "'$TEMPLATE_PATH'",
 *                     "parameters": {
 *                         "bootstrapServers": "broker_1:9091, broker_2:9092",
 *                         "inputTopics": "topic1, topic2",
 *                         "outputTopic": "projects/'$PROJECT'/topics/your-topic-name",
 *                         "secretStoreUrl": "http(s)://host:port/path/to/credentials",
 *                         "vaultToken": "your-token"
 *                     }
 *                 }
 *             }
 *            '
 *            "${TEMPLATES_LAUNCH_API}"
 * </pre>
 *
 * <p><b>Example Avro usage</b>
 *
 * <pre>
 * This template contains an example Class to deserialize AVRO from Kafka and serialize it to AVRO in Pub/Sub.
 *
 * To use this example in the specific case, follow the few steps:
 * <ul>
 * <li> Create your own class to describe AVRO schema. As an example use {@link TaxiRide}. Just define necessary fields.
 * <li> Create your own Avro Deserializer class. As an example use {@link org.apache.beam.templates.avro.TaxiRidesKafkaAvroDeserializer}. Just rename it, and put your own Schema class as the necessary types.
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

    run(options);
  }

  /**
   * Runs a pipeline which reads message from Kafka and writes it to GCS.
   *
   * @param options arguments to the pipeline
   */
  public static PipelineResult run(KafkaToPubsubOptions options) {
    // Configure Kafka consumer properties
    Map<String, Object> kafkaConfig = new HashMap<>();
    if (options.getSecretStoreUrl() != null && options.getVaultToken() != null) {
      Map<String, Map<String, String>> credentials =
          getKafkaCredentialsFromVault(options.getSecretStoreUrl(), options.getVaultToken());
      kafkaConfig = configureKafka(credentials.get(KafkaPubsubConstants.KAFKA_CREDENTIALS));
    } else {
      LOG.warn(
          "No information to retrieve Kafka credentials was provided. "
              + "Trying to initiate an unauthorized connection.");
    }

    Map<String, String> sslConfig = new HashMap<>();
    if (isSslSpecified(options)) {
      sslConfig.putAll(configureSsl(options));
    } else {
      LOG.info(
          "No information to retrieve SSL certificate was provided. "
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

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
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
          .apply("writeAvrosToPubSub", PubsubIO.writeAvros(TaxiRide.class));

    } else if (options.getOutputFormat() == FormatTransform.FORMAT.AVRO) {
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
