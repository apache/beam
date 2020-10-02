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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.templates.options.KafkaToPubsubOptions;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToPubsub {

  /**
   * The {@link KafkaToPubsub} pipeline is a streaming pipeline which ingests data in JSON format
   * from Kafka, and outputs the resulting records to PubSub. Input topic, output topic, Bootstrap
   * servers are specified by the user as template parameters.
   *
   * <p><b>Pipeline Requirements</b>
   *
   * <ul>
   *   <li>Kafka Bootstrap Server(s).
   *   <li>Kafka Topic(s) exists.
   *   <li>The PubSub output topic exists.
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
   * BASE_CONTAINER_IMAGE_VERSION=my-base-container-image-version
   * TEMPLATE_PATH="gs://${BUCKET_NAME}/templates/kafka-pubsub.json"
   * TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
   *
   * # Go to the beam folder
   * cd /path/to/beam
   *
   * # Create bucket in the cloud storage
   * gsutil mb gs://${BUCKET_NAME}
   *
   * <b>FLEX TEMPLATE</b>
   * # Assemble uber-jar
   * ./gradlew -p templates/kafka-to-pubsub clean shadowJar
   *
   * # Go to the template folder
   *cd /path/to/beam/templates/kafka-to-pubsub
   *
   * # Build the flex template
   * gcloud dataflow flex-template build $TEMPLATE_PATH \
   *       --image-gcr-path "{$TARGET_GCR_IMAGE}" \
   *       --sdk-language "JAVA" \
   *       --flex-template-base-image ${BASE_CONTAINER_IMAGE} \
   *       --metadata-file "src/main/resources/kafka_to_pubsub_metadata.json" \
   *       --jar "build/libs/beam-templates-kafka-to-pubsub-2.25.0-SNAPSHOT-all.jar" \
   *       --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.apache.beam.templates.KafkaToPubsub"
   *
   * # Execute template:
   * API_ROOT_URL="https://dataflow.googleapis.com"
   * TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/templates:launch"
   * JOB_NAME="kafka-to-pubsub-`date +%Y%m%d-%H%M%S-%N`"
   *
   * time curl -X POST -H "Content-Type: application/json"     \
   *     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
   *     "${TEMPLATES_LAUNCH_API}"`
   *     `"?validateOnly=false"`
   *     `"&dynamicTemplate.gcsPath=${BUCKET_NAME}/path/to/image-spec"`
   *     `"&dynamicTemplate.stagingLocation=${BUCKET_NAME}/staging" \
   *     -d '
   *      {
   *       "jobName":"'$JOB_NAME'",
   *       "parameters": {
   *           "bootstrapServers":"broker_1:9092,broker_2:9092",
   *           "inputTopics":"topic1,topic2",
   *           "outputTopic":"projects/${PROJECT}/topics/your-topic-name"
   *        }
   *       }
   *      '
   * </pre>
   */

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
     *  3) Write successful records to PubSub
     */
    pipeline
        .apply(
            "ReadFromKafka",
            KafkaIO.<String, String>read()
                .withBootstrapServers(options.getBootstrapServers())
                .withTopics(topicsList)
                .withKeyDeserializerAndCoder(
                    StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                .withValueDeserializerAndCoder(
                    StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                .withoutMetadata())
        .apply(Values.create())
        .apply("Write PubSub Events", PubsubIO.writeStrings().to(options.getOutputTopic()));

    return pipeline.run();
  }
}
