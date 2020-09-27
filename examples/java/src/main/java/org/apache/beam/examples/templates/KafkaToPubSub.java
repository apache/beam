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
package org.apache.beam.examples.templates;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Values;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link KafkaToPubSub} pipeline is a streaming pipeline which ingests data in JSON format from
 * Kafka, and outputs the resulting records to PubSub. Input topic, output topic, Bootstrap servers
 * are specified by the user as template parameters.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Kafka topic exists and the message is encoded in a valid JSON format.
 *   <li>The PubSub output topic exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * export PROJECT=ID_OF_MY_PROJECT
 * export BUCKET_NAME=MY_BUCKET
 * export TEMPLATE_PATH="gs://$BUCKET/samples/dataflow/templates/kafka-pubsub.json"
 * export TEMPLATE_IMAGE="gcr.io/$PROJECT/samples/dataflow/kafka-pubsub:latest"
 *
 * # Go to the beam folder
 * cd /path/to/beam
 *
 * # Create bucket in the cloud storage
 * gsutil mb gs://${BUCKET_NAME}
 *
 * <b>FLEX TEMPLATE</b>
 * # Assemble uber-jar
 * ./gradlew -p examples/java clean shadowJar
 *
 * # Build the flex template
 * gcloud dataflow flex-template build $TEMPLATE_PATH \
 *       --image-gcr-path "$TEMPLATE_IMAGE" \
 *       --sdk-language "JAVA" \
 *       --flex-template-base-image JAVA11 \
 *       --metadata-file "src/main/java/org/apache/beam/examples/templates/kafka_to_pubsub_metadata.json" \
 *       --jar "build/libs/beam-examples-java-2.25.0-SNAPSHOT-all.jar" \
 *       --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.apache.beam.examples.templates.KafkaToPubSub"
 */
public class KafkaToPubSub {

  /** The log to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToPubSub.class);

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions {

    @Description("Kafka Bootstrap Servers")
    @Validation.Required
    String getBootstrapServers();

    void setBootstrapServers(String value);

    @Description("Kafka topic to read the input from")
    @Validation.Required
    String getInputTopic();

    void setInputTopic(String value);

    @Description(
        "The Cloud Pub/Sub topic to publish to. "
            + "The name should be in the format of "
            + "projects/<project-id>/topics/<topic-name>.")
    @Validation.Required
    String getOutputTopic();

    void setOutputTopic(String outputTopic);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * KafkaToPubSub#run(Options)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(Options options) {

    Pipeline pipeline = Pipeline.create(options);

    // Register the coder for pipeline

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
                .withTopic(options.getInputTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                // NumSplits is hard-coded to 1 for single-partition use cases (e.g., Debezium
                // Change Data Capture). Once Dataflow dynamic templates are available, this can
                // be deprecated.
                .withoutMetadata())
        .apply(Values.create())
        .apply("Write PubSub Events", PubsubIO.writeStrings().to(options.getOutputTopic()));

    return pipeline.run();
  }
}
