package org.apache.beam.examples.templates;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Values;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToPubsub {
    /*
     * The {@link KafkaToPubSub} pipeline is a streaming pipeline which ingests data in JSON format
     * from Kafka, and outputs the resulting records to PubSub. Input topic, output topic, Bootstrap servers
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
     * PROJECT_ID=PROJECT ID HERE
     * BUCKET_NAME=BUCKET NAME HERE
     * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/kafka-to-pubsub
     *
     * # Set the runner
     * RUNNER=DataflowRunner
     *
     * # Build the template
     * mvn compile exec:java \
     * -Dexec.mainClass=com.google.cloud.teleport.templates.KafkaToPubsub \
     * -Dexec.cleanupDaemonThreads=false \
     * -Dexec.args=" \
     * --project=${PROJECT_ID} \
     * --stagingLocation=${PIPELINE_FOLDER}/staging \
     * --tempLocation=${PIPELINE_FOLDER}/temp \
     * --templateLocation=${PIPELINE_FOLDER}/template \
     * --runner=${RUNNER}"
     *
     * # Execute the template
     * JOB_NAME=kafka-to-pubsub-$USER-`date +"%Y%m%d-%H%M%S%z"`
     *
     * gcloud dataflow jobs run ${JOB_NAME} \
     * --gcs-location=${PIPELINE_FOLDER}/template \
     * --zone=us-east1-d \
     * --parameters \
     * "bootstrapServers=my_host:9092,inputTopic=kafka-test,\
     * outputPubsubTopic=pubsub-test"
     * </pre>
     */

    /**
     * The log to output status messages to.
     */
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToPubsub.class);


    /**
     * The {@link Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface Options extends PipelineOptions {

        @Description("Kafka Bootstrap Servers")
        ValueProvider<String> getBootstrapServers();

        void setBootstrapServers(ValueProvider<String> value);

        @Description("Kafka topic to read the input from")
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);

        @Description(
                "The Cloud Pub/Sub topic to publish to. "
                        + "The name should be in the format of "
                        + "projects/<project-id>/topics/<topic-name>.")
        @Validation.Required
        ValueProvider<String> getOutputTopic();

        void setOutputTopic(ValueProvider<String> outputTopic);
    }

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the {@link
     * KafkaToPubsub#run(Options)} method to start the pipeline and invoke {@code
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
        pipeline.apply(
                "ReadFromKafka",
                KafkaIO.<String, String>read()
                        .withBootstrapServers(options.getBootstrapServers().get())
                        .withTopic(options.getInputTopic().get())
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




