Prompt:
Write a sample Java code snippet that writes data to a Apache Kafka topic using Apache Beam.

Response:
Your Apache Beam pipeline can write data to a Kafka topic using the Apache Beam PubSubIO connector.

Here is an example of how to use the KafkaIO connector to accomplish this:

```java
package kafka;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

// Pipeline to write data to a Kafka topic using Apache Beam KafkaIO connector
public class WriteKafkaTopic {

    private static final Logger LOG = LoggerFactory.getLogger(WriteKafkaTopic.class);

    // Pipeline options for writing data to Kafka topic
    public interface WriteKafkaTopicOptions extends WriteKafkaTopicOptions {
        @Description("Kafka Bootstrap Server")
        String getBootstrapServer();

        void setBootstrapServer(String value);

        @Description("Kafka Topic Name")
        String getTopicName();

        void setTopicName(String value);
    }

    // Main method to run the pipeline
    public static void main(String[] args) {

        // Parse the pipeline options from the command line
        WriteKafkaTopicOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteKafkaTopicOptions.class);

        // Create the pipeline
        Pipeline p = Pipeline.create(options);

        // Create sample messages to write to the Kafka topic
        final List<KV<Integer, String>> messages = Arrays.asList(
                KV.of(1, "Sample message 1"),
                KV.of(2, "Sample message 2"),
                KV.of(3, "Sample message 3")
        );

        // Write the messages to the Kafka topic
        p
                // Create PCollection from the list of sample messages
                .apply(Create.of(messages))
                // Write the messages to the Kafka topic using the KafkaIO connector
                .apply(KafkaIO.<Integer, String>write()
                        .withBootstrapServers(options.getBootstrapServer())
                        .withTopic(options.getTopicName())
                        // Specify the key and value serializers for the Kafka topic
                        .withValueSerializer(StringSerializer.class)
                        .withKeySerializer(IntegerSerializer.class)
                );
        // Run the pipeline
        p.run();
    }
}
```

This code snippet utilizes the pipeline options pattern to parse command-line arguments.