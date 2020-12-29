package org.apache.beam.examples.complete.kafkatopubsub;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Run kafka container in separate thread to produce message.
 */
public class RunKafkaContainer {

    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:5.4.3";
    private final String topicName;
    private final KafkaProducer<String, String> producer;
    private final String bootstrapServer;

    RunKafkaContainer(String pubsubMessage) {
        bootstrapServer = setupKafkaContainer();
        topicName = "messages-topic";
        producer = new KafkaProducer<>(
            ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer,
                ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
            ),
            new StringSerializer(),
            new StringSerializer()
        );
        Runnable kafkaProducer = () -> {
            try {
                producer.send(new ProducerRecord<>(topicName,
                        "testcontainers", pubsubMessage)).get();
                System.out.println("Producer sent");
            } catch (ExecutionException | InterruptedException e) {
                System.out.println("Something went wrong in kafka producer");
                e.printStackTrace();
            }
        };
        Executors.newSingleThreadScheduledExecutor()
                .schedule(kafkaProducer, 10, TimeUnit.SECONDS);
    }

    public String getTopicName() {
        return topicName;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    private static String setupKafkaContainer() {
        KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse(KAFKA_IMAGE_NAME));
        kafkaContainer.start();
        return kafkaContainer.getBootstrapServers();
    }
}