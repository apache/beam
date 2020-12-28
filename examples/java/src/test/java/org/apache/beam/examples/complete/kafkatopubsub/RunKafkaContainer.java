package org.apache.beam.examples.complete.kafkatopubsub;

import com.google.common.collect.ImmutableMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Run kafka container in seperate thread to produce message.
 */
public class RunKafkaContainer implements Runnable {

    Thread kafkaThread;
    private String topicName;
    private final KafkaProducer<String, String> producer;
    private String bootstrapServer;
    private String pubsubMessage;

    RunKafkaContainer(String pubsubMessage) {
        kafkaThread = new Thread(this, "Kafka container thread");
        setBootstrapServer(setupKafkaContainer());
        setTopicName("messages-topic");
        setPubsubMessage(pubsubMessage);
        producer = new KafkaProducer<>(
            ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServer(),
                ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
            ),
            new StringSerializer(),
            new StringSerializer()
        );
        kafkaThread.start();
    }

    public void run() {
        try {
            Thread.sleep(20000);
            producer.send(new ProducerRecord<>(getTopicName(),
                "testcontainers", "rulezzz")).get();
            System.out.println("Producer sent");
        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Something went wrong in kafka producer");
            e.printStackTrace();
        }
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    private void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    private static String setupKafkaContainer() {
        KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));
        kafkaContainer.start();
        return kafkaContainer.getBootstrapServers();
    }

    public String getPubsubMessage() {
        return pubsubMessage;
    }

    public void setPubsubMessage(String pubsubMessage) {
        this.pubsubMessage = pubsubMessage;
    }
}