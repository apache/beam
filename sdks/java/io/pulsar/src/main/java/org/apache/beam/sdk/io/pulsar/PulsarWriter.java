package org.apache.beam.sdk.io.pulsar;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.pulsar.client.api.*;

public class PulsarWriter extends DoFn<Message, Void> {

    private Producer producer;
    private PulsarClient client;
    private String clientUrl;
    private String topic;

    PulsarWriter(WriteRecords transform) {
        clientUrl = transform.getClientUrl();
        topic = transform.getTopic();
    }

    @Setup
    public void setup() throws PulsarClientException {
        // CHANGE TO A GENERAL CLIENT
        client = PulsarClient.builder()
                .serviceUrl(clientUrl)
                .build();

        producer = client.newProducer()
                .topic(topic)
                .compressionType(CompressionType.LZ4)
                .create();

    }

    @ProcessElement
    public void processElement(ProcessContext ctx) throws Exception {
        Message message = ctx.element();
        Long offset = message.getPublishTime();
        //TODO validate message exists

        producer.send(message);
    }

    @Teardown
    public void teardown() throws PulsarClientException {
        producer.close();
    }
}
