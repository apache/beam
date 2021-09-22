package org.apache.beam.sdk.io.pulsar;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;

public class PulsarIO {


    private PulsarClient pulsarClient;
    private String serviceUrl;
    private String pulsarRecord;
    private Consumer<GenericRecord> consumer;

    public PulsarIO(String serviceUrl) throws PulsarClientException{
        this.initPulsarClient(serviceUrl);
    }

    public PulsarIO() throws PulsarClientException {
        this.initPulsarClient(PulsarIOUtils.SERVICE_URL);
    }

    private void initPulsarClient(String serviceUrl) throws PulsarClientException {
        this.serviceUrl = serviceUrl;
        this.pulsarClient = PulsarClient.builder()
                                        .serviceUrl(serviceUrl)
                                        .build();

    }

    //TODO Call Pulsar SDF Implementation

}


