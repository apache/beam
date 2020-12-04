#!/usr/bin/env bash

java -cp build/libs/beam-examples-kafka-to-pubsub-2.27.0-SNAPSHOT.jar org.apache.beam.examples.KafkaToPubsub \
    --runner=DataflowRunner \
    --project=ktp-295315 \
    --region=us-central1 \
    --bootstrapServers=localhost:9092 \
    --inputTopics=input \
    --outputTopic=projects/ktp-295315/topics/ktp-topic \
    --outputFormat=PUBSUB
