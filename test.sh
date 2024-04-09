#!/usr/bin/env bash

for i in $(seq 1 100); do
  ./gradlew :runners:google-cloud-dataflow-java:worker:test --tests :runners:google-cloud-dataflow-java:worker:test --tests org.apache.beam.runners.dataflow.worker.windmill.client.grpc.StreamingEngineClientTest
done
