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
package org.apache.beam.examples;

// beam-playground:
//   name: KafkaPassengerCount
//   description: Read NYC Taxi dataset from Kafka server to count passengers for each vendor
//   multifile: false
//   default_example: false
//   context_line: 72
//   categories:
//     - Emulated Data Source
//     - IO
//   complexity: BASIC
//   tags:
//     - strings
//     - emulator
//   emulators:
//      - type: kafka
//        topic:
//          id: NYCTaxi1000_simple
//          source_dataset: NYCTaxi1000_simple
//   datasets:
//      NYCTaxi1000_simple:
//          location: local
//          format: json

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaPassengerCount {
  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);
    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.offset.reset", "earliest");
    ObjectMapper om = new ObjectMapper();

    p.apply(
            "ReadFromKafka",
            KafkaIO.<String, String>read()
                .withBootstrapServers(
                    "kafka_server:9092") // The argument is hardcoded to a predefined value. Do not
                // change it manually. It's replaced to the correct Kafka
                // cluster address when code starts in backend.

                // NYCTaxi1000_simple is a small subset of NYC Taxi dataset with VendorID and
                // passenger_count fields
                .withTopicPartitions(
                    Collections.singletonList(
                        new TopicPartition(
                            "NYCTaxi1000_simple",
                            0))) // The argument is hardcoded to a predefined value. Do not change
                // it manually. It's replaced to the correct Kafka cluster address
                // when code starts in backend.
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerConfig)
                .withMaxNumRecords(998)
                .withoutMetadata())
        .apply("CreateValues", Values.create())
        .apply(
            "ExtractData",
            ParDo.of(
                new DoFn<String, KV<Integer, Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws JsonProcessingException {
                    final VendorToPassengerDTO result =
                        om.readValue(c.element(), new TypeReference<VendorToPassengerDTO>() {});
                    c.output(KV.of(result.getVendorIdField(), result.getPassengerCountField()));
                  }
                }))
        .apply(
            "Sum passengers per vendor",
            Combine.<Integer, Integer, Integer>perKey(Sum.ofIntegers()))
        .apply(
            "FormatResults",
            ParDo.of(
                new DoFn<KV<Integer, Integer>, KV<Integer, Integer>>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext c, OutputReceiver<KV<Integer, Integer>> out) {
                    System.out.printf(
                        "Vendor: %s, Passengers: %s%n",
                        c.element().getKey(), c.element().getValue());
                    out.output(c.element());
                  }
                }));
    p.run().waitUntilFinish();
  }
}

class VendorToPassengerDTO {
  private Integer passengerCountField;
  private Integer vendorIdField;

  public VendorToPassengerDTO(Integer passengerCount, Integer vendorId) {

    this.passengerCountField = passengerCount;
    this.vendorIdField = vendorId;
  }

  public VendorToPassengerDTO() {

    super();
    this.passengerCountField = 0;
    this.vendorIdField = 0;
  }

  public Integer getVendorIdField() {

    return this.vendorIdField;
  }

  public Integer getPassengerCountField() {

    return this.passengerCountField;
  }

  @JsonSetter("VendorID")
  public void setVendorIdField(Integer vendorId) {
    this.vendorIdField = vendorId;
  }

  @JsonSetter("passenger_count")
  public void setPassengerCountField(Integer passengerCount) {
    this.passengerCountField = passengerCount;
  }
}
