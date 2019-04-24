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
package org.apache.beam.sdk.expansion;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.core.construction.expansion.ExpansionService;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ServerBuilder;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * An {@link org.apache.beam.runners.core.construction.expansion.ExpansionService} useful for tests.
 */
public class TestExpansionService {

  private static final String TEST_COUNT_URN = "beam:transforms:xlang:count";
  private static final String TEST_FILTER_URN = "beam:transforms:xlang:filter_less_than_eq";
  private static final String TEST_PARQUET_READ_URN = "beam:transforms:xlang:parquet_read";
  private static final String TEST_PARQUET_WRITE_URN = "beam:transforms:xlang:parquet_write";
  private static final String TEST_KAFKA_WRITE_URN = "beam:transforms:xlang:kafka_write";
  private static final String TEST_KAFKA_READ_URN = "beam:transforms:xlang:kafka_read";

  /** Registers a single test transformation. */
  @AutoService(ExpansionService.ExpansionServiceRegistrar.class)
  public static class TestTransforms implements ExpansionService.ExpansionServiceRegistrar {
    String rawSchema =
        "{ \"type\": \"record\", \"name\": \"testrecord\", \"fields\": "
            + "[ {\"name\": \"name\", \"type\": \"string\"} ]}";

    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      Schema schema = new Schema.Parser().parse(rawSchema);
      ImmutableMap.Builder<String, ExpansionService.TransformProvider> builder =
          ImmutableMap.builder();
      builder.put(TEST_COUNT_URN, spec -> Count.perElement());
      builder.put(
          TEST_FILTER_URN,
          spec ->
              Filter.lessThanEq(
                  // TODO(BEAM-6587): Use strings directly rather than longs.
                  (long) spec.getPayload().toStringUtf8().charAt(0)));
      builder.put(
          TEST_PARQUET_READ_URN,
          spec ->
              new PTransform<PCollection<String>, PCollection<GenericRecord>>() {
                @Override
                public PCollection<GenericRecord> expand(PCollection<String> input) {
                  return input
                      .apply(FileIO.matchAll())
                      .apply(FileIO.readMatches())
                      .apply(ParquetIO.readFiles(schema))
                      .setCoder(AvroGenericCoder.of(schema));
                }
              });
      builder.put(
          TEST_PARQUET_WRITE_URN,
          spec ->
              new PTransform<PCollection<GenericRecord>, PCollection<String>>() {
                @Override
                public PCollection<String> expand(PCollection<GenericRecord> input) {
                  return input
                      .apply(
                          FileIO.<GenericRecord>write()
                              .via(ParquetIO.sink(schema))
                              .to(spec.getPayload().toStringUtf8()))
                      .getPerDestinationOutputFilenames()
                      .apply(Values.create());
                }
              });
      builder.put(
          TEST_KAFKA_WRITE_URN,
          spec ->
              new PTransform<PCollection<String>, PDone>() {
                @Override
                public PDone expand(PCollection<String> input) {
                  return input.apply(
                      KafkaIO.<Void, String>write()
                          .withBootstrapServers(spec.getPayload().toStringUtf8())
                          .withTopic("beam-kafkaio-test")
                          .withValueSerializer(StringSerializer.class)
                          .values());
                }
              });
      builder.put(
          TEST_KAFKA_READ_URN,
          spec ->
              new PTransform<PBegin, PCollection<String>>() {
                @Override
                public PCollection<String> expand(PBegin input) {
                  return input
                      .apply(
                          KafkaIO.<String, String>read()
                              .withBootstrapServers(spec.getPayload().toStringUtf8())
                              .withTopic("beam-kafkaio-test")
                              .withKeyDeserializer(StringDeserializer.class)
                              .withValueDeserializerAndCoder(
                                  StringDeserializer.class, StringUtf8Coder.of())
                              .withReadCommitted()
                              .withMaxNumRecords(3)
                              .updateConsumerProperties(
                                  ImmutableMap.of(
                                      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
                              .withoutMetadata())
                      .apply(Values.create());
                }
              });
      return builder.build();
    }
  }

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[0]);
    System.out.println("Starting expansion service at localhost:" + port);
    Server server = ServerBuilder.forPort(port).addService(new ExpansionService()).build();
    server.start();
    server.awaitTermination();
  }
}
