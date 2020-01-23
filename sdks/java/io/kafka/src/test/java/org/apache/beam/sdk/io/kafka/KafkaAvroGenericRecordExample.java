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
package org.apache.beam.sdk.io.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaAvroGenericRecordExample {

  private static final String TOPIC_NAME = "transactions";

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    p.apply(
            KafkaIO.<String, GenericRecord>read()
                .withBootstrapServers("localhost:9092")
                .withTopic(TOPIC_NAME)
                .withKeyDeserializer(StringDeserializer.class)
                .withCSRClientProvider("http://localhost:8081", null, TOPIC_NAME + "-value"))
        .apply(
            ParDo.of(
                new DoFn<KafkaRecord<String, GenericRecord>, Void>() {
                  @ProcessElement
                  public void processElement(ProcessContext ctx) {
                    System.out.println("ctx.element().getKV() = " + ctx.element().getKV());
                  }
                }));

    p.run().waitUntilFinish();
  }
}
