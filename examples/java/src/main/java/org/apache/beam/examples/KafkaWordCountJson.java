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
//   name: KafkaWordCountJson
//   description: Read CountWords dataset (CountWords.json) from Kafka to count words
//   multifile: false
//   context_line: 65
//   categories:
//     - Emulated Data Source
//     - IO
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - strings
//     - emulator
//   emulators:
//      - type: kafka
//        topic:
//          id: CountWords
//          source_dataset: CountWordsJson
//   datasets:
//     CountWordsJson:
//       location: local
//       format: json

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaWordCountJson {
  static final String TOKENIZER_PATTERN = "[^\\p{L}]+"; // Java pattern for letters

  public interface KafkaStreamingOptions extends PipelineOptions {
    /**
     * By default, this example uses Playground's Kafka server. Set this option to different value
     * to use your own Kafka server.
     */
    @Description("Kafka server host")
    @Default.String("kafka_server:9092")
    String getKafkaHost();

    void setKafkaHost(String value);
  }

  public static void main(String[] args) {
    KafkaStreamingOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaStreamingOptions.class);
    final Pipeline p = Pipeline.create(options);

    final Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.offset.reset", "earliest");

    p.apply(
            KafkaIO.<Long, String>read()
                .withBootstrapServers(
                    options.getKafkaHost()) // Set KafkaHost pipeline option to redefine
                // default value (valid for Playground environment)
                .withTopicPartitions(
                    Collections.singletonList(
                        new TopicPartition(
                            "CountWords", 0))) // Kafka topic is preloaded in Playground environment
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerConfig)
                .withMaxNumRecords(5)
                .withoutMetadata())
        .apply(Values.create())
        .apply(
            "ExtractWords",
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    for (String word : c.element().split(TOKENIZER_PATTERN, 0)) {
                      if (!word.isEmpty()) {
                        c.output(word);
                      }
                    }
                  }
                }))
        .apply(Count.perElement())
        .apply(
            "FormatResults",
            MapElements.via(
                new SimpleFunction<KV<String, Long>, String>() {
                  @Override
                  public String apply(KV<String, Long> input) {
                    System.out.printf("key: %s, value: %d%n", input.getKey(), input.getValue());
                    return input.getKey() + ": " + input.getValue();
                  }
                }))
        .apply(TextIO.write().to("word-counts"));

    p.run().waitUntilFinish();
  }
}
