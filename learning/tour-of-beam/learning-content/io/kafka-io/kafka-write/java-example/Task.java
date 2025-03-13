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

// beam-playground:
//   name: kafka-write
//   description: KafkaIO write example
//   multifile: false
//   default_example: false
//   context_line: 45
//   categories:
//     - IO
//   complexity: ADVANCED

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Task {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> input = pipeline.apply(Create.of("Hello", "KafkaIO"));

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "kafka_server:9092");
        producerProps.setProperty("key.serializer", StringSerializer.class.getName());
        producerProps.setProperty("value.serializer", ByteArraySerializer.class.getName());

        // This pipeline is an example of how you can use Apache Beam's KafkaIO to write data to a Kafka topic.
        // Make sure your Kafka server is accessible and running, and the topic exists.

        input.apply(KafkaIO.<String, String>write()
                .withBootstrapServers("kafka_server:9092")
                .withTopic("my-topic")
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class)
                .withProducerConfigUpdates(new HashMap<>()).values());

        pipeline.run().waitUntilFinish();
    }
}