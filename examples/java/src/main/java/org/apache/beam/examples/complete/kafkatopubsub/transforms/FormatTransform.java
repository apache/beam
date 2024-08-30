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
package org.apache.beam.examples.complete.kafkatopubsub.transforms;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.beam.examples.complete.kafkatopubsub.avro.AvroDataClass;
import org.apache.beam.examples.complete.kafkatopubsub.avro.AvroDataClassKafkaAvroDeserializer;
import org.apache.beam.examples.complete.kafkatopubsub.kafka.consumer.SslConsumerFactoryFn;
import org.apache.beam.examples.complete.kafkatopubsub.options.KafkaToPubsubOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Different transformations over the processed data in the pipeline. */
public class FormatTransform {

  public enum FORMAT {
    PUBSUB,
    AVRO
  }

  /**
   * Configures Kafka consumer.
   *
   * @param bootstrapServers Kafka servers to read from
   * @param topicsList Kafka topics to read from
   * @param kafkaConfig configuration for the Kafka consumer
   * @param sslConfig configuration for the SSL connection
   * @return configured reading from Kafka
   */
  public static PTransform<PBegin, PCollection<KV<String, String>>> readFromKafka(
      String bootstrapServers,
      List<String> topicsList,
      Map<String, Object> kafkaConfig,
      Map<String, String> sslConfig) {
    return KafkaIO.<String, String>read()
        .withBootstrapServers(bootstrapServers)
        .withTopics(topicsList)
        .withKeyDeserializerAndCoder(
            StringDeserializer.class, (Coder<String>) NullableCoder.of(StringUtf8Coder.of()))
        .withValueDeserializerAndCoder(
            StringDeserializer.class, (Coder<String>) NullableCoder.of(StringUtf8Coder.of()))
        .withConsumerConfigUpdates(kafkaConfig)
        .withConsumerFactoryFn(new SslConsumerFactoryFn(sslConfig))
        .withoutMetadata();
  }

  /**
   * Configures Kafka consumer to read avros to {@link AvroDataClass} format.
   *
   * @param bootstrapServers Kafka servers to read from
   * @param topicsList Kafka topics to read from
   * @param config configuration for the Kafka consumer
   * @return configured reading from Kafka
   */
  public static PTransform<PBegin, PCollection<KV<String, AvroDataClass>>> readAvrosFromKafka(
      String bootstrapServers,
      List<String> topicsList,
      Map<String, Object> config,
      Map<String, String> sslConfig) {
    return KafkaIO.<String, AvroDataClass>read()
        .withBootstrapServers(bootstrapServers)
        .withTopics(topicsList)
        .withKeyDeserializerAndCoder(
            StringDeserializer.class, (Coder<String>) NullableCoder.of(StringUtf8Coder.of()))
        .withValueDeserializerAndCoder(
            AvroDataClassKafkaAvroDeserializer.class, AvroCoder.of(AvroDataClass.class))
        .withConsumerConfigUpdates(config)
        .withConsumerFactoryFn(new SslConsumerFactoryFn(sslConfig))
        .withoutMetadata();
  }

  /**
   * The {@link FormatOutput} wraps a String serializable messages with the {@link PubsubMessage}
   * class.
   */
  public static class FormatOutput extends PTransform<PCollection<String>, PDone> {

    private final KafkaToPubsubOptions options;

    public FormatOutput(KafkaToPubsubOptions options) {
      this.options = options;
    }

    @Override
    public PDone expand(PCollection<String> input) {
      return input
          .apply(
              "convertMessagesToPubsubMessages",
              MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                  .via(
                      (String json) ->
                          new PubsubMessage(
                              json.getBytes(StandardCharsets.UTF_8), ImmutableMap.of())))
          .apply(
              "writePubsubMessagesToPubSub", PubsubIO.writeMessages().to(options.getOutputTopic()));
    }
  }
}
