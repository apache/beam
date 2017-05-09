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

package org.apache.beam.sdk.io.kafka.serialization;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableMap;

import java.io.InputStream;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Implements a Kafka {@link Deserializer} and {@link Serializer} with a {@link Coder}.
 *
 * <p>This is useful for with KafkaIO sources when a Beam {@link Coder}is available and is able to
 * decode Kafka messages, but corresponding Kafka deserializer isn't available. The messages are
 * deserialized using {@link Coder#decode(InputStream)}. See example usage below that uses
 * {@link AvroCoder} for Kafka source and sink.
 *
 * <p>Usage: <pre>{@code
 *
 *    // Source : we want to use {@link AvroCoder} for deserializing Kafka records.
 *
 *    pipline.apply(
 *        CoderBasedKafkaSerializer.<Key, Value>readerWithCoders(AvroCoder.of(Key.class),
 *                                                               AvroCoder.of(Value.class))
 *        // Normal KafkaIO.read() configuration (except deserializers which are already set up):
 *        .withBootstrapServers("broker_1:9092,broker_2:9092")
 *        .withTopic("my_topic")
 *        // ...
 *    )
 *
 *    // Sink : write Key and Value objects using Avro serialization
 *
 *    pipeline
 *      .apply(...)
 *      .apply(CoderBasedKafkaSerializer.<Key, Value>writerWithCoders(AvroCoder.of(Key.class),
 *                                                                    AvroCoder.of(Value.class))
 *             // Normal KafkIO.write() configuration (except serializers):
 *             .withBootstrapServers("broker_1:9092,broker_2:9092")
 *             .withTopic("results")
 *             // ...
 *      )
 *
 *  }</pre>
 */
public class CoderBasedKafkaSerializer<T> implements Deserializer<T>, Serializer<T> {

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    String configKey = isKey ? configForKey() : configForValue();
    coder = (Coder<T>) configs.get(configKey);
    checkNotNull(coder, "could not instantiate coder for Kafka serialization");
  }

  @Override
  public T deserialize(String topic, @Nullable byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      return CoderUtils.decodeFromByteArray(coder, data);
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] serialize(String topic, @Nullable T data) {
    if (data == null) {
      return null; // common for keys to be null
    }

    try {
      return CoderUtils.encodeToByteArray(coder, data);
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
  }

  /**
   * An utility method to set up Kafka deserializers using Beam coders. Sets up appropriate Kafka
   * consumer config and returns a {@link KafkaIO.Read} transform.
   * See {@link CoderBasedKafkaSerializer} for example usage.
   */
  @SuppressWarnings("unchecked")
  public static <K, V> KafkaIO.Read<K, V> readerWithCoders(Coder<K> keyCoder, Coder<V> valueCoder) {
    return KafkaIO.<K, V>read()
        // Store actual coders in Kafka config so that these are available at runtime.
        .updateConsumerProperties(ImmutableMap.<String, Object>of(
            CoderBasedKafkaSerializer.configForKey(), keyCoder,
            CoderBasedKafkaSerializer.configForValue(), valueCoder))
        .withKeyDeserializerAndCoder((Class) CoderBasedKafkaSerializer.class, keyCoder)
        .withValueDeserializerAndCoder(CoderBasedKafkaSerializer.class, valueCoder);
  }

  /**
   * An utility method to set up Kafka serializers using Beam coders. Sets up appropriate Kafka
   * producer config and returns a {@link KafkaIO.Write} transform.
   * See {@link CoderBasedKafkaSerializer} for example usage.
   */
  @SuppressWarnings("unchecked")
  public static <K, V> KafkaIO.Write<K, V> writerWithCoders(Coder<K> keyCoder,
                                                            Coder<V> valueCoder) {
    return KafkaIO.<K, V>write()
        // Store actual coders in Kafka config so that these are available at runtime.
        .updateProducerProperties(ImmutableMap.<String, Object>of(
            CoderBasedKafkaSerializer.configForKey(), keyCoder,
            CoderBasedKafkaSerializer.configForValue(), valueCoder))
        .withKeySerializer((Class) CoderBasedKafkaSerializer.class)
        .withValueSerializer(CoderBasedKafkaSerializer.class);
  }


  public static String configForKey() {
    return String.format(CoderBasedKafkaSerializer.CONFIG_FORMAT, "key");
  }

  public static String configForValue() {
    return String.format(CoderBasedKafkaSerializer.CONFIG_FORMAT, "value");
  }

  private Coder<T> coder = null;
  private static final String CONFIG_FORMAT = "beam.coder.based.kafka.%s.serializer";
}
