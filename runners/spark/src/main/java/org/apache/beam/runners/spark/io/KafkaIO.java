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
package org.apache.beam.runners.spark.io;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Set;
import kafka.serializer.Decoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Read stream from Kafka.
 */
public final class KafkaIO {

  private KafkaIO() {
  }

  /**
   * Read operation from Kafka topics.
   */
  public static final class Read {

    private Read() {
    }

    /**
     * Define the Kafka consumption.
     *
     * @param keyDecoder    {@link Decoder} to decode the Kafka message key
     * @param valueDecoder  {@link Decoder} to decode the Kafka message value
     * @param key           Kafka message key Class
     * @param value         Kafka message value Class
     * @param topics        Kafka topics to subscribe
     * @param kafkaParams   map of Kafka parameters
     * @param <K>           Kafka message key Class type
     * @param <V>           Kafka message value Class type
     * @return KafkaIO Unbound input
     */
    public static <K, V> Unbound<K, V> from(Class<? extends Decoder<K>> keyDecoder,
                                            Class<? extends Decoder<V>> valueDecoder,
                                            Class<K> key,
                                            Class<V> value, Set<String> topics,
                                            Map<String, String> kafkaParams) {
      return new Unbound<>(keyDecoder, valueDecoder, key, value, topics, kafkaParams);
    }

    /**
     * A {@link PTransform} reading from Kafka topics and providing {@link PCollection}.
     */
    public static class Unbound<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {

      private final Class<? extends Decoder<K>> keyDecoderClass;
      private final Class<? extends Decoder<V>> valueDecoderClass;
      private final Class<K> keyClass;
      private final Class<V> valueClass;
      private final Set<String> topics;
      private final Map<String, String> kafkaParams;

      Unbound(Class<? extends Decoder<K>> keyDecoder,
              Class<? extends Decoder<V>> valueDecoder, Class<K> key,
              Class<V> value, Set<String> topics, Map<String, String> kafkaParams) {
        checkNotNull(keyDecoder, "need to set the key decoder class of a KafkaIO.Read transform");
        checkNotNull(
            valueDecoder, "need to set the value decoder class of a KafkaIO.Read transform");
        checkNotNull(key, "need to set the key class of a KafkaIO.Read transform");
        checkNotNull(value, "need to set the value class of a KafkaIO.Read transform");
        checkNotNull(topics, "need to set the topics of a KafkaIO.Read transform");
        checkNotNull(kafkaParams, "need to set the kafkaParams of a KafkaIO.Read transform");
        this.keyDecoderClass = keyDecoder;
        this.valueDecoderClass = valueDecoder;
        this.keyClass = key;
        this.valueClass = value;
        this.topics = topics;
        this.kafkaParams = kafkaParams;
      }

      public Class<? extends Decoder<K>> getKeyDecoderClass() {
        return keyDecoderClass;
      }

      public Class<? extends Decoder<V>> getValueDecoderClass() {
        return valueDecoderClass;
      }

      public Class<V> getValueClass() {
        return valueClass;
      }

      public Class<K> getKeyClass() {
        return keyClass;
      }

      public Set<String> getTopics() {
        return topics;
      }

      public Map<String, String> getKafkaParams() {
        return kafkaParams;
      }

      @Override
      public PCollection<KV<K, V>> apply(PBegin input) {
        // Spark streaming micro batches are bounded by default
        return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
            WindowingStrategy.globalDefault(), PCollection.IsBounded.UNBOUNDED);
      }
    }

  }
}
