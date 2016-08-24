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

package org.apache.beam.runners.spark.io.kafka8;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A Spark Kafka source single primitive PTransform.
 * Acts as an adapter with Beam's {@link KafkaIO} for portability, while using Spark's native
 * Kafka integration which is optimized to the Spark supported version (0.8.2.1) and
 * architecture.
 * This PTransform simply supplies the configuration needed to use Spark's
 * {@link org.apache.spark.streaming.kafka.KafkaUtils} with direct stream.
 */
public class SparkKafkaSourcePTransform<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {
  private final Map<String, String> kafkaParams;
  private final Set<String> topics;
  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;
  private final SerializableFunction<KV<K, V>, Instant> kvTimestampFn;


  public SparkKafkaSourcePTransform(KafkaIO.TypedRead<K, V> beamKafkaIO) {
    validate(beamKafkaIO);
    kafkaParams = Kafka8Utils.toKafkaParams(beamKafkaIO.consumerConfig);
    topics = new HashSet<>(beamKafkaIO.topics);
    keyCoder = beamKafkaIO.keyCoder;
    valueCoder = beamKafkaIO.valueCoder;
    kvTimestampFn = Kafka8Utils.wrapKafkaRecordAndThen(beamKafkaIO.timestampFn);
  }

  /** Validate support for the user-defined KafkaIO properties. */
  private void validate(KafkaIO.TypedRead<K, V> beamKafkaIO) {
    if (beamKafkaIO.watermarkFn != null) {
      throw new UnsupportedOperationException("Watermarks are currently not supported by the "
          + "SparkRunner.");
    }
  }

  @Override
  public PCollection<KV<K, V>> apply(PBegin input) {
    PCollection<KV<K, V>> pcol = PCollection.createPrimitiveOutputInternal(
        input.getPipeline(), WindowingStrategy.globalDefault(), PCollection.IsBounded.UNBOUNDED);
    return pcol.setCoder(getDefaultOutputCoder());
  }

  @Override
  protected Coder<KV<K, V>> getDefaultOutputCoder() {
    return KvCoder.of(keyCoder, valueCoder);
  }

  public Map<String, String> getKafkaParams() {
    return kafkaParams;
  }

  public Set<String> getTopics() {
    return topics;
  }

  public Coder<K> getKeyCoder() {
    return keyCoder;
  }

  public Coder<V> getValueCoder() {
    return valueCoder;
  }

  public SerializableFunction<KV<K, V>, Instant> getKvTimestampFn() {
    return kvTimestampFn;
  }
}
