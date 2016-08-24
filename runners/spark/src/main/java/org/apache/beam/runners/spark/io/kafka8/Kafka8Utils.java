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

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

import com.google.api.client.util.Maps;

import org.apache.spark.streaming.kafka.KafkaUtils;
import org.joda.time.Instant;

import java.util.Map;

/**
 * Utility class to help adapt Beam's {@link KafkaIO} into {@link SparkKafkaSourcePTransform}.
 */
public class Kafka8Utils {

  /**
   * Clear out all configurations that do not match a value of type String as they will not match
   * the Spark streaming Kafka API. See {@link KafkaUtils#createDirectStream}.
   */
  public static Map<String, String> toKafkaParams(Map<String, Object> consumerConfig09) {
    Map<String, String> params = Maps.newHashMap();
    for (Map.Entry<String, Object> en: consumerConfig09.entrySet()) {
      if (en.getValue() instanceof String) {
        params.put(en.getKey(), en.getValue().toString());
      }
    }
    return params;
  }

  /**
   * Wrap the input with a dummy {@link KafkaRecord} so that the supplied timestampFn
   * from {@link KafkaIO} could be used.
   */
  public static <K, V> SerializableFunction<KV<K, V>, Instant>
  wrapKafkaRecordAndThen(final SerializableFunction<KafkaRecord<K, V>, Instant> fn) {
    return new SerializableFunction<KV<K, V>, Instant>() {
      @Override
      public Instant apply(KV<K, V> input) {
        // wrap the KV with KafkaRecord just as an adapter.
        // Use values that will fail if used - they shouldn't be used because the KafkaIO is
        // the one that adds the Kafka coordinates (topic, partition, offset) to the user defined
        // time extractor.
        return fn == null ? Instant.now() : fn.apply(new KafkaRecord<K, V>(null, -1, -1, input));
      }
    };
  }

}
