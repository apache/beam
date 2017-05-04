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

import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Implements a Kafka {@link Deserializer} with a {@link Coder}.
 *
 * <p>As Kafka instantiates serializers directly, the coder must be stored as serialized value in
 * the producer configuration map.
 */
public class CoderBasedKafkaDeserializer<T> implements Deserializer<T> {
  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    String configKey = isKey ? configForKeyDeserializer() : configForValueDeserializer();
    coder = (Coder<T>) configs.get(configKey);
    checkNotNull(coder, "could not instantiate coder for Kafka deserialization");
  }

  @Override
  public T deserialize(String topic, byte[] data) {
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
  public void close() {}

  public static String configForKeyDeserializer() {
    return String.format(CoderBasedKafkaDeserializer.CONFIG_FORMAT, "key");
  }

  public static String configForValueDeserializer() {
    return String.format(CoderBasedKafkaDeserializer.CONFIG_FORMAT, "value");
  }

  private Coder<T> coder = null;
  private static final String CONFIG_FORMAT = "beam.coder.based.kafka.%s.deserializer";
}
