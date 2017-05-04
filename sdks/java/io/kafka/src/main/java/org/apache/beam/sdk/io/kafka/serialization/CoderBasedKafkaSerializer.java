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
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Implements Kafka's {@link Serializer} with a {@link Coder}.
 *
 * <p>As Kafka instantiates serializers directly, the coder
 * must be stored as serialized value in the producer configuration map.
 */
public class CoderBasedKafkaSerializer<T> implements Serializer<T> {
  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    String configKey = isKey ? configForKeySerializer() : configForValueSerializer();
    coder = (Coder<T>) configs.get(configKey);
    checkNotNull(coder, "could not instantiate coder for Kafka serialization");
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

  public static String configForKeySerializer() {
    return String.format(CoderBasedKafkaSerializer.CONFIG_FORMAT, "key");
  }

  public static String configForValueSerializer() {
    return String.format(CoderBasedKafkaSerializer.CONFIG_FORMAT, "value");
  }

  private Coder<T> coder = null;
  private static final String CONFIG_FORMAT = "beam.coder.based.kafka.%s.serializer";
}
