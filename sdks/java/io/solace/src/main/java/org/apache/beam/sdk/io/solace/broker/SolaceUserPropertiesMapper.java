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
package org.apache.beam.sdk.io.solace.broker;

import com.solacesystems.common.util.ByteArray;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.Topic;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Bytes;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SolaceUserPropertiesMapper {

  private static final Logger LOG = LoggerFactory.getLogger(SolaceUserPropertiesMapper.class);

  private SolaceUserPropertiesMapper() {}

  public static Map<String, Solace.UserPropertyValue> toUserPropertyValueMap(
      @Nullable SDTMap properties) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, Solace.UserPropertyValue> userProperties = new HashMap<>();
    for (String key : properties.keySet()) {
      try {
        Object value = properties.get(key);
        if (value == null) {
          continue;
        }

        Solace.UserPropertyValue userPropertyValue = toUserPropertyValue(value);
        if (userPropertyValue.getKind() == Solace.UserPropertyValue.Kind.NONE) {
          LOG.info("Unsupported user property type: {}. ", value.getClass());
          continue;
        }

        userProperties.put(key, userPropertyValue);
      } catch (SDTException e) {
        throw new RuntimeException(e);
      }
    }
    return Collections.unmodifiableMap(userProperties);
  }

  public static SDTMap toSDTMap(@Nullable Map<String, Solace.UserPropertyValue> properties) {
    SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();

    if (properties == null) {
      return sdtMap;
    }

    for (Map.Entry<String, Solace.UserPropertyValue> entry : properties.entrySet()) {
      try {
        putUserProperty(sdtMap, entry.getKey(), entry.getValue());
      } catch (SDTException e) {
        throw new RuntimeException(e);
      }
    }
    return sdtMap;
  }

  private static Solace.UserPropertyValue toUserPropertyValue(@NonNull Object value) {
    if (value instanceof Boolean) {
      return Solace.UserPropertyValue.of((Boolean) value);
    }
    if (value instanceof Byte) {
      return Solace.UserPropertyValue.of((Byte) value);
    }
    if (value instanceof Short) {
      return Solace.UserPropertyValue.of((Short) value);
    }
    if (value instanceof Integer) {
      return Solace.UserPropertyValue.of((Integer) value);
    }
    if (value instanceof Long) {
      return Solace.UserPropertyValue.of((Long) value);
    }
    if (value instanceof Float) {
      return Solace.UserPropertyValue.of((Float) value);
    }
    if (value instanceof Double) {
      return Solace.UserPropertyValue.of((Double) value);
    }
    if (value instanceof Character) {
      return Solace.UserPropertyValue.of((Character) value);
    }
    if (value instanceof String) {
      return Solace.UserPropertyValue.of((String) value);
    }
    if (value instanceof byte[]) {
      return Solace.UserPropertyValue.of(Bytes.asList((byte[]) value));
    }
    if (value instanceof ByteArray) {
      return Solace.UserPropertyValue.of(Bytes.asList(((ByteArray) value).asBytes()));
    }
    if (value instanceof Destination) {
      return Solace.UserPropertyValue.of(toSolaceDestination((Destination) value));
    }
    return Solace.UserPropertyValue.of();
  }

  private static Solace.Destination toSolaceDestination(Destination destination) {
    return Solace.Destination.builder()
        .setType(
            destination instanceof Topic
                ? Solace.DestinationType.TOPIC
                : Solace.DestinationType.QUEUE)
        .setName(destination.getName())
        .build();
  }

  private static Destination toDestination(Solace.Destination destination) {
    if (destination.getType() == Solace.DestinationType.QUEUE) {
      return JCSMPFactory.onlyInstance().createQueue(destination.getName());
    }
    return JCSMPFactory.onlyInstance().createTopic(destination.getName());
  }

  private static void putUserProperty(
      SDTMap map, String key, Solace.UserPropertyValue propertyValue)
      throws UnsupportedOperationException, SDTException {
    if (propertyValue == null) {
      return;
    }
    switch (propertyValue.getKind()) {
      case BOOLEAN:
        ifNotNull(propertyValue.getBoolean(), v -> map.putBoolean(key, v));
        return;
      case BYTE:
        ifNotNull(propertyValue.getByte(), v -> map.putByte(key, v));
        return;
      case SHORT:
        ifNotNull(propertyValue.getShort(), v -> map.putShort(key, v));
        return;
      case INTEGER:
        ifNotNull(propertyValue.getInteger(), v -> map.putInteger(key, v));
        return;
      case LONG:
        ifNotNull(propertyValue.getLong(), v -> map.putLong(key, v));
        return;
      case FLOAT:
        ifNotNull(propertyValue.getFloat(), v -> map.putFloat(key, v));
        return;
      case DOUBLE:
        ifNotNull(propertyValue.getDouble(), v -> map.putDouble(key, v));
        return;
      case CHARACTER:
        ifNotNull(propertyValue.getCharacter(), v -> map.putCharacter(key, v));
        return;
      case STRING:
        ifNotNull(propertyValue.getString(), v -> map.putString(key, v));
        return;
      case BYTES:
        ifNotNull(propertyValue.getBytes(), v -> map.putBytes(key, Bytes.toArray(v)));
        return;
      case DESTINATION:
        ifNotNull(propertyValue.getDestination(), v -> map.putDestination(key, toDestination(v)));
        return;
      case NONE:
      default:
    }
  }

  @FunctionalInterface
  private interface SDTConsumer<T> {
    void accept(@NonNull T value) throws SDTException;
  }

  private static <T> void ifNotNull(@Nullable T value, SDTConsumer<T> consumer)
      throws SDTException {
    if (value != null) {
      consumer.accept(value);
    }
  }
}
