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
package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Checks the options of a {@code WITH} clause against a Managed configuration schema. */
final class ManagedOptions {
  private ManagedOptions() {}

  /** Maps each key to {@code reason}, why {@code WITH} cannot set it. */
  static Map<String, String> owned(String reason, String... keys) {
    ImmutableMap.Builder<String, String> owned = ImmutableMap.builder();
    for (String key : keys) {
      owned.put(key, reason);
    }
    return owned.build();
  }

  /**
   * Returns the options as the Java values {@code configSchema} expects.
   *
   * @param owned the keys that {@code WITH} cannot set, each mapped to the reason
   * @param use what the options configure, for error messages
   * @throws IllegalArgumentException for an owned or unknown key, or a value of the wrong type
   */
  static Map<String, Object> check(
      Map<String, Object> options, Schema configSchema, Map<String, String> owned, String use) {
    ImmutableMap.Builder<String, Object> checked = ImmutableMap.builder();
    for (Map.Entry<String, Object> option : options.entrySet()) {
      String key = option.getKey();
      @Nullable String reason = owned.get(key);
      checkArgument(reason == null, "Option '%s' cannot be set in WITH: %s", key, reason);
      if (!configSchema.hasField(key)) {
        List<String> accepted =
            configSchema.getFieldNames().stream()
                .filter(name -> !owned.containsKey(name))
                .sorted()
                .collect(Collectors.toList());
        throw new IllegalArgumentException(
            String.format("Unknown option '%s' for %s. Accepted options: %s", key, use, accepted));
      }
      FieldType type = configSchema.getField(key).getType();
      @Nullable Object value = convert(option.getValue(), type);
      if (value == null) {
        throw new IllegalArgumentException(
            String.format("Option '%s' must be of type %s", key, sqlType(type)));
      }
      checked.put(key, value);
    }
    return checked.build();
  }

  /** Returns {@code value} as the Java type of {@code type}, or null if it is not of that type. */
  private static @Nullable Object convert(@Nullable Object value, FieldType type) {
    switch (type.getTypeName()) {
      case STRING:
        return value instanceof String ? value : null;
      case BOOLEAN:
        return value instanceof Boolean ? value : null;
      case INT32:
        return value instanceof Long && (Long) value == ((Long) value).intValue()
            ? ((Long) value).intValue()
            : null;
      case ARRAY:
        if (!(value instanceof List)) {
          return null;
        }
        FieldType elementType = checkStateNotNull(type.getCollectionElementType());
        ImmutableList.Builder<Object> list = ImmutableList.builder();
        for (Object element : (List<?>) value) {
          @Nullable Object converted = convert(element, elementType);
          if (converted == null) {
            return null;
          }
          list.add(converted);
        }
        return list.build();
      case MAP:
        if (!(value instanceof Map)) {
          return null;
        }
        FieldType keyType = checkStateNotNull(type.getMapKeyType());
        FieldType valueType = checkStateNotNull(type.getMapValueType());
        ImmutableMap.Builder<Object, Object> map = ImmutableMap.builder();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          @Nullable Object convertedKey = convert(entry.getKey(), keyType);
          @Nullable Object convertedValue = convert(entry.getValue(), valueType);
          if (convertedKey == null || convertedValue == null) {
            return null;
          }
          map.put(convertedKey, convertedValue);
        }
        return map.build();
      default:
        return null;
    }
  }

  private static String sqlType(FieldType type) {
    switch (type.getTypeName()) {
      case STRING:
        return "VARCHAR";
      case INT32:
        return "INTEGER";
      case ARRAY:
        return "ARRAY<" + sqlType(checkStateNotNull(type.getCollectionElementType())) + ">";
      case MAP:
        return "MAP<"
            + sqlType(checkStateNotNull(type.getMapKeyType()))
            + ", "
            + sqlType(checkStateNotNull(type.getMapValueType()))
            + ">";
      default:
        return type.getTypeName().name();
    }
  }
}
