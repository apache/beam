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
package org.apache.beam.sdk.schemas.utils;

import static org.apache.beam.sdk.values.Row.toRow;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.yaml.snakeyaml.Yaml;

public class YamlUtils {
  private static final Map<Schema.TypeName, Function<String, @Nullable Object>> YAML_VALUE_PARSERS =
      ImmutableMap
          .<Schema.TypeName,
              Function<String, @org.checkerframework.checker.nullness.qual.Nullable Object>>
              builder()
          .put(Schema.TypeName.BYTE, Byte::valueOf)
          .put(Schema.TypeName.INT16, Short::valueOf)
          .put(Schema.TypeName.INT32, Integer::valueOf)
          .put(Schema.TypeName.INT64, Long::valueOf)
          .put(Schema.TypeName.FLOAT, Float::valueOf)
          .put(Schema.TypeName.DOUBLE, Double::valueOf)
          .put(Schema.TypeName.DECIMAL, BigDecimal::new)
          .put(Schema.TypeName.BOOLEAN, Boolean::valueOf)
          .put(Schema.TypeName.STRING, str -> str)
          .put(Schema.TypeName.BYTES, str -> BaseEncoding.base64().decode(str))
          .build();

  public static Row toBeamRow(@Nullable String yamlString, Schema schema) {
    return toBeamRow(yamlString, schema, false);
  }

  public static Row toBeamRow(
      @Nullable String yamlString, Schema schema, boolean convertNamesToCamelCase) {
    if (yamlString == null || yamlString.isEmpty()) {
      List<Field> requiredFields =
          schema.getFields().stream()
              .filter(field -> !field.getType().getNullable())
              .collect(Collectors.toList());
      if (requiredFields.isEmpty()) {
        return Row.nullRow(schema);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Received an empty YAML string, but output schema contains required fields: %s",
                requiredFields));
      }
    }
    Yaml yaml = new Yaml();
    Object yamlMap = yaml.load(yamlString);

    Preconditions.checkArgument(
        yamlMap instanceof Map,
        "Expected a YAML mapping but got type '%s' instead.",
        Preconditions.checkNotNull(yamlMap).getClass());

    return toBeamRow(
        (Map<String, Object>) Preconditions.checkNotNull(yamlMap), schema, convertNamesToCamelCase);
  }

  private static @Nullable Object toBeamValue(
      Field field, @Nullable Object yamlValue, boolean convertNamesToCamelCase) {
    FieldType fieldType = field.getType();

    if (yamlValue == null) {
      if (fieldType.getNullable()) {
        return null;
      } else {
        throw new IllegalArgumentException(
            "Received null value for non-nullable field \"" + field.getName() + "\"");
      }
    }

    if (yamlValue instanceof String
        || yamlValue instanceof Number
        || yamlValue instanceof Boolean) {
      String yamlStringValue = yamlValue.toString();
      if (YAML_VALUE_PARSERS.containsKey(fieldType.getTypeName())) {
        return YAML_VALUE_PARSERS.get(fieldType.getTypeName()).apply(yamlStringValue);
      }
    }

    if (yamlValue instanceof byte[] && fieldType.getTypeName() == Schema.TypeName.BYTES) {
      return yamlValue;
    }

    if (yamlValue instanceof List) {
      FieldType innerType =
          Preconditions.checkNotNull(
              fieldType.getCollectionElementType(),
              "Cannot convert YAML type '%s` to `%s` because the YAML value is a List, but the output schema field does not define a collection type.",
              yamlValue.getClass(),
              fieldType);
      return ((List<Object>) yamlValue)
          .stream()
              .map(
                  v ->
                      Preconditions.checkNotNull(
                          toBeamValue(field.withType(innerType), v, convertNamesToCamelCase)))
              .collect(Collectors.toList());
    }

    if (yamlValue instanceof Map) {
      if (fieldType.getTypeName() == Schema.TypeName.ROW) {
        Schema nestedSchema =
            Preconditions.checkNotNull(
                fieldType.getRowSchema(),
                "Received a YAML '%s' type, but output schema field '%s' does not define a Row Schema",
                yamlValue.getClass(),
                fieldType);
        return toBeamRow((Map<String, Object>) yamlValue, nestedSchema, convertNamesToCamelCase);
      } else if (fieldType.getTypeName() == Schema.TypeName.MAP) {
        return yamlValue;
      }
    }

    throw new UnsupportedOperationException(
        String.format(
            "Converting YAML type '%s' to '%s' is not supported", yamlValue.getClass(), fieldType));
  }

  @SuppressWarnings("nullness")
  public static Row toBeamRow(
      @Nullable Map<String, Object> map, Schema rowSchema, boolean toCamelCase) {
    if (map == null || map.isEmpty()) {
      List<Field> requiredFields =
          rowSchema.getFields().stream()
              .filter(field -> !field.getType().getNullable())
              .collect(Collectors.toList());
      if (requiredFields.isEmpty()) {
        return Row.nullRow(rowSchema);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Received an empty Map, but output schema contains required fields: %s",
                requiredFields));
      }
    }
    return rowSchema.getFields().stream()
        .map(
            field ->
                toBeamValue(
                    field, map.get(maybeGetSnakeCase(field.getName(), toCamelCase)), toCamelCase))
        .collect(toRow(rowSchema));
  }

  private static String maybeGetSnakeCase(String str, boolean getSnakeCase) {
    return getSnakeCase ? CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, str) : str;
  }

  public static String yamlStringFromMap(@Nullable Map<String, Object> map) {
    if (map == null || map.isEmpty()) {
      return "";
    }
    return new Yaml().dumpAsMap(map);
  }

  public static Map<String, Object> yamlStringToMap(@Nullable String yaml) {
    if (yaml == null || yaml.isEmpty()) {
      return Collections.emptyMap();
    }
    return new Yaml().load(yaml);
  }
}
