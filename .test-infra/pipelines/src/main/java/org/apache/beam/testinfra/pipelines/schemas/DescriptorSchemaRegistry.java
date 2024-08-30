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
package org.apache.beam.testinfra.pipelines.schemas;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.NonNull;

/** Registers and builds {@link Schema}s of {@link Descriptor} based types. */
@Internal
public class DescriptorSchemaRegistry {

  public static final DescriptorSchemaRegistry INSTANCE = new DescriptorSchemaRegistry();

  private DescriptorSchemaRegistry() {}

  static final String KEY_FIELD_NAME = "key";
  static final String VALUE_FIELD_NAME = "value";
  private static final Map<@NonNull String, @NonNull Schema> SCHEMA_CACHE = new HashMap<>();

  boolean hasNoCachedBuild(Descriptor descriptor) {
    return !SCHEMA_CACHE.containsKey(descriptor.getFullName());
  }

  public @NonNull Schema getOrBuild(Descriptor descriptor) {
    if (hasNoCachedBuild(descriptor)) {
      build(descriptor);
    }
    return checkStateNotNull(SCHEMA_CACHE.get(descriptor.getFullName()));
  }

  public void build(Descriptor descriptor) {
    DependencyDrivenDescriptorQueue queue = new DependencyDrivenDescriptorQueue();
    queue.enqueue(descriptor);
    for (Descriptor queuedDescriptor : queue) {
      Builder builder = new Builder();
      Schema schema = builder.build(queuedDescriptor);
      SCHEMA_CACHE.put(queuedDescriptor.getFullName(), schema);
    }
  }

  static class Builder {
    private static final Map<JavaType, FieldType> FIELD_TYPE_MAP =
        ImmutableMap.<JavaType, FieldType>builder()
            .put(JavaType.BOOLEAN, FieldType.BOOLEAN)
            .put(JavaType.INT, FieldType.INT32)
            .put(JavaType.LONG, FieldType.INT64)
            .put(JavaType.FLOAT, FieldType.FLOAT)
            .put(JavaType.DOUBLE, FieldType.DOUBLE)
            .put(JavaType.ENUM, FieldType.STRING)
            .put(JavaType.STRING, FieldType.STRING)
            .build();

    private static final Map<@NonNull String, @NonNull FieldType> FULL_NAME_TYPE_MAP =
        ImmutableMap.<String, FieldType>builder()
            .put("google.protobuf.Value", FieldType.STRING)
            .put("google.protobuf.Timestamp", FieldType.DATETIME)
            .put("google.protobuf.Any", FieldType.STRING)
            .build();

    private final Schema.Builder schemaBuilder = Schema.builder();

    Schema build(Descriptor descriptor) {
      parse(descriptor);
      Schema schema = schemaBuilder.build();
      SCHEMA_CACHE.put(descriptor.getFullName(), schema);
      return schema;
    }

    void parse(Descriptor descriptor) {
      for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
        if (fieldDescriptor.getJavaType().equals(JavaType.BYTE_STRING)) {
          continue;
        }
        FieldType type = build(fieldDescriptor);
        schemaBuilder.addField(Field.of(fieldDescriptor.getName(), type));
      }
    }

    FieldType build(FieldDescriptor fieldDescriptor) {
      if (fieldDescriptor.isMapField()) {
        return buildMapType(fieldDescriptor);
      }
      if (fieldDescriptor.getJavaType().equals(JavaType.MESSAGE)) {
        return buildNestedType(fieldDescriptor);
      }
      FieldType type = checkStateNotNull(FIELD_TYPE_MAP.get(fieldDescriptor.getJavaType()));
      if (fieldDescriptor.isRepeated()) {
        type = FieldType.array(type);
      }
      return type;
    }

    FieldType buildMapType(FieldDescriptor fieldDescriptor) {
      Descriptor mapDescriptor = fieldDescriptor.getMessageType();
      FieldDescriptor keyField = checkStateNotNull(mapDescriptor.findFieldByName(KEY_FIELD_NAME));
      FieldType keyType = checkStateNotNull(FIELD_TYPE_MAP.get(keyField.getJavaType()));
      FieldDescriptor valueField =
          checkStateNotNull(mapDescriptor.findFieldByName(VALUE_FIELD_NAME));
      FieldType valueType = build(valueField);
      return FieldType.array(
          FieldType.row(
              Schema.of(Field.of(KEY_FIELD_NAME, keyType), Field.of(VALUE_FIELD_NAME, valueType))));
    }

    FieldType buildNestedType(FieldDescriptor fieldDescriptor) {
      Descriptor messageDescriptor = fieldDescriptor.getMessageType();
      if (FULL_NAME_TYPE_MAP.containsKey(messageDescriptor.getFullName())) {
        return checkStateNotNull(FULL_NAME_TYPE_MAP.get(messageDescriptor.getFullName()));
      }
      if (!SCHEMA_CACHE.containsKey(messageDescriptor.getFullName())) {
        Builder builder = new Builder();
        Schema schema = builder.build(messageDescriptor);
        SCHEMA_CACHE.put(messageDescriptor.getFullName(), schema);
      }
      Schema schema =
          checkStateNotNull(
              SCHEMA_CACHE.get(messageDescriptor.getFullName()),
              "nested type not cached: %s",
              messageDescriptor.getFullName());
      FieldType type = FieldType.row(schema);
      if (fieldDescriptor.isRepeated()) {
        type = FieldType.array(type);
      }
      return type;
    }
  }
}
