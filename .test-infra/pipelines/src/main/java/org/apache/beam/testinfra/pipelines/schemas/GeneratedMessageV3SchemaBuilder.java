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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.NonNull;

class GeneratedMessageV3SchemaBuilder<T extends GeneratedMessageV3> {
  private static final Map<JavaType, FieldType> JAVA_TYPE_FIELD_TYPE_MAP =
      ImmutableMap.<JavaType, FieldType>builder()
          .put(JavaType.BOOLEAN, FieldType.BOOLEAN)
          .put(JavaType.INT, FieldType.INT32)
          .put(JavaType.LONG, FieldType.INT64)
          .put(JavaType.FLOAT, FieldType.FLOAT)
          .put(JavaType.DOUBLE, FieldType.DOUBLE)
          .put(JavaType.ENUM, FieldType.STRING)
          .put(JavaType.STRING, FieldType.STRING)
          .build();

  private static final Map<String, Schema> CACHED_SCHEMAS = Maps.newConcurrentMap();

  private final Schema.Builder schemaBuilder;
  private final GeneratedMessageV3Reflection<T> reflection;

  GeneratedMessageV3SchemaBuilder(
      Schema.Builder schemaBuilder, GeneratedMessageV3Reflection<T> reflection) {
    this.schemaBuilder = schemaBuilder;
    this.reflection = reflection;
  }

  Schema build() {
    for (FieldDescriptor field : reflection.getFields()) {
      if (field.getJavaType().equals(JavaType.BYTE_STRING)) {
        continue;
      }
      schemaBuilder.addField(buildField(field));
    }
    return schemaBuilder.build();
  }

  static @NonNull Field buildField(FieldDescriptor field) {
    FieldType type = determineFieldType(field);
    return Field.of(field.getName(), type).withNullable(field.hasOptionalKeyword());
  }

  static @NonNull FieldType determineFieldType(FieldDescriptor field) {

    if (field.isMapField()) {
      return buildMapFieldType(field);
    }

    if (field.getJavaType().equals(JavaType.MESSAGE)) {
      return buildRowFieldType(field);
    }

    FieldType type =
        checkStateNotNull(
            JAVA_TYPE_FIELD_TYPE_MAP.get(field.getJavaType()),
            "%s: %s does not map to a known type",
            JavaType.class,
            field.getJavaType());

    if (field.isRepeated()) {
      type = FieldType.array(type);
    }

    return type;
  }

  static FieldType buildRowFieldType(FieldDescriptor field) {

    Descriptors.Descriptor descriptor = field.getMessageType();

    if (!CACHED_SCHEMAS.containsKey(descriptor.getFullName())) {
      DescriptorReflection descriptorReflection = new DescriptorReflection(descriptor);
      Schema.Builder nestedSchemaBuilder = Schema.builder();
      for (FieldDescriptor fieldDescriptor : descriptorReflection.getFields()) {
        if (fieldDescriptor.getJavaType().equals(JavaType.BYTE_STRING)) {
          continue;
        }
        Field nestedField = buildField(fieldDescriptor);
        nestedSchemaBuilder.addField(nestedField);
      }
      CACHED_SCHEMAS.put(descriptor.getFullName(), nestedSchemaBuilder.build());
    }

    Schema schema = checkStateNotNull(CACHED_SCHEMAS.get(descriptor.getFullName()));

    FieldType type = FieldType.row(schema);
    if (field.isRepeated()) {
      type = FieldType.array(type);
    }

    return type;
  }

  static FieldType buildMapFieldType(FieldDescriptor field) {
    checkArgument(field.isMapField());
    return FieldType.map(FieldType.STRING, FieldType.STRING);
  }
}
