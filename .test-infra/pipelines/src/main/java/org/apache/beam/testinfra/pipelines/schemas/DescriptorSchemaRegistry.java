package org.apache.beam.testinfra.pipelines.schemas;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.NonNull;

class DescriptorSchemaRegistry {
  static final String KEY_FIELD_NAME = "key";
  static final String VALUE_FIELD_NAME = "value";
  private static final Map<@NonNull String, @NonNull Schema> SCHEMA_CACHE = new HashMap<>();

  @NonNull
  Schema getOrBuild(Descriptor descriptor) {
    if (!SCHEMA_CACHE.containsKey(descriptor.getFullName())) {
      build(descriptor);
    }
    return checkStateNotNull(SCHEMA_CACHE.get(descriptor.getFullName()));
  }

  @NonNull
  Schema getSchema(Descriptor descriptor) {
    return checkStateNotNull(SCHEMA_CACHE.get(descriptor.getFullName()));
  }

  void build(Descriptor descriptor) {
    DependencyDrivenDescriptorQueue queue = new DependencyDrivenDescriptorQueue();
    queue.enqueue(descriptor);
    for (Descriptor queuedDescriptor : queue) {
      Builder builder = new Builder();
      Schema schema = builder.build(queuedDescriptor);
      SCHEMA_CACHE.put(queuedDescriptor.getFullName(), schema);
    }
  }

  static class Builder {
    private static final Map<JavaType, FieldType> FIELD_TYPE_MAP = ImmutableMap
        .<JavaType, FieldType>builder()
        .put(JavaType.BOOLEAN, FieldType.BOOLEAN)
        .put(JavaType.INT, FieldType.INT32)
        .put(JavaType.LONG, FieldType.INT64)
        .put(JavaType.FLOAT, FieldType.FLOAT)
        .put(JavaType.DOUBLE, FieldType.DOUBLE)
        .put(JavaType.ENUM, FieldType.STRING)
        .put(JavaType.STRING, FieldType.STRING)
        .build();

    private static final Map<@NonNull String, @NonNull FieldType> FULL_NAME_TYPE_MAP = ImmutableMap
            .<String, FieldType>builder()
            .put("google.protobuf.Value", FieldType.STRING)
            .put("google.dataflow.v1beta3.DisplayData", FieldType.STRING)
            .put("google.protobuf.Timestamp", FieldType.DATETIME)
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
      FieldDescriptor valueField = checkStateNotNull(mapDescriptor.findFieldByName(VALUE_FIELD_NAME));
      FieldType valueType = build(valueField);
      return FieldType.map(keyType, valueType);
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
      Schema schema = checkStateNotNull(SCHEMA_CACHE.get(messageDescriptor.getFullName()), "nested type not cached: %s", messageDescriptor.getFullName());
      FieldType type = FieldType.row(schema);
      if (fieldDescriptor.isRepeated()) {
        type = FieldType.array(type);
      }
      return type;
    }
  }
}
