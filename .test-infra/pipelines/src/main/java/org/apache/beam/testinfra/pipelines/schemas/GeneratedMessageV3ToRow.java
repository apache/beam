package org.apache.beam.testinfra.pipelines.schemas;

import com.google.dataflow.v1beta3.Job;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Triple;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.testinfra.pipelines.schemas.DescriptorSchemaRegistry.KEY_FIELD_NAME;
import static org.apache.beam.testinfra.pipelines.schemas.DescriptorSchemaRegistry.VALUE_FIELD_NAME;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

@SuppressWarnings({"unused"})
class GeneratedMessageV3ToRow<T extends GeneratedMessageV3> implements SerializableFunction<T, Row> {

    private static final Map<JavaType, Class<?>> JAVA_TYPE_CLASS_MAP = ImmutableMap
            .<JavaType, Class<?>>builder()
            .put(JavaType.BOOLEAN, Boolean.class)
            .put(JavaType.INT, Integer.class)
            .put(JavaType.LONG, Long.class)
            .put(JavaType.FLOAT, Float.class)
            .put(JavaType.DOUBLE, Double.class)
            .put(JavaType.ENUM, String.class)
            .put(JavaType.STRING, String.class)
            .build();

    private final @NonNull DescriptorSchemaRegistry schemaRegistry;

    GeneratedMessageV3ToRow(@NonNull DescriptorSchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public Row apply(T input) {
        Builder<T> builder = new Builder<>(schemaRegistry, input);
        return builder.build();
    }

    static class Builder<T extends GeneratedMessageV3> {

        private final List<Builder<T>> queue = new ArrayList<>();
        private final @NonNull T source;
        private final Row.FieldValueBuilder builder;
        private final DescriptorSchemaRegistry schemaRegistry;
        Builder(DescriptorSchemaRegistry schemaRegistry, @NonNull T source) {
            this.source = source;
            this.schemaRegistry = schemaRegistry;
            Schema schema = checkStateNotNull(schemaRegistry.getSchema(source.getDescriptorForType()));
            this.builder = Row.withSchema(schema).withFieldValues(ImmutableMap.of());
        }

        Row build() {
            queue.add(this);
            parse();
            return builder.build();
        }

        void parse() {
        }

        Object getValue(FieldDescriptor fieldDescriptor) {
            if (fieldDescriptor.isRepeated()) {
                return listOf(fieldDescriptor);
            }
            if (fieldDescriptor.isMapField()) {
                return mapOf(fieldDescriptor);
            }
            if (fieldDescriptor.getJavaType().equals(JavaType.MESSAGE)) {
                return rowOf(fieldDescriptor);
            }
            return getPrimitiveValue(fieldDescriptor);
        }

        List<Object> listOf(FieldDescriptor fieldDescriptor) {
            if (fieldDescriptor.getJavaType().equals(JavaType.MESSAGE)) {
                return rowListOf(fieldDescriptor);
            }
            List<Object> result = new ArrayList<>();
            int size = source.getRepeatedFieldCount(fieldDescriptor);
            for (int i = 0; i < size; i++) {
                result.add(source.getRepeatedField(fieldDescriptor, i));
            }
            return result;
        }

        Map<String, Object> mapOf(FieldDescriptor fieldDescriptor) {
            Descriptor mapDescriptor = fieldDescriptor.getMessageType();
            FieldDescriptor keyField = checkStateNotNull(mapDescriptor.findFieldByName(KEY_FIELD_NAME));
            FieldDescriptor valueField = checkStateNotNull(mapDescriptor.findFieldByName(VALUE_FIELD_NAME));
            Map<String, Object> result = new HashMap<>();
            int size = source.getRepeatedFieldCount(fieldDescriptor);
            for (int i = 0; i < size; i++) {
                Object entryObj = source.getRepeatedField(fieldDescriptor, i);
                checkState(entryObj instanceof GeneratedMessageV3);
                GeneratedMessageV3 entry = (GeneratedMessageV3) entryObj;
                Object keyObj = entry.getField(keyField);
                checkState(keyObj instanceof String);
                String key = (String) keyObj;
                Object value = mapEntryValueOf(valueField);
                result.put(key, value);
            }
            return result;
        }

        Object mapEntryValueOf(GeneratedMessageV3 source, FieldDescriptor fieldDescriptor) {
            if (fieldDescriptor.getJavaType().equals(JavaType.MESSAGE)) {
                return rowOf(fieldDescriptor);
            }

        }

        List<Object> rowListOf(FieldDescriptor fieldDescriptor) {
            List<Object> result = new ArrayList<>();
            return result;
        }

        Object rowOf(FieldDescriptor fieldDescriptor) {
            return null;
        }

        Object getPrimitiveValue(FieldDescriptor fieldDescriptor) {
            Class<?> clazz = checkStateNotNull(JAVA_TYPE_CLASS_MAP.get(fieldDescriptor.getJavaType()));
            Object value = source.getField(fieldDescriptor);
            checkState(clazz.isInstance(value));
            return value;
        }
    }
}
