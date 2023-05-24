package org.apache.beam.testinfra.pipelines.schemas;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Timestamp;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.testinfra.pipelines.schemas.DescriptorSchemaRegistry.KEY_FIELD_NAME;
import static org.apache.beam.testinfra.pipelines.schemas.DescriptorSchemaRegistry.VALUE_FIELD_NAME;

class GeneratedMessageV3RowBuilder<T extends GeneratedMessageV3> {
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final ListeningExecutorService service = MoreExecutors.listeningDecorator(threadPool);

    private final List<ListenableFuture<Pair<String, Row>>> futureMessageCallbacks = new ArrayList<>();
    private static final Map<@NonNull String, Function<Object, Object>> FULL_NAME_TYPE_MAP = ImmutableMap
            .<String, Function<Object, Object>>builder()
            .put("google.protobuf.Value", Object::toString)
            .put("google.dataflow.v1beta3.DisplayData", Object::toString)
            .put("google.protobuf.Timestamp", o -> Instant.ofEpochSecond(((Timestamp)o).getSeconds()))
            .build();
    private final @NonNull DescriptorSchemaRegistry schemaRegistry;
    private final @NonNull T source;

    private Row.@NonNull FieldValueBuilder builder;
    GeneratedMessageV3RowBuilder(DescriptorSchemaRegistry schemaRegistry, T source) {
        this.schemaRegistry = schemaRegistry;
        this.source = source;
        Schema schema = checkStateNotNull(schemaRegistry.getSchema(source.getDescriptorForType()));
        builder = Row.withSchema(schema).withFieldValues(ImmutableMap.of());
    }

    Row build() {
        for (FieldDescriptor fieldDescriptor : source.getDescriptorForType().getFields()) {
            if (fieldDescriptor.getJavaType().equals(JavaType.BYTE_STRING)) {
                continue;
            }

            if (fieldDescriptor.isMapField()) {
                visitMapField(fieldDescriptor);
                continue;
            }

            if (fieldDescriptor.isRepeated()) {
                visitRepeatedField(fieldDescriptor);
                continue;
            }

            if (fieldDescriptor.getJavaType().equals(JavaType.MESSAGE)) {
                visitMessageField(fieldDescriptor);
            }

            Object value = getPrimitiveValue(fieldDescriptor);
            builder.withFieldValue(fieldDescriptor.getName(), value);
        }

        try {
            for (Pair<String, Row> pair : Futures.allAsList(futureMessageCallbacks).get()) {
                String name = checkStateNotNull(pair.getKey());
                Row value = checkStateNotNull(pair.getValue());
                builder.withFieldValue(name, value);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }

        return builder.build();
    }

    Object getPrimitiveValue(FieldDescriptor fieldDescriptor) {
        Object value = source.getField(fieldDescriptor);
        return convert(fieldDescriptor, value);
    }

    Object getPrimitiveValue(FieldDescriptor fieldDescriptor, int i) {
        Object value = source.getRepeatedField(fieldDescriptor, i);
        return convert(fieldDescriptor, value);
    }

    Object convert(FieldDescriptor fieldDescriptor, Object value) {
        if (fieldDescriptor.getJavaType().equals(JavaType.ENUM)) {
            return value.toString();
        }
        if (fieldDescriptor.getJavaType().equals(JavaType.MESSAGE)) {
            Descriptor descriptor = checkStateNotNull(fieldDescriptor.getMessageType());
            if (FULL_NAME_TYPE_MAP.containsKey(descriptor.getFullName())) {
                Function<Object, Object> converter = checkStateNotNull(FULL_NAME_TYPE_MAP.get(descriptor.getFullName()));
                value = converter.apply(value);
            }
        }
        return value;
    }

    void visitMessageField(FieldDescriptor fieldDescriptor) {
        Object value = source.getField(fieldDescriptor);
        Descriptor descriptor = fieldDescriptor.getMessageType();
        if (FULL_NAME_TYPE_MAP.containsKey(descriptor.getFullName())) {
            value = checkStateNotNull(FULL_NAME_TYPE_MAP.get(descriptor.getFullName())).apply(value);
            builder.withFieldValue(fieldDescriptor.getName(), value);
            return;
        }
        GeneratedMessageV3 message = (GeneratedMessageV3) value;
        futureMessageCallbacks.add(futureOf(fieldDescriptor, message));
    }


    void visitMapField(FieldDescriptor fieldDescriptor) {
        Descriptor mapDescriptor = fieldDescriptor.getMessageType();
        FieldDescriptor keyType = checkStateNotNull(mapDescriptor.findFieldByName(KEY_FIELD_NAME));
        FieldDescriptor valueType = checkStateNotNull(mapDescriptor.findFieldByName(VALUE_FIELD_NAME));
        if (!valueType.getJavaType().equals(JavaType.MESSAGE)) {
            visitPrimitiveMapField(fieldDescriptor, keyType, valueType);
            return;
        }
        if (FULL_NAME_TYPE_MAP.containsKey(valueType.getMessageType().getFullName())) {
            visitPrimitiveMapField(fieldDescriptor, keyType, valueType);
            return;
        }
        visitMessageMapField(fieldDescriptor, keyType, valueType);
    }

    void visitMessageMapField(
            FieldDescriptor fieldDescriptor,
            FieldDescriptor keyType,
            FieldDescriptor valueType
    ) {
        List<ListenableFuture<Pair<String, Row>>> callbacks = new ArrayList<>();
        int size = source.getRepeatedFieldCount(fieldDescriptor);
        for (int i = 0; i < size; i++) {
            Object entryObj = source.getRepeatedField(fieldDescriptor, i);
            GeneratedMessageV3 entry = (GeneratedMessageV3) entryObj;
            Object keyObj = entry.getField(keyType);
            String key = keyObj.toString();
            Object valueObj = entry.getField(valueType);
            GeneratedMessageV3 value = (GeneratedMessageV3) valueObj;
            callbacks.add(futureOf(key, value));
        }
        Map<String, Row> result = new HashMap<>();
        try {
            for (Pair<String, Row> pair : Futures.allAsList(callbacks).get()) {
                String key = checkStateNotNull(pair.getKey());
                Row value = checkStateNotNull(pair.getValue());
                result.put(key, value);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
        builder.withFieldValue(fieldDescriptor.getName(), result);
    }

    void visitPrimitiveMapField(
            FieldDescriptor fieldDescriptor,
            FieldDescriptor keyType,
            FieldDescriptor valueType
    ) {
        Map<String, Object> result = new HashMap<>();
        int size = source.getRepeatedFieldCount(fieldDescriptor);
        for (int i = 0; i < size; i++) {
            Object entryObj = source.getRepeatedField(fieldDescriptor, i);
            GeneratedMessageV3 entry = (GeneratedMessageV3) entryObj;
            Object keyObj = entry.getField(keyType);
            String key = keyObj.toString();
            Object valueObj = entry.getField(valueType);
            if (valueType.getJavaType().equals(JavaType.ENUM)) {
                valueObj = valueObj.toString();
            }
            if (valueType.getJavaType().equals(JavaType.MESSAGE)) {
                Descriptor descriptor = checkStateNotNull(valueType.getMessageType());
                Function<Object, Object> converter = checkStateNotNull(FULL_NAME_TYPE_MAP.get(descriptor.getFullName()));
                valueObj = converter.apply(valueObj);
            }
            result.put(key, valueObj);
        }
        builder.withFieldValue(fieldDescriptor.getName(), result);
    }

    void visitRepeatedField(FieldDescriptor fieldDescriptor) {
        if (!fieldDescriptor.getJavaType().equals(JavaType.MESSAGE)) {
            visitRepeatedPrimitiveField(fieldDescriptor);
            return;
        }
        if (FULL_NAME_TYPE_MAP.containsKey(fieldDescriptor.getMessageType().getFullName())) {
            visitRepeatedPrimitiveField(fieldDescriptor);
            return;
        }
        visitRepeatedMessageField(fieldDescriptor);
    }

    void visitRepeatedPrimitiveField(FieldDescriptor fieldDescriptor) {
        List<Object> result = new ArrayList<>();
        int size = source.getRepeatedFieldCount(fieldDescriptor);
        for (int i = 0; i < size; i++) {
            Object value = getPrimitiveValue(fieldDescriptor, i);
            result.add(value);
        }
        builder.withFieldValue(fieldDescriptor.getName(), result);
    }

    void visitRepeatedMessageField(FieldDescriptor fieldDescriptor) {
        List<ListenableFuture<Row>> futures = new ArrayList<>();
        int size = source.getRepeatedFieldCount(fieldDescriptor);
        for (int i = 0; i < size; i++) {
            Object valueObj = source.getRepeatedField(fieldDescriptor, i);
            GeneratedMessageV3 message = (GeneratedMessageV3) valueObj;
            GeneratedMessageV3RowBuilder<GeneratedMessageV3> rowBuilder = new GeneratedMessageV3RowBuilder<>(
                    schemaRegistry,
                    message
            );
            futures.add(service.submit(rowBuilder::build));
        }
        List<Row> result;
        try {
            result = new ArrayList<>(Futures.allAsList(futures).get());
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
        builder.withFieldValue(fieldDescriptor.getName(), result);
    }

    ListenableFuture<Pair<String, Row>> futureOf(String name, GeneratedMessageV3 message) {
        GeneratedMessageV3RowBuilder<GeneratedMessageV3> rowBuilder = new GeneratedMessageV3RowBuilder<>(
                schemaRegistry,
                message
        );
        return service.submit(()->{
            Row row = rowBuilder.build();
            return Pair.of(name, row);
        });
    }

    ListenableFuture<Pair<String, Row>> futureOf(FieldDescriptor fieldDescriptor, GeneratedMessageV3 message) {
        return futureOf(fieldDescriptor.getName(), message);
    }

}
