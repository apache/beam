package org.apache.beam.testinfra.pipelines.schemas;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.ProtocolMessageEnum;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.NonNull;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

class GeneratedMessageV3RowBuilder<T extends GeneratedMessageV3> {
    private static final Map<JavaType, Class<?>> JAVA_TYPE_CLASS_MAP =
        ImmutableMap.<JavaType, Class<?>>builder()
            .put(JavaType.BOOLEAN, Boolean.class)
            .put(JavaType.INT, Integer.class)
            .put(JavaType.LONG, Long.class)
            .put(JavaType.FLOAT, Float.class)
            .put(JavaType.DOUBLE, Double.class)
            .put(JavaType.STRING, String.class)
            .put(JavaType.MESSAGE, GeneratedMessageV3.class)
            .put(JavaType.ENUM, ProtocolMessageEnum.class)
            .build();
    private final Row.Builder builder;
    private final Schema schema;
    private final Map<String, Object> target = new HashMap<>();
    private final T source;
    private final Descriptor descriptor;

    GeneratedMessageV3RowBuilder(Row.Builder builder, T source) {
        this.schema = builder.getSchema();
        this.builder = builder;
        this.source = source;
        this.descriptor = source.getDescriptorForType();
    }

    Row build() {
        parse();
        return builder.withFieldValues(target).build();
    }

    private void parse() {
        DescriptorReflection reflection = new DescriptorReflection(descriptor);
        for (FieldDescriptor field : reflection.getFields()) {
            parse(field);
        }
    }

    private void parse(FieldDescriptor field) {
        if (!JAVA_TYPE_CLASS_MAP.containsKey(field.getJavaType())) {
            return;
        }
        Object value = getValue(field);
        target.put(field.getName(), value);
    }

    @NonNull
    Object getValue(FieldDescriptor field) {
        if (field.isRepeated()) {
            return getCollection(field);
        }
        if (field.isMapField()) {
            return getMap(field);
        }
        Class<?> valueTClass = checkStateNotNull(JAVA_TYPE_CLASS_MAP.get(field.getJavaType()));
        Object value = getValue(field, valueTClass);
        if (field.getJavaType().equals(JavaType.MESSAGE)) {
            GeneratedMessageV3 nested = (GeneratedMessageV3) value;
            return convertToRow(field, nested);
        }
        if (field.getJavaType().equals(JavaType.ENUM)) {
            ProtocolMessageEnum enumMessage = (ProtocolMessageEnum) value;
            EnumDescriptor enumDescriptor = enumMessage.getDescriptorForType();
            EnumValueDescriptor enumValueDescriptor = enumMessage.getValueDescriptor();
            return enumDescriptor.findValueByNumber(enumValueDescriptor.getNumber()).getName();
        }
        return value;
    }

    Map<String, String> getMap(FieldDescriptor field) {
        return ImmutableMap.of();
    }

    Object getCollection(FieldDescriptor field) {
        Class<?> valueTClass = JAVA_TYPE_CLASS_MAP.get(field.getJavaType());
        List<?> collection = getCollection(field, valueTClass);
        if (!field.getJavaType().equals(JavaType.MESSAGE)) {
            return collection;
        }
        List<Row> result = new ArrayList<>();
        for (Object value : collection) {
            GeneratedMessageV3 message = (GeneratedMessageV3) value;
            Row row = convertToRow(field, message);
            result.add(row);
        }
        return result;
    }

    <ValueT> List<ValueT> getCollection(FieldDescriptor field, Class<ValueT> valueTClass) {
        List<ValueT> result = new ArrayList<>();
        int size = source.getRepeatedFieldCount(field);
        for (int i = 0; i < size; i++) {
            ValueT value = getValue(field, valueTClass, i);
            result.add(value);
        }
        return result;
    }

    <ValueT> ValueT getValue(FieldDescriptor field, Class<ValueT> valueTClass) {
        checkArgument(!field.isRepeated());
        Object value = source.getField(field);
        checkState(valueTClass.isInstance(value));
        return valueTClass.cast(value);
    }

    <ValueT> ValueT getValue(FieldDescriptor field, Class<ValueT> valueTClass, int index) {
        checkArgument(field.isRepeated());
        checkState(index < source.getRepeatedFieldCount(field));
        Object value = source.getRepeatedField(field, index);
        checkState(valueTClass.isInstance(value));
        return valueTClass.cast(value);
    }

    <ValueT extends GeneratedMessageV3> Row convertToRow(FieldDescriptor field, ValueT value) {
        checkArgument(field.getJavaType().equals(JavaType.MESSAGE));
        checkState(schema.hasField(field.getName()));
        Schema.Field rowField = schema.getField(field.getName());
        Schema rowSchema = checkStateNotNull(rowField.getType().getRowSchema());
        Row.Builder rowBuilder = Row.withSchema(rowSchema);
        GeneratedMessageV3RowBuilder<ValueT> v3RowBuilder = new GeneratedMessageV3RowBuilder<>(rowBuilder, value);
        return v3RowBuilder.build();
    }
}
