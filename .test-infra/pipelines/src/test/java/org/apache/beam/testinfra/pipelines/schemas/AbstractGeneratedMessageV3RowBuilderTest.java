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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.dataflow.v1beta3.DisplayData;
import com.google.dataflow.v1beta3.ExecutionStageSummary.ComponentSource;
import com.google.dataflow.v1beta3.ExecutionStageSummary.ComponentTransform;
import com.google.dataflow.v1beta3.ExecutionStageSummary.StageSource;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Duration;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.ListValue;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.ReadableDateTime;
import org.junit.jupiter.api.Test;

/** Base class for {@link GeneratedMessageV3RowBuilder} based tests of various types. */
abstract class AbstractGeneratedMessageV3RowBuilderTest<T extends GeneratedMessageV3> {
  private static final Map<@NonNull String, GeneratedMessageV3> DEFAULT_INSTANCE_MAP =
      ImmutableMap.<@NonNull String, GeneratedMessageV3>builder()
          .put(StageSource.getDescriptor().getFullName(), StageSource.getDefaultInstance())
          .put(
              ComponentTransform.getDescriptor().getFullName(),
              ComponentTransform.getDefaultInstance())
          .put(ComponentSource.getDescriptor().getFullName(), ComponentSource.getDefaultInstance())
          .build();

  protected abstract @NonNull Descriptor getDescriptorForType();

  protected abstract @NonNull T getDefaultInstance();

  protected abstract @NonNull Class<T> getDefaultInstanceClass();

  protected abstract @NonNull Set<@NonNull String> getStringFields();

  protected abstract @NonNull Set<@NonNull String> getStringArrayFields();

  protected abstract @NonNull Set<@NonNull String> getBooleanFields();

  protected abstract @NonNull Set<@NonNull String> getStructFields();

  protected abstract @NonNull Set<@NonNull String> getEnumFields();

  protected abstract @NonNull Set<@NonNull String> getDisplayDataFields();

  protected abstract @NonNull Set<@NonNull String> getRepeatedMessageFields();

  @Test
  void defaultInstance() {
    GeneratedMessageV3RowBuilder<T> builder = builderOf(getDefaultInstance());
    Row row = builder.build();
    assertNotNull(row);
    assertEquals(
        getDescriptorForType().getFields().stream()
            .map(FieldDescriptor::getName)
            .collect(Collectors.toSet()),
        new HashSet<>(row.getSchema().getFieldNames()));
  }

  @Test
  void strings() {
    Descriptor descriptor = getDescriptorForType();
    for (String fieldName : getStringFields()) {
      FieldDescriptor field = descriptor.findFieldByName(fieldName);
      assertNotNull(field);
      String value = fieldName + "_value";
      Message message = getDefaultInstance().toBuilder().setField(field, value).build();
      T source = cast(message);
      Row row = builderOf(source).build();
      assertRowValueEquals(row, fieldName, value);
    }
  }

  @Test
  void booleans() {
    Descriptor descriptor = getDescriptorForType();
    for (String fieldName : getBooleanFields()) {
      FieldDescriptor field = descriptor.findFieldByName(fieldName);
      assertNotNull(field);
      Boolean value = true;
      Message message = getDefaultInstance().toBuilder().setField(field, value).build();
      T source = cast(message);
      Row row = builderOf(source).build();
      assertRowValueEquals(row, fieldName, value);
    }
  }

  @Test
  void stringArrays() {
    Descriptor descriptor = getDescriptorForType();
    for (String fieldName : getStringArrayFields()) {
      FieldDescriptor field = descriptor.findFieldByName(fieldName);
      assertNotNull(field);

      for (int n = 0; n < 3; n++) {
        Message.Builder messageBuilder = getDefaultInstance().toBuilder();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < n; i++) {
          String value = String.format("%s_%s_value", fieldName, i);
          expected.add(value);
          messageBuilder.addRepeatedField(field, value);
        }
        Message message = messageBuilder.build();
        T source = cast(message);
        Row row = builderOf(source).build();
        assertRowRepeatedValueEquals(row, fieldName, expected);
      }
    }
  }

  @Test
  void structs() {
    Descriptor descriptor = getDescriptorForType();
    for (String fieldName : getStructFields()) {
      Row defaultRow = builderOf(getDefaultInstance()).build();
      Row defaultStruct = defaultRow.getRow(fieldName);
      assertNotNull(defaultStruct);
      Collection<Row> defaultFields = defaultStruct.getArray("fields");
      assertNotNull(defaultFields);
      assertTrue(defaultFields.isEmpty());

      FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
      assertNotNull(fieldDescriptor);
      Message message =
          getDefaultInstance().toBuilder().setField(fieldDescriptor, structOfAllTypes()).build();
      T source = cast(message);
      Row row = builderOf(source).build();
      assertNotNull(row);
      Row struct = row.getRow(fieldName);
      assertNotNull(struct);
      Collection<Row> fields = struct.getArray("fields");
      assertNotNull(fields);
      Map<@NonNull String, @NonNull Object> expectedFields =
          ImmutableMap.of(
              "struct",
              "{\"bool\":true,\"string\":\"string_value\",\"number\":1.234567,\"list\":[true,\"string_value\",1.234567]}");
      Map<@NonNull String, @NonNull Object> actualFields = new HashMap<>();
      for (Row field : fields) {
        String key = field.getString("key");
        assertNotNull(key);
        Object value = field.getValue("value");
        assertNotNull(value);
        actualFields.put(key, value);
      }
      assertEquals(expectedFields, actualFields);
    }
  }

  @Test
  void enums() {
    Descriptor descriptor = getDescriptorForType();
    for (String fieldName : getEnumFields()) {
      Row defaultRow = builderOf(getDefaultInstance()).build();
      assertNotNull(defaultRow);
      FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
      assertNotNull(fieldDescriptor);
      assertEquals(JavaType.ENUM, fieldDescriptor.getJavaType());
      EnumDescriptor enumDescriptor = fieldDescriptor.getEnumType();
      String defaultEnum = defaultRow.getString(fieldName);
      assertEquals(enumDescriptor.findValueByNumber(0).getName(), defaultEnum);

      Message message =
          getDefaultInstance()
              .toBuilder()
              .setField(fieldDescriptor, enumDescriptor.findValueByNumber(1))
              .build();
      T source = cast(message);
      Row row = builderOf(source).build();
      assertNotNull(row);
      assertEquals(enumDescriptor.findValueByNumber(1).getName(), row.getString(fieldName));
    }
  }

  @Test
  void displayData() {
    Descriptor descriptor = getDescriptorForType();
    for (String fieldName : getDisplayDataFields()) {
      FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
      assertNotNull(fieldDescriptor);

      T defaultInstance = getDefaultInstance();
      Row defaultRow = builderOf(defaultInstance).build();
      assertNotNull(defaultRow);
      Collection<Row> defaultCollection = defaultRow.getArray(fieldName);
      assertNotNull(defaultCollection);
      assertTrue(defaultCollection.isEmpty());

      Map<String, DisplayData> expected = new HashMap<>();
      for (DisplayData data : displayDataOf()) {
        expected.put(data.getKey(), data);
      }

      Message message =
          getDefaultInstance().toBuilder().setField(fieldDescriptor, displayDataOf()).build();

      T instance = cast(message);

      Row row = builderOf(instance).build();
      assertNotNull(row);
      Collection<Row> displayData = row.getArray(fieldName);
      assertNotNull(displayData);
      assertEquals(expected.size(), displayData.size());
      for (Row actual : displayData) {
        String key = actual.getString("key");
        assertNotNull(key);
        DisplayData expectedData = expected.get(key);
        assertNotNull(expectedData);
        assertEquals(expectedData.getNamespace(), actual.getString("namespace"));
        assertEquals(expectedData.getShortStrValue(), actual.getString("short_str_value"));
        assertEquals(expectedData.getUrl(), actual.getString("url"));
        assertEquals(expectedData.getLabel(), actual.getString("label"));
        assertEquals(expectedData.getBoolValue(), actual.getBoolean("bool_value"));
        assertEquals(expectedData.getInt64Value(), actual.getInt64("int64_value"));
        assertEquals(expectedData.getFloatValue(), actual.getFloat("float_value"));
        assertEquals(expectedData.getJavaClassValue(), actual.getString("java_class_value"));
        if (expectedData.hasTimestampValue()) {
          Timestamp expectedTimestamp = expectedData.getTimestampValue();
          ReadableDateTime actualTimestamp = actual.getDateTime("timestamp_value");
          assertNotNull(actualTimestamp);
          assertEquals(expectedTimestamp.getSeconds() * 1000, actualTimestamp.getMillis());
        }
      }
    }
  }

  @Test
  void repeatedMessages() {
    Descriptor descriptor = getDescriptorForType();
    for (String fieldName : getRepeatedMessageFields()) {
      FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
      assertNotNull(fieldDescriptor);

      Row defaultRow = builderOf(getDefaultInstance()).build();
      assertNotNull(defaultRow);
      Collection<Row> defaultcollection = defaultRow.getArray(fieldName);
      assertNotNull(defaultcollection);

      GeneratedMessageV3 member =
          DEFAULT_INSTANCE_MAP.get(fieldDescriptor.getMessageType().getFullName());
      assertNotNull(member);

      Message message =
          getDefaultInstance().toBuilder().addRepeatedField(fieldDescriptor, member).build();

      T instance = cast(message);
      Row row = builderOf(instance).build();
      assertNotNull(row);
      Collection<Row> collection = row.getArray(fieldName);
      assertNotNull(collection);
      assertEquals(1, collection.size());
      Set<String> actualFieldNames =
          new HashSet<>(new ArrayList<>(collection).get(0).getSchema().getFieldNames());
      Set<String> expectedFieldNames =
          fieldDescriptor.getMessageType().getFields().stream()
              .map(FieldDescriptor::getName)
              .collect(Collectors.toSet());
      assertEquals(expectedFieldNames, actualFieldNames);
    }
  }

  private static <ValueT> void assertRowValueEquals(Row source, String fieldName, ValueT expected) {
    Object value = source.getValue(fieldName);
    assertEquals(value, expected);
  }

  private static <ValueT> void assertRowRepeatedValueEquals(
      Row source, String fieldName, List<ValueT> expected) {
    Collection<ValueT> value = source.getArray(fieldName);
    assertEquals(value, expected);
  }

  protected GeneratedMessageV3RowBuilder<T> builderOf(@NonNull T instance) {
    if (DescriptorSchemaRegistry.INSTANCE.hasNoCachedBuild(getDescriptorForType())) {
      DescriptorSchemaRegistry.INSTANCE.build(getDescriptorForType());
    }
    return new GeneratedMessageV3RowBuilder<>(instance);
  }

  static Struct structOfAllTypes() {
    Value boolV = Value.newBuilder().setBoolValue(true).build();
    Value stringV = Value.newBuilder().setStringValue("string_value").build();
    Value numberV = Value.newBuilder().setNumberValue(1.234567).build();
    Value listV =
        Value.newBuilder()
            .setListValue(
                ListValue.getDefaultInstance()
                    .toBuilder()
                    .addValues(boolV)
                    .addValues(stringV)
                    .addValues(numberV)
                    .build())
            .build();
    Struct base =
        structOf(
            Pair.of("bool", boolV),
            Pair.of("string", stringV),
            Pair.of("number", numberV),
            Pair.of("list", listV));

    Value baseStruct = Value.newBuilder().setStructValue(base).build();

    return structOf(Pair.of("struct", baseStruct));
  }

  static Struct structOf(Pair<String, Value>... pairs) {
    Struct.Builder builder = Struct.newBuilder();
    for (Pair<String, Value> pair : pairs) {
      builder.putFields(pair.getKey(), pair.getValue());
    }
    return builder.build();
  }

  static List<DisplayData> displayDataOf() {
    return ImmutableList.of(
        withBoolean(true),
        withString("display_string"),
        withInt64(1000L),
        withFloat(2f),
        withJavaClass("com.example.JavaBean"),
        withTimestamp(Timestamp.getDefaultInstance().toBuilder().setSeconds(1000L).build()),
        withDuration(Duration.getDefaultInstance().toBuilder().setSeconds(10L).build()));
  }

  static DisplayData emptyDisplayData() {
    return DisplayData.getDefaultInstance();
  }

  static DisplayData withoutValue() {
    return DisplayData.getDefaultInstance()
        .toBuilder()
        .setKey("empty")
        .setNamespace("namespace_")
        .setUrl("https://example.com")
        .setLabel("label_")
        .setShortStrValue("short_str_value_")
        .build();
  }

  static DisplayData withBoolean(boolean value) {
    return withoutValue().toBuilder().setKey("boolean").setBoolValue(value).build();
  }

  static DisplayData withString(String value) {
    return withoutValue().toBuilder().setKey("string").setStrValue(value).build();
  }

  static DisplayData withInt64(long value) {
    return withoutValue().toBuilder().setKey("long").setInt64Value(value).build();
  }

  static DisplayData withFloat(float value) {
    return withoutValue().toBuilder().setKey("float").setFloatValue(value).build();
  }

  static DisplayData withJavaClass(String value) {
    return withoutValue().toBuilder().setKey("javaClass").setJavaClassValue(value).build();
  }

  static DisplayData withTimestamp(Timestamp value) {
    return withoutValue().toBuilder().setKey("timestamp").setTimestampValue(value).build();
  }

  static DisplayData withDuration(Duration value) {
    return withoutValue().toBuilder().setKey("duration").setDurationValue(value).build();
  }

  private T cast(Message message) {
    assertTrue(getDefaultInstanceClass().isInstance(message));
    return getDefaultInstanceClass().cast(message);
  }
}
