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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Functions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
/** Unit tests form {@link AvroGenericRecordToStorageApiProto}. */
public class AvroGenericRecordToStorageApiProtoTest {

  enum TestEnum {
    ONE,
    TWO,
    RED,
    BLUE
  }

  private static final String[] TEST_ENUM_STRS =
      EnumSet.allOf(TestEnum.class).stream().map(TestEnum::name).toArray(String[]::new);

  private static final Schema BASE_SCHEMA =
      SchemaBuilder.record("TestRecord")
          .fields()
          .optionalBytes("bytesValue")
          .requiredInt("intValue")
          .optionalLong("longValue")
          .optionalFloat("floatValue")
          .optionalDouble("doubleValue")
          .optionalString("stringValue")
          .optionalBoolean("booleanValue")
          .name("arrayValue")
          .type()
          .array()
          .items()
          .stringType()
          .noDefault()
          .name("enumValue")
          .type()
          .optional()
          .enumeration("testEnum")
          .symbols(TEST_ENUM_STRS)
          .name("fixedValue")
          .type()
          .fixed("MD5")
          .size(16)
          .noDefault()
          .endRecord();

  private static final Schema LOGICAL_TYPES_SCHEMA =
      SchemaBuilder.record("LogicalTypesRecord")
          .fields()
          .name("decimalValue")
          .type(LogicalTypes.decimal(1, 1).addToSchema(Schema.create(Schema.Type.BYTES)))
          .noDefault()
          .name("dateValue")
          .type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))
          .noDefault()
          .name("timestampMicrosValue")
          .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name("timestampMillisValue")
          .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name("timestampMicrosAsInstantValue")
          .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name("timestampMillisAsInstantValue")
          .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name("uuidValue")
          .type(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)))
          .noDefault()
          .endRecord();

  private static final DescriptorProto BASE_SCHEMA_PROTO =
      DescriptorProto.newBuilder()
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("bytesvalue")
                  .setNumber(1)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("intvalue")
                  .setNumber(2)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("longvalue")
                  .setNumber(3)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("floatvalue")
                  .setNumber(4)
                  .setType(Type.TYPE_DOUBLE)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("doublevalue")
                  .setNumber(5)
                  .setType(Type.TYPE_DOUBLE)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("stringvalue")
                  .setNumber(6)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("booleanvalue")
                  .setNumber(7)
                  .setType(Type.TYPE_BOOL)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("arrayvalue")
                  .setNumber(8)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_REPEATED)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("enumvalue")
                  .setNumber(9)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("fixedvalue")
                  .setNumber(10)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_REQUIRED)
                  .build())
          .build();

  private static final DescriptorProto LOGICAL_TYPES_SCHEMA_PROTO =
      DescriptorProto.newBuilder()
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("decimalvalue")
                  .setNumber(1)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("datevalue")
                  .setNumber(2)
                  .setType(Type.TYPE_INT32)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampmicrosvalue")
                  .setNumber(3)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampmillisvalue")
                  .setNumber(4)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampmicrosasinstantvalue")
                  .setNumber(5)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampmillisasinstantvalue")
                  .setNumber(6)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("uuidvalue")
                  .setNumber(7)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .build();

  private static final byte[] BYTES = "BYTE BYTE BYTE".getBytes(StandardCharsets.UTF_8);

  private static final Schema NESTED_SCHEMA =
      SchemaBuilder.record("TestNestedRecord")
          .fields()
          .name("nested")
          .type()
          .optional()
          .type(BASE_SCHEMA)
          .name("nestedArray")
          .type()
          .array()
          .items(BASE_SCHEMA)
          .noDefault()
          .endRecord();

  private static final Schema SCHEMA_WITH_MAP =
      SchemaBuilder.record("TestMap")
          .fields()
          .name("nested")
          .type()
          .optional()
          .type(BASE_SCHEMA)
          .name("aMap")
          .type()
          .map()
          .values()
          .stringType()
          .mapDefault(ImmutableMap.<String, Object>builder().put("key1", "value1").build())
          .endRecord();

  private static final Schema SCHEMA_WITH_NULLABLE_ARRAY =
      SchemaBuilder.record("TestNullableArray")
          .fields()
          .name("aNullableArray")
          .type()
          .nullable()
          .array()
          .items()
          .stringType()
          .noDefault()
          .endRecord();

  private static GenericRecord baseRecord;
  private static GenericRecord logicalTypesRecord;
  private static Map<String, Object> baseProtoExpectedFields;
  private static Map<String, Object> logicalTypesProtoExpectedFields;
  private static GenericRecord nestedRecord;

  static {
    try {
      byte[] md5 = MessageDigest.getInstance("MD5").digest(BYTES);
      Instant now = Instant.now();
      baseRecord =
          new GenericRecordBuilder(BASE_SCHEMA)
              .set("bytesValue", BYTES)
              .set("intValue", (int) 3)
              .set("longValue", (long) 4)
              .set("floatValue", (float) 3.14)
              .set("doubleValue", (double) 2.68)
              .set("stringValue", "I am a string. Hear me roar.")
              .set("booleanValue", true)
              .set("arrayValue", ImmutableList.of("one", "two", "red", "blue"))
              .set(
                  "enumValue",
                  new GenericData.EnumSymbol(
                      BASE_SCHEMA.getField("enumValue").schema(), TestEnum.TWO))
              .set(
                  "fixedValue",
                  new GenericData.Fixed(BASE_SCHEMA.getField("fixedValue").schema(), md5))
              .build();
      BigDecimal bd = BigDecimal.valueOf(1.1D);
      UUID uuid = UUID.randomUUID();
      logicalTypesRecord =
          new GenericRecordBuilder(LOGICAL_TYPES_SCHEMA)
              .set(
                  "decimalValue",
                  new Conversions.DecimalConversion()
                      .toBytes(
                          bd,
                          Schema.create(Schema.Type.NULL),
                          LogicalTypes.decimal(bd.precision(), bd.scale())))
              .set("dateValue", now)
              .set("timestampMicrosValue", now.getMillis() * 1000)
              .set("timestampMicrosAsInstantValue", now)
              .set("timestampMillisValue", now.getMillis())
              .set("timestampMillisAsInstantValue", now)
              .set("uuidValue", uuid)
              .build();
      baseProtoExpectedFields =
          ImmutableMap.<String, Object>builder()
              .put("bytesvalue", ByteString.copyFrom(BYTES))
              .put("intvalue", (long) 3)
              .put("longvalue", (long) 4)
              .put("floatvalue", (double) 3.14)
              .put("doublevalue", (double) 2.68)
              .put("stringvalue", "I am a string. Hear me roar.")
              .put("booleanvalue", true)
              .put("arrayvalue", ImmutableList.of("one", "two", "red", "blue"))
              .put("enumvalue", TEST_ENUM_STRS[1])
              .put("fixedvalue", ByteString.copyFrom(md5))
              .build();
      logicalTypesProtoExpectedFields =
          ImmutableMap.<String, Object>builder()
              .put("decimalvalue", BeamRowToStorageApiProto.serializeBigDecimalToNumeric(bd))
              .put(
                  "datevalue",
                  Days.daysBetween(Instant.EPOCH.toDateTime(), now.toDateTime()).getDays())
              .put("timestampmicrosvalue", now.getMillis() * 1000)
              .put("timestampmicrosasinstantvalue", now.getMillis() * 1000)
              .put("timestampmillisvalue", now.getMillis())
              .put("timestampmillisasinstantvalue", now.getMillis())
              .put("uuidvalue", uuid.toString())
              .build();
      nestedRecord =
          new GenericRecordBuilder(NESTED_SCHEMA)
              .set("nested", baseRecord)
              .set("nestedArray", ImmutableList.of(baseRecord, baseRecord))
              .build();
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException("Error initializing test data", ex);
    }
  }

  void validateDescriptorAgainstSchema(Schema originalSchema, DescriptorProto schemaProto) {
    DescriptorProto descriptor =
        TableRowToStorageApiProto.descriptorSchemaFromTableSchema(
            AvroGenericRecordToStorageApiProto.protoTableSchemaFromAvroSchema(originalSchema),
            true,
            false);
    Map<String, Type> types =
        descriptor.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    Map<String, Type> expectedTypes =
        schemaProto.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedTypes, types);

    Map<String, String> nameMapping =
        originalSchema.getFields().stream()
            .collect(Collectors.toMap(f -> f.name().toLowerCase(), f -> f.name()));
    descriptor
        .getFieldList()
        .forEach(
            p -> {
              AvroUtils.TypeWithNullability fieldSchema =
                  AvroUtils.TypeWithNullability.create(
                      originalSchema.getField(nameMapping.get(p.getName())).schema());
              Label label =
                  fieldSchema.getType().getType() == Schema.Type.ARRAY
                      ? Label.LABEL_REPEATED
                      : fieldSchema.isNullable() ? Label.LABEL_OPTIONAL : Label.LABEL_REQUIRED;
              assertEquals(label, p.getLabel());
            });
  }

  @Test
  public void testDescriptorFromSchema() {
    validateDescriptorAgainstSchema(BASE_SCHEMA, BASE_SCHEMA_PROTO);
  }

  @Test
  public void testDescriptorFromSchemaLogicalTypes() {
    validateDescriptorAgainstSchema(LOGICAL_TYPES_SCHEMA, LOGICAL_TYPES_SCHEMA_PROTO);
  }

  @Test
  public void testNestedFromSchema() {
    DescriptorProto descriptor =
        TableRowToStorageApiProto.descriptorSchemaFromTableSchema(
            AvroGenericRecordToStorageApiProto.protoTableSchemaFromAvroSchema(NESTED_SCHEMA),
            true,
            false);
    Map<String, Type> expectedBaseTypes =
        BASE_SCHEMA_PROTO.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));

    Map<String, Type> types =
        descriptor.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    Map<String, String> typeNames =
        descriptor.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getTypeName));
    Map<String, Label> typeLabels =
        descriptor.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getLabel));

    assertEquals(2, types.size());

    Map<String, DescriptorProto> nestedTypes =
        descriptor.getNestedTypeList().stream()
            .collect(Collectors.toMap(DescriptorProto::getName, Functions.identity()));
    assertEquals(2, nestedTypes.size());
    assertEquals(Type.TYPE_MESSAGE, types.get("nested"));
    assertEquals(Label.LABEL_OPTIONAL, typeLabels.get("nested"));
    String nestedTypeName1 = typeNames.get("nested");
    Map<String, Type> nestedTypes1 =
        nestedTypes.get(nestedTypeName1).getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedBaseTypes, nestedTypes1);

    assertEquals(Type.TYPE_MESSAGE, types.get("nestedarray"));
    assertEquals(Label.LABEL_REPEATED, typeLabels.get("nestedarray"));
    String nestedTypeName2 = typeNames.get("nestedarray");
    Map<String, Type> nestedTypes2 =
        nestedTypes.get(nestedTypeName2).getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedBaseTypes, nestedTypes2);
  }

  private void assertBaseRecord(DynamicMessage msg, Map<String, Object> expectedFields) {
    Map<String, Object> recordFields =
        msg.getAllFields().entrySet().stream()
            .collect(
                Collectors.toMap(entry -> entry.getKey().getName(), entry -> entry.getValue()));
    assertEquals(expectedFields, recordFields);
  }

  @Test
  public void testMessageFromGenericRecord() throws Exception {
    Descriptors.Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            AvroGenericRecordToStorageApiProto.protoTableSchemaFromAvroSchema(NESTED_SCHEMA),
            true,
            false);
    DynamicMessage msg =
        AvroGenericRecordToStorageApiProto.messageFromGenericRecord(
            descriptor, nestedRecord, null, -1);

    assertEquals(2, msg.getAllFields().size());

    Map<String, Descriptors.FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(Descriptors.FieldDescriptor::getName, Functions.identity()));
    DynamicMessage nestedMsg = (DynamicMessage) msg.getField(fieldDescriptors.get("nested"));
    assertBaseRecord(nestedMsg, baseProtoExpectedFields);
  }

  @Test
  public void testCdcFields() throws Exception {
    Descriptors.Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            AvroGenericRecordToStorageApiProto.protoTableSchemaFromAvroSchema(NESTED_SCHEMA),
            true,
            true);
    assertNotNull(descriptor.findFieldByName(StorageApiCDC.CHANGE_TYPE_COLUMN));
    assertNotNull(descriptor.findFieldByName(StorageApiCDC.CHANGE_SQN_COLUMN));

    DynamicMessage msg =
        AvroGenericRecordToStorageApiProto.messageFromGenericRecord(
            descriptor, nestedRecord, "UPDATE", 42);

    assertEquals(4, msg.getAllFields().size());
    Map<String, Descriptors.FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(Descriptors.FieldDescriptor::getName, Functions.identity()));
    assertEquals("UPDATE", msg.getField(fieldDescriptors.get(StorageApiCDC.CHANGE_TYPE_COLUMN)));
    assertEquals(
        Long.toHexString(42L), msg.getField(fieldDescriptors.get(StorageApiCDC.CHANGE_SQN_COLUMN)));
  }

  @Test
  public void testMessageFromGenericRecordLogicalTypes() throws Exception {
    Descriptors.Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            AvroGenericRecordToStorageApiProto.protoTableSchemaFromAvroSchema(LOGICAL_TYPES_SCHEMA),
            true,
            false);
    DynamicMessage msg =
        AvroGenericRecordToStorageApiProto.messageFromGenericRecord(
            descriptor, logicalTypesRecord, null, -1);
    assertEquals(7, msg.getAllFields().size());
    assertBaseRecord(msg, logicalTypesProtoExpectedFields);
  }

  @Test
  public void testMessageFromGenericRecordWithMap() throws Exception {
    // Create a GenericRecord with a map field
    Map<String, String> mapData = new HashMap<>();
    mapData.put("key1", "value1");
    mapData.put("key2", "value2");
    GenericRecord recordWithMap =
        new GenericRecordBuilder(SCHEMA_WITH_MAP)
            .set("nested", baseRecord)
            .set("aMap", mapData)
            .build();

    Descriptors.Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            AvroGenericRecordToStorageApiProto.protoTableSchemaFromAvroSchema(SCHEMA_WITH_MAP),
            true,
            false);
    DynamicMessage msg =
        AvroGenericRecordToStorageApiProto.messageFromGenericRecord(
            descriptor, recordWithMap, null, -1);

    assertEquals(2, msg.getAllFields().size());

    Map<String, Descriptors.FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(Descriptors.FieldDescriptor::getName, Functions.identity()));
    DynamicMessage nestedMsg = (DynamicMessage) msg.getField(fieldDescriptors.get("nested"));
    assertBaseRecord(nestedMsg, baseProtoExpectedFields);

    // Assert the map field
    List<DynamicMessage> list = (List<DynamicMessage>) msg.getField(fieldDescriptors.get("amap"));
    // Convert the list of DynamicMessages back to a map
    Map<String, String> actualMap = new HashMap<>();
    for (DynamicMessage entry : list) {
      String key = (String) entry.getField(entry.getDescriptorForType().findFieldByName("key"));
      String value = (String) entry.getField(entry.getDescriptorForType().findFieldByName("value"));
      actualMap.put(key, value);
    }
    assertEquals(mapData, actualMap);
  }

  @Test
  public void testMessageFromGenericRecordWithNullableArrayWithNonNullValue() throws Exception {
    ImmutableList<String> aList = ImmutableList.of("one", "two", "red", "blue");
    GenericRecord recordWithArray =
        new GenericRecordBuilder(AvroGenericRecordToStorageApiProtoTest.SCHEMA_WITH_NULLABLE_ARRAY)
            .set("aNullableArray", aList)
            .build();

    Descriptors.Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            AvroGenericRecordToStorageApiProto.protoTableSchemaFromAvroSchema(
                SCHEMA_WITH_NULLABLE_ARRAY),
            true,
            false);
    DynamicMessage msg =
        AvroGenericRecordToStorageApiProto.messageFromGenericRecord(
            descriptor, recordWithArray, null, -1);

    assertEquals(1, msg.getAllFields().size());

    Map<String, Descriptors.FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(Descriptors.FieldDescriptor::getName, Functions.identity()));

    List<String> list = (List<String>) msg.getField(fieldDescriptors.get("anullablearray"));
    assertEquals(aList, list);
  }

  @Test
  public void testMessageFromGenericRecordWithNullableArrayWithNullValue() throws Exception {
    ImmutableList<String> aNullList = null;
    GenericRecord recordWithNullArray =
        new GenericRecordBuilder(AvroGenericRecordToStorageApiProtoTest.SCHEMA_WITH_NULLABLE_ARRAY)
            .set("aNullableArray", aNullList)
            .build();

    Descriptors.Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            AvroGenericRecordToStorageApiProto.protoTableSchemaFromAvroSchema(
                SCHEMA_WITH_NULLABLE_ARRAY),
            true,
            false);
    DynamicMessage msg =
        AvroGenericRecordToStorageApiProto.messageFromGenericRecord(
            descriptor, recordWithNullArray, null, -1);

    assertEquals(0, msg.getAllFields().size());

    Map<String, Descriptors.FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(Descriptors.FieldDescriptor::getName, Functions.identity()));

    List<String> list = (List<String>) msg.getField(fieldDescriptors.get("anullablearray"));
    assertEquals(Collections.emptyList(), list);
  }
}
