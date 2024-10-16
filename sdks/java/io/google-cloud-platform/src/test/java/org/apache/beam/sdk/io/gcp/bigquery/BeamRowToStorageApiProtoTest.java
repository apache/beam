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
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Functions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.util.Pair;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests form {@link BeamRowToStorageApiProto}. */
@RunWith(JUnit4.class)
public class BeamRowToStorageApiProtoTest {
  private static final EnumerationType TEST_ENUM =
      EnumerationType.create("ONE", "TWO", "RED", "BLUE");
  private static final Schema BASE_SCHEMA =
      Schema.builder()
          .addField("byteValue", FieldType.BYTE.withNullable(true))
          .addField("int16Value", FieldType.INT16)
          .addField("int32Value", FieldType.INT32.withNullable(true))
          .addField("int64Value", FieldType.INT64.withNullable(true))
          .addField("decimalValue", FieldType.DECIMAL.withNullable(true))
          .addField("floatValue", FieldType.FLOAT.withNullable(true))
          .addField("doubleValue", FieldType.DOUBLE.withNullable(true))
          .addField("stringValue", FieldType.STRING.withNullable(true))
          .addField("datetimeValue", FieldType.DATETIME.withNullable(true))
          .addField("booleanValue", FieldType.BOOLEAN.withNullable(true))
          .addField("bytesValue", FieldType.BYTES.withNullable(true))
          .addField("arrayValue", FieldType.array(FieldType.STRING))
          .addField("arrayNullValue", FieldType.array(FieldType.STRING).withNullable(true))
          .addField("iterableValue", FieldType.array(FieldType.STRING))
          .addField("sqlDateValue", FieldType.logicalType(SqlTypes.DATE).withNullable(true))
          .addField("sqlTimeValue", FieldType.logicalType(SqlTypes.TIME).withNullable(true))
          .addField("sqlDatetimeValue", FieldType.logicalType(SqlTypes.DATETIME).withNullable(true))
          .addField(
              "sqlTimestampValue", FieldType.logicalType(SqlTypes.TIMESTAMP).withNullable(true))
          .addField("enumValue", FieldType.logicalType(TEST_ENUM).withNullable(true))
          .build();

  private static final DescriptorProto BASE_SCHEMA_PROTO =
      DescriptorProto.newBuilder()
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("bytevalue")
                  .setNumber(1)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("int16value")
                  .setNumber(2)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_REQUIRED)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("int32value")
                  .setNumber(3)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("int64value")
                  .setNumber(4)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("decimalvalue")
                  .setNumber(5)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("floatvalue")
                  .setNumber(6)
                  .setType(Type.TYPE_DOUBLE)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("doublevalue")
                  .setNumber(7)
                  .setType(Type.TYPE_DOUBLE)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("stringvalue")
                  .setNumber(8)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("datetimevalue")
                  .setNumber(9)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("booleanvalue")
                  .setNumber(10)
                  .setType(Type.TYPE_BOOL)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("bytesvalue")
                  .setNumber(11)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("arrayvalue")
                  .setNumber(12)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_REPEATED)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("arraynullvalue")
                  .setNumber(13)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_REPEATED)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("iterablevalue")
                  .setNumber(14)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_REPEATED)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("sqldatevalue")
                  .setNumber(15)
                  .setType(Type.TYPE_INT32)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("sqltimevalue")
                  .setNumber(16)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("sqldatetimevalue")
                  .setNumber(17)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("sqltimestampvalue")
                  .setNumber(18)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("enumvalue")
                  .setNumber(19)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .build();

  private static final byte[] BYTES = "BYTE BYTE BYTE".getBytes(StandardCharsets.UTF_8);
  private static final Row BASE_ROW =
      Row.withSchema(BASE_SCHEMA)
          .withFieldValue("byteValue", (byte) 1)
          .withFieldValue("int16Value", (short) 2)
          .withFieldValue("int32Value", (int) 3)
          .withFieldValue("int64Value", (long) 4)
          .withFieldValue("decimalValue", BigDecimal.valueOf(5))
          .withFieldValue("floatValue", (float) 3.14)
          .withFieldValue("doubleValue", (double) 2.68)
          .withFieldValue("stringValue", "I am a string. Hear me roar.")
          .withFieldValue("datetimeValue", Instant.now())
          .withFieldValue("booleanValue", true)
          .withFieldValue("bytesValue", BYTES)
          .withFieldValue("arrayValue", ImmutableList.of("one", "two", "red", "blue"))
          .withFieldValue("arrayNullValue", null)
          .withFieldValue("iterableValue", ImmutableList.of("blue", "red", "two", "one"))
          .withFieldValue("sqlDateValue", LocalDate.now())
          .withFieldValue("sqlTimeValue", LocalTime.now())
          .withFieldValue("sqlDatetimeValue", LocalDateTime.now())
          .withFieldValue("sqlTimestampValue", java.time.Instant.now().plus(123, ChronoUnit.MICROS))
          .withFieldValue("enumValue", TEST_ENUM.valueOf("RED"))
          .build();
  private static final Map<String, Object> BASE_PROTO_EXPECTED_FIELDS =
      ImmutableMap.<String, Object>builder()
          .put("bytevalue", 1L)
          .put("int16value", 2L)
          .put("int32value", 3L)
          .put("int64value", 4L)
          .put(
              "decimalvalue",
              BeamRowToStorageApiProto.serializeBigDecimalToNumeric(BigDecimal.valueOf(5)))
          .put("floatvalue", (double) 3.14)
          .put("doublevalue", (double) 2.68)
          .put("stringvalue", "I am a string. Hear me roar.")
          .put("datetimevalue", BASE_ROW.getDateTime("datetimeValue").getMillis() * 1000)
          .put("booleanvalue", true)
          .put("bytesvalue", ByteString.copyFrom(BYTES))
          .put("arrayvalue", ImmutableList.of("one", "two", "red", "blue"))
          .put("arraynullvalue", Collections.emptyList())
          .put("iterablevalue", ImmutableList.of("blue", "red", "two", "one"))
          .put(
              "sqldatevalue",
              (int) BASE_ROW.getLogicalTypeValue("sqlDateValue", LocalDate.class).toEpochDay())
          .put(
              "sqltimevalue",
              CivilTimeEncoder.encodePacked64TimeMicros(
                  BASE_ROW.getLogicalTypeValue("sqlTimeValue", LocalTime.class)))
          .put(
              "sqldatetimevalue",
              CivilTimeEncoder.encodePacked64DatetimeMicros(
                  BASE_ROW.getLogicalTypeValue("sqlDatetimeValue", LocalDateTime.class)))
          .put(
              "sqltimestampvalue",
              ChronoUnit.MICROS.between(
                  java.time.Instant.EPOCH,
                  BASE_ROW.getLogicalTypeValue("sqlTimestampValue", java.time.Instant.class)))
          .put("enumvalue", "RED")
          .build();

  private static final Schema NESTED_SCHEMA =
      Schema.builder()
          .addField("nested", FieldType.row(BASE_SCHEMA).withNullable(true))
          .addField("nestedArray", FieldType.array(FieldType.row(BASE_SCHEMA)))
          .addField("nestedIterable", FieldType.iterable(FieldType.row(BASE_SCHEMA)))
          .addField("nestedMap", FieldType.map(FieldType.STRING, FieldType.row(BASE_SCHEMA)))
          .build();
  private static final Row NESTED_ROW =
      Row.withSchema(NESTED_SCHEMA)
          .withFieldValue("nested", BASE_ROW)
          .withFieldValue("nestedArray", ImmutableList.of(BASE_ROW, BASE_ROW))
          .withFieldValue("nestedIterable", ImmutableList.of(BASE_ROW, BASE_ROW))
          .withFieldValue("nestedMap", ImmutableMap.of("key1", BASE_ROW, "key2", BASE_ROW))
          .build();

  @Test
  public void testDescriptorFromSchema() {
    DescriptorProto descriptor =
        TableRowToStorageApiProto.descriptorSchemaFromTableSchema(
            BeamRowToStorageApiProto.protoTableSchemaFromBeamSchema(BASE_SCHEMA), true, false);
    Map<String, Type> types =
        descriptor.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    Map<String, Type> expectedTypes =
        BASE_SCHEMA_PROTO.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedTypes, types);

    Map<String, String> nameMapping =
        BASE_SCHEMA.getFields().stream()
            .collect(Collectors.toMap(f -> f.getName().toLowerCase(), Field::getName));
    descriptor
        .getFieldList()
        .forEach(
            p -> {
              FieldType schemaFieldType =
                  BASE_SCHEMA.getField(nameMapping.get(p.getName())).getType();
              Label label =
                  schemaFieldType.getTypeName().isCollectionType()
                      ? Label.LABEL_REPEATED
                      : schemaFieldType.getNullable() ? Label.LABEL_OPTIONAL : Label.LABEL_REQUIRED;
              assertEquals(label, p.getLabel());
            });
  }

  @Test
  public void testNestedFromSchema() {
    DescriptorProto descriptor =
        TableRowToStorageApiProto.descriptorSchemaFromTableSchema(
            BeamRowToStorageApiProto.protoTableSchemaFromBeamSchema((NESTED_SCHEMA)), true, false);
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

    assertEquals(4, types.size());

    Map<String, DescriptorProto> nestedTypes =
        descriptor.getNestedTypeList().stream()
            .collect(Collectors.toMap(DescriptorProto::getName, Functions.identity()));
    assertEquals(4, nestedTypes.size());
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

    assertEquals(Type.TYPE_MESSAGE, types.get("nestediterable"));
    assertEquals(Label.LABEL_REPEATED, typeLabels.get("nestediterable"));
    String nestedTypeName3 = typeNames.get("nestediterable");
    Map<String, Type> nestedTypes3 =
        nestedTypes.get(nestedTypeName3).getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedBaseTypes, nestedTypes3);

    assertEquals(Type.TYPE_MESSAGE, types.get("nestedmap"));
    assertEquals(Label.LABEL_REPEATED, typeLabels.get("nestedmap"));
    String nestedTypeName4 = typeNames.get("nestedmap");
    // expects 2 fields in the nested map, key and value
    assertEquals(2, nestedTypes.get(nestedTypeName4).getFieldList().size());
    Supplier<Stream<FieldDescriptorProto>> stream =
        () -> nestedTypes.get(nestedTypeName4).getFieldList().stream();
    assertTrue(stream.get().anyMatch(fdp -> fdp.getName().equals("key")));
    assertTrue(stream.get().anyMatch(fdp -> fdp.getName().equals("value")));

    Map<String, Type> nestedTypes4 =
        nestedTypes.get(nestedTypeName4).getNestedTypeList().stream()
            .flatMap(vdesc -> vdesc.getFieldList().stream())
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedBaseTypes, nestedTypes4);
  }

  @Test
  public void testMapArrayFromSchemas() {
    Schema nestedMapSchemaVariations =
        Schema.builder()
            .addField(
                "nestedArrayOfMaps",
                FieldType.array(FieldType.map(FieldType.STRING, FieldType.STRING)))
            .addField(
                "nestedMapNullable",
                FieldType.map(FieldType.STRING, FieldType.DOUBLE).withNullable(true))
            .build();

    DescriptorProto descriptor =
        TableRowToStorageApiProto.descriptorSchemaFromTableSchema(
            BeamRowToStorageApiProto.protoTableSchemaFromBeamSchema((nestedMapSchemaVariations)),
            true,
            false);

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

    Map<String, DescriptorProto> nestedTypes =
        descriptor.getNestedTypeList().stream()
            .collect(Collectors.toMap(DescriptorProto::getName, Functions.identity()));
    assertEquals(2, nestedTypes.size());

    assertEquals(Type.TYPE_MESSAGE, types.get("nestedarrayofmaps"));
    assertEquals(Label.LABEL_REPEATED, typeLabels.get("nestedarrayofmaps"));
    String nestedArrayOfMapsName = typeNames.get("nestedarrayofmaps");
    // expects 2 fields for the nested array of maps, key and value
    assertEquals(2, nestedTypes.get(nestedArrayOfMapsName).getFieldList().size());
    Supplier<Stream<FieldDescriptorProto>> stream =
        () -> nestedTypes.get(nestedArrayOfMapsName).getFieldList().stream();
    assertTrue(stream.get().filter(fdp -> fdp.getName().equals("key")).count() == 1);
    assertTrue(stream.get().filter(fdp -> fdp.getName().equals("value")).count() == 1);

    assertEquals(Type.TYPE_MESSAGE, types.get("nestedmapnullable"));
    // even though the field is marked as optional in the row we will should see repeated in proto
    assertEquals(Label.LABEL_REPEATED, typeLabels.get("nestedmapnullable"));
    String nestedMapNullableName = typeNames.get("nestedmapnullable");
    // expects 2 fields in the nullable maps, key and value
    assertEquals(2, nestedTypes.get(nestedMapNullableName).getFieldList().size());
    stream = () -> nestedTypes.get(nestedMapNullableName).getFieldList().stream();
    assertTrue(stream.get().filter(fdp -> fdp.getName().equals("key")).count() == 1);
    assertTrue(stream.get().filter(fdp -> fdp.getName().equals("value")).count() == 1);
  }

  private void assertBaseRecord(DynamicMessage msg) {
    Map<String, Object> recordFields =
        msg.getAllFields().entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue));
    assertEquals(BASE_PROTO_EXPECTED_FIELDS, recordFields);
  }

  @Test
  public void testMessageFromTableRow() throws Exception {
    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            BeamRowToStorageApiProto.protoTableSchemaFromBeamSchema(NESTED_SCHEMA), true, false);
    DynamicMessage msg =
        BeamRowToStorageApiProto.messageFromBeamRow(descriptor, NESTED_ROW, null, -1);
    assertEquals(4, msg.getAllFields().size());

    Map<String, FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(FieldDescriptor::getName, Functions.identity()));
    DynamicMessage nestedMsg = (DynamicMessage) msg.getField(fieldDescriptors.get("nested"));
    assertBaseRecord(nestedMsg);
  }

  @Test
  public void testMessageFromTableRowForArraysAndMaps() throws Exception {
    Schema nestedMapSchemaVariations =
        Schema.builder()
            .addField(
                "nestedArrayOfMaps",
                FieldType.array(FieldType.map(FieldType.STRING, FieldType.STRING)))
            .addField(
                "nestedIterableOfMaps",
                FieldType.iterable(FieldType.map(FieldType.STRING, FieldType.STRING)))
            .addField("nestedArrayNullable", FieldType.array(FieldType.STRING).withNullable(true))
            .addField("nestedMap", FieldType.map(FieldType.STRING, FieldType.STRING))
            .addField(
                "nestedMultiMap",
                FieldType.map(FieldType.STRING, FieldType.iterable(FieldType.STRING)))
            .addField(
                "nestedMapNullable",
                FieldType.map(FieldType.STRING, FieldType.DOUBLE).withNullable(true))
            .build();

    Row nestedRow =
        Row.withSchema(nestedMapSchemaVariations)
            .withFieldValue(
                "nestedArrayOfMaps", ImmutableList.of(ImmutableMap.of("arraykey1", "arrayvalue1")))
            .withFieldValue(
                "nestedIterableOfMaps",
                ImmutableList.of(ImmutableMap.of("iterablekey1", "iterablevalue1")))
            .withFieldValue("nestedArrayNullable", null)
            .withFieldValue("nestedMap", ImmutableMap.of("key1", "value1"))
            .withFieldValue(
                "nestedMultiMap",
                ImmutableMap.of("multikey1", ImmutableList.of("multivalue1", "multivalue2")))
            .withFieldValue("nestedMapNullable", null)
            .build();

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            BeamRowToStorageApiProto.protoTableSchemaFromBeamSchema(nestedMapSchemaVariations),
            true,
            false);
    DynamicMessage msg =
        BeamRowToStorageApiProto.messageFromBeamRow(descriptor, nestedRow, null, -1);

    Map<String, FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(FieldDescriptor::getName, Functions.identity()));

    DynamicMessage nestedArrayOfMapEntryMsg =
        (DynamicMessage) msg.getRepeatedField(fieldDescriptors.get("nestedarrayofmaps"), 0);
    String value =
        (String)
            nestedArrayOfMapEntryMsg.getField(
                fieldDescriptors
                    .get("nestedarrayofmaps")
                    .getMessageType()
                    .findFieldByName("value"));
    assertEquals("arrayvalue1", value);

    DynamicMessage nestedIterableOfMapEntryMsg =
        (DynamicMessage) msg.getRepeatedField(fieldDescriptors.get("nestediterableofmaps"), 0);
    value =
        (String)
            nestedIterableOfMapEntryMsg.getField(
                fieldDescriptors
                    .get("nestediterableofmaps")
                    .getMessageType()
                    .findFieldByName("value"));
    assertEquals("iterablevalue1", value);

    DynamicMessage nestedMapEntryMsg =
        (DynamicMessage) msg.getRepeatedField(fieldDescriptors.get("nestedmap"), 0);
    value =
        (String)
            nestedMapEntryMsg.getField(
                fieldDescriptors.get("nestedmap").getMessageType().findFieldByName("value"));
    assertEquals("value1", value);

    DynamicMessage nestedMultiMapEntryMsg =
        (DynamicMessage) msg.getRepeatedField(fieldDescriptors.get("nestedmultimap"), 0);
    List<String> values =
        (List<String>)
            nestedMultiMapEntryMsg.getField(
                fieldDescriptors.get("nestedmultimap").getMessageType().findFieldByName("value"));
    assertTrue(values.size() == 2);
    assertEquals("multivalue1", values.get(0));

    assertTrue(msg.getRepeatedFieldCount(fieldDescriptors.get("nestedarraynullable")) == 0);
    assertTrue(msg.getRepeatedFieldCount(fieldDescriptors.get("nestedmapnullable")) == 0);
  }

  @Test
  public void testCdcFields() throws Exception {
    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            BeamRowToStorageApiProto.protoTableSchemaFromBeamSchema(NESTED_SCHEMA), true, true);
    assertNotNull(descriptor.findFieldByName(StorageApiCDC.CHANGE_TYPE_COLUMN));
    assertNotNull(descriptor.findFieldByName(StorageApiCDC.CHANGE_SQN_COLUMN));
    DynamicMessage msg =
        BeamRowToStorageApiProto.messageFromBeamRow(descriptor, NESTED_ROW, "UPDATE", 42);
    assertEquals(6, msg.getAllFields().size());

    Map<String, FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(FieldDescriptor::getName, Functions.identity()));
    DynamicMessage nestedMsg = (DynamicMessage) msg.getField(fieldDescriptors.get("nested"));
    assertBaseRecord(nestedMsg);
    assertEquals(
        "UPDATE", msg.getField(descriptor.findFieldByName(StorageApiCDC.CHANGE_TYPE_COLUMN)));
    assertEquals(
        Long.toHexString(42L),
        msg.getField(descriptor.findFieldByName(StorageApiCDC.CHANGE_SQN_COLUMN)));
  }

  @Test
  public void testScalarToProtoValue() {
    Map<FieldType, Iterable<Pair<Object, Object>>> testCases =
        ImmutableMap.<FieldType, Iterable<Pair<Object, Object>>>builder()
            .put(
                FieldType.BYTES,
                ImmutableList.of(
                    Pair.create(BYTES, ByteString.copyFrom(BYTES)),
                    Pair.create(ByteBuffer.wrap(BYTES), ByteString.copyFrom(BYTES)),
                    Pair.create(
                        new String(BYTES, StandardCharsets.UTF_8), ByteString.copyFrom(BYTES))))
            .build();
    for (Map.Entry<FieldType, Iterable<Pair<Object, Object>>> entry : testCases.entrySet()) {
      entry
          .getValue()
          .forEach(
              p -> {
                assertEquals(
                    p.getValue(),
                    BeamRowToStorageApiProto.scalarToProtoValue(entry.getKey(), p.getKey()));
              });
    }
  }
}
