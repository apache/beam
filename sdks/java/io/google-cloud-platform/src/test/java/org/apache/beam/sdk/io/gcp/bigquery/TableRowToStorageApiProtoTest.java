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
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Functions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
/** Unit tests for {@link org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto}. */
public class TableRowToStorageApiProtoTest {
  // Schemas we test.
  // The TableRow class has special semantics for fields named "f". To ensure we handel them
  // properly, we test schemas
  // both with and without a field named "f".
  private static final TableSchema BASE_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.<TableFieldSchema>builder()
                  .add(new TableFieldSchema().setType("STRING").setName("stringValue"))
                  .add(new TableFieldSchema().setType("STRING").setName("f"))
                  .add(new TableFieldSchema().setType("BYTES").setName("bytesValue"))
                  .add(new TableFieldSchema().setType("INT64").setName("int64Value"))
                  .add(new TableFieldSchema().setType("INTEGER").setName("intValue"))
                  .add(new TableFieldSchema().setType("FLOAT64").setName("float64Value"))
                  .add(new TableFieldSchema().setType("FLOAT").setName("floatValue"))
                  .add(new TableFieldSchema().setType("BOOL").setName("boolValue"))
                  .add(new TableFieldSchema().setType("BOOLEAN").setName("booleanValue"))
                  .add(new TableFieldSchema().setType("TIMESTAMP").setName("timestampValue"))
                  .add(new TableFieldSchema().setType("TIME").setName("timeValue"))
                  .add(new TableFieldSchema().setType("DATETIME").setName("datetimeValue"))
                  .add(new TableFieldSchema().setType("DATE").setName("dateValue"))
                  .add(new TableFieldSchema().setType("NUMERIC").setName("numericValue"))
                  .add(
                      new TableFieldSchema()
                          .setType("STRING")
                          .setMode("REPEATED")
                          .setName("arrayValue"))
                  .build());

  private static final TableSchema BASE_TABLE_SCHEMA_NO_F =
      new TableSchema()
          .setFields(
              ImmutableList.<TableFieldSchema>builder()
                  .add(new TableFieldSchema().setType("STRING").setName("stringValue"))
                  .add(new TableFieldSchema().setType("BYTES").setName("bytesValue"))
                  .add(new TableFieldSchema().setType("INT64").setName("int64Value"))
                  .add(new TableFieldSchema().setType("INTEGER").setName("intValue"))
                  .add(new TableFieldSchema().setType("FLOAT64").setName("float64Value"))
                  .add(new TableFieldSchema().setType("FLOAT").setName("floatValue"))
                  .add(new TableFieldSchema().setType("BOOL").setName("boolValue"))
                  .add(new TableFieldSchema().setType("BOOLEAN").setName("booleanValue"))
                  .add(new TableFieldSchema().setType("TIMESTAMP").setName("timestampValue"))
                  .add(new TableFieldSchema().setType("TIME").setName("timeValue"))
                  .add(new TableFieldSchema().setType("DATETIME").setName("datetimeValue"))
                  .add(new TableFieldSchema().setType("DATE").setName("dateValue"))
                  .add(new TableFieldSchema().setType("NUMERIC").setName("numericValue"))
                  .add(
                      new TableFieldSchema()
                          .setType("STRING")
                          .setMode("REPEATED")
                          .setName("arrayValue"))
                  .build());

  private static final DescriptorProto BASE_TABLE_SCHEMA_PROTO =
      DescriptorProto.newBuilder()
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("stringvalue")
                  .setNumber(1)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("f")
                  .setNumber(2)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("bytesvalue")
                  .setNumber(3)
                  .setType(Type.TYPE_BYTES)
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
                  .setName("intvalue")
                  .setNumber(5)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("float64value")
                  .setNumber(6)
                  .setType(Type.TYPE_DOUBLE)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("floatvalue")
                  .setNumber(7)
                  .setType(Type.TYPE_DOUBLE)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("boolvalue")
                  .setNumber(8)
                  .setType(Type.TYPE_BOOL)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("booleanvalue")
                  .setNumber(9)
                  .setType(Type.TYPE_BOOL)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvalue")
                  .setNumber(10)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timevalue")
                  .setNumber(11)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("datetimevalue")
                  .setNumber(12)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("datevalue")
                  .setNumber(13)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("numericvalue")
                  .setNumber(14)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("arrayvalue")
                  .setNumber(15)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_REPEATED)
                  .build())
          .build();

  private static final DescriptorProto BASE_TABLE_SCHEMA_NO_F_PROTO =
      DescriptorProto.newBuilder()
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("stringvalue")
                  .setNumber(1)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("bytesvalue")
                  .setNumber(2)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("int64value")
                  .setNumber(3)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("intvalue")
                  .setNumber(4)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("float64value")
                  .setNumber(5)
                  .setType(Type.TYPE_DOUBLE)
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
                  .setName("boolvalue")
                  .setNumber(7)
                  .setType(Type.TYPE_BOOL)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("booleanvalue")
                  .setNumber(8)
                  .setType(Type.TYPE_BOOL)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvalue")
                  .setNumber(9)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timevalue")
                  .setNumber(10)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("datetimevalue")
                  .setNumber(11)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("datevalue")
                  .setNumber(2)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("numericvalue")
                  .setNumber(13)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("arrayvalue")
                  .setNumber(14)
                  .setType(Type.TYPE_STRING)
                  .setLabel(Label.LABEL_REPEATED)
                  .build())
          .build();
  private static final TableSchema NESTED_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.<TableFieldSchema>builder()
                  .add(
                      new TableFieldSchema()
                          .setType("STRUCT")
                          .setName("nestedValue1")
                          .setFields(BASE_TABLE_SCHEMA.getFields()))
                  .add(
                      new TableFieldSchema()
                          .setType("RECORD")
                          .setName("nestedValue2")
                          .setFields(BASE_TABLE_SCHEMA.getFields()))
                  .add(
                      new TableFieldSchema()
                          .setType("STRUCT")
                          .setName("nestedValueNoF1")
                          .setFields(BASE_TABLE_SCHEMA_NO_F.getFields()))
                  .add(
                      new TableFieldSchema()
                          .setType("RECORD")
                          .setName("nestedValueNoF2")
                          .setFields(BASE_TABLE_SCHEMA_NO_F.getFields()))
                  .build());

  @Test
  public void testDescriptorFromTableSchema() {
    DescriptorProto descriptor =
        TableRowToStorageApiProto.descriptorSchemaFromTableSchema(BASE_TABLE_SCHEMA);
    Map<String, Type> types =
        descriptor.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    Map<String, Type> expectedTypes =
        BASE_TABLE_SCHEMA_PROTO.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedTypes, types);
  }

  @Test
  public void testNestedFromTableSchema() {
    DescriptorProto descriptor =
        TableRowToStorageApiProto.descriptorSchemaFromTableSchema(NESTED_TABLE_SCHEMA);
    Map<String, Type> expectedBaseTypes =
        BASE_TABLE_SCHEMA_PROTO.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    Map<String, Type> expectedBaseTypesNoF =
        BASE_TABLE_SCHEMA_NO_F_PROTO.getFieldList().stream()
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
    assertEquals(4, types.size());

    Map<String, DescriptorProto> nestedTypes =
        descriptor.getNestedTypeList().stream()
            .collect(Collectors.toMap(DescriptorProto::getName, Functions.identity()));
    assertEquals(4, nestedTypes.size());
    assertEquals(Type.TYPE_MESSAGE, types.get("nestedvalue1"));
    String nestedTypeName1 = typeNames.get("nestedvalue1");
    Map<String, Type> nestedTypes1 =
        nestedTypes.get(nestedTypeName1).getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedBaseTypes, nestedTypes1);

    assertEquals(Type.TYPE_MESSAGE, types.get("nestedvalue2"));
    String nestedTypeName2 = typeNames.get("nestedvalue2");
    Map<String, Type> nestedTypes2 =
        nestedTypes.get(nestedTypeName2).getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedBaseTypes, nestedTypes2);

    assertEquals(Type.TYPE_MESSAGE, types.get("nestedvaluenof1"));
    String nestedTypeNameNoF1 = typeNames.get("nestedvaluenof1");
    Map<String, Type> nestedTypesNoF1 =
        nestedTypes.get(nestedTypeNameNoF1).getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedBaseTypesNoF, nestedTypesNoF1);
    assertEquals(Type.TYPE_MESSAGE, types.get("nestedvaluenof2"));
    String nestedTypeNameNoF2 = typeNames.get("nestedvaluenof2");
    Map<String, Type> nestedTypesNoF2 =
        nestedTypes.get(nestedTypeNameNoF2).getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedBaseTypesNoF, nestedTypesNoF2);
  }

  private static final TableRow BASE_TABLE_ROW =
      new TableRow()
          .setF(
              Lists.newArrayList(
                  new TableCell().setV("string"),
                  new TableCell().setV("fff"),
                  new TableCell()
                      .setV(
                          BaseEncoding.base64().encode("string".getBytes(StandardCharsets.UTF_8))),
                  new TableCell().setV("42"),
                  new TableCell().setV("43"),
                  new TableCell().setV("2.8168"),
                  new TableCell().setV("2.817"),
                  new TableCell().setV("true"),
                  new TableCell().setV("true"),
                  new TableCell().setV("43"),
                  new TableCell().setV("00:52:07[.123]|[.123456] UTC"),
                  new TableCell().setV("2019-08-16 00:52:07[.123]|[.123456] UTC"),
                  new TableCell().setV("2019-08-16"),
                  new TableCell().setV("23.4"),
                  new TableCell().setV(ImmutableList.of("hello", "goodbye"))));

  private static final TableRow BASE_TABLE_ROW_NO_F =
      new TableRow()
          .set("stringValue", "string")
          .set(
              "bytesValue", BaseEncoding.base64().encode("string".getBytes(StandardCharsets.UTF_8)))
          .set("int64Value", "42")
          .set("intValue", "43")
          .set("float64Value", "2.8168")
          .set("floatValue", "2.817")
          .set("boolValue", "true")
          .set("booleanValue", "true")
          .set("timestampValue", "43")
          .set("timeValue", "00:52:07[.123]|[.123456] UTC")
          .set("datetimeValue", "2019-08-16 00:52:07[.123]|[.123456] UTC")
          .set("dateValue", "2019-08-16")
          .set("numericValue", "23.4")
          .set("arrayValue", ImmutableList.of("hello", "goodbye"));

  private static final Map<String, Object> BASE_ROW_EXPECTED_PROTO_VALUES =
      ImmutableMap.<String, Object>builder()
          .put("stringvalue", "string")
          .put("f", "fff")
          .put("bytesvalue", ByteString.copyFrom("string".getBytes(StandardCharsets.UTF_8)))
          .put("int64value", (long) 42)
          .put("intvalue", (long) 43)
          .put("float64value", (double) 2.8168)
          .put("floatvalue", (double) 2.817)
          .put("boolvalue", true)
          .put("booleanvalue", true)
          .put("timestampvalue", "43")
          .put("timevalue", "00:52:07[.123]|[.123456] UTC")
          .put("datetimevalue", "2019-08-16 00:52:07[.123]|[.123456] UTC")
          .put("datevalue", "2019-08-16")
          .put("numericvalue", "23.4")
          .put("arrayvalue", ImmutableList.of("hello", "goodbye"))
          .build();

  private static final Map<String, Object> BASE_ROW_NO_F_EXPECTED_PROTO_VALUES =
      ImmutableMap.<String, Object>builder()
          .put("stringvalue", "string")
          .put("bytesvalue", ByteString.copyFrom("string".getBytes(StandardCharsets.UTF_8)))
          .put("int64value", (long) 42)
          .put("intvalue", (long) 43)
          .put("float64value", (double) 2.8168)
          .put("floatvalue", (double) 2.817)
          .put("boolvalue", true)
          .put("booleanvalue", true)
          .put("timestampvalue", "43")
          .put("timevalue", "00:52:07[.123]|[.123456] UTC")
          .put("datetimevalue", "2019-08-16 00:52:07[.123]|[.123456] UTC")
          .put("datevalue", "2019-08-16")
          .put("numericvalue", "23.4")
          .put("arrayvalue", ImmutableList.of("hello", "goodbye"))
          .build();

  private void assertBaseRecord(DynamicMessage msg, boolean withF) {
    Map<String, Object> recordFields =
        msg.getAllFields().entrySet().stream()
            .collect(
                Collectors.toMap(entry -> entry.getKey().getName(), entry -> entry.getValue()));
    assertEquals(
        withF ? BASE_ROW_EXPECTED_PROTO_VALUES : BASE_ROW_NO_F_EXPECTED_PROTO_VALUES, recordFields);
  }

  @Test
  public void testMessageFromTableRow() throws Exception {
    TableRow tableRow =
        new TableRow()
            .set("nestedValue1", BASE_TABLE_ROW)
            .set("nestedValue2", BASE_TABLE_ROW)
            .set("nestedvalueNoF1", BASE_TABLE_ROW_NO_F)
            .set("nestedvalueNoF2", BASE_TABLE_ROW_NO_F);

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(NESTED_TABLE_SCHEMA);
    DynamicMessage msg = TableRowToStorageApiProto.messageFromTableRow(descriptor, tableRow);
    assertEquals(4, msg.getAllFields().size());

    Map<String, FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(FieldDescriptor::getName, Functions.identity()));
    assertBaseRecord((DynamicMessage) msg.getField(fieldDescriptors.get("nestedvalue1")), true);
    assertBaseRecord((DynamicMessage) msg.getField(fieldDescriptors.get("nestedvalue2")), true);
    assertBaseRecord((DynamicMessage) msg.getField(fieldDescriptors.get("nestedvaluenof1")), false);
    assertBaseRecord((DynamicMessage) msg.getField(fieldDescriptors.get("nestedvaluenof2")), false);
  }

  @Test
  public void testMessageWithFFromTableRow() throws Exception {
    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(BASE_TABLE_SCHEMA);
    DynamicMessage msg = TableRowToStorageApiProto.messageFromTableRow(descriptor, BASE_TABLE_ROW);
    assertBaseRecord(msg, true);
  }

  private static final TableSchema REPEATED_MESSAGE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema()
                      .setType("STRUCT")
                      .setName("repeated1")
                      .setFields(BASE_TABLE_SCHEMA.getFields())
                      .setMode("REPEATED"),
                  new TableFieldSchema()
                      .setType("RECORD")
                      .setName("repeated2")
                      .setFields(BASE_TABLE_SCHEMA.getFields())
                      .setMode("REPEATED"),
                  new TableFieldSchema()
                      .setType("STRUCT")
                      .setName("repeatednof1")
                      .setFields(BASE_TABLE_SCHEMA_NO_F.getFields())
                      .setMode("REPEATED"),
                  new TableFieldSchema()
                      .setType("RECORD")
                      .setName("repeatednof2")
                      .setFields(BASE_TABLE_SCHEMA_NO_F.getFields())
                      .setMode("REPEATED")));

  @Test
  public void testRepeatedDescriptorFromTableSchema() throws Exception {
    TableRow repeatedRow =
        new TableRow()
            .set("repeated1", ImmutableList.of(BASE_TABLE_ROW, BASE_TABLE_ROW))
            .set("repeated2", ImmutableList.of(BASE_TABLE_ROW, BASE_TABLE_ROW))
            .set("repeatednof1", ImmutableList.of(BASE_TABLE_ROW_NO_F, BASE_TABLE_ROW_NO_F))
            .set("repeatednof2", ImmutableList.of(BASE_TABLE_ROW_NO_F, BASE_TABLE_ROW_NO_F));
    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(REPEATED_MESSAGE_SCHEMA);
    DynamicMessage msg = TableRowToStorageApiProto.messageFromTableRow(descriptor, repeatedRow);
    assertEquals(4, msg.getAllFields().size());

    Map<String, FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(FieldDescriptor::getName, Functions.identity()));
    List<DynamicMessage> repeated1 =
        (List<DynamicMessage>) msg.getField(fieldDescriptors.get("repeated1"));
    assertEquals(2, repeated1.size());
    assertBaseRecord(repeated1.get(0), true);
    assertBaseRecord(repeated1.get(1), true);

    List<DynamicMessage> repeated2 =
        (List<DynamicMessage>) msg.getField(fieldDescriptors.get("repeated2"));
    assertEquals(2, repeated2.size());
    assertBaseRecord(repeated2.get(0), true);
    assertBaseRecord(repeated2.get(1), true);

    List<DynamicMessage> repeatednof1 =
        (List<DynamicMessage>) msg.getField(fieldDescriptors.get("repeatednof1"));
    assertEquals(2, repeatednof1.size());
    assertBaseRecord(repeatednof1.get(0), false);
    assertBaseRecord(repeatednof1.get(1), false);

    List<DynamicMessage> repeatednof2 =
        (List<DynamicMessage>) msg.getField(fieldDescriptors.get("repeatednof2"));
    assertEquals(2, repeatednof2.size());
    assertBaseRecord(repeatednof2.get(0), false);
    assertBaseRecord(repeatednof2.get(1), false);
  }

  @Test
  public void testNullRepeatedDescriptorFromTableSchema() throws Exception {
    TableRow repeatedRow =
        new TableRow()
            .set("repeated1", null)
            .set("repeated2", null)
            .set("repeatednof1", null)
            .set("repeatednof2", null);
    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(REPEATED_MESSAGE_SCHEMA);
    DynamicMessage msg = TableRowToStorageApiProto.messageFromTableRow(descriptor, repeatedRow);

    Map<String, FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(FieldDescriptor::getName, Functions.identity()));
    List<DynamicMessage> repeated1 =
        (List<DynamicMessage>) msg.getField(fieldDescriptors.get("repeated1"));
    assertTrue(repeated1.isEmpty());
    List<DynamicMessage> repeated2 =
        (List<DynamicMessage>) msg.getField(fieldDescriptors.get("repeated2"));
    assertTrue(repeated2.isEmpty());
    List<DynamicMessage> repeatednof1 =
        (List<DynamicMessage>) msg.getField(fieldDescriptors.get("repeatednof1"));
    assertTrue(repeatednof1.isEmpty());
    List<DynamicMessage> repeatednof2 =
        (List<DynamicMessage>) msg.getField(fieldDescriptors.get("repeatednof2"));
    assertTrue(repeatednof2.isEmpty());
  }
}
