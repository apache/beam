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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Functions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
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
  private static final TableSchema BASE_TABLE_SCHEMA =
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
                  .setNumber(12)
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
                  .build());

  // For now, test that no exceptions are thrown.
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

    Map<String, Type> types =
        descriptor.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    Map<String, String> typeNames =
        descriptor.getFieldList().stream()
            .collect(
                Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getTypeName));
    assertEquals(2, types.size());

    Map<String, DescriptorProto> nestedTypes =
        descriptor.getNestedTypeList().stream()
            .collect(Collectors.toMap(DescriptorProto::getName, Functions.identity()));
    assertEquals(2, nestedTypes.size());
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
  }

  @Test
  public void testRepeatedDescriptorFromTableSchema() {
    TableRowToStorageApiProto.descriptorSchemaFromTableSchema(BASE_TABLE_SCHEMA);
  }

  private static final TableRow BASE_TABLE_ROW =
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

  private void assertBaseRecord(DynamicMessage msg) {
    Map<String, Object> recordFields =
        msg.getAllFields().entrySet().stream()
            .collect(
                Collectors.toMap(entry -> entry.getKey().getName(), entry -> entry.getValue()));
    assertEquals(BASE_ROW_EXPECTED_PROTO_VALUES, recordFields);
  }

  @Test
  public void testMessageFromTableRow() throws Exception {
    TableRow tableRow =
        new TableRow().set("nestedValue1", BASE_TABLE_ROW).set("nestedValue2", BASE_TABLE_ROW);
    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(NESTED_TABLE_SCHEMA);
    DynamicMessage msg = TableRowToStorageApiProto.messageFromTableRow(descriptor, tableRow);
    assertEquals(2, msg.getAllFields().size());

    Map<String, FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(FieldDescriptor::getName, Functions.identity()));
    assertBaseRecord((DynamicMessage) msg.getField(fieldDescriptors.get("nestedvalue1")));
    assertBaseRecord((DynamicMessage) msg.getField(fieldDescriptors.get("nestedvalue2")));
  }
}
