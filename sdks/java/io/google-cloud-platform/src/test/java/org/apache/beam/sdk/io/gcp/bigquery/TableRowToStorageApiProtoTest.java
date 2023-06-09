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
import static org.junit.Assert.fail;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.SchemaConversionException;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.SchemaInformation;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Functions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
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
                  .add(new TableFieldSchema().setType("BIGNUMERIC").setName("bigNumericValue"))
                  .add(new TableFieldSchema().setType("NUMERIC").setName("numericValue2"))
                  .add(new TableFieldSchema().setType("BIGNUMERIC").setName("bigNumericValue2"))
                  .add(
                      new TableFieldSchema()
                          .setType("BYTES")
                          .setMode("REPEATED")
                          .setName("arrayValue"))
                  .add(new TableFieldSchema().setType("TIMESTAMP").setName("timestampISOValue"))
                  .add(
                      new TableFieldSchema()
                          .setType("TIMESTAMP")
                          .setName("timestampISOValueOffsetHH"))
                  .add(new TableFieldSchema().setType("TIMESTAMP").setName("timestampValueLong"))
                  .add(new TableFieldSchema().setType("TIMESTAMP").setName("timestampValueSpace"))
                  .add(
                      new TableFieldSchema().setType("TIMESTAMP").setName("timestampValueSpaceUtc"))
                  .add(
                      new TableFieldSchema()
                          .setType("TIMESTAMP")
                          .setName("timestampValueZoneRegion"))
                  .add(
                      new TableFieldSchema()
                          .setType("TIMESTAMP")
                          .setName("timestampValueSpaceMilli"))
                  .add(
                      new TableFieldSchema()
                          .setType("TIMESTAMP")
                          .setName("timestampValueSpaceTrailingZero"))
                  .add(new TableFieldSchema().setType("DATETIME").setName("datetimeValueSpace"))
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
                  .add(new TableFieldSchema().setType("BIGNUMERIC").setName("bigNumericValue"))
                  .add(new TableFieldSchema().setType("NUMERIC").setName("numericValue2"))
                  .add(new TableFieldSchema().setType("BIGNUMERIC").setName("bigNumericValue2"))
                  .add(
                      new TableFieldSchema()
                          .setType("BYTES")
                          .setMode("REPEATED")
                          .setName("arrayValue"))
                  .add(new TableFieldSchema().setType("TIMESTAMP").setName("timestampISOValue"))
                  .add(
                      new TableFieldSchema()
                          .setType("TIMESTAMP")
                          .setName("timestampISOValueOffsetHH"))
                  .add(new TableFieldSchema().setType("TIMESTAMP").setName("timestampValueLong"))
                  .add(new TableFieldSchema().setType("TIMESTAMP").setName("timestampValueSpace"))
                  .add(
                      new TableFieldSchema().setType("TIMESTAMP").setName("timestampValueSpaceUtc"))
                  .add(
                      new TableFieldSchema()
                          .setType("TIMESTAMP")
                          .setName("timestampValueZoneRegion"))
                  .add(
                      new TableFieldSchema()
                          .setType("TIMESTAMP")
                          .setName("timestampValueSpaceMilli"))
                  .add(
                      new TableFieldSchema()
                          .setType("TIMESTAMP")
                          .setName("timestampValueSpaceTrailingZero"))
                  .add(new TableFieldSchema().setType("DATETIME").setName("datetimeValueSpace"))
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
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timevalue")
                  .setNumber(11)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("datetimevalue")
                  .setNumber(12)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("datevalue")
                  .setNumber(13)
                  .setType(Type.TYPE_INT32)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("numericvalue")
                  .setNumber(14)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("bignumericvalue")
                  .setNumber(15)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("numericvalue2")
                  .setNumber(16)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("bignumericvalue2")
                  .setNumber(17)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("arrayvalue")
                  .setNumber(18)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_REPEATED)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampisovalue")
                  .setNumber(19)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampisovalueoffsethh")
                  .setNumber(20)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvaluelong")
                  .setNumber(21)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvaluespace")
                  .setNumber(22)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvaluespaceutc")
                  .setNumber(23)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvaluezoneregion")
                  .setNumber(24)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvaluespacemilli")
                  .setNumber(25)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvaluespacetrailingzero")
                  .setNumber(26)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("datetimevaluespace")
                  .setNumber(27)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
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
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timevalue")
                  .setNumber(10)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("datetimevalue")
                  .setNumber(11)
                  .setType(Type.TYPE_INT64)
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
                  .setName("numericvalue")
                  .setNumber(13)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("bignumericvalue")
                  .setNumber(14)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("numericvalue2")
                  .setNumber(15)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("bignumericvalue2")
                  .setNumber(16)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("arrayvalue")
                  .setNumber(17)
                  .setType(Type.TYPE_BYTES)
                  .setLabel(Label.LABEL_REPEATED)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampisovalue")
                  .setNumber(18)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampisovalueoffsethh")
                  .setNumber(19)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvaluelong")
                  .setNumber(20)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvaluespace")
                  .setNumber(21)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvaluespaceutc")
                  .setNumber(22)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvaluezoneregion")
                  .setNumber(23)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvaluespacemilli")
                  .setNumber(24)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("timestampvaluespacetrailingzero")
                  .setNumber(25)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
                  .build())
          .addField(
              FieldDescriptorProto.newBuilder()
                  .setName("datetimevaluespace")
                  .setNumber(26)
                  .setType(Type.TYPE_INT64)
                  .setLabel(Label.LABEL_OPTIONAL)
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

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testDescriptorFromTableSchema() {
    DescriptorProto descriptor =
        TableRowToStorageApiProto.descriptorSchemaFromTableSchema(BASE_TABLE_SCHEMA, true, false);
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
        TableRowToStorageApiProto.descriptorSchemaFromTableSchema(NESTED_TABLE_SCHEMA, true, false);
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

  private static final List<Object> REPEATED_BYTES =
      ImmutableList.of(
          BaseEncoding.base64().encode("hello".getBytes(StandardCharsets.UTF_8)),
          "goodbye".getBytes(StandardCharsets.UTF_8),
          ByteString.copyFrom("solong".getBytes(StandardCharsets.UTF_8)));

  private static final List<Object> EXPECTED_PROTO_REPEATED_BYTES =
      ImmutableList.of(
          ByteString.copyFrom("hello".getBytes(StandardCharsets.UTF_8)),
          ByteString.copyFrom("goodbye".getBytes(StandardCharsets.UTF_8)),
          ByteString.copyFrom("solong".getBytes(StandardCharsets.UTF_8)));

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
                  new TableCell().setV("2"),
                  new TableCell().setV("true"),
                  new TableCell().setV("true"),
                  new TableCell().setV("1970-01-01T00:00:00.000043Z"),
                  new TableCell().setV("00:52:07.123456"),
                  new TableCell().setV("2019-08-16T00:52:07.123456"),
                  new TableCell().setV("2019-08-16"),
                  new TableCell().setV("23.4"),
                  new TableCell().setV("2312345.4"),
                  new TableCell().setV(23),
                  new TableCell().setV(123456789012345678L),
                  new TableCell().setV(REPEATED_BYTES),
                  new TableCell().setV("1970-01-01T00:00:00.000+01:00"),
                  new TableCell().setV("1970-01-01T00:00:00.000+01"),
                  new TableCell().setV("1234567"),
                  new TableCell().setV("1970-01-01 00:00:00.000343"),
                  new TableCell().setV("1970-01-01 00:00:00.000343 UTC"),
                  new TableCell().setV("1970-01-01 00:00:00.123456 America/New_York"),
                  new TableCell().setV("1970-01-01 00:00:00.123"),
                  new TableCell().setV("1970-01-01 00:00:00.1230"),
                  new TableCell().setV("2019-08-16 00:52:07.123456")));

  private static final TableRow BASE_TABLE_ROW_NO_F =
      new TableRow()
          .set("stringValue", "string")
          .set(
              "bytesValue", BaseEncoding.base64().encode("string".getBytes(StandardCharsets.UTF_8)))
          .set("int64Value", "42")
          .set("intValue", "43")
          .set("float64Value", "2.8168")
          .set("floatValue", "2")
          .set("boolValue", "true")
          .set("booleanValue", "true")
          // UTC time
          .set("timestampValue", "1970-01-01T00:00:00.000043Z")
          .set("timeValue", "00:52:07.123456")
          .set("datetimeValue", "2019-08-16T00:52:07.123456")
          .set("dateValue", "2019-08-16")
          .set("numericValue", "23.4")
          .set("bigNumericValue", "2312345.4")
          .set("numericValue2", 23)
          .set("bigNumericValue2", 123456789012345678L)
          .set("arrayValue", REPEATED_BYTES)
          .set("timestampISOValue", "1970-01-01T00:00:00.000+01:00")
          .set("timestampISOValueOffsetHH", "1970-01-01T00:00:00.000+01")
          .set("timestampValueLong", "1234567")
          // UTC time for backwards compatibility
          .set("timestampValueSpace", "1970-01-01 00:00:00.000343")
          .set("timestampValueSpaceUtc", "1970-01-01 00:00:00.000343 UTC")
          .set("timestampValueZoneRegion", "1970-01-01 00:00:00.123456 America/New_York")
          .set("timestampValueSpaceMilli", "1970-01-01 00:00:00.123")
          .set("timestampValueSpaceTrailingZero", "1970-01-01 00:00:00.1230")
          .set("datetimeValueSpace", "2019-08-16 00:52:07.123456");

  private static final Map<String, Object> BASE_ROW_EXPECTED_PROTO_VALUES =
      ImmutableMap.<String, Object>builder()
          .put("stringvalue", "string")
          .put("f", "fff")
          .put("bytesvalue", ByteString.copyFrom("string".getBytes(StandardCharsets.UTF_8)))
          .put("int64value", (long) 42)
          .put("intvalue", (long) 43)
          .put("float64value", (double) 2.8168)
          .put("floatvalue", (double) 2)
          .put("boolvalue", true)
          .put("booleanvalue", true)
          .put("timestampvalue", 43L)
          .put("timevalue", 3497124416L)
          .put("datetimevalue", 142111881387172416L)
          .put("datevalue", (int) LocalDate.of(2019, 8, 16).toEpochDay())
          .put(
              "numericvalue",
              BigDecimalByteStringEncoder.encodeToNumericByteString(new BigDecimal("23.4")))
          .put(
              "bignumericvalue",
              BigDecimalByteStringEncoder.encodeToBigNumericByteString(new BigDecimal("2312345.4")))
          .put(
              "numericvalue2",
              BigDecimalByteStringEncoder.encodeToNumericByteString(new BigDecimal("23")))
          .put(
              "bignumericvalue2",
              BigDecimalByteStringEncoder.encodeToBigNumericByteString(
                  new BigDecimal("123456789012345678")))
          .put("arrayvalue", EXPECTED_PROTO_REPEATED_BYTES)
          .put("timestampisovalue", -3600000000L)
          .put("timestampisovalueoffsethh", -3600000000L)
          .put("timestampvaluelong", 1234567000L)
          .put("timestampvaluespace", 343L)
          .put("timestampvaluespaceutc", 343L)
          .put("timestampvaluezoneregion", 18000123456L)
          .put("timestampvaluespacemilli", 123000L)
          .put("timestampvaluespacetrailingzero", 123000L)
          .put("datetimevaluespace", 142111881387172416L)
          .build();

  private static final Map<String, Object> BASE_ROW_NO_F_EXPECTED_PROTO_VALUES =
      ImmutableMap.<String, Object>builder()
          .put("stringvalue", "string")
          .put("bytesvalue", ByteString.copyFrom("string".getBytes(StandardCharsets.UTF_8)))
          .put("int64value", (long) 42)
          .put("intvalue", (long) 43)
          .put("float64value", (double) 2.8168)
          .put("floatvalue", (double) 2)
          .put("boolvalue", true)
          .put("booleanvalue", true)
          .put("timestampvalue", 43L)
          .put("timevalue", 3497124416L)
          .put("datetimevalue", 142111881387172416L)
          .put("datevalue", (int) LocalDate.parse("2019-08-16").toEpochDay())
          .put(
              "numericvalue",
              BigDecimalByteStringEncoder.encodeToNumericByteString(new BigDecimal("23.4")))
          .put(
              "bignumericvalue",
              BigDecimalByteStringEncoder.encodeToBigNumericByteString(new BigDecimal("2312345.4")))
          .put(
              "numericvalue2",
              BigDecimalByteStringEncoder.encodeToNumericByteString(new BigDecimal("23")))
          .put(
              "bignumericvalue2",
              BigDecimalByteStringEncoder.encodeToBigNumericByteString(
                  new BigDecimal("123456789012345678")))
          .put("arrayvalue", EXPECTED_PROTO_REPEATED_BYTES)
          .put("timestampisovalue", -3600000000L)
          .put("timestampisovalueoffsethh", -3600000000L)
          .put("timestampvaluelong", 1234567000L)
          .put("timestampvaluespace", 343L)
          .put("timestampvaluespaceutc", 343L)
          .put("timestampvaluezoneregion", 18000123456L)
          .put("timestampvaluespacemilli", 123000L)
          .put("timestampvaluespacetrailingzero", 123000L)
          .put("datetimevaluespace", 142111881387172416L)
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
            .set("nestedValueNoF1", BASE_TABLE_ROW_NO_F)
            .set("nestedValueNoF2", BASE_TABLE_ROW_NO_F);

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(NESTED_TABLE_SCHEMA, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(NESTED_TABLE_SCHEMA);
    DynamicMessage msg =
        TableRowToStorageApiProto.messageFromTableRow(
            schemaInformation, descriptor, tableRow, false, false, null, null, -1);
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
        TableRowToStorageApiProto.getDescriptorFromTableSchema(BASE_TABLE_SCHEMA, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(BASE_TABLE_SCHEMA);
    DynamicMessage msg =
        TableRowToStorageApiProto.messageFromTableRow(
            schemaInformation, descriptor, BASE_TABLE_ROW, false, false, null, null, -1);
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
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            REPEATED_MESSAGE_SCHEMA, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(REPEATED_MESSAGE_SCHEMA);
    DynamicMessage msg =
        TableRowToStorageApiProto.messageFromTableRow(
            schemaInformation, descriptor, repeatedRow, false, false, null, null, -1);
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
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            REPEATED_MESSAGE_SCHEMA, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(REPEATED_MESSAGE_SCHEMA);
    DynamicMessage msg =
        TableRowToStorageApiProto.messageFromTableRow(
            schemaInformation, descriptor, repeatedRow, false, false, null, null, -1);

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

  @Test
  public void testIntegerTypeConversion() throws DescriptorValidationException {
    String intFieldName = "int_field";
    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.<TableFieldSchema>builder()
                    .add(
                        new TableFieldSchema()
                            .setType("INTEGER")
                            .setName(intFieldName)
                            .setMode("REQUIRED"))
                    .build());
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(tableSchema);
    SchemaInformation fieldSchema = schemaInformation.getSchemaForField(intFieldName);
    Descriptor schemaDescriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(tableSchema, true, false);
    FieldDescriptor fieldDescriptor = schemaDescriptor.findFieldByName(intFieldName);

    Object[][] validIntValues =
        new Object[][] {
          // Source and expected converted values.
          {"123", 123L},
          {123L, 123L},
          {123, 123L},
          {new BigDecimal("123"), 123L},
          {new BigInteger("123"), 123L}
        };
    for (Object[] validValue : validIntValues) {
      Object sourceValue = validValue[0];
      Long expectedConvertedValue = (Long) validValue[1];
      try {
        Object converted =
            TableRowToStorageApiProto.singularFieldToProtoValue(
                fieldSchema, fieldDescriptor, sourceValue, false, false, () -> null);
        assertEquals(expectedConvertedValue, converted);
      } catch (SchemaConversionException e) {
        fail(
            "Failed to convert value "
                + sourceValue
                + " of type "
                + validValue.getClass()
                + " to INTEGER: "
                + e);
      }
    }

    Object[][] invalidIntValues =
        new Object[][] {
          // Value and expected error message
          {
            "12.123",
            "Column: "
                + intFieldName
                + " (INT64). Value: 12.123 (java.lang.String). Reason: java.lang.NumberFormatException: For input string: \"12.123\""
          },
          {
            Long.toString(Long.MAX_VALUE) + '0',
            "Column: "
                + intFieldName
                + " (INT64). Value: 92233720368547758070 (java.lang.String). Reason: java.lang.NumberFormatException: For input string: \"92233720368547758070\""
          },
          {
            new BigDecimal("12.123"),
            "Column: "
                + intFieldName
                + " (INT64). Value: 12.123 (java.math.BigDecimal). Reason: java.lang.ArithmeticException: Rounding necessary"
          },
          {
            new BigInteger(String.valueOf(Long.MAX_VALUE)).add(new BigInteger("10")),
            "Column: "
                + intFieldName
                + " (INT64). Value: 9223372036854775817 (java.math.BigInteger). Reason: java.lang.ArithmeticException: BigInteger out of long range"
          }
        };
    for (Object[] invalidValue : invalidIntValues) {
      Object sourceValue = invalidValue[0];
      String expectedError = (String) invalidValue[1];
      try {
        TableRowToStorageApiProto.singularFieldToProtoValue(
            fieldSchema, fieldDescriptor, sourceValue, false, false, () -> null);
        fail(
            "Expected to throw an exception converting "
                + sourceValue
                + " of type "
                + invalidValue.getClass()
                + " to INTEGER");
      } catch (SchemaConversionException e) {
        assertEquals("Exception message", expectedError, e.getMessage());
      }
    }
  }

  @Test
  public void testRejectUnknownField() throws Exception {
    TableRow row = new TableRow();
    row.putAll(BASE_TABLE_ROW_NO_F);
    row.set("unknown", "foobar");

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(BASE_TABLE_SCHEMA_NO_F, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(BASE_TABLE_SCHEMA_NO_F);

    thrown.expect(TableRowToStorageApiProto.SchemaConversionException.class);
    TableRowToStorageApiProto.messageFromTableRow(
        schemaInformation, descriptor, row, false, false, null, null, -1);
  }

  @Test
  public void testRejectUnknownFieldF() throws Exception {
    TableRow row = new TableRow();
    List<TableCell> cells = Lists.newArrayList(BASE_TABLE_ROW.getF());
    cells.add(new TableCell().setV("foobar"));
    row.setF(cells);

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(BASE_TABLE_SCHEMA, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(BASE_TABLE_SCHEMA);

    thrown.expect(TableRowToStorageApiProto.SchemaConversionException.class);
    TableRowToStorageApiProto.messageFromTableRow(
        schemaInformation, descriptor, row, false, false, null, null, -1);
  }

  @Test
  public void testRejectUnknownNestedField() throws Exception {
    TableRow rowNoF = new TableRow();
    rowNoF.putAll(BASE_TABLE_ROW_NO_F);
    rowNoF.set("unknown", "foobar");

    TableRow topRow = new TableRow().set("nestedValueNoF1", rowNoF);

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(NESTED_TABLE_SCHEMA, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(NESTED_TABLE_SCHEMA);

    thrown.expect(TableRowToStorageApiProto.SchemaConversionException.class);
    TableRowToStorageApiProto.messageFromTableRow(
        schemaInformation, descriptor, topRow, false, false, null, null, -1);
  }

  @Test
  public void testRejectUnknownNestedFieldF() throws Exception {
    TableRow rowWithF = new TableRow();
    List<TableCell> cells = Lists.newArrayList(BASE_TABLE_ROW.getF());
    cells.add(new TableCell().setV("foobar"));
    rowWithF.setF(cells);

    TableRow topRow = new TableRow().set("nestedValue1", rowWithF);

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(NESTED_TABLE_SCHEMA, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(NESTED_TABLE_SCHEMA);

    thrown.expect(TableRowToStorageApiProto.SchemaConversionException.class);

    TableRowToStorageApiProto.messageFromTableRow(
        schemaInformation, descriptor, topRow, false, false, null, null, -1);
  }

  @Test
  public void testIgnoreUnknownField() throws Exception {
    TableRow row = new TableRow();
    row.putAll(BASE_TABLE_ROW_NO_F);
    row.set("unknown", "foobar");

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(BASE_TABLE_SCHEMA_NO_F, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(BASE_TABLE_SCHEMA_NO_F);

    TableRow ignored = new TableRow();
    TableRowToStorageApiProto.messageFromTableRow(
        schemaInformation, descriptor, row, true, false, ignored, null, -1);
    assertEquals(1, ignored.size());
    assertEquals("foobar", ignored.get("unknown"));
  }

  @Test
  public void testIgnoreUnknownFieldF() throws Exception {
    TableRow row = new TableRow();
    List<TableCell> cells = Lists.newArrayList(BASE_TABLE_ROW.getF());
    cells.add(new TableCell().setV("foobar"));
    row.setF(cells);

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(BASE_TABLE_SCHEMA, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(BASE_TABLE_SCHEMA);

    TableRow ignored = new TableRow();
    TableRowToStorageApiProto.messageFromTableRow(
        schemaInformation, descriptor, row, true, false, ignored, null, -1);
    assertEquals(BASE_TABLE_ROW.getF().size() + 1, ignored.getF().size());
    assertEquals("foobar", ignored.getF().get(BASE_TABLE_ROW.getF().size()).getV());
  }

  @Test
  public void testIgnoreUnknownNestedField() throws Exception {
    TableRow rowNoF = new TableRow();
    rowNoF.putAll(BASE_TABLE_ROW_NO_F);
    rowNoF.set("unknown", "foobar");
    TableRow rowWithF = new TableRow();
    List<TableCell> cells = Lists.newArrayList(BASE_TABLE_ROW.getF());
    cells.add(new TableCell().setV("foobar"));
    rowWithF.setF(cells);
    TableRow topRow =
        new TableRow()
            .set("nestedValueNoF1", rowNoF)
            .set("nestedValue1", rowWithF)
            .set("unknowntop", "foobar");

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(NESTED_TABLE_SCHEMA, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(NESTED_TABLE_SCHEMA);

    TableRow unknown = new TableRow();
    TableRowToStorageApiProto.messageFromTableRow(
        schemaInformation, descriptor, topRow, true, false, unknown, null, -1);
    assertEquals(3, unknown.size());
    assertEquals("foobar", unknown.get("unknowntop"));
    assertEquals(1, ((TableRow) unknown.get("nestedvalue1")).size());
    assertEquals(1, ((TableRow) unknown.get("nestedvaluenof1")).size());
    assertEquals(
        "foobar",
        ((TableRow) unknown.get("nestedvalue1")).getF().get(BASE_TABLE_ROW.getF().size()).getV());
    assertEquals("foobar", ((TableRow) unknown.get("nestedvaluenof1")).get("unknown"));
  }

  @Test
  public void testCdcFields() throws Exception {
    TableRow tableRow =
        new TableRow()
            .set("nestedValue1", BASE_TABLE_ROW)
            .set("nestedValue2", BASE_TABLE_ROW)
            .set("nestedValueNoF1", BASE_TABLE_ROW_NO_F)
            .set("nestedValueNoF2", BASE_TABLE_ROW_NO_F);

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(NESTED_TABLE_SCHEMA, true, true);
    assertNotNull(descriptor.findFieldByName(StorageApiCDC.CHANGE_TYPE_COLUMN));
    assertNotNull(descriptor.findFieldByName(StorageApiCDC.CHANGE_SQN_COLUMN));

    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(NESTED_TABLE_SCHEMA);
    DynamicMessage msg =
        TableRowToStorageApiProto.messageFromTableRow(
            schemaInformation, descriptor, tableRow, false, false, null, "UPDATE", 42);
    assertEquals(6, msg.getAllFields().size());

    Map<String, FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(FieldDescriptor::getName, Functions.identity()));
    assertEquals(6, fieldDescriptors.size());
    assertBaseRecord((DynamicMessage) msg.getField(fieldDescriptors.get("nestedvalue1")), true);
    assertBaseRecord((DynamicMessage) msg.getField(fieldDescriptors.get("nestedvalue2")), true);
    assertBaseRecord((DynamicMessage) msg.getField(fieldDescriptors.get("nestedvaluenof1")), false);
    assertBaseRecord((DynamicMessage) msg.getField(fieldDescriptors.get("nestedvaluenof2")), false);
    assertEquals("UPDATE", msg.getField(fieldDescriptors.get(StorageApiCDC.CHANGE_TYPE_COLUMN)));
    assertEquals(42L, msg.getField(fieldDescriptors.get(StorageApiCDC.CHANGE_SQN_COLUMN)));
  }
}
