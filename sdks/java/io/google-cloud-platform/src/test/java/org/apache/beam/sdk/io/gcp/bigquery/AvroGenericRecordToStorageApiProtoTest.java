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

import com.google.protobuf.ByteString;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.schemas.utils.AvroUtils.TypeWithNullability;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Functions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
/**
 * Unit tests form {@link AvroGenericRecordToStorageApiProto}.
 */
public class AvroGenericRecordToStorageApiProtoTest {

  enum TEST_ENUM {
    ONE, TWO, RED, BLUE
  }

  private static final String[] TEST_ENUM_STRS
          = EnumSet.allOf(TEST_ENUM.class).stream().map(TEST_ENUM::name).toArray(String[]::new);
  private static final Schema BASE_SCHEMA
          = SchemaBuilder
                  .record("TestRecord")
                  .fields()
                  .optionalBytes("bytesValue")
                  .requiredInt("int32Value")
                  .optionalLong("int64Value")
                  .optionalFloat("floatValue")
                  .optionalDouble("doubleValue")
                  .optionalString("stringValue")
                  .optionalBoolean("booleanValue")
                  .name("arrayValue").type().array().items().stringType().noDefault()
                  .name("enumValue").type().optional().enumeration("testEnum").symbols(TEST_ENUM_STRS)
                  .name("fixedValue").type().fixed("MD5").size(16).noDefault()
                  .endRecord();

  private static final DescriptorProto BASE_SCHEMA_PROTO
          = DescriptorProto.newBuilder()
                  .addField(
                          FieldDescriptorProto.newBuilder()
                                  .setName("bytesvalue")
                                  .setNumber(1)
                                  .setType(Type.TYPE_BYTES)
                                  .setLabel(Label.LABEL_OPTIONAL)
                                  .build())
                  .addField(
                          FieldDescriptorProto.newBuilder()
                                  .setName("int32value")
                                  .setNumber(2)
                                  .setType(Type.TYPE_INT32)
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
                                  .setName("floatvalue")
                                  .setNumber(4)
                                  .setType(Type.TYPE_FLOAT)
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

  private static final byte[] BYTES = "BYTE BYTE BYTE".getBytes(StandardCharsets.UTF_8);
  private static final Schema NESTED_SCHEMA
          = SchemaBuilder
                  .record("TestNestedRecord")
                  .fields()
                  .name("nested").type().optional().type(BASE_SCHEMA)
                  .name("nestedArray").type().array().items(BASE_SCHEMA).noDefault()
                  .endRecord();
  private static GenericRecord BASE_RECORD;
  private static Map<String, Object> BASE_PROTO_EXPECTED_FIELDS;
  private static GenericRecord NESTED_RECORD;

  static {
    try {
      byte[] MD5 = MessageDigest.getInstance("MD5").digest(BYTES);
      BASE_RECORD = new GenericRecordBuilder(BASE_SCHEMA)
              .set("bytesValue", BYTES)
              .set("int32Value", (int) 3)
              .set("int64Value", (long) 4)
              .set("floatValue", (float) 3.14)
              .set("doubleValue", (double) 2.68)
              .set("stringValue", "I am a string. Hear me roar.")
              .set("booleanValue", true)
              .set("arrayValue", ImmutableList.of("one", "two", "red", "blue"))
              .set("enumValue",
                      new GenericData.EnumSymbol(
                              BASE_SCHEMA.getField("enumValue").schema(),
                              TEST_ENUM.TWO))
              .set("fixedValue",
                      new GenericData.Fixed(
                              BASE_SCHEMA.getField("fixedValue").schema(),
                              MD5))
              .build();
      BASE_PROTO_EXPECTED_FIELDS = ImmutableMap.<String, Object>builder()
              .put("bytesvalue", ByteString.copyFrom(BYTES))
              .put("int32value", (int) 3)
              .put("int64value", (long) 4)
              .put("floatvalue", (float) 3.14)
              .put("doublevalue", (double) 2.68)
              .put("stringvalue", "I am a string. Hear me roar.")
              .put("booleanvalue", true)
              .put("arrayvalue", ImmutableList.of("one", "two", "red", "blue"))
              .put("enumvalue", TEST_ENUM_STRS[1])
              .put("fixedvalue", ByteString.copyFrom(MD5))
              .build();
      NESTED_RECORD
              = new GenericRecordBuilder(NESTED_SCHEMA)
                      .set("nested", BASE_RECORD)
                      .set("nestedArray", ImmutableList.of(BASE_RECORD, BASE_RECORD))
                      .build();
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException("Error initializing test data", ex);
    }
  }

  @Test
  public void testDescriptorFromSchema() {
    DescriptorProto descriptor
            = AvroGenericRecordToStorageApiProto.descriptorSchemaFromAvroSchema(BASE_SCHEMA);
    Map<String, Type> types
            = descriptor.getFieldList().stream()
                    .collect(
                            Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    Map<String, Type> expectedTypes
            = BASE_SCHEMA_PROTO.getFieldList().stream()
                    .collect(
                            Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedTypes, types);

    Map<String, String> nameMapping
            = BASE_SCHEMA.getFields().stream()
                    .collect(Collectors.toMap(f -> f.name().toLowerCase(), f -> f.name()));
    descriptor
            .getFieldList()
            .forEach(
                    p -> {
                      TypeWithNullability fieldSchema
                      = TypeWithNullability.create(BASE_SCHEMA.getField(nameMapping.get(p.getName())).schema());
                      Label label
                      = fieldSchema.getType().getType() == Schema.Type.ARRAY
                      ? Label.LABEL_REPEATED
                      : fieldSchema.isNullable() ? Label.LABEL_OPTIONAL : Label.LABEL_REQUIRED;
                      assertEquals(label, p.getLabel());
                    });
  }

  @Test
  public void testNestedFromSchema() {
    DescriptorProto descriptor
            = AvroGenericRecordToStorageApiProto.descriptorSchemaFromAvroSchema(NESTED_SCHEMA);
    Map<String, Type> expectedBaseTypes
            = BASE_SCHEMA_PROTO.getFieldList().stream()
                    .collect(
                            Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));

    Map<String, Type> types
            = descriptor.getFieldList().stream()
                    .collect(
                            Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    Map<String, String> typeNames
            = descriptor.getFieldList().stream()
                    .collect(
                            Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getTypeName));
    Map<String, Label> typeLabels
            = descriptor.getFieldList().stream()
                    .collect(
                            Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getLabel));

    assertEquals(2, types.size());

    Map<String, DescriptorProto> nestedTypes
            = descriptor.getNestedTypeList().stream()
                    .collect(Collectors.toMap(DescriptorProto::getName, Functions.identity()));
    assertEquals(2, nestedTypes.size());
    assertEquals(Type.TYPE_MESSAGE, types.get("nested"));
    assertEquals(Label.LABEL_OPTIONAL, typeLabels.get("nested"));
    String nestedTypeName1 = typeNames.get("nested");
    Map<String, Type> nestedTypes1
            = nestedTypes.get(nestedTypeName1).getFieldList().stream()
                    .collect(
                            Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedBaseTypes, nestedTypes1);

    assertEquals(Type.TYPE_MESSAGE, types.get("nestedarray"));
    assertEquals(Label.LABEL_REPEATED, typeLabels.get("nestedarray"));
    String nestedTypeName2 = typeNames.get("nestedarray");
    Map<String, Type> nestedTypes2
            = nestedTypes.get(nestedTypeName2).getFieldList().stream()
                    .collect(
                            Collectors.toMap(FieldDescriptorProto::getName, FieldDescriptorProto::getType));
    assertEquals(expectedBaseTypes, nestedTypes2);

  }

  private void assertBaseRecord(DynamicMessage msg) {
    Map<String, Object> recordFields
            = msg.getAllFields().entrySet().stream()
                    .collect(
                            Collectors.toMap(entry -> entry.getKey().getName(), entry -> entry.getValue()));
    assertEquals(BASE_PROTO_EXPECTED_FIELDS, recordFields);
  }

  @Test
  public void testMessageFromTableRow() throws Exception {
    Descriptors.Descriptor descriptor
            = AvroGenericRecordToStorageApiProto.getDescriptorFromSchema(NESTED_SCHEMA);
    DynamicMessage msg
            = AvroGenericRecordToStorageApiProto.messageFromGenericRecord(
                    descriptor, NESTED_RECORD);

    assertEquals(2, msg.getAllFields().size());

    Map<String, Descriptors.FieldDescriptor> fieldDescriptors
            = descriptor.getFields().stream()
                    .collect(Collectors.toMap(Descriptors.FieldDescriptor::getName, Functions.identity()));
    DynamicMessage nestedMsg = (DynamicMessage) msg.getField(fieldDescriptors.get("nested"));
    assertBaseRecord(nestedMsg);
  }
}
