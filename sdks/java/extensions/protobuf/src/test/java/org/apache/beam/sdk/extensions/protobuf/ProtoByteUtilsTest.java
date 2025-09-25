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
package org.apache.beam.sdk.extensions.protobuf;

import java.util.Objects;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProtoByteUtilsTest {

  private static final String PROTO_STRING_SCHEMA =
      "syntax = \"proto3\";\n"
          + "\n"
          + "message MyMessage {\n"
          + "  int32 id = 1;\n"
          + "  string name = 2;\n"
          + "  bool active = 3;\n"
          + "\n"
          + "  // Nested field\n"
          + "  message Address {\n"
          + "    string street = 1;\n"
          + "    string city = 2;\n"
          + "    string state = 3;\n"
          + "    string zip_code = 4;\n"
          + "  }\n"
          + "\n"
          + "  Address address = 4;\n"
          + "}";

  private static final String PROTO_STRING_PACKAGE_SCHEMA =
      "syntax = \"proto3\";\n"
          + "package com.test.proto;"
          + "\n"
          + "message MyMessage {\n"
          + "  int32 id = 1;\n"
          + "  string name = 2;\n"
          + "  bool active = 3;\n"
          + "\n"
          + "  // Nested field\n"
          + "  message Address {\n"
          + "    string street = 1;\n"
          + "    string city = 2;\n"
          + "    string state = 3;\n"
          + "    string zip_code = 4;\n"
          + "  }\n"
          + "\n"
          + "  Address address = 4;\n"
          + "}";

  private static final String DESCRIPTOR_PATH =
      Objects.requireNonNull(
              ProtoByteUtilsTest.class.getResource(
                  "/proto_byte/file_descriptor/proto_byte_utils.pb"))
          .getPath();

  private static final String MESSAGE_NAME = "MyMessage";

  private static final Schema SCHEMA =
      Schema.builder()
          .addField("id", Schema.FieldType.INT32)
          .addField("name", Schema.FieldType.STRING)
          .addField("active", Schema.FieldType.BOOLEAN)
          .addField(
              "address",
              Schema.FieldType.row(
                  Schema.builder()
                      .addField("city", Schema.FieldType.STRING)
                      .addField("street", Schema.FieldType.STRING)
                      .addField("state", Schema.FieldType.STRING)
                      .addField("zip_code", Schema.FieldType.STRING)
                      .build()))
          .build();

  @Test
  public void testProtoSchemaToBeamSchema() {
    Schema schema = ProtoByteUtils.getBeamSchemaFromProto(DESCRIPTOR_PATH, MESSAGE_NAME);
    Assert.assertEquals(schema.getFieldNames(), SCHEMA.getFieldNames());
  }

  @Test
  public void testProtoSchemaStringToBeamSchema() {
    Schema schema = ProtoByteUtils.getBeamSchemaFromProtoSchema(PROTO_STRING_SCHEMA, "MyMessage");
    Assert.assertEquals(schema.getFieldNames(), SCHEMA.getFieldNames());
  }

  @Test
  public void testProtoSchemaWitPackageStringToBeamSchema() {
    Schema schema =
        ProtoByteUtils.getBeamSchemaFromProtoSchema(
            PROTO_STRING_PACKAGE_SCHEMA, "com.test.proto.MyMessage");
    Assert.assertEquals(schema.getFieldNames(), SCHEMA.getFieldNames());
  }

  @Test
  public void testProtoBytesToRowFunctionGenerateSerializableFunction() {
    SerializableFunction<byte[], Row> protoBytesToRowFunction =
        ProtoByteUtils.getProtoBytesToRowFunction(DESCRIPTOR_PATH, MESSAGE_NAME);
    Assert.assertNotNull(protoBytesToRowFunction);
  }

  @Test
  public void testProtoBytesToRowSchemaStringGenerateSerializableFunction() {
    SerializableFunction<byte[], Row> protoBytesToRowFunction =
        ProtoByteUtils.getProtoBytesToRowFromSchemaFunction(PROTO_STRING_SCHEMA, "MyMessage");
    Assert.assertNotNull(protoBytesToRowFunction);
  }

  @Test(expected = java.lang.RuntimeException.class)
  public void testProtoBytesToRowFunctionReturnsRowFailure() {
    // Create a proto bytes to row function
    SerializableFunction<byte[], Row> protoBytesToRowFunction =
        ProtoByteUtils.getProtoBytesToRowFunction(DESCRIPTOR_PATH, MESSAGE_NAME);

    // Create some test input bytes that are not matching
    byte[] inputBytes = new byte[] {1, 2, 3, 4, 5};

    // Call the proto bytes to row function that should fail because the input does not match
    protoBytesToRowFunction.apply(inputBytes);
  }

  @Test
  public void testProtoBytesToRowFunctionReturnsRowSuccess() {
    // Create a proto bytes to row function
    SerializableFunction<byte[], Row> protoBytesToRowFunction =
        ProtoByteUtils.getProtoBytesToRowFunction(DESCRIPTOR_PATH, MESSAGE_NAME);

    byte[] byteArray = {
      8, -46, 9, 18, 3, 68, 111, 101, 34, 35, 10, 7, 115, 101, 97, 116, 116, 108, 101, 18, 11, 102,
      97, 107, 101, 32, 115, 116, 114, 101, 101, 116, 26, 2, 119, 97, 34, 7, 84, 79, 45, 49, 50, 51,
      52
    };

    Row row = protoBytesToRowFunction.apply(byteArray);
    Assert.assertEquals("Doe", row.getValue("name"));
  }

  @Test
  public void testRowToProtoFunction() {
    Row row =
        Row.withSchema(SCHEMA)
            .withFieldValue("id", 1234)
            .withFieldValue("name", "Doe")
            .withFieldValue("active", false)
            .withFieldValue("address.city", "seattle")
            .withFieldValue("address.street", "fake street")
            .withFieldValue("address.zip_code", "TO-1234")
            .withFieldValue("address.state", "wa")
            .build();

    Assert.assertNotNull(
        ProtoByteUtils.getRowToProtoBytes(DESCRIPTOR_PATH, MESSAGE_NAME).apply(row));
  }

  @Test
  public void testRowToProtoSchemaFunction() {
    Row row =
        Row.withSchema(SCHEMA)
            .withFieldValue("id", 1234)
            .withFieldValue("name", "Doe")
            .withFieldValue("active", false)
            .withFieldValue("address.city", "seattle")
            .withFieldValue("address.street", "fake street")
            .withFieldValue("address.zip_code", "TO-1234")
            .withFieldValue("address.state", "wa")
            .build();

    Assert.assertNotNull(
        ProtoByteUtils.getRowToProtoBytesFromSchema(PROTO_STRING_SCHEMA, "MyMessage").apply(row));
  }

  @Test
  public void testRowToProtoSchemaWithPackageFunction() {
    Row row =
        Row.withSchema(SCHEMA)
            .withFieldValue("id", 1234)
            .withFieldValue("name", "Doe")
            .withFieldValue("active", false)
            .withFieldValue("address.city", "seattle")
            .withFieldValue("address.street", "fake street")
            .withFieldValue("address.zip_code", "TO-1234")
            .withFieldValue("address.state", "wa")
            .build();

    byte[] byteArray = {
      8, -46, 9, 18, 3, 68, 111, 101, 34, 35, 10, 7, 115, 101, 97, 116, 116, 108, 101, 18, 11, 102,
      97, 107, 101, 32, 115, 116, 114, 101, 101, 116, 26, 2, 119, 97, 34, 7, 84, 79, 45, 49, 50, 51,
      52
    };

    byte[] resultBytes =
        ProtoByteUtils.getRowToProtoBytesFromSchema(
                PROTO_STRING_PACKAGE_SCHEMA, "com.test.proto.MyMessage")
            .apply(row);

    Assert.assertNotNull(resultBytes);
    Assert.assertArrayEquals(byteArray, resultBytes);
  }
}
