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

import static org.junit.Assert.assertEquals;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.Objects;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Collection of standard tests for Protobuf Schema support. */
@RunWith(JUnit4.class)
public class ProtoSchemaTest {

  private static final Schema PRIMITIVE_SCHEMA =
      Schema.builder()
          .addDoubleField("primitive_double")
          .addFloatField("primitive_float")
          .addInt32Field("primitive_int32")
          .addInt64Field("primitive_int64")
          .addInt32Field("primitive_uint32")
          .addInt64Field("primitive_uint64")
          .addInt32Field("primitive_sint32")
          .addInt64Field("primitive_sint64")
          .addInt32Field("primitive_fixed32")
          .addInt64Field("primitive_fixed64")
          .addInt32Field("primitive_sfixed32")
          .addInt64Field("primitive_sfixed64")
          .addBooleanField("primitive_bool")
          .addStringField("primitive_string")
          .addByteArrayField("primitive_bytes")
          .build();
  static final Row PRIMITIVE_DEFAULT_ROW =
      Row.withSchema(PRIMITIVE_SCHEMA)
          .addValue((double) 0)
          .addValue((float) 0)
          .addValue(0)
          .addValue(0L)
          .addValue(0)
          .addValue(0L)
          .addValue(0)
          .addValue(0L)
          .addValue(0)
          .addValue(0L)
          .addValue(0)
          .addValue(0L)
          .addValue(Boolean.FALSE)
          .addValue("")
          .addValue(new byte[] {})
          .build();
  static final Schema MESSAGE_SCHEMA =
      Schema.builder()
          .addField("message", Schema.FieldType.row(PRIMITIVE_SCHEMA).withNullable(true))
          .addField(
              "repeated_message",
              Schema.FieldType.array(
                      // TODO: are the nullable's correct
                      Schema.FieldType.row(PRIMITIVE_SCHEMA).withNullable(true))
                  .withNullable(true))
          .build();
  private static final Row MESSAGE_DEFAULT_ROW =
      Row.withSchema(MESSAGE_SCHEMA).addValue(null).addValue(null).build();
  private static final Schema REPEAT_PRIMITIVE_SCHEMA =
      Schema.builder()
          .addField(
              "repeated_double", Schema.FieldType.array(Schema.FieldType.DOUBLE).withNullable(true))
          .addField(
              "repeated_float", Schema.FieldType.array(Schema.FieldType.FLOAT).withNullable(true))
          .addField(
              "repeated_int32", Schema.FieldType.array(Schema.FieldType.INT32).withNullable(true))
          .addField(
              "repeated_int64", Schema.FieldType.array(Schema.FieldType.INT64).withNullable(true))
          .addField(
              "repeated_uint32", Schema.FieldType.array(Schema.FieldType.INT32).withNullable(true))
          .addField(
              "repeated_uint64", Schema.FieldType.array(Schema.FieldType.INT64).withNullable(true))
          .addField(
              "repeated_sint32", Schema.FieldType.array(Schema.FieldType.INT32).withNullable(true))
          .addField(
              "repeated_sint64", Schema.FieldType.array(Schema.FieldType.INT64).withNullable(true))
          .addField(
              "repeated_fixed32", Schema.FieldType.array(Schema.FieldType.INT32).withNullable(true))
          .addField(
              "repeated_fixed64", Schema.FieldType.array(Schema.FieldType.INT64).withNullable(true))
          .addField(
              "repeated_sfixed32",
              Schema.FieldType.array(Schema.FieldType.INT32).withNullable(true))
          .addField(
              "repeated_sfixed64",
              Schema.FieldType.array(Schema.FieldType.INT64).withNullable(true))
          .addField(
              "repeated_bool", Schema.FieldType.array(Schema.FieldType.BOOLEAN).withNullable(true))
          .addField(
              "repeated_string", Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true))
          .addField(
              "repeated_bytes", Schema.FieldType.array(Schema.FieldType.BYTES).withNullable(true))
          .build();
  static final Row REPEAT_PRIMITIVE_DEFAULT_ROW =
      Row.withSchema(REPEAT_PRIMITIVE_SCHEMA)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .build();
  private static final Schema COMPLEX_SCHEMA =
      Schema.builder()
          .addField("special_enum", Schema.FieldType.STRING)
          .addField(
              "repeated_enum", Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true))
          .addField("oneof_int32", Schema.FieldType.INT32.withNullable(true))
          .addField("oneof_bool", Schema.FieldType.BOOLEAN.withNullable(true))
          .addField("oneof_string", Schema.FieldType.STRING.withNullable(true))
          .addField("oneof_primitive", Schema.FieldType.row(PRIMITIVE_SCHEMA).withNullable(true))
          .addField(
              "x",
              Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.INT32)
                  .withNullable(true))
          .addField(
              "y",
              Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING)
                  .withNullable(true))
          .addField(
              "z",
              // TODO: null in map, does it make sense.
              Schema.FieldType.map(
                      Schema.FieldType.STRING,
                      Schema.FieldType.row(PRIMITIVE_SCHEMA).withNullable(true))
                  .withNullable(true))
          .addField("oneof_int64", Schema.FieldType.INT64.withNullable(true))
          .addField("oneof_double", Schema.FieldType.DOUBLE.withNullable(true))
          .build();
  static final Row COMPLEX_DEFAULT_ROW =
      Row.withSchema(COMPLEX_SCHEMA)
          .addValue("UNKNOWN")
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .build();
  private static final Schema WKT_MESSAGE_SCHEMA =
      Schema.builder()
          .addField("nullable_double", Schema.FieldType.DOUBLE.withNullable(true))
          .addField("nullable_float", Schema.FieldType.FLOAT.withNullable(true))
          .addField("nullable_int32", Schema.FieldType.INT32.withNullable(true))
          .addField("nullable_int64", Schema.FieldType.INT64.withNullable(true))
          .addField("nullable_uint32", Schema.FieldType.INT32.withNullable(true))
          .addField("nullable_uint64", Schema.FieldType.INT64.withNullable(true))
          // xxx
          .addField("nullable_bool", Schema.FieldType.BOOLEAN.withNullable(true))
          .addField("nullable_string", Schema.FieldType.STRING.withNullable(true))
          .addField("nullable_bytes", Schema.FieldType.BYTES.withNullable(true))
          //
          .addField("wkt_timestamp", Schema.FieldType.DATETIME.withNullable(true))
          .build();
  static final Row WKT_MESSAGE_DEFAULT_ROW =
      Row.withSchema(WKT_MESSAGE_SCHEMA)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .build();

  @Test
  public void testPrimitiveSchema() {
    Schema schema =
        new ProtoSchemaProvider()
            .schemaFor(TypeDescriptor.of(Proto3SchemaMessages.Primitive.class));
    assertEquals(PRIMITIVE_SCHEMA, schema);
  }

  @Test
  public void testPrimitiveDefaultRow() {
    SerializableFunction<Proto3SchemaMessages.Primitive, Row> toRowFunction =
        new ProtoSchemaProvider()
            .toRowFunction(TypeDescriptor.of(Proto3SchemaMessages.Primitive.class));
    Row row = toRowFunction.apply(Proto3SchemaMessages.Primitive.newBuilder().build());
    assertEquals(PRIMITIVE_DEFAULT_ROW, row);
  }

  @Test
  public void testMessageSchema() {
    Schema schema =
        new ProtoSchemaProvider().schemaFor(TypeDescriptor.of(Proto3SchemaMessages.Message.class));
    assertEquals(MESSAGE_SCHEMA, schema);
  }

  @Test
  public void testMessageDefaultRow() {
    SerializableFunction<Proto3SchemaMessages.Message, Row> toRowFunction =
        new ProtoSchemaProvider()
            .toRowFunction(TypeDescriptor.of(Proto3SchemaMessages.Message.class));
    Row row = toRowFunction.apply(Proto3SchemaMessages.Message.newBuilder().build());
    assertEquals(MESSAGE_DEFAULT_ROW, row);
  }

  @Test
  public void testRepeatPrimitiveSchema() {
    Schema schema =
        new ProtoSchemaProvider()
            .schemaFor(TypeDescriptor.of(Proto3SchemaMessages.RepeatPrimitive.class));
    assertEquals(REPEAT_PRIMITIVE_SCHEMA, schema);
  }

  @Test
  public void testRepeatPrimitiveDefaultRow() {
    SerializableFunction<Proto3SchemaMessages.RepeatPrimitive, Row> toRowFunction =
        new ProtoSchemaProvider()
            .toRowFunction(TypeDescriptor.of(Proto3SchemaMessages.RepeatPrimitive.class));
    Row row = toRowFunction.apply(Proto3SchemaMessages.RepeatPrimitive.newBuilder().build());
    assertEquals(REPEAT_PRIMITIVE_DEFAULT_ROW, row);
  }

  @Test
  public void testComplexSchema() {
    Schema schema =
        new ProtoSchemaProvider().schemaFor(TypeDescriptor.of(Proto3SchemaMessages.Complex.class));
    assertEquals(COMPLEX_SCHEMA, schema);
  }

  @Test
  public void testComplexDefaultRow() {
    SerializableFunction<Proto3SchemaMessages.Complex, Row> toRowFunction =
        new ProtoSchemaProvider()
            .toRowFunction(TypeDescriptor.of(Proto3SchemaMessages.Complex.class));
    Row row = toRowFunction.apply(Proto3SchemaMessages.Complex.newBuilder().build());
    assertEquals(COMPLEX_DEFAULT_ROW, row);
  }

  @Test
  public void testWktMessageSchema() {
    Schema schema =
        new ProtoSchemaProvider()
            .schemaFor(TypeDescriptor.of(Proto3SchemaMessages.WktMessage.class));
    assertEquals(WKT_MESSAGE_SCHEMA, schema);
  }

  @Test
  public void testWktMessageDefaultRow() {
    SerializableFunction<Proto3SchemaMessages.WktMessage, Row> toRowFunction =
        new ProtoSchemaProvider()
            .toRowFunction(TypeDescriptor.of(Proto3SchemaMessages.WktMessage.class));
    Row row = toRowFunction.apply(Proto3SchemaMessages.WktMessage.newBuilder().build());
    assertEquals(WKT_MESSAGE_DEFAULT_ROW, row);
  }

  @Test
  public void testCoder() throws Exception {
    SchemaCoder<Message> schemaCoder =
        ProtoSchema.newBuilder().forType(Proto3SchemaMessages.Complex.class).getSchemaCoder();
    RowCoder rowCoder = RowCoder.of(schemaCoder.getSchema());

    byte[] schemaCoderBytes = SerializableUtils.serializeToByteArray(schemaCoder);
    SchemaCoder<Message> schemaCoderCoded =
        (SchemaCoder<Message>) SerializableUtils.deserializeFromByteArray(schemaCoderBytes, "");
    byte[] rowCoderBytes = SerializableUtils.serializeToByteArray(rowCoder);
    RowCoder rowCoderCoded =
        (RowCoder) SerializableUtils.deserializeFromByteArray(rowCoderBytes, "");

    Proto3SchemaMessages.Complex message =
        Proto3SchemaMessages.Complex.newBuilder()
            .setOneofString("foobar")
            .setSpecialEnum(Proto3SchemaMessages.Complex.EnumNested.FOO)
            .build();

    Row row = schemaCoder.getToRowFunction().apply(message);
    byte[] rowBytes = CoderUtils.encodeToByteArray(rowCoder, row);

    Row rowCoded = CoderUtils.decodeFromByteArray(rowCoderCoded, rowBytes);
    assertEquals(row, rowCoded);

    Message messageVerify = schemaCoder.getFromRowFunction().apply(rowCoded);
    assertEquals(message, messageVerify);

    Message messageCoded = schemaCoderCoded.getFromRowFunction().apply(rowCoded);
    assertEquals(message, messageCoded);
  }

  @Test
  public void testCoderOnDynamic() throws Exception {
    Descriptors.Descriptor descriptor = Proto3SchemaMessages.Complex.getDescriptor();
    Descriptors.FieldDescriptor oneofString = descriptor.findFieldByName("oneof_string");
    Descriptors.FieldDescriptor specialEnum = descriptor.findFieldByName("special_enum");

    SchemaCoder<Message> schemaCoder =
        ProtoSchema.newBuilder(ProtoDomain.buildFrom(descriptor))
            .forDescriptor(descriptor)
            .getSchemaCoder();
    RowCoder rowCoder = RowCoder.of(schemaCoder.getSchema());

    byte[] schemaCoderBytes = SerializableUtils.serializeToByteArray(schemaCoder);
    SchemaCoder<Message> schemaCoderCoded =
        (SchemaCoder<Message>) SerializableUtils.deserializeFromByteArray(schemaCoderBytes, "");
    byte[] rowCoderBytes = SerializableUtils.serializeToByteArray(rowCoder);
    RowCoder rowCoderCoded =
        (RowCoder) SerializableUtils.deserializeFromByteArray(rowCoderBytes, "");

    DynamicMessage message =
        DynamicMessage.newBuilder(descriptor)
            .setField(oneofString, "foobar")
            .setField(specialEnum, Proto3SchemaMessages.Complex.EnumNested.FOO.getValueDescriptor())
            .build();

    Row row = schemaCoder.getToRowFunction().apply(message);
    byte[] rowBytes = CoderUtils.encodeToByteArray(rowCoder, row);

    Row rowCoded = CoderUtils.decodeFromByteArray(rowCoderCoded, rowBytes);
    assertEquals(row, rowCoded);

    Message messageVerify = schemaCoder.getFromRowFunction().apply(rowCoded);
    assertEquals(message, messageVerify);

    Message messageCoded = schemaCoderCoded.getFromRowFunction().apply(rowCoded);
    Descriptors.FieldDescriptor oneofStringCoded =
        messageCoded.getDescriptorForType().findFieldByName("oneof_string");
    Descriptors.FieldDescriptor specialEnumCoded =
        messageCoded.getDescriptorForType().findFieldByName("special_enum");
    assertEquals(message.getField(oneofString), messageCoded.getField(oneofStringCoded));
    assertEquals(
        ((Descriptors.EnumValueDescriptor) message.getField(specialEnum)).getFullName(),
        ((Descriptors.EnumValueDescriptor) messageCoded.getField(specialEnumCoded)).getFullName());
  }

  @Test
  public void testLogicalTypeRegistration() {
    ProtoSchemaProvider protoSchemaProvider =
        new ProtoSchemaProvider(
            ProtoSchema.newBuilder()
                .addTypeMapping("proto3_schema_messages.LatLng", LatLngOverlay.class));

    SerializableFunction<Proto3SchemaMessages.LogicalTypes, Row> toRowFunction =
        protoSchemaProvider.toRowFunction(
            TypeDescriptor.of(Proto3SchemaMessages.LogicalTypes.class));
    Row row =
        toRowFunction.apply(
            Proto3SchemaMessages.LogicalTypes.newBuilder()
                .setGps(
                    Proto3SchemaMessages.LatLng.newBuilder()
                        .setLatitude(1.2)
                        .setLongitude(3.5)
                        .build())
                .build());
    assertEquals(new LatLng(1.2, 3.5), row.getValue("gps"));
  }

  /** Example Java type for testing LogicalTypes. */
  public static class LatLng {
    double lat;
    double lng;

    public LatLng(Double lat, Double lng) {
      this.lat = lat;
      this.lng = lng;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LatLng latLng = (LatLng) o;
      return Double.compare(latLng.lat, lat) == 0 && Double.compare(latLng.lng, lng) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(lat, lng);
    }
  }

  /**
   * Example of a LogicalType converter. It will make sure that the type is convertible to a
   * FieldType.
   */
  public static class LatLngLogicalType implements Schema.LogicalType<LatLng, Row> {

    static final Schema BASE_TYPE =
        Schema.builder()
            .addField("lat", Schema.FieldType.DOUBLE)
            .addField("lng", Schema.FieldType.DOUBLE)
            .build();

    @Override
    public String getIdentifier() {
      return "LatLngLogicalType";
    }

    @Override
    public Schema.FieldType getBaseType() {
      return Schema.FieldType.row(LatLngLogicalType.BASE_TYPE);
    }

    @Override
    public Row toBaseType(LatLng input) {
      return Row.withSchema(BASE_TYPE).addValue(input.lat).addValue(input.lng).build();
    }

    @Override
    public LatLng toInputType(Row base) {
      return new LatLng(base.getDouble("lat"), base.getDouble("lng"));
    }
  }

  /** Custom Protobuf field overlay that returns a custom LogicalType. */
  public static class LatLngOverlay implements ProtoFieldOverlay<LatLng> {
    private Descriptors.FieldDescriptor fieldDescriptor;
    private Descriptors.FieldDescriptor latitudeFieldDescriptor;
    private Descriptors.FieldDescriptor longitudeFieldDescriptor;

    public LatLngOverlay(Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
      latitudeFieldDescriptor = fieldDescriptor.getMessageType().findFieldByName("latitude");
      longitudeFieldDescriptor = fieldDescriptor.getMessageType().findFieldByName("longitude");
    }

    @Override
    public LatLng get(Message message) {
      Message latLngMessage = (Message) message.getField(fieldDescriptor);
      return new LatLng(
          (double) latLngMessage.getField(latitudeFieldDescriptor),
          (double) latLngMessage.getField(longitudeFieldDescriptor));
    }

    @Override
    public String name() {
      return fieldDescriptor.getName();
    }

    @Override
    public LatLng convertGetObject(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
      return null;
    }

    @Override
    public void set(Message.Builder message, LatLng value) {
      message.setField(
          fieldDescriptor,
          Proto3SchemaMessages.LatLng.newBuilder()
              .setLongitude(value.lng)
              .setLatitude(value.lat)
              .build());
    }

    @Override
    public Object convertSetObject(Descriptors.FieldDescriptor fieldDescriptor, Object value) {
      return null;
    }

    @Override
    public Schema.Field getSchemaField() {
      return Schema.Field.of(
          fieldDescriptor.getName(), Schema.FieldType.logicalType(new LatLngLogicalType()));
    }
  }

  @Test
  public void testMessageWithMetaSchema() {
    Schema schema =
        new ProtoSchemaProvider()
            .schemaFor(TypeDescriptor.of(Proto3SchemaMessages.MessageWithMeta.class));
    Schema.Field fieldWithDescription = schema.getField("field_with_description");
    assertEquals(
        "Cool field",
        fieldWithDescription
            .getType()
            .getMetadataString("proto3_schema_messages.field_meta.description"));
    assertEquals(
        "0",
        fieldWithDescription
            .getType()
            .getMetadataString("proto3_schema_messages.field_meta.foobar"));
    assertEquals("", fieldWithDescription.getType().getMetadataString("deprecated"));

    Schema.Field fieldWithFoobar = schema.getField("field_with_foobar");
    assertEquals(
        "",
        fieldWithFoobar
            .getType()
            .getMetadataString("proto3_schema_messages.field_meta.description"));
    assertEquals(
        "42",
        fieldWithFoobar.getType().getMetadataString("proto3_schema_messages.field_meta.foobar"));
    assertEquals("", fieldWithFoobar.getType().getMetadataString("deprecated"));

    Schema.Field fieldWithDeprecation = schema.getField("field_with_deprecation");
    assertEquals(
        "",
        fieldWithDeprecation
            .getType()
            .getMetadataString("proto3_schema_messages.field_meta.description"));
    assertEquals(
        "",
        fieldWithDeprecation
            .getType()
            .getMetadataString("proto3_schema_messages.field_meta.foobar"));
    assertEquals("true", fieldWithDeprecation.getType().getMetadataString("deprecated"));
  }

  @Test
  public void testMessageWithMetaDynamicSchema() throws IOException {
    ProtoDomain domain = ProtoDomain.buildFrom(getClass().getResourceAsStream("test_option_v1.pb"));
    Descriptors.Descriptor descriptor = domain.getDescriptor("test.option.v1.MessageWithOptions");
    Schema schema = ProtoSchema.newBuilder(domain).forDescriptor(descriptor).getSchema();
    Schema.Field field;
    field = schema.getField("field_with_fieldoption_double");
    assertEquals("100.1", field.getType().getMetadataString("test.option.v1.fieldoption_double"));
    field = schema.getField("field_with_fieldoption_float");
    assertEquals("101.2", field.getType().getMetadataString("test.option.v1.fieldoption_float"));
    field = schema.getField("field_with_fieldoption_int32");
    assertEquals("102", field.getType().getMetadataString("test.option.v1.fieldoption_int32"));
    field = schema.getField("field_with_fieldoption_int64");
    assertEquals("103", field.getType().getMetadataString("test.option.v1.fieldoption_int64"));
    field = schema.getField("field_with_fieldoption_bool");
    assertEquals("true", field.getType().getMetadataString("test.option.v1.fieldoption_bool"));
    field = schema.getField("field_with_fieldoption_string");
    assertEquals("Oh yeah", field.getType().getMetadataString("test.option.v1.fieldoption_string"));
    field = schema.getField("field_with_fieldoption_enum");
    assertEquals("ENUM1", field.getType().getMetadataString("test.option.v1.fieldoption_enum"));
    field = schema.getField("field_with_fieldoption_repeated_string");
    assertEquals(
        "Oh yeah\nOh no",
        field.getType().getMetadataString("test.option.v1.fieldoption_repeated_string"));
  }
}
