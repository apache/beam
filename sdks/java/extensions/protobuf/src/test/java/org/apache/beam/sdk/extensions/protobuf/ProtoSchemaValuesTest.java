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

import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTest.COMPLEX_DEFAULT_ROW;
import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTest.MESSAGE_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTest.PRIMITIVE_DEFAULT_ROW;
import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTest.REPEAT_PRIMITIVE_DEFAULT_ROW;
import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTest.WKT_MESSAGE_DEFAULT_ROW;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Collection of tests for values on Protobuf Messages and Rows. */
@RunWith(Parameterized.class)
public class ProtoSchemaValuesTest {

  private final Message proto;
  private final Row rowObject;
  private SerializableFunction<Message, Row> toRowFunction;
  private SerializableFunction<Row, Message> fromRowFunction;

  public ProtoSchemaValuesTest(String description, Message proto, Row rowObject) {
    this.proto = proto;
    this.rowObject = rowObject;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    List<Object[]> data = new ArrayList<>();
    data.add(
        new Object[] {
          "primitive_int32",
          Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveInt32(Integer.MAX_VALUE).build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_int32", Integer.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "primitive_int64",
          Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveInt64(Long.MAX_VALUE).build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_int64", Long.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "primitive_uint32",
          Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveUint32(Integer.MAX_VALUE).build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_uint32", Integer.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "primitive_uint64",
          Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveUint64(Long.MAX_VALUE).build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_uint64", Long.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "primitive_sint32",
          Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveSint32(Integer.MAX_VALUE).build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_sint32", Integer.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "primitive_sint64",
          Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveSint64(Long.MAX_VALUE).build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_sint64", Long.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "primitive_fixed32",
          Proto3SchemaMessages.Primitive.newBuilder()
              .setPrimitiveFixed32(Integer.MAX_VALUE)
              .build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_fixed32", Integer.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "primitive_fixed64",
          Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveFixed64(Long.MAX_VALUE).build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_fixed64", Long.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "primitive_sfixed32",
          Proto3SchemaMessages.Primitive.newBuilder()
              .setPrimitiveSfixed32(Integer.MAX_VALUE)
              .build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_sfixed32", Integer.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "primitive_sfixed64",
          Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveSfixed64(Long.MAX_VALUE).build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_sfixed64", Long.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "primitive_bool",
          Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveBool(true).build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_bool", true)
        });
    data.add(
        new Object[] {
          "primitive_string",
          Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveString("lovely string").build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_string", "lovely string")
        });
    data.add(
        new Object[] {
          "primitive_bytes",
          Proto3SchemaMessages.Primitive.newBuilder()
              .setPrimitiveBytes(ByteString.copyFrom(new byte[] {(byte) 0x0F}))
              .build(),
          change(PRIMITIVE_DEFAULT_ROW, "primitive_bytes", new byte[] {(byte) 0x0F})
        });

    data.add(
        new Object[] {
          "repeated_double",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedDouble(Double.MAX_VALUE)
              .addRepeatedDouble(0.0)
              .addRepeatedDouble(Double.MIN_VALUE)
              .build(),
          change(
              REPEAT_PRIMITIVE_DEFAULT_ROW,
              "repeated_double",
              Arrays.asList(Double.MAX_VALUE, 0.0, Double.MIN_VALUE))
        });
    data.add(
        new Object[] {
          "repeated_float",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedFloat(Float.MAX_VALUE)
              .addRepeatedFloat((float) 0.0)
              .addRepeatedFloat(Float.MIN_VALUE)
              .build(),
          change(
              REPEAT_PRIMITIVE_DEFAULT_ROW,
              "repeated_float",
              Arrays.asList(Float.MAX_VALUE, (float) 0.0, Float.MIN_VALUE))
        });
    data.add(
        new Object[] {
          "repeated_int32",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedInt32(Integer.MAX_VALUE)
              .addRepeatedInt32(0)
              .addRepeatedInt32(Integer.MIN_VALUE)
              .build(),
          change(
              REPEAT_PRIMITIVE_DEFAULT_ROW,
              "repeated_int32",
              Arrays.asList(Integer.MAX_VALUE, 0, Integer.MIN_VALUE))
        });
    data.add(
        new Object[] {
          "repeated_int64",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedInt64(Long.MAX_VALUE)
              .addRepeatedInt64(0L)
              .addRepeatedInt64(Long.MIN_VALUE)
              .build(),
          change(
              REPEAT_PRIMITIVE_DEFAULT_ROW,
              "repeated_int64",
              Arrays.asList(Long.MAX_VALUE, 0L, Long.MIN_VALUE))
        });

    data.add(
        new Object[] {
          "repeated_uint32",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedUint32(Integer.MAX_VALUE)
              .addRepeatedUint32(0)
              .build(),
          change(
              REPEAT_PRIMITIVE_DEFAULT_ROW, "repeated_uint32", Arrays.asList(Integer.MAX_VALUE, 0))
        });
    data.add(
        new Object[] {
          "repeated_uint64",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedUint64(Long.MAX_VALUE)
              .addRepeatedUint64(0L)
              .build(),
          change(REPEAT_PRIMITIVE_DEFAULT_ROW, "repeated_uint64", Arrays.asList(Long.MAX_VALUE, 0L))
        });

    data.add(
        new Object[] {
          "repeated_sint32",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedSint32(Integer.MAX_VALUE)
              .addRepeatedSint32(0)
              .build(),
          change(
              REPEAT_PRIMITIVE_DEFAULT_ROW, "repeated_sint32", Arrays.asList(Integer.MAX_VALUE, 0))
        });
    data.add(
        new Object[] {
          "repeated_sint64",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedSint64(Long.MAX_VALUE)
              .addRepeatedSint64(0L)
              .build(),
          change(REPEAT_PRIMITIVE_DEFAULT_ROW, "repeated_sint64", Arrays.asList(Long.MAX_VALUE, 0L))
        });
    data.add(
        new Object[] {
          "repeated_fixed32",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedFixed32(Integer.MAX_VALUE)
              .addRepeatedFixed32(0)
              .build(),
          change(
              REPEAT_PRIMITIVE_DEFAULT_ROW, "repeated_fixed32", Arrays.asList(Integer.MAX_VALUE, 0))
        });
    data.add(
        new Object[] {
          "repeated_fixed64",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedFixed64(Long.MAX_VALUE)
              .addRepeatedFixed64(0)
              .build(),
          change(
              REPEAT_PRIMITIVE_DEFAULT_ROW, "repeated_fixed64", Arrays.asList(Long.MAX_VALUE, 0L))
        });
    data.add(
        new Object[] {
          "repeated_sfixed32",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedSfixed32(Integer.MAX_VALUE)
              .addRepeatedSfixed32(0)
              .build(),
          change(
              REPEAT_PRIMITIVE_DEFAULT_ROW,
              "repeated_sfixed32",
              Arrays.asList(Integer.MAX_VALUE, 0))
        });
    data.add(
        new Object[] {
          "repeated_sfixed64",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedSfixed64(Long.MAX_VALUE)
              .addRepeatedSfixed64(0L)
              .build(),
          change(
              REPEAT_PRIMITIVE_DEFAULT_ROW, "repeated_sfixed64", Arrays.asList(Long.MAX_VALUE, 0L))
        });
    data.add(
        new Object[] {
          "repeated_bool",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedBool(true)
              .addRepeatedBool(false)
              .build(),
          change(REPEAT_PRIMITIVE_DEFAULT_ROW, "repeated_bool", Arrays.asList(true, false))
        });
    data.add(
        new Object[] {
          "repeated_string",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedString("foo")
              .addRepeatedString("bar")
              .build(),
          change(REPEAT_PRIMITIVE_DEFAULT_ROW, "repeated_string", Arrays.asList("foo", "bar"))
        });
    data.add(
        new Object[] {
          "repeated_bytes",
          Proto3SchemaMessages.RepeatPrimitive.newBuilder()
              .addRepeatedBytes(ByteString.copyFrom(new byte[] {(byte) 0x0F}))
              .build(),
          change(
              REPEAT_PRIMITIVE_DEFAULT_ROW,
              "repeated_bytes",
              Arrays.asList(new byte[][] {new byte[] {(byte) 0x0F}}))
        });

    data.add(
        new Object[] {
          "nullable_double_nvs",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableDouble(DoubleValue.newBuilder().build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_double", 0.0)
        });
    data.add(
        new Object[] {
          "nullable_double",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableDouble(DoubleValue.newBuilder().setValue(Double.MAX_VALUE).build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_double", Double.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "nullable_float_nvs",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableFloat(FloatValue.newBuilder().build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_float", (float) 0)
        });
    data.add(
        new Object[] {
          "nullable_float",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableFloat(FloatValue.newBuilder().setValue(Float.MAX_VALUE).build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_float", Float.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "nullable_int32",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableInt32(Int32Value.newBuilder().setValue(Integer.MAX_VALUE).build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_int32", Integer.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "nullable_int32_nvs",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableInt32(Int32Value.newBuilder().build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_int32", 0)
        });
    data.add(
        new Object[] {
          "nullable_int64",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableInt64(Int64Value.newBuilder().setValue(Long.MAX_VALUE).build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_int64", Long.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "nullable_int64_nvs",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableInt64(Int64Value.newBuilder().build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_int64", 0L)
        });
    data.add(
        new Object[] {
          "nullable_uint32",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableUint32(UInt32Value.newBuilder().setValue(Integer.MAX_VALUE).build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_uint32", Integer.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "nullable_uint32_nvs",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableUint32(UInt32Value.newBuilder().build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_uint32", 0)
        });
    data.add(
        new Object[] {
          "nullable_uint64",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableUint64(UInt64Value.newBuilder().setValue(Long.MAX_VALUE).build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_uint64", Long.MAX_VALUE)
        });
    data.add(
        new Object[] {
          "nullable_uint64_nvs",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableUint64(UInt64Value.newBuilder().build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_uint64", 0L)
        });
    data.add(
        new Object[] {
          "nullable_bool",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableBool(BoolValue.newBuilder().setValue(Boolean.TRUE).build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_bool", Boolean.TRUE)
        });
    data.add(
        new Object[] {
          "nullable_bool_nvs",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableBool(BoolValue.newBuilder().build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_bool", Boolean.FALSE)
        });
    data.add(
        new Object[] {
          "nullable_string",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableString(StringValue.newBuilder().setValue("bar").build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_string", "bar")
        });
    data.add(
        new Object[] {
          "nullable_string_nvs",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableString(StringValue.newBuilder().build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_string", "")
        });
    data.add(
        new Object[] {
          "nullable_bytes",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableBytes(
                  BytesValue.newBuilder()
                      .setValue(ByteString.copyFrom(new byte[] {(byte) 0x0F}))
                      .build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_bytes", new byte[] {(byte) 0x0F})
        });
    data.add(
        new Object[] {
          "nullable_bytes_nvs",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setNullableBytes(BytesValue.newBuilder().build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "nullable_bytes", new byte[] {})
        });
    data.add(
        new Object[] {
          "wkt_timestamp",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setWktTimestamp(
                  Timestamp.newBuilder().setSeconds(1558680742).setNanos(123000000).build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "wkt_timestamp", new Instant(1558680742123L))
        });
    data.add(
        new Object[] {
          "wkt_timestamp_nvs",
          Proto3SchemaMessages.WktMessage.newBuilder()
              .setWktTimestamp(Timestamp.newBuilder().build())
              .build(),
          change(WKT_MESSAGE_DEFAULT_ROW, "wkt_timestamp", new Instant(0))
        });

    data.add(
        new Object[] {
          "special_enum",
          Proto3SchemaMessages.Complex.newBuilder()
              .setSpecialEnum(Proto3SchemaMessages.Complex.EnumNested.FOO)
              .build(),
          change(COMPLEX_DEFAULT_ROW, "special_enum", "FOO")
        });
    data.add(
        new Object[] {
          "repeated_enum",
          Proto3SchemaMessages.Complex.newBuilder()
              .addRepeatedEnum(Proto3SchemaMessages.Complex.EnumNested.FOO)
              .addRepeatedEnum(Proto3SchemaMessages.Complex.EnumNested.BAR)
              .build(),
          change(COMPLEX_DEFAULT_ROW, "repeated_enum", Arrays.asList("FOO", "BAR"))
        });
    data.add(
        new Object[] {
          "oneof_int32",
          Proto3SchemaMessages.Complex.newBuilder().setOneofInt32(42).build(),
          change(COMPLEX_DEFAULT_ROW, "oneof_int32", 42)
        });
    data.add(
        new Object[] {
          "oneof_bool",
          Proto3SchemaMessages.Complex.newBuilder().setOneofBool(true).build(),
          change(COMPLEX_DEFAULT_ROW, "oneof_bool", Boolean.TRUE)
        });
    data.add(
        new Object[] {
          "oneof_string",
          Proto3SchemaMessages.Complex.newBuilder().setOneofString("one_of_string").build(),
          change(COMPLEX_DEFAULT_ROW, "oneof_string", "one_of_string")
        });
    data.add(
        new Object[] {
          "oneof_primitive",
          Proto3SchemaMessages.Complex.newBuilder()
              .setOneofPrimitive(
                  Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveInt32(42).build())
              .build(),
          change(
              COMPLEX_DEFAULT_ROW,
              "oneof_primitive",
              change(PRIMITIVE_DEFAULT_ROW, "primitive_int32", 42))
        });
    Map<String, Integer> mapInt = new HashMap<>();
    mapInt.put("one", 1);
    mapInt.put("two", 2);
    data.add(
        new Object[] {
          "map_int",
          Proto3SchemaMessages.Complex.newBuilder().putX("one", 1).putX("two", 2).build(),
          change(COMPLEX_DEFAULT_ROW, "x", mapInt)
        });
    Map<String, String> mapString = new HashMap<>();
    mapString.put("one", "eno");
    mapString.put("two", "owt");
    data.add(
        new Object[] {
          "map_int",
          Proto3SchemaMessages.Complex.newBuilder().putY("one", "eno").putY("two", "owt").build(),
          change(COMPLEX_DEFAULT_ROW, "y", mapString)
        });
    Map<String, Row> mapRow = new HashMap<>();
    mapRow.put("one", change(PRIMITIVE_DEFAULT_ROW, "primitive_int32", 1));
    mapRow.put("two", change(PRIMITIVE_DEFAULT_ROW, "primitive_string", "two"));
    data.add(
        new Object[] {
          "map_row",
          Proto3SchemaMessages.Complex.newBuilder()
              .putZ("one", Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveInt32(1).build())
              .putZ(
                  "two",
                  Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveString("two").build())
              .build(),
          change(COMPLEX_DEFAULT_ROW, "z", mapRow)
        });
    data.add(
        new Object[] {
          "subRow",
          Proto3SchemaMessages.Message.newBuilder()
              .setMessage(
                  Proto3SchemaMessages.Primitive.newBuilder()
                      .setPrimitiveString("we love strings")
                      .build())
              .build(),
          Row.withSchema(MESSAGE_SCHEMA)
              .addValue(change(PRIMITIVE_DEFAULT_ROW, "primitive_string", "we love strings"))
              .addValue(null)
              .build()
        });
    data.add(
        new Object[] {
          "subRow+subArrayOfRow",
          Proto3SchemaMessages.Message.newBuilder()
              .setMessage(Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveInt32(42).build())
              .addRepeatedMessage(
                  Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveInt32(69).build())
              .addRepeatedMessage(
                  Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveInt32(70).build())
              .addRepeatedMessage(
                  Proto3SchemaMessages.Primitive.newBuilder().setPrimitiveInt32(71).build())
              .build(),
          Row.withSchema(MESSAGE_SCHEMA)
              .addValue(change(PRIMITIVE_DEFAULT_ROW, "primitive_int32", 42))
              .addArray(
                  change(PRIMITIVE_DEFAULT_ROW, "primitive_int32", 69),
                  change(PRIMITIVE_DEFAULT_ROW, "primitive_int32", 70),
                  change(PRIMITIVE_DEFAULT_ROW, "primitive_int32", 71))
              .build()
        });
    return data;
  }

  private static Row change(Row row, Object field, Object value) {
    int index = -1;
    List<Schema.Field> fields = row.getSchema().getFields();
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field f = fields.get(i);
      if (f.getName().equals(field)) {
        index = i;
        break;
      }
    }

    Object[] objects = row.getValues().toArray();
    objects[index] = value;
    return Row.withSchema(row.getSchema()).addValues(objects).build();
  }

  private void setup() {
    ProtoSchemaProvider protoSchemaProvider = new ProtoSchemaProvider();
    TypeDescriptor typeDescriptor = TypeDescriptor.of(this.proto.getClass());

    toRowFunction = protoSchemaProvider.toRowFunction(typeDescriptor);
    fromRowFunction = protoSchemaProvider.fromRowFunction(typeDescriptor);
  }

  private void setupForDynamicMessage() {
    ProtoDomain domain = ProtoDomain.buildFrom(proto.getDescriptorForType());
    ProtoSchema protoSchema =
        ProtoSchema.newBuilder(domain).forDescriptor(proto.getDescriptorForType());

    toRowFunction = protoSchema.getSchemaCoder().getToRowFunction();
    fromRowFunction = protoSchema.getSchemaCoder().getFromRowFunction();
  }

  @Test
  public void testRowAndBack() {
    setup();
    Row row = toRowFunction.apply(this.proto);
    Message message = fromRowFunction.apply(row);
    assertEquals(proto, message);
  }

  @Test
  public void testToRow() {
    setup();
    Row row = toRowFunction.apply(this.proto);
    assertEquals(rowObject, row);
  }

  @Test
  public void testFromRow() {
    setup();
    Message message = fromRowFunction.apply(this.rowObject);
    assertEquals(this.proto, message);
  }

  @Test
  public void testToRowFromDynamicMessage() {
    setupForDynamicMessage();
    Row row = toRowFunction.apply(DynamicMessage.newBuilder(this.proto).build());
    assertEquals(rowObject, row);
  }

  @Test
  public void testFromRowToDynamicMessage() {
    setupForDynamicMessage();
    Message message = fromRowFunction.apply(this.rowObject);
    assertEquals(this.proto, message);
  }
}
