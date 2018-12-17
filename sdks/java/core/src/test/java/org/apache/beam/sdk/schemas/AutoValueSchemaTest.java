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
package org.apache.beam.sdk.schemas;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.auto.value.AutoValue;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.utils.SchemaTestUtils;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Test;

public class AutoValueSchemaTest {
  static final DateTime DATE = DateTime.parse("1979-03-14");
  static final byte[] BYTE_ARRAY = "bytearray".getBytes(Charset.defaultCharset());
  static final StringBuilder STRING_BUILDER = new StringBuilder("stringbuilder");

  public static final Schema SIMPLE_SCHEMA =
      Schema.builder()
          .addStringField("str")
          .addByteField("aByte")
          .addInt16Field("aShort")
          .addInt32Field("anInt")
          .addInt64Field("aLong")
          .addBooleanField("aBoolean")
          .addDateTimeField("dateTime")
          .addDateTimeField("instant")
          .addByteArrayField("bytes")
          .addByteArrayField("byteBuffer")
          .addDecimalField("bigDecimal")
          .addStringField("stringBuilder")
          .build();

  private Row createSimpleRow(String name) {
    return Row.withSchema(SIMPLE_SCHEMA)
        .addValues(
            name,
            (byte) 1,
            (short) 2,
            3,
            4L,
            true,
            DATE,
            DATE,
            BYTE_ARRAY,
            BYTE_ARRAY,
            BigDecimal.ONE,
            new StringBuilder(name).append("builder").toString())
        .build();
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class SimpleAutoValue {
    public abstract String getStr();

    public abstract byte getaByte();

    public abstract short getaShort();

    public abstract int getAnInt();

    public abstract long getaLong();

    public abstract boolean isaBoolean();

    public abstract DateTime getDateTime();

    @SuppressWarnings("mutable")
    public abstract byte[] getBytes();

    public abstract ByteBuffer getByteBuffer();

    public abstract Instant getInstant();

    public abstract BigDecimal getBigDecimal();

    public abstract StringBuilder getStringBuilder();
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class SimpleAutoValueWithBuilder {
    public abstract String getStr();

    public abstract byte getaByte();

    public abstract short getaShort();

    public abstract int getAnInt();

    public abstract long getaLong();

    public abstract boolean isaBoolean();

    public abstract DateTime getDateTime();

    @SuppressWarnings("mutable")
    public abstract byte[] getBytes();

    public abstract ByteBuffer getByteBuffer();

    public abstract Instant getInstant();

    public abstract BigDecimal getBigDecimal();

    public abstract StringBuilder getStringBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      public abstract Builder setStr(String str);

      public abstract Builder setaByte(byte aByte);

      public abstract Builder setaShort(short aShort);

      public abstract Builder setAnInt(int anInt);

      public abstract Builder setaLong(long aLong);

      public abstract Builder setaBoolean(boolean aBoolean);

      public abstract Builder setDateTime(DateTime dateTime);

      public abstract Builder setBytes(byte[] bytes);

      public abstract Builder setByteBuffer(ByteBuffer byteBuffer);

      public abstract Builder setInstant(Instant instant);

      public abstract Builder setBigDecimal(BigDecimal bigDecimal);

      public abstract Builder setStringBuilder(StringBuilder stringBuilder);

      abstract SimpleAutoValueWithBuilder build();
    }
  }

  @Test
  public void testSchema() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema schema = registry.getSchema(SimpleAutoValue.class);
    SchemaTestUtils.assertSchemaEquivalent(SIMPLE_SCHEMA, schema);
  }

  @Test
  public void testToRowConstructor() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimpleAutoValue value =
        new AutoValue_AutoValueSchemaTest_SimpleAutoValue(
            "string",
            (byte) 1,
            (short) 2,
            (int) 3,
            (long) 4,
            true,
            DATE,
            BYTE_ARRAY,
            ByteBuffer.wrap(BYTE_ARRAY),
            DATE.toInstant(),
            BigDecimal.ONE,
            STRING_BUILDER);
    Row row = registry.getToRowFunction(SimpleAutoValue.class).apply(value);
    assertEquals(12, row.getFieldCount());
    assertEquals("string", row.getString("str"));
    assertEquals((byte) 1, (Object) row.getByte("aByte"));
    assertEquals((short) 2, (Object) row.getInt16("aShort"));
    assertEquals((int) 3, (Object) row.getInt32("anInt"));
    assertEquals((long) 4, (Object) row.getInt64("aLong"));
    assertEquals(true, (Object) row.getBoolean("aBoolean"));
    assertEquals(DATE.toInstant(), row.getDateTime("dateTime"));
    assertEquals(DATE.toInstant(), row.getDateTime("instant"));
    assertArrayEquals(BYTE_ARRAY, row.getBytes("bytes"));
    assertArrayEquals(BYTE_ARRAY, row.getBytes("byteBuffer"));
    assertEquals(BigDecimal.ONE, row.getDecimal("bigDecimal"));
    assertEquals("stringbuilder", row.getString("stringBuilder"));
  }

  @Test
  public void testFromRowConstructor() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row row = createSimpleRow("string");
    SimpleAutoValue value = registry.getFromRowFunction(SimpleAutoValue.class).apply(row);
    assertEquals("string", value.getStr());
    assertEquals((byte) 1, value.getaByte());
    assertEquals((short) 2, value.getaShort());
    assertEquals((int) 3, value.getAnInt());
    assertEquals((long) 4, value.getaLong());
    assertEquals(true, value.isaBoolean());
    assertEquals(DATE, value.getDateTime());
    assertEquals(DATE.toInstant(), value.getInstant());
    assertArrayEquals("not equal", BYTE_ARRAY, value.getBytes());
    assertArrayEquals("not equal", BYTE_ARRAY, value.getByteBuffer().array());
    assertEquals(BigDecimal.ONE, value.getBigDecimal());
    assertEquals("stringbuilder", value.getStringBuilder().toString());
  }

  @Test
  public void testToRowBuilder() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimpleAutoValueWithBuilder value =
        new AutoValue_AutoValueSchemaTest_SimpleAutoValueWithBuilder.Builder()
            .setStr("string")
            .setaByte((byte) 1)
            .setaShort((short) 2)
            .setAnInt((int) 3)
            .setaLong((long) 4)
            .setaBoolean(true)
            .setDateTime(DATE)
            .setBytes(BYTE_ARRAY)
            .setByteBuffer(ByteBuffer.wrap(BYTE_ARRAY))
            .setInstant(DATE.toInstant())
            .setBigDecimal(BigDecimal.ONE)
            .setStringBuilder(STRING_BUILDER)
            .build();

    Row row = registry.getToRowFunction(SimpleAutoValueWithBuilder.class).apply(value);
    assertEquals(12, row.getFieldCount());
    assertEquals("string", row.getString("str"));
    assertEquals((byte) 1, (Object) row.getByte("aByte"));
    assertEquals((short) 2, (Object) row.getInt16("aShort"));
    assertEquals((int) 3, (Object) row.getInt32("anInt"));
    assertEquals((long) 4, (Object) row.getInt64("aLong"));
    assertEquals(true, (Object) row.getBoolean("aBoolean"));
    assertEquals(DATE.toInstant(), row.getDateTime("dateTime"));
    assertEquals(DATE.toInstant(), row.getDateTime("instant"));
    assertArrayEquals(BYTE_ARRAY, row.getBytes("bytes"));
    assertArrayEquals(BYTE_ARRAY, row.getBytes("byteBuffer"));
    assertEquals(BigDecimal.ONE, row.getDecimal("bigDecimal"));
    assertEquals("stringbuilder", row.getString("stringBuilder"));
  }

  @Test
  public void testFromRowBuilder() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row row = createSimpleRow("string");
    SimpleAutoValueWithBuilder value =
        registry.getFromRowFunction(SimpleAutoValueWithBuilder.class).apply(row);
    assertEquals("string", value.getStr());
    assertEquals((byte) 1, value.getaByte());
    assertEquals((short) 2, value.getaShort());
    assertEquals((int) 3, value.getAnInt());
    assertEquals((long) 4, value.getaLong());
    assertEquals(true, value.isaBoolean());
    assertEquals(DATE, value.getDateTime());
    assertEquals(DATE.toInstant(), value.getInstant());
    assertArrayEquals("not equal", BYTE_ARRAY, value.getBytes());
    assertArrayEquals("not equal", BYTE_ARRAY, value.getByteBuffer().array());
    assertEquals(BigDecimal.ONE, value.getBigDecimal());
    assertEquals("stringbuilder", value.getStringBuilder().toString());
  }
}
