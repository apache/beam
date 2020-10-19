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
import static org.junit.Assert.assertTrue;

import com.google.auto.value.AutoValue;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.utils.SchemaTestUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link AutoValueSchema}. */
@RunWith(JUnit4.class)
public class AutoValueSchemaTest {
  static final DateTime DATE = DateTime.parse("1979-03-14");
  static final byte[] BYTE_ARRAY = "bytearray".getBytes(Charset.defaultCharset());
  static final StringBuilder STRING_BUILDER = new StringBuilder("stringbuilder");

  static final Schema SIMPLE_SCHEMA =
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
  static final Schema OUTER_SCHEMA = Schema.builder().addRowField("inner", SIMPLE_SCHEMA).build();

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

  // A base interface for the different varieties of our AutoValue schema to ease in testing.
  interface SimpleSchema {
    String getStr();

    byte getaByte();

    short getaShort();

    int getAnInt();

    long getaLong();

    boolean isaBoolean();

    DateTime getDateTime();

    @SuppressWarnings("mutable")
    byte[] getBytes();

    ByteBuffer getByteBuffer();

    Instant getInstant();

    BigDecimal getBigDecimal();

    StringBuilder getStringBuilder();
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  abstract static class SimpleAutoValue implements SimpleSchema {
    @Override
    public abstract String getStr();

    @Override
    public abstract byte getaByte();

    @Override
    public abstract short getaShort();

    @Override
    public abstract int getAnInt();

    @Override
    public abstract long getaLong();

    @Override
    public abstract boolean isaBoolean();

    @Override
    public abstract DateTime getDateTime();

    @Override
    @SuppressWarnings("mutable")
    public abstract byte[] getBytes();

    @Override
    public abstract ByteBuffer getByteBuffer();

    @Override
    public abstract Instant getInstant();

    @Override
    public abstract BigDecimal getBigDecimal();

    @Override
    public abstract StringBuilder getStringBuilder();
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  abstract static class SimpleAutoValueWithBuilder implements SimpleSchema {
    @Override
    public abstract String getStr();

    @Override
    public abstract byte getaByte();

    @Override
    public abstract short getaShort();

    @Override
    public abstract int getAnInt();

    @Override
    public abstract long getaLong();

    @Override
    public abstract boolean isaBoolean();

    @Override
    public abstract DateTime getDateTime();

    @Override
    @SuppressWarnings("mutable")
    public abstract byte[] getBytes();

    @Override
    public abstract ByteBuffer getByteBuffer();

    @Override
    public abstract Instant getInstant();

    @Override
    public abstract BigDecimal getBigDecimal();

    @Override
    public abstract StringBuilder getStringBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setStr(String str);

      abstract Builder setaByte(byte aByte);

      abstract Builder setaShort(short aShort);

      abstract Builder setAnInt(int anInt);

      abstract Builder setaLong(long aLong);

      abstract Builder setaBoolean(boolean aBoolean);

      abstract Builder setDateTime(DateTime dateTime);

      abstract Builder setBytes(byte[] bytes);

      abstract Builder setByteBuffer(ByteBuffer byteBuffer);

      abstract Builder setInstant(Instant instant);

      abstract Builder setBigDecimal(BigDecimal bigDecimal);

      abstract Builder setStringBuilder(StringBuilder stringBuilder);

      abstract SimpleAutoValueWithBuilder build();
    }
  }

  private void verifyRow(Row row) {
    assertEquals("string", row.getString("str"));
    assertEquals((byte) 1, (Object) row.getByte("aByte"));
    assertEquals((short) 2, (Object) row.getInt16("aShort"));
    assertEquals((int) 3, (Object) row.getInt32("anInt"));
    assertEquals((long) 4, (Object) row.getInt64("aLong"));
    assertTrue(row.getBoolean("aBoolean"));
    assertEquals(DATE.toInstant(), row.getDateTime("dateTime"));
    assertEquals(DATE.toInstant(), row.getDateTime("instant"));
    assertArrayEquals(BYTE_ARRAY, row.getBytes("bytes"));
    assertArrayEquals(BYTE_ARRAY, row.getBytes("byteBuffer"));
    assertEquals(BigDecimal.ONE, row.getDecimal("bigDecimal"));
    assertEquals("stringbuilder", row.getString("stringBuilder"));
  }

  private void verifyAutoValue(SimpleSchema value) {
    assertEquals("string", value.getStr());
    assertEquals((byte) 1, value.getaByte());
    assertEquals((short) 2, value.getaShort());
    assertEquals((int) 3, value.getAnInt());
    assertEquals((long) 4, value.getaLong());
    assertTrue(value.isaBoolean());
    assertEquals(DATE, value.getDateTime());
    assertEquals(DATE.toInstant(), value.getInstant());
    assertArrayEquals("not equal", BYTE_ARRAY, value.getBytes());
    assertArrayEquals("not equal", BYTE_ARRAY, value.getByteBuffer().array());
    assertEquals(BigDecimal.ONE, value.getBigDecimal());
    assertEquals("stringbuilder", value.getStringBuilder().toString());
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
    verifyRow(row);
  }

  @Test
  public void testFromRowConstructor() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row row = createSimpleRow("string");
    SimpleAutoValue value = registry.getFromRowFunction(SimpleAutoValue.class).apply(row);
    verifyAutoValue(value);
  }

  @Test
  public void testToRowSerializable() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SerializableUtils.ensureSerializableRoundTrip(registry.getToRowFunction(SimpleAutoValue.class));
  }

  @Test
  public void testFromRowSerializable() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SerializableUtils.ensureSerializableRoundTrip(
        registry.getFromRowFunction(SimpleAutoValue.class));
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
    verifyRow(row);
  }

  @Test
  public void testFromRowBuilder() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row row = createSimpleRow("string");
    SimpleAutoValueWithBuilder value =
        registry.getFromRowFunction(SimpleAutoValueWithBuilder.class).apply(row);
    verifyAutoValue(value);
  }

  @Test
  public void testToRowBuilderSerializable() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SerializableUtils.ensureSerializableRoundTrip(
        registry.getToRowFunction(SimpleAutoValueWithBuilder.class));
  }

  @Test
  public void testFromRowBuilderSerializable() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SerializableUtils.ensureSerializableRoundTrip(
        registry.getFromRowFunction(SimpleAutoValueWithBuilder.class));
  }

  // Test nested classes.
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  abstract static class AutoValueOuter {
    abstract SimpleAutoValue getInner();
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  abstract static class AutoValueOuterWithBuilder {
    abstract SimpleAutoValue getInner();

    @AutoValue.Builder
    abstract static class Builder {
      public abstract Builder setInner(SimpleAutoValue inner);

      abstract AutoValueOuterWithBuilder build();
    }
  }

  @Test
  public void testToRowNestedConstructor() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimpleAutoValue inner =
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
    AutoValueOuter outer = new AutoValue_AutoValueSchemaTest_AutoValueOuter(inner);
    Row row = registry.getToRowFunction(AutoValueOuter.class).apply(outer);
    verifyRow(row.getRow("inner"));
  }

  @Test
  public void testToRowNestedBuilder() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimpleAutoValue inner =
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
    AutoValueOuterWithBuilder outer =
        new AutoValue_AutoValueSchemaTest_AutoValueOuterWithBuilder.Builder()
            .setInner(inner)
            .build();
    Row row = registry.getToRowFunction(AutoValueOuterWithBuilder.class).apply(outer);
    verifyRow(row.getRow("inner"));
  }

  @Test
  public void testFromRowNestedConstructor() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row inner = createSimpleRow("string");
    Row outer = Row.withSchema(OUTER_SCHEMA).addValue(inner).build();
    AutoValueOuter value = registry.getFromRowFunction(AutoValueOuter.class).apply(outer);
    verifyAutoValue(value.getInner());
  }

  @Test
  public void testFromRowNestedBuilder() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row inner = createSimpleRow("string");
    Row outer = Row.withSchema(OUTER_SCHEMA).addValue(inner).build();
    AutoValueOuterWithBuilder value =
        registry.getFromRowFunction(AutoValueOuterWithBuilder.class).apply(outer);
    verifyAutoValue(value.getInner());
  }

  // Test that Beam annotations can be used to specify a creator method.
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  abstract static class SimpleAutoValueWithStaticFactory implements SimpleSchema {
    @Override
    public abstract String getStr();

    @Override
    public abstract byte getaByte();

    @Override
    public abstract short getaShort();

    @Override
    public abstract int getAnInt();

    @Override
    public abstract long getaLong();

    @Override
    public abstract boolean isaBoolean();

    @Override
    public abstract DateTime getDateTime();

    @Override
    @SuppressWarnings("mutable")
    public abstract byte[] getBytes();

    @Override
    public abstract ByteBuffer getByteBuffer();

    @Override
    public abstract Instant getInstant();

    @Override
    public abstract BigDecimal getBigDecimal();

    @Override
    public abstract StringBuilder getStringBuilder();

    @SchemaCreate
    static SimpleAutoValueWithStaticFactory create(
        String str,
        byte aByte,
        short aShort,
        int anInt,
        long aLong,
        boolean aBoolean,
        DateTime dateTime,
        byte[] bytes,
        ByteBuffer byteBuffer,
        Instant instant,
        BigDecimal bigDecimal,
        StringBuilder stringBuilder) {
      return new AutoValue_AutoValueSchemaTest_SimpleAutoValueWithStaticFactory(
          str,
          aByte,
          aShort,
          anInt,
          aLong,
          aBoolean,
          dateTime,
          bytes,
          byteBuffer,
          instant,
          bigDecimal,
          stringBuilder);
    }
  }

  @Test
  public void testToRowStaticFactory() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimpleAutoValueWithStaticFactory value =
        SimpleAutoValueWithStaticFactory.create(
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
    Row row = registry.getToRowFunction(SimpleAutoValueWithStaticFactory.class).apply(value);
    verifyRow(row);
  }

  @Test
  public void testFromRowStaticFactory() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row row = createSimpleRow("string");
    SimpleAutoValueWithStaticFactory value =
        registry.getFromRowFunction(SimpleAutoValueWithStaticFactory.class).apply(row);
    verifyAutoValue(value);
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  abstract static class SchemaFieldNameSimpleClass {
    @SchemaFieldName("renamed")
    abstract String getStr();
  }

  @Test
  public void testSchema_SchemaFieldName() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema schema = registry.getSchema(SchemaFieldNameSimpleClass.class);
    SchemaTestUtils.assertSchemaEquivalent(
        Schema.of(Field.of("renamed", FieldType.STRING)), schema);
  }

  @Test
  public void testFromRow_SchemaFieldName() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row row =
        Row.withSchema(Schema.of(Field.of("renamed", FieldType.STRING)))
            .withFieldValue("renamed", "value!")
            .build();
    SchemaFieldNameSimpleClass value =
        registry.getFromRowFunction(SchemaFieldNameSimpleClass.class).apply(row);
    assertEquals("value!", value.getStr());
  }

  @Test
  public void testToRow_SchemaFieldName() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    AutoValue_AutoValueSchemaTest_SchemaFieldNameSimpleClass value =
        new AutoValue_AutoValueSchemaTest_SchemaFieldNameSimpleClass("another value!");
    Row row = registry.getToRowFunction(SchemaFieldNameSimpleClass.class).apply(value);
    assertEquals("another value!", row.getValue("renamed"));
  }
}
