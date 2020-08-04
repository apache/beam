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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Tests for AVRO schema classes. */
public class AvroSchemaTest {
  /** A test POJO that corresponds to our AVRO schema. */
  public static class AvroSubPojo {
    @AvroName("bool_non_nullable")
    public boolean boolNonNullable;

    @AvroName("int")
    @org.apache.avro.reflect.Nullable
    public Integer anInt;

    public AvroSubPojo(boolean boolNonNullable, Integer anInt) {
      this.boolNonNullable = boolNonNullable;
      this.anInt = anInt;
    }

    public AvroSubPojo() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AvroSubPojo)) {
        return false;
      }
      AvroSubPojo that = (AvroSubPojo) o;
      return boolNonNullable == that.boolNonNullable && Objects.equals(anInt, that.anInt);
    }

    @Override
    public int hashCode() {
      return Objects.hash(boolNonNullable, anInt);
    }

    @Override
    public String toString() {
      return "AvroSubPojo{" + "boolNonNullable=" + boolNonNullable + ", anInt=" + anInt + '}';
    }
  }

  /** A test POJO that corresponds to our AVRO schema. */
  public static class AvroPojo {
    public @AvroName("bool_non_nullable") boolean boolNonNullable;

    @org.apache.avro.reflect.Nullable
    public @AvroName("int") Integer anInt;

    @org.apache.avro.reflect.Nullable
    public @AvroName("long") Long aLong;

    @AvroName("float")
    @org.apache.avro.reflect.Nullable
    public Float aFloat;

    @AvroName("double")
    @org.apache.avro.reflect.Nullable
    public Double aDouble;

    @org.apache.avro.reflect.Nullable public String string;
    @org.apache.avro.reflect.Nullable public ByteBuffer bytes;

    @AvroSchema("{\"type\": \"fixed\", \"size\": 4, \"name\": \"fixed4\"}")
    public byte[] fixed;

    @AvroSchema("{\"type\": \"int\", \"logicalType\": \"date\"}")
    public LocalDate date;

    @AvroSchema("{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}")
    public DateTime timestampMillis;

    @AvroSchema("{\"name\": \"TestEnum\", \"type\": \"enum\", \"symbols\": [\"abc\",\"cde\"] }")
    public TestEnum testEnum;

    @org.apache.avro.reflect.Nullable public AvroSubPojo row;
    @org.apache.avro.reflect.Nullable public List<AvroSubPojo> array;
    @org.apache.avro.reflect.Nullable public Map<String, AvroSubPojo> map;
    @AvroIgnore String extraField;

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AvroPojo)) {
        return false;
      }
      AvroPojo avroPojo = (AvroPojo) o;
      return boolNonNullable == avroPojo.boolNonNullable
          && Objects.equals(anInt, avroPojo.anInt)
          && Objects.equals(aLong, avroPojo.aLong)
          && Objects.equals(aFloat, avroPojo.aFloat)
          && Objects.equals(aDouble, avroPojo.aDouble)
          && Objects.equals(string, avroPojo.string)
          && Objects.equals(bytes, avroPojo.bytes)
          && Arrays.equals(fixed, avroPojo.fixed)
          && Objects.equals(date, avroPojo.date)
          && Objects.equals(timestampMillis, avroPojo.timestampMillis)
          && Objects.equals(testEnum, avroPojo.testEnum)
          && Objects.equals(row, avroPojo.row)
          && Objects.equals(array, avroPojo.array)
          && Objects.equals(map, avroPojo.map);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          boolNonNullable,
          anInt,
          aLong,
          aFloat,
          aDouble,
          string,
          bytes,
          Arrays.hashCode(fixed),
          date,
          timestampMillis,
          testEnum,
          row,
          array,
          map);
    }

    public AvroPojo(
        boolean boolNonNullable,
        int anInt,
        long aLong,
        float aFloat,
        double aDouble,
        String string,
        ByteBuffer bytes,
        byte[] fixed,
        LocalDate date,
        DateTime timestampMillis,
        TestEnum testEnum,
        AvroSubPojo row,
        List<AvroSubPojo> array,
        Map<String, AvroSubPojo> map) {
      this.boolNonNullable = boolNonNullable;
      this.anInt = anInt;
      this.aLong = aLong;
      this.aFloat = aFloat;
      this.aDouble = aDouble;
      this.string = string;
      this.bytes = bytes;
      this.fixed = fixed;
      this.date = date;
      this.timestampMillis = timestampMillis;
      this.testEnum = testEnum;
      this.row = row;
      this.array = array;
      this.map = map;
      this.extraField = "";
    }

    public AvroPojo() {}

    @Override
    public String toString() {
      return "AvroPojo{"
          + "boolNonNullable="
          + boolNonNullable
          + ", anInt="
          + anInt
          + ", aLong="
          + aLong
          + ", aFloat="
          + aFloat
          + ", aDouble="
          + aDouble
          + ", string='"
          + string
          + '\''
          + ", bytes="
          + bytes
          + ", fixed="
          + Arrays.toString(fixed)
          + ", date="
          + date
          + ", timestampMillis="
          + timestampMillis
          + ", testEnum="
          + testEnum
          + ", row="
          + row
          + ", array="
          + array
          + ", map="
          + map
          + ", extraField='"
          + extraField
          + '\''
          + '}';
    }
  }

  private static final Schema SUBSCHEMA =
      Schema.builder()
          .addField("bool_non_nullable", FieldType.BOOLEAN)
          .addNullableField("int", FieldType.INT32)
          .build();
  private static final FieldType SUB_TYPE = FieldType.row(SUBSCHEMA).withNullable(true);

  private static final EnumerationType TEST_ENUM_TYPE = EnumerationType.create("abc", "cde");

  private static final Schema SCHEMA =
      Schema.builder()
          .addField("bool_non_nullable", FieldType.BOOLEAN)
          .addNullableField("int", FieldType.INT32)
          .addNullableField("long", FieldType.INT64)
          .addNullableField("float", FieldType.FLOAT)
          .addNullableField("double", FieldType.DOUBLE)
          .addNullableField("string", FieldType.STRING)
          .addNullableField("bytes", FieldType.BYTES)
          .addField("fixed", FieldType.logicalType(FixedBytes.of(4)))
          .addField("date", FieldType.DATETIME)
          .addField("timestampMillis", FieldType.DATETIME)
          .addField("testEnum", FieldType.logicalType(TEST_ENUM_TYPE))
          .addNullableField("row", SUB_TYPE)
          .addNullableField("array", FieldType.array(SUB_TYPE))
          .addNullableField("map", FieldType.map(FieldType.STRING, SUB_TYPE))
          .build();

  private static final Schema POJO_SCHEMA =
      Schema.builder()
          .addField("bool_non_nullable", FieldType.BOOLEAN)
          .addNullableField("int", FieldType.INT32)
          .addNullableField("long", FieldType.INT64)
          .addNullableField("float", FieldType.FLOAT)
          .addNullableField("double", FieldType.DOUBLE)
          .addNullableField("string", FieldType.STRING)
          .addNullableField("bytes", FieldType.BYTES)
          .addField("fixed", FieldType.logicalType(FixedBytes.of(4)))
          .addField("date", FieldType.DATETIME)
          .addField("timestampMillis", FieldType.DATETIME)
          .addField("testEnum", FieldType.logicalType(TEST_ENUM_TYPE))
          .addNullableField("row", SUB_TYPE)
          .addNullableField("array", FieldType.array(SUB_TYPE.withNullable(false)))
          .addNullableField("map", FieldType.map(FieldType.STRING, SUB_TYPE.withNullable(false)))
          .build();

  private static final byte[] BYTE_ARRAY = new byte[] {1, 2, 3, 4};
  private static final DateTime DATE_TIME =
      new DateTime().withDate(1979, 3, 14).withTime(1, 2, 3, 4);
  private static final LocalDate DATE = new LocalDate(1979, 3, 14);
  private static final TestAvroNested AVRO_NESTED_SPECIFIC_RECORD = new TestAvroNested(true, 42);
  private static final TestAvro AVRO_SPECIFIC_RECORD =
      new TestAvro(
          true,
          43,
          44L,
          (float) 44.1,
          (double) 44.2,
          "mystring",
          ByteBuffer.wrap(BYTE_ARRAY),
          new fixed4(BYTE_ARRAY),
          DATE,
          DATE_TIME,
          TestEnum.abc,
          AVRO_NESTED_SPECIFIC_RECORD,
          ImmutableList.of(AVRO_NESTED_SPECIFIC_RECORD, AVRO_NESTED_SPECIFIC_RECORD),
          ImmutableMap.of("k1", AVRO_NESTED_SPECIFIC_RECORD, "k2", AVRO_NESTED_SPECIFIC_RECORD));
  private static final GenericRecord AVRO_NESTED_GENERIC_RECORD =
      new GenericRecordBuilder(TestAvroNested.SCHEMA$)
          .set("bool_non_nullable", true)
          .set("int", 42)
          .build();
  private static final GenericRecord AVRO_GENERIC_RECORD =
      new GenericRecordBuilder(TestAvro.SCHEMA$)
          .set("bool_non_nullable", true)
          .set("int", 43)
          .set("long", 44L)
          .set("float", (float) 44.1)
          .set("double", (double) 44.2)
          .set("string", new Utf8("mystring"))
          .set("bytes", ByteBuffer.wrap(BYTE_ARRAY))
          .set(
              "fixed",
              GenericData.get()
                  .createFixed(
                      null, BYTE_ARRAY, org.apache.avro.Schema.createFixed("fixed4", "", "", 4)))
          .set("date", (int) Days.daysBetween(new LocalDate(1970, 1, 1), DATE).getDays())
          .set("timestampMillis", DATE_TIME.getMillis())
          .set("testEnum", TestEnum.abc)
          .set("row", AVRO_NESTED_GENERIC_RECORD)
          .set("array", ImmutableList.of(AVRO_NESTED_GENERIC_RECORD, AVRO_NESTED_GENERIC_RECORD))
          .set(
              "map",
              ImmutableMap.of(
                  new Utf8("k1"), AVRO_NESTED_GENERIC_RECORD,
                  new Utf8("k2"), AVRO_NESTED_GENERIC_RECORD))
          .build();

  private static final Row NESTED_ROW = Row.withSchema(SUBSCHEMA).addValues(true, 42).build();
  private static final Row ROW =
      Row.withSchema(SCHEMA)
          .addValues(
              true,
              43,
              44L,
              (float) 44.1,
              (double) 44.2,
              "mystring",
              ByteBuffer.wrap(BYTE_ARRAY),
              BYTE_ARRAY,
              DATE.toDateTimeAtStartOfDay(DateTimeZone.UTC),
              DATE_TIME,
              TEST_ENUM_TYPE.valueOf("abc"),
              NESTED_ROW,
              ImmutableList.of(NESTED_ROW, NESTED_ROW),
              ImmutableMap.of("k1", NESTED_ROW, "k2", NESTED_ROW))
          .build();

  @Test
  public void testSpecificRecordSchema() {
    assertEquals(SCHEMA, new AvroRecordSchema().schemaFor(TypeDescriptor.of(TestAvro.class)));
  }

  @Test
  public void testPojoSchema() {
    assertEquals(POJO_SCHEMA, new AvroRecordSchema().schemaFor(TypeDescriptor.of(AvroPojo.class)));
  }

  @Test
  public void testSpecificRecordToRow() {
    SerializableFunction<TestAvro, Row> toRow =
        new AvroRecordSchema().toRowFunction(TypeDescriptor.of(TestAvro.class));
    assertEquals(ROW, toRow.apply(AVRO_SPECIFIC_RECORD));
  }

  @Test
  public void testRowToSpecificRecord() {
    SerializableFunction<Row, TestAvro> fromRow =
        new AvroRecordSchema().fromRowFunction(TypeDescriptor.of(TestAvro.class));
    assertEquals(AVRO_SPECIFIC_RECORD, fromRow.apply(ROW));
  }

  @Test
  public void testGenericRecordToRow() {
    SerializableFunction<GenericRecord, Row> toRow =
        AvroUtils.getGenericRecordToRowFunction(SCHEMA);
    assertEquals(ROW, toRow.apply(AVRO_GENERIC_RECORD));
  }

  @Test
  public void testRowToGenericRecord() {
    SerializableFunction<Row, GenericRecord> fromRow =
        AvroUtils.getRowToGenericRecordFunction(TestAvro.SCHEMA$);
    assertEquals(AVRO_GENERIC_RECORD, fromRow.apply(ROW));
  }

  private static final AvroSubPojo SUB_POJO = new AvroSubPojo(true, 42);
  private static final AvroPojo AVRO_POJO =
      new AvroPojo(
          true,
          43,
          44L,
          (float) 44.1,
          (double) 44.2,
          "mystring",
          ByteBuffer.wrap(BYTE_ARRAY),
          BYTE_ARRAY,
          DATE,
          DATE_TIME,
          TestEnum.abc,
          SUB_POJO,
          ImmutableList.of(SUB_POJO, SUB_POJO),
          ImmutableMap.of("k1", SUB_POJO, "k2", SUB_POJO));

  private static final Row ROW_FOR_POJO =
      Row.withSchema(POJO_SCHEMA)
          .addValues(
              true,
              43,
              44L,
              (float) 44.1,
              (double) 44.2,
              "mystring",
              ByteBuffer.wrap(BYTE_ARRAY),
              BYTE_ARRAY,
              DATE.toDateTimeAtStartOfDay(DateTimeZone.UTC),
              DATE_TIME,
              TEST_ENUM_TYPE.valueOf("abc"),
              NESTED_ROW,
              ImmutableList.of(NESTED_ROW, NESTED_ROW),
              ImmutableMap.of("k1", NESTED_ROW, "k2", NESTED_ROW))
          .build();

  @Test
  public void testPojoRecordToRow() {
    SerializableFunction<AvroPojo, Row> toRow =
        new AvroRecordSchema().toRowFunction(TypeDescriptor.of(AvroPojo.class));
    assertEquals(ROW_FOR_POJO, toRow.apply(AVRO_POJO));
  }

  @Test
  public void testRowToPojo() {

    LocalDate test = new LocalDate(((Instant) ROW_FOR_POJO.getValue(8)).getMillis());
    SerializableFunction<Row, AvroPojo> fromRow =
        new AvroRecordSchema().fromRowFunction(TypeDescriptor.of(AvroPojo.class));
    assertEquals(AVRO_POJO, fromRow.apply(ROW_FOR_POJO));
  }

  @Test
  public void testPojoRecordToRowSerializable() {
    SerializableUtils.ensureSerializableRoundTrip(
        new AvroRecordSchema().toRowFunction(TypeDescriptor.of(AvroPojo.class)));
  }

  @Test
  public void testPojoRecordFromRowSerializable() {
    SerializableUtils.ensureSerializableRoundTrip(
        new AvroRecordSchema().fromRowFunction(TypeDescriptor.of(AvroPojo.class)));
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testAvroPipelineGroupBy() {
    PCollection<Row> input = pipeline.apply(Create.of(ROW_FOR_POJO).withRowSchema(POJO_SCHEMA));

    PCollection<Row> output = input.apply(Group.byFieldNames("string"));
    Schema keySchema = Schema.builder().addStringField("string").build();
    Schema outputSchema =
        Schema.builder()
            .addRowField("key", keySchema)
            .addIterableField("value", FieldType.row(POJO_SCHEMA))
            .build();
    PAssert.that(output)
        .containsInAnyOrder(
            Row.withSchema(outputSchema)
                .addValue(Row.withSchema(keySchema).addValue("mystring").build())
                .addIterable(ImmutableList.of(ROW_FOR_POJO))
                .build());

    pipeline.run();
  }
}
