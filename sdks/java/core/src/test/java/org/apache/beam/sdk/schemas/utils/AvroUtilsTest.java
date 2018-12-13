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
package org.apache.beam.sdk.schemas.utils;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.RandomData;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.utils.AvroGenerators.RecordSchemaGenerator;
import org.apache.beam.sdk.schemas.utils.AvroUtils.TypeWithNullability;
import org.apache.beam.sdk.values.Row;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.runner.RunWith;

/** Tests for conversion between AVRO records and Beam rows. */
@RunWith(JUnitQuickcheck.class)
public class AvroUtilsTest {

  private static final org.apache.avro.Schema NULL_SCHEMA =
      org.apache.avro.Schema.create(Type.NULL);

  @Property(trials = 1000)
  @SuppressWarnings("unchecked")
  public void supportsAnyAvroSchema(
      @From(RecordSchemaGenerator.class) org.apache.avro.Schema avroSchema) {
    // not everything is possible to translate
    assumeThat(avroSchema, not(containsField(AvroUtilsTest::hasNonNullUnion)));

    Schema schema = AvroUtils.toBeamSchema(avroSchema);
    Iterable iterable = new RandomData(avroSchema, 10);
    List<GenericRecord> records = Lists.newArrayList((Iterable<GenericRecord>) iterable);

    for (GenericRecord record : records) {
      AvroUtils.toBeamRowStrict(record, schema);
    }
  }

  @Property(trials = 1000)
  @SuppressWarnings("unchecked")
  public void avroToBeamRoudTrip(
      @From(RecordSchemaGenerator.class) org.apache.avro.Schema avroSchema) throws IOException {
    // not everything is possible to translate
    assumeThat(avroSchema, not(containsField(AvroUtilsTest::hasNonNullUnion)));
    // roundtrip for enums returns strings because Beam doesn't have enum type
    assumeThat(avroSchema, not(containsField(x -> x.getType() == Type.ENUM)));
    // roundtrip for fixed returns bytes because Beam doesn't have FIXED type
    assumeThat(avroSchema, not(containsField(x -> x.getType() == Type.FIXED)));

    Schema schema = AvroUtils.toBeamSchema(avroSchema);
    Iterable iterable = new RandomData(avroSchema, 10);
    List<GenericRecord> records = Lists.newArrayList((Iterable<GenericRecord>) iterable);

    for (GenericRecord record : records) {
      Row row = AvroUtils.toBeamRowStrict(record, schema);
      GenericRecord out = AvroUtils.toGenericRecord(row, avroSchema);
      assertEquals(record, out);
    }
  }

  @Test
  public void testUnwrapNullableSchema() {
    org.apache.avro.Schema avroSchema =
        org.apache.avro.Schema.createUnion(
            org.apache.avro.Schema.create(Type.NULL), org.apache.avro.Schema.create(Type.STRING));

    TypeWithNullability typeWithNullability = new TypeWithNullability(avroSchema);
    assertTrue(typeWithNullability.nullable);
    assertEquals(org.apache.avro.Schema.create(Type.STRING), typeWithNullability.type);
  }

  @Test
  public void testUnwrapNullableSchemaReordered() {
    org.apache.avro.Schema avroSchema =
        org.apache.avro.Schema.createUnion(
            org.apache.avro.Schema.create(Type.STRING), org.apache.avro.Schema.create(Type.NULL));

    TypeWithNullability typeWithNullability = new TypeWithNullability(avroSchema);
    assertTrue(typeWithNullability.nullable);
    assertEquals(org.apache.avro.Schema.create(Type.STRING), typeWithNullability.type);
  }

  @Test
  public void testUnwrapNullableSchemaToUnion() {
    org.apache.avro.Schema avroSchema =
        org.apache.avro.Schema.createUnion(
            org.apache.avro.Schema.create(Type.STRING),
            org.apache.avro.Schema.create(Type.LONG),
            org.apache.avro.Schema.create(Type.NULL));

    TypeWithNullability typeWithNullability = new TypeWithNullability(avroSchema);
    assertTrue(typeWithNullability.nullable);
    assertEquals(
        org.apache.avro.Schema.createUnion(
            org.apache.avro.Schema.create(Type.STRING), org.apache.avro.Schema.create(Type.LONG)),
        typeWithNullability.type);
  }

  private org.apache.avro.Schema getAvroSubSchema() {
    List<org.apache.avro.Schema.Field> fields = Lists.newArrayList();
    fields.add(
        new org.apache.avro.Schema.Field(
            "bool", org.apache.avro.Schema.create(Type.BOOLEAN), "", null));
    fields.add(
        new org.apache.avro.Schema.Field("int", org.apache.avro.Schema.create(Type.INT), "", null));
    return org.apache.avro.Schema.createRecord(fields);
  }

  private org.apache.avro.Schema getAvroSchema() {
    List<org.apache.avro.Schema.Field> fields = Lists.newArrayList();
    fields.add(
        new org.apache.avro.Schema.Field(
            "bool", org.apache.avro.Schema.create(Type.BOOLEAN), "", (Object) null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "int", org.apache.avro.Schema.create(Type.INT), "", (Object) null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "long", org.apache.avro.Schema.create(Type.LONG), "", (Object) null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "float", org.apache.avro.Schema.create(Type.FLOAT), "", (Object) null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "double", org.apache.avro.Schema.create(Type.DOUBLE), "", (Object) null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "string", org.apache.avro.Schema.create(Type.STRING), "", (Object) null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "bytes", org.apache.avro.Schema.create(Type.BYTES), "", (Object) null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "decimal",
            LogicalTypes.decimal(Integer.MAX_VALUE)
                .addToSchema(org.apache.avro.Schema.create(Type.BYTES)),
            "",
            (Object) null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "timestampMillis",
            LogicalTypes.timestampMillis().addToSchema(org.apache.avro.Schema.create(Type.LONG)),
            "",
            (Object) null));
    fields.add(new org.apache.avro.Schema.Field("row", getAvroSubSchema(), "", (Object) null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "array", org.apache.avro.Schema.createArray(getAvroSubSchema()), "", (Object) null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "map", org.apache.avro.Schema.createMap(getAvroSubSchema()), "", (Object) null));
    return org.apache.avro.Schema.createRecord(fields);
  }

  private Schema getBeamSubSchema() {
    return new Schema.Builder()
        .addField(Field.of("bool", FieldType.BOOLEAN))
        .addField(Field.of("int", FieldType.INT32))
        .build();
  }

  private Schema getBeamSchema() {
    Schema subSchema = getBeamSubSchema();
    return new Schema.Builder()
        .addField(Field.of("bool", FieldType.BOOLEAN))
        .addField(Field.of("int", FieldType.INT32))
        .addField(Field.of("long", FieldType.INT64))
        .addField(Field.of("float", FieldType.FLOAT))
        .addField(Field.of("double", FieldType.DOUBLE))
        .addField(Field.of("string", FieldType.STRING))
        .addField(Field.of("bytes", FieldType.BYTES))
        .addField(Field.of("decimal", FieldType.DECIMAL))
        .addField(Field.of("timestampMillis", FieldType.DATETIME))
        .addField(Field.of("row", FieldType.row(subSchema)))
        .addField(Field.of("array", FieldType.array(FieldType.row(subSchema))))
        .addField(Field.of("map", FieldType.map(FieldType.STRING, FieldType.row(subSchema))))
        .build();
  }

  static final byte[] BYTE_ARRAY = new byte[] {1, 2, 3, 4};
  static final DateTime DATE_TIME =
      new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4).withZone(DateTimeZone.UTC);
  static final BigDecimal BIG_DECIMAL = new BigDecimal(3600);

  private Row getBeamRow() {
    Row subRow = Row.withSchema(getBeamSubSchema()).addValues(true, 42).build();
    return Row.withSchema(getBeamSchema())
        .addValue(true)
        .addValue(43)
        .addValue(44L)
        .addValue((float) 44.1)
        .addValue((double) 44.2)
        .addValue("string")
        .addValue(BYTE_ARRAY)
        .addValue(BIG_DECIMAL)
        .addValue(DATE_TIME)
        .addValue(subRow)
        .addValue(ImmutableList.of(subRow, subRow))
        .addValue(ImmutableMap.of("k1", subRow, "k2", subRow))
        .build();
  }

  private GenericRecord getGenericRecord() {

    GenericRecord subRecord =
        new GenericRecordBuilder(getAvroSubSchema()).set("bool", true).set("int", 42).build();

    LogicalType decimalType =
        LogicalTypes.decimal(Integer.MAX_VALUE)
            .addToSchema(org.apache.avro.Schema.create(Type.BYTES))
            .getLogicalType();
    ByteBuffer encodedDecimal =
        new Conversions.DecimalConversion().toBytes(BIG_DECIMAL, null, decimalType);

    return new GenericRecordBuilder(getAvroSchema())
        .set("bool", true)
        .set("int", 43)
        .set("long", 44L)
        .set("float", (float) 44.1)
        .set("double", (double) 44.2)
        .set("string", new Utf8("string"))
        .set("bytes", ByteBuffer.wrap(BYTE_ARRAY))
        .set("decimal", encodedDecimal)
        .set("timestampMillis", DATE_TIME.getMillis())
        .set("row", subRecord)
        .set("array", ImmutableList.of(subRecord, subRecord))
        .set("map", ImmutableMap.of(new Utf8("k1"), subRecord, new Utf8("k2"), subRecord))
        .build();
  }

  @Test
  public void testFromAvroSchema() {
    assertEquals(getBeamSchema(), AvroUtils.toBeamSchema(getAvroSchema()));
  }

  @Test
  public void testFromBeamSchema() {
    Schema beamSchema = getBeamSchema();
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);
    assertEquals(getAvroSchema(), avroSchema);
  }

  @Test
  public void testNullableFieldInAvroSchema() {
    List<org.apache.avro.Schema.Field> fields = Lists.newArrayList();
    fields.add(
        new org.apache.avro.Schema.Field(
            "int", ReflectData.makeNullable(org.apache.avro.Schema.create(Type.INT)), "", null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "array",
            org.apache.avro.Schema.createArray(
                ReflectData.makeNullable(org.apache.avro.Schema.create(Type.BYTES))),
            "",
            null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "map",
            org.apache.avro.Schema.createMap(
                ReflectData.makeNullable(org.apache.avro.Schema.create(Type.INT))),
            "",
            null));
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord(fields);

    Schema expectedSchema =
        Schema.builder()
            .addNullableField("int", FieldType.INT32)
            .addArrayField("array", FieldType.BYTES.withNullable(true))
            .addMapField("map", FieldType.STRING, FieldType.INT32.withNullable(true))
            .build();
    assertEquals(expectedSchema, AvroUtils.toBeamSchema(avroSchema));

    Map<String, Object> nullMap = Maps.newHashMap();
    nullMap.put("k1", null);
    GenericRecord genericRecord =
        new GenericRecordBuilder(avroSchema)
            .set("int", null)
            .set("array", Lists.newArrayList((Object) null))
            .set("map", nullMap)
            .build();
    Row expectedRow =
        Row.withSchema(expectedSchema)
            .addValue(null)
            .addValue(Lists.newArrayList((Object) null))
            .addValue(nullMap)
            .build();
    assertEquals(expectedRow, AvroUtils.toBeamRowStrict(genericRecord, expectedSchema));
  }

  @Test
  public void testNullableFieldsInBeamSchema() {
    Schema beamSchema =
        Schema.builder()
            .addNullableField("int", FieldType.INT32)
            .addArrayField("array", FieldType.INT32.withNullable(true))
            .addMapField("map", FieldType.STRING, FieldType.INT32.withNullable(true))
            .build();

    List<org.apache.avro.Schema.Field> fields = Lists.newArrayList();
    fields.add(
        new org.apache.avro.Schema.Field(
            "int", ReflectData.makeNullable(org.apache.avro.Schema.create(Type.INT)), "", null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "array",
            org.apache.avro.Schema.createArray(
                ReflectData.makeNullable(org.apache.avro.Schema.create(Type.INT))),
            "",
            null));
    fields.add(
        new org.apache.avro.Schema.Field(
            "map",
            org.apache.avro.Schema.createMap(
                ReflectData.makeNullable(org.apache.avro.Schema.create(Type.INT))),
            "",
            null));
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord(fields);
    assertEquals(avroSchema, AvroUtils.toAvroSchema(beamSchema));

    Map<Utf8, Object> nullMapUtf8 = Maps.newHashMap();
    nullMapUtf8.put(new Utf8("k1"), null);
    Map<String, Object> nullMapString = Maps.newHashMap();
    nullMapString.put("k1", null);

    GenericRecord expectedGenericRecord =
        new GenericRecordBuilder(avroSchema)
            .set("int", null)
            .set("array", Lists.newArrayList((Object) null))
            .set("map", nullMapUtf8)
            .build();
    Row row =
        Row.withSchema(beamSchema)
            .addValue(null)
            .addValue(Lists.newArrayList((Object) null))
            .addValue(nullMapString)
            .build();
    assertEquals(expectedGenericRecord, AvroUtils.toGenericRecord(row, avroSchema));
  }

  @Test
  public void testBeamRowToGenericRecord() {
    GenericRecord genericRecord = AvroUtils.toGenericRecord(getBeamRow(), null);
    assertEquals(getAvroSchema(), genericRecord.getSchema());
    assertEquals(getGenericRecord(), genericRecord);
  }

  @Test
  public void testGenericRecordToBeamRow() {
    Row row = AvroUtils.toBeamRowStrict(getGenericRecord(), null);
    assertEquals(getBeamRow(), row);
  }

  public static ContainsField containsField(Function<org.apache.avro.Schema, Boolean> predicate) {
    return new ContainsField(predicate);
  }

  // doesn't work because Beam doesn't have unions, only nullable fields
  public static boolean hasNonNullUnion(org.apache.avro.Schema schema) {
    if (schema.getType() == Type.UNION) {
      final List<org.apache.avro.Schema> types = schema.getTypes();

      if (types.size() == 2) {
        return !types.contains(NULL_SCHEMA);
      } else {
        return true;
      }
    }

    return false;
  }

  static class ContainsField extends BaseMatcher<org.apache.avro.Schema> {

    private final Function<org.apache.avro.Schema, Boolean> predicate;

    ContainsField(final Function<org.apache.avro.Schema, Boolean> predicate) {
      this.predicate = predicate;
    }

    @Override
    public boolean matches(final Object item0) {
      if (!(item0 instanceof org.apache.avro.Schema)) {
        return false;
      }

      org.apache.avro.Schema item = (org.apache.avro.Schema) item0;

      if (predicate.apply(item)) {
        return true;
      }

      switch (item.getType()) {
        case RECORD:
          return item.getFields().stream().anyMatch(x -> matches(x.schema()));

        case UNION:
          return item.getTypes().stream().anyMatch(this::matches);

        case ARRAY:
          return matches(item.getElementType());

        case MAP:
          return matches(item.getValueType());

        default:
          return false;
      }
    }

    @Override
    public void describeTo(final Description description) {}
  }
}
