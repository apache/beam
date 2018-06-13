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
import static org.junit.Assert.assertSame;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Test;

public class JavaFieldSchemaTest {
  static final DateTime DATE = DateTime.parse("1979-03-14");
  static final byte[] BYTE_ARRAY = "bytearray".getBytes(Charset.defaultCharset());

  @DefaultSchema(JavaFieldSchema.class)
  public static class SimplePojo {
    public String str;
    public byte aByte;
    public short aShort;
    public int anInt;
    public long aLong;
    public boolean aBoolean;
    public DateTime dateTime;
    public Instant instant;
    public byte[] bytes;
    public ByteBuffer byteBuffer;
    public BigDecimal bigDecimal;
    public StringBuilder stringBuilder;

    public SimplePojo() { }

    public SimplePojo(String str, byte aByte, short aShort, int anInt, long aLong, boolean aBoolean,
                      DateTime dateTime, Instant instant, byte[] bytes, BigDecimal bigDecimal,
                      StringBuilder stringBuilder) {
      this.str = str;
      this.aByte = aByte;
      this.aShort = aShort;
      this.anInt = anInt;
      this.aLong = aLong;
      this.aBoolean = aBoolean;
      this.dateTime = dateTime;
      this.instant = instant;
      this.bytes = bytes;
      this.byteBuffer = ByteBuffer.wrap(bytes);
      this.bigDecimal = bigDecimal;
      this.stringBuilder = stringBuilder;
    }
  }

  static final Schema SIMPLE_SCHEMA = Schema.builder()
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

  private SimplePojo createSimple(String name) {
    return new SimplePojo(name, (byte) 1, (short) 2, 3, 4L,true,
        DATE, DATE.toInstant(), BYTE_ARRAY, BigDecimal.ONE,
        new StringBuilder(name).append("builder"));
  }

  private Row createSimpleRow(String name) {
    return Row.withSchema(SIMPLE_SCHEMA)
        .addValues(name, (byte) 1, (short) 2, 3, 4L, true, DATE, DATE, BYTE_ARRAY, BYTE_ARRAY,
            BigDecimal.ONE, new StringBuilder(name).append("builder").toString()).build();
  }

  @Test
  public void testSchema() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema schema = registry.getSchema(SimplePojo.class);
    assertEquals(SIMPLE_SCHEMA, schema);
  }

  @Test
  public void testToRow() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimplePojo pojo = createSimple("string");
    Row row = registry.getToRowFunction(SimplePojo.class).apply(pojo);

    assertEquals(12, row.getFieldCount());
    assertEquals("string", row.getString(0));
    assertEquals((byte) 1, row.getByte(1).byteValue());
    assertEquals((short) 2, row.getInt16(2).shortValue());
    assertEquals((int) 3, row.getInt32(3).intValue());
    assertEquals((long) 4, row.getInt64(4).longValue());
    assertEquals(true, row.getBoolean(5));
    assertEquals(DATE, row.getDateTime(6));
    assertEquals(DATE, row.getDateTime(7));
    assertArrayEquals(BYTE_ARRAY, row.getBytes(8));
    assertArrayEquals(BYTE_ARRAY, row.getBytes(9));
    assertEquals(BigDecimal.ONE, row.getDecimal(10));
    assertEquals("stringbuilder", row.getString(11));
  }

  @Test
  public void testFromRow() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row row = createSimpleRow("string");

    SimplePojo pojo = registry.getFromRowFunction(SimplePojo.class).apply(row);
    assertEquals("string", pojo.str);
    assertEquals((byte) 1, pojo.aByte);
    assertEquals((short) 2, pojo.aShort);
    assertEquals((int) 3, pojo.anInt);
    assertEquals((long) 4, pojo.aLong);
    assertEquals(true, pojo.aBoolean);
    assertEquals(DATE, pojo.dateTime);
    assertEquals(DATE.toInstant(), pojo.instant);
    assertArrayEquals("not equal", BYTE_ARRAY, pojo.bytes);
    assertArrayEquals("not equal", BYTE_ARRAY, pojo.byteBuffer.array());
    assertEquals(BigDecimal.ONE, pojo.bigDecimal);
    assertEquals("stringbuilder", pojo.stringBuilder.toString());
  }

  @Test
  public void testFromRowWithGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimplePojo pojo = createSimple("string");
    Row row = registry.getToRowFunction(SimplePojo.class).apply(pojo);
    // Test that the fromRowFunction simply returns the original object back.
    SimplePojo extracted = registry.getFromRowFunction(SimplePojo.class).apply(row);
    assertSame(pojo, extracted);
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class RecursivePojo {
    public String str;
    public SimplePojo nested;

    public RecursivePojo(String str, SimplePojo nested) {
      this.str = str;
      this.nested = nested;
    }

    public RecursivePojo() {
    }
  }
  static final Schema RECURSIVE_SCHEMA = Schema.builder()
      .addStringField("str")
      .addRowField("nested", SIMPLE_SCHEMA)
      .build();

  @Test
  public void testRecursiveGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    assertEquals(RECURSIVE_SCHEMA, registry.getSchema(RecursivePojo.class));

    RecursivePojo pojo = new RecursivePojo("str", createSimple("string"));
    Row row = registry.getToRowFunction(RecursivePojo.class)
        .apply(pojo);

    assertEquals("str", row.getString("str"));
    Row nestedRow = row.getRow("nested");
    assertEquals("string", nestedRow.getString(0));
    assertEquals((byte) 1, nestedRow.getByte(1).byteValue());
    assertEquals((short) 2, nestedRow.getInt16(2).shortValue());
    assertEquals((int) 3, nestedRow.getInt32(3).intValue());
    assertEquals((long) 4, nestedRow.getInt64(4).longValue());
    assertEquals(true, nestedRow.getBoolean(5));
    assertEquals(DATE, nestedRow.getDateTime(6));
    assertEquals(DATE, nestedRow.getDateTime(7));
    assertArrayEquals("not equal", BYTE_ARRAY, nestedRow.getBytes(8));
    assertArrayEquals("not equal", BYTE_ARRAY, nestedRow.getBytes(9));
    assertEquals(BigDecimal.ONE, nestedRow.getDecimal(10));
    assertEquals("stringbuilder", nestedRow.getString(11));
  }

  @Test
  public void testRecursiveSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row nestedRow = createSimpleRow("string");

    Row row = Row.withSchema(RECURSIVE_SCHEMA)
        .addValue("str")
        .addValue(nestedRow)
        .build();
    RecursivePojo pojo = registry.getFromRowFunction(RecursivePojo.class).apply(row);
    assertEquals("str", pojo.str);
    assertEquals("string", pojo.nested.str);
    assertEquals((byte) 1, pojo.nested.aByte);
    assertEquals((short) 2, pojo.nested.aShort);
    assertEquals((int) 3, pojo.nested.anInt);
    assertEquals((long) 4, pojo.nested.aLong);
    assertEquals(true, pojo.nested.aBoolean);
    assertEquals(DATE, pojo.nested.dateTime);
    assertEquals(DATE.toInstant(), pojo.nested.instant);
    assertArrayEquals("not equal", BYTE_ARRAY, pojo.nested.bytes);
    assertArrayEquals("not equal", BYTE_ARRAY, pojo.nested.byteBuffer.array());
    assertEquals(BigDecimal.ONE, pojo.nested.bigDecimal);
    assertEquals("stringbuilder", pojo.nested.stringBuilder.toString());
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class PrimitiveArrayPojo {
    // Test every type of array parameter supported.
    public List<String> strings;
    public int[] integers;
    public Long[] longs;

    public PrimitiveArrayPojo(List<String> strings, int[] integers, Long[] longs) {
      this.strings = strings;
      this.integers = integers;
      this.longs = longs;
    }

    public PrimitiveArrayPojo() {
    }
  }
  static final Schema PRIMITIVE_ARRAY_SCHEMA = Schema.builder()
      .addArrayField("strings", FieldType.STRING)
      .addArrayField("integers", FieldType.INT32)
      .addArrayField("longs", FieldType.INT64)
      .build();

  @Test
  public void testPrimitiveArrayGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    assertEquals(PRIMITIVE_ARRAY_SCHEMA, registry.getSchema(PrimitiveArrayPojo.class));

    List<String> strList = ImmutableList.of("a", "b", "c");
    int[] intArray = {1, 2, 3, 4};
    Long[] longArray = {42L, 43L, 44L};
    PrimitiveArrayPojo pojo = new PrimitiveArrayPojo(strList, intArray, longArray);
    Row row = registry.getToRowFunction(PrimitiveArrayPojo.class).apply(pojo);
    assertEquals(strList, row.getArray("strings"));
    assertEquals(Ints.asList(intArray), row.getArray("integers"));
    assertEquals(Arrays.asList(longArray), row.getArray("longs"));

    // Ensure that list caching works.
    assertSame(row.getArray("strings"), row.getArray("strings"));
    assertSame(row.getArray("integers"), row.getArray("integers"));
    assertSame(row.getArray("longs"), row.getArray("longs"));
  }

  @Test
  public void testPrimitiveArraySetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row row = Row.withSchema(PRIMITIVE_ARRAY_SCHEMA)
        .addArray("a", "b", "c", "d")
        .addArray(1, 2, 3, 4)
        .addArray(42L, 43L, 44L, 45L)
        .build();
    PrimitiveArrayPojo pojo = registry.getFromRowFunction(PrimitiveArrayPojo.class).apply(row);
    assertEquals(row.getArray(0), pojo.strings);
    assertEquals(row.getArray(1), Ints.asList(pojo.integers));
    assertEquals(row.getArray(2), Arrays.asList(pojo.longs));
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class RecursiveArrayPojo {
    public List<SimplePojo> pojos;

    public RecursiveArrayPojo(List<SimplePojo> pojos) {
      this.pojos = pojos;
    }

    public RecursiveArrayPojo() {
    }
  }
  static final Schema RECURSIVE_ARRAY_SCHEMA = Schema.builder()
      .addArrayField("pojos", FieldType.row(SIMPLE_SCHEMA))
      .build();

  @Test
  public void testRecursiveArrayGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    assertEquals(RECURSIVE_ARRAY_SCHEMA, registry.getSchema(RecursiveArrayPojo.class));

    SimplePojo simple1 = createSimple("string1");
    SimplePojo simple2 = createSimple("string2");
    SimplePojo simple3 = createSimple("string3");

    RecursiveArrayPojo pojo = new RecursiveArrayPojo(Lists.newArrayList(simple1, simple2, simple3));
    Row row = registry.getToRowFunction(RecursiveArrayPojo.class).apply(pojo);
    List<Row> rows = row.getArray("pojos");
    assertSame(simple1, registry.getFromRowFunction(RecursiveArrayPojo.class).apply(rows.get(0)));
    assertSame(simple2, registry.getFromRowFunction(RecursiveArrayPojo.class).apply(rows.get(1)));
    assertSame(simple3, registry.getFromRowFunction(RecursiveArrayPojo.class).apply(rows.get(2)));
  }

  @Test
  public void testRecursiveArraySetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row row1 = createSimpleRow("string1");
    Row row2 = createSimpleRow("string2");
    Row row3 = createSimpleRow("string3");;

    Row row = Row.withSchema(RECURSIVE_ARRAY_SCHEMA)
        .addArray(row1, row2, row3)
        .build();
    RecursiveArrayPojo pojo = registry.getFromRowFunction(RecursiveArrayPojo.class).apply(row);
    assertEquals(3, pojo.pojos.size());
    assertEquals("string1", pojo.pojos.get(0).str);
    assertEquals("string2", pojo.pojos.get(1).str);
    assertEquals("string3", pojo.pojos.get(2).str);

  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class NestedArraysPojo {
    public List<List<String>> lists;

    public NestedArraysPojo(List<List<String>> lists) {
      this.lists = lists;
    }

    public NestedArraysPojo() {
    }
  }
  static final Schema NESTED_ARRAY_SCHEMA = Schema.builder()
      .addArrayField("lists", FieldType.array(FieldType.STRING))
      .build();

  @Test
  public void testNestedArraysGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    assertEquals(NESTED_ARRAY_SCHEMA, registry.getSchema(NestedArraysPojo.class));

    List<List<String>> listOfLists = Lists.newArrayList(
        Lists.newArrayList("a", "b", "c"),
        Lists.newArrayList("d", "e", "f"),
        Lists.newArrayList("g", "h", "i"));
    NestedArraysPojo pojo = new NestedArraysPojo(listOfLists);
    Row row = registry.getToRowFunction(NestedArraysPojo.class).apply(pojo);
    assertEquals(listOfLists, row.getArray("lists"));
  }

  @Test
  public void testNestedArraysSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    List<List<String>> listOfLists = Lists.newArrayList(
        Lists.newArrayList("a", "b", "c"),
        Lists.newArrayList("d", "e", "f"),
        Lists.newArrayList("g", "h", "i"));
    Row row = Row.withSchema(NESTED_ARRAY_SCHEMA)
        .addArray(listOfLists)
        .build();
    NestedArraysPojo pojo = registry.getFromRowFunction(NestedArraysPojo.class).apply(row);
    assertEquals(listOfLists, pojo.lists);
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class MapPojo {
    public Map<String, SimplePojo> map;

    public MapPojo(Map<String, SimplePojo> map) {
      this.map = map;
    }

    public MapPojo() {
    }
  }
  static final Schema MAP_SCHEMA = Schema.builder()
      .addMapField("map", FieldType.STRING, FieldType.row(SIMPLE_SCHEMA))
      .build();

  @Test
  public void testMapFieldGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    assertEquals(MAP_SCHEMA, registry.getSchema(MapPojo.class));

    SimplePojo simple1 = createSimple("string1");
    SimplePojo simple2 = createSimple("string2");
    SimplePojo simple3 = createSimple("string3");

    MapPojo pojo = new MapPojo(ImmutableMap.of(
        "simple1",simple1 ,
        "simple2", simple2,
        "simple3", simple3));
    Row row = registry.getToRowFunction(MapPojo.class).apply(pojo);
    Map<String, Row> extractedMap = row.getMap("map");
    assertEquals(3, extractedMap.size());
    assertEquals("string1", extractedMap.get("simple1").getString(0));
    assertEquals("string2", extractedMap.get("simple2").getString(0));
    assertEquals("string3", extractedMap.get("simple3").getString(0));
  }

  @Test
  public void testMapFieldSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row row1 = createSimpleRow("string1");
    Row row2 = createSimpleRow("string2");
    Row row3 = createSimpleRow("string3");
    Row row = Row.withSchema(MAP_SCHEMA)
        .addValue(ImmutableMap.of(
            "simple1", row1,
            "simple2", row2,
            "simple3", row3))
        .build();
    MapPojo pojo = registry.getFromRowFunction(MapPojo.class).apply(row);
    assertEquals(3, pojo.map.size());
    assertEquals("string1", pojo.map.get("simple1").str);
    assertEquals("string2", pojo.map.get("simple2").str);
    assertEquals("string3", pojo.map.get("simple3").str);
  }
}
