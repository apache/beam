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

import static org.apache.beam.sdk.schemas.utils.TestPOJOs.NESTED_ARRAYS_POJO_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.NESTED_ARRAY_POJO_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.NESTED_MAP_POJO_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.NESTED_NULLABLE_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.NESTED_POJO_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.NULLABLES_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.PRIMITIVE_ARRAY_POJO_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.SIMPLE_POJO_SCHEMA;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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
import org.apache.beam.sdk.schemas.utils.SchemaTestUtils;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedArrayPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedArraysPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedMapPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.POJOWithNestedNullable;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.POJOWithNullables;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.PrimitiveArrayPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.SimplePOJO;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for the {@link JavaFieldSchema} schema provider. */
public class JavaFieldSchemaTest {
  static final DateTime DATE = DateTime.parse("1979-03-14");
  static final Instant INSTANT = DateTime.parse("1979-03-15").toInstant();
  static final byte[] BYTE_ARRAY = "bytearray".getBytes(Charset.defaultCharset());
  static final ByteBuffer BYTE_BUFFER =
      ByteBuffer.wrap("byteBuffer".getBytes(Charset.defaultCharset()));

  private SimplePOJO createSimple(String name) {
    return new SimplePOJO(
        name,
        (byte) 1,
        (short) 2,
        3,
        4L,
        true,
        DATE,
        INSTANT,
        BYTE_ARRAY,
        BYTE_BUFFER,
        BigDecimal.ONE,
        new StringBuilder(name).append("builder"));
  }

  private Row createSimpleRow(String name) {
    return Row.withSchema(SIMPLE_POJO_SCHEMA)
        .addValues(
            name,
            (byte) 1,
            (short) 2,
            3,
            4L,
            true,
            DATE,
            INSTANT,
            BYTE_ARRAY,
            BYTE_BUFFER.array(),
            BigDecimal.ONE,
            new StringBuilder(name).append("builder").toString())
        .build();
  }

  @Test
  public void testSchema() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema schema = registry.getSchema(SimplePOJO.class);
    SchemaTestUtils.assertSchemaEquivalent(SIMPLE_POJO_SCHEMA, schema);
  }

  @Test
  public void testToRow() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimplePOJO pojo = createSimple("string");
    Row row = registry.getToRowFunction(SimplePOJO.class).apply(pojo);

    assertEquals(12, row.getFieldCount());
    assertEquals("string", row.getString("str"));
    assertEquals((byte) 1, (Object) row.getByte("aByte"));
    assertEquals((short) 2, (Object) row.getInt16("aShort"));
    assertEquals((int) 3, (Object) row.getInt32("anInt"));
    assertEquals((long) 4, (Object) row.getInt64("aLong"));
    assertEquals(true, row.getBoolean("aBoolean"));
    assertEquals(DATE.toInstant(), row.getDateTime("dateTime"));
    assertEquals(INSTANT, row.getDateTime("instant").toInstant());
    assertArrayEquals(BYTE_ARRAY, row.getBytes("bytes"));
    assertArrayEquals(BYTE_BUFFER.array(), row.getBytes("byteBuffer"));
    assertEquals(BigDecimal.ONE, row.getDecimal("bigDecimal"));
    assertEquals("stringbuilder", row.getString("stringBuilder"));
  }

  @Test
  public void testFromRow() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row row = createSimpleRow("string");

    SimplePOJO pojo = registry.getFromRowFunction(SimplePOJO.class).apply(row);
    assertEquals("string", pojo.str);
    assertEquals((byte) 1, pojo.aByte);
    assertEquals((short) 2, pojo.aShort);
    assertEquals((int) 3, pojo.anInt);
    assertEquals((long) 4, pojo.aLong);
    assertEquals(true, pojo.aBoolean);
    assertEquals(DATE, pojo.dateTime);
    assertEquals(INSTANT, pojo.instant);
    assertArrayEquals("not equal", BYTE_ARRAY, pojo.bytes);
    assertEquals(BYTE_BUFFER, pojo.byteBuffer);
    assertEquals(BigDecimal.ONE, pojo.bigDecimal);
    assertEquals("stringbuilder", pojo.stringBuilder.toString());
  }

  @Test
  public void testFromRowWithGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimplePOJO pojo = createSimple("string");
    Row row = registry.getToRowFunction(SimplePOJO.class).apply(pojo);
    // Test that the fromRowFunction simply returns the original object back.
    SimplePOJO extracted = registry.getFromRowFunction(SimplePOJO.class).apply(row);
    assertSame(pojo, extracted);
  }

  @Test
  public void testRecursiveGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaTestUtils.assertSchemaEquivalent(
        NESTED_POJO_SCHEMA, registry.getSchema(NestedPOJO.class));

    NestedPOJO pojo = new NestedPOJO(createSimple("string"));
    Row row = registry.getToRowFunction(NestedPOJO.class).apply(pojo);

    Row nestedRow = row.getRow("nested");
    assertEquals("string", nestedRow.getString("str"));
    assertEquals((byte) 1, (Object) nestedRow.getByte("aByte"));
    assertEquals((short) 2, (Object) nestedRow.getInt16("aShort"));
    assertEquals((int) 3, (Object) nestedRow.getInt32("anInt"));
    assertEquals((long) 4, (Object) nestedRow.getInt64("aLong"));
    assertEquals(true, nestedRow.getBoolean("aBoolean"));
    assertEquals(DATE.toInstant(), nestedRow.getDateTime("dateTime"));
    assertEquals(INSTANT, nestedRow.getDateTime("instant").toInstant());
    assertArrayEquals("not equal", BYTE_ARRAY, nestedRow.getBytes("bytes"));
    assertArrayEquals("not equal", BYTE_BUFFER.array(), nestedRow.getBytes("byteBuffer"));
    assertEquals(BigDecimal.ONE, nestedRow.getDecimal("bigDecimal"));
    assertEquals("stringbuilder", nestedRow.getString("stringBuilder"));
  }

  @Test
  public void testRecursiveSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row nestedRow = createSimpleRow("string");

    Row row = Row.withSchema(NESTED_POJO_SCHEMA).addValue(nestedRow).build();
    NestedPOJO pojo = registry.getFromRowFunction(NestedPOJO.class).apply(row);
    assertEquals("string", pojo.nested.str);
    assertEquals((byte) 1, pojo.nested.aByte);
    assertEquals((short) 2, pojo.nested.aShort);
    assertEquals((int) 3, pojo.nested.anInt);
    assertEquals((long) 4, pojo.nested.aLong);
    assertEquals(true, pojo.nested.aBoolean);
    assertEquals(DATE, pojo.nested.dateTime);
    assertEquals(INSTANT, pojo.nested.instant);
    assertArrayEquals("not equal", BYTE_ARRAY, pojo.nested.bytes);
    assertEquals(BYTE_BUFFER, pojo.nested.byteBuffer);
    assertEquals(BigDecimal.ONE, pojo.nested.bigDecimal);
    assertEquals("stringbuilder", pojo.nested.stringBuilder.toString());
  }

  @Test
  public void testPrimitiveArrayGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaTestUtils.assertSchemaEquivalent(
        PRIMITIVE_ARRAY_POJO_SCHEMA, registry.getSchema(PrimitiveArrayPOJO.class));

    List<String> strList = ImmutableList.of("a", "b", "c");
    int[] intArray = {1, 2, 3, 4};
    Long[] longArray = {42L, 43L, 44L};
    PrimitiveArrayPOJO pojo = new PrimitiveArrayPOJO(strList, intArray, longArray);
    Row row = registry.getToRowFunction(PrimitiveArrayPOJO.class).apply(pojo);
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
    Row row =
        Row.withSchema(PRIMITIVE_ARRAY_POJO_SCHEMA)
            .addArray("a", "b", "c", "d")
            .addArray(1, 2, 3, 4)
            .addArray(42L, 43L, 44L, 45L)
            .build();
    PrimitiveArrayPOJO pojo = registry.getFromRowFunction(PrimitiveArrayPOJO.class).apply(row);
    assertEquals(row.getArray("strings"), pojo.strings);
    assertEquals(row.getArray("integers"), Ints.asList(pojo.integers));
    assertEquals(row.getArray("longs"), Arrays.asList(pojo.longs));
  }

  @Test
  public void testRecursiveArrayGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaTestUtils.assertSchemaEquivalent(
        NESTED_ARRAY_POJO_SCHEMA, registry.getSchema(NestedArrayPOJO.class));

    SimplePOJO simple1 = createSimple("string1");
    SimplePOJO simple2 = createSimple("string2");
    SimplePOJO simple3 = createSimple("string3");

    NestedArrayPOJO pojo = new NestedArrayPOJO(simple1, simple2, simple3);
    Row row = registry.getToRowFunction(NestedArrayPOJO.class).apply(pojo);
    List<Row> rows = row.getArray("pojos");
    assertSame(simple1, registry.getFromRowFunction(NestedArrayPOJO.class).apply(rows.get(0)));
    assertSame(simple2, registry.getFromRowFunction(NestedArrayPOJO.class).apply(rows.get(1)));
    assertSame(simple3, registry.getFromRowFunction(NestedArrayPOJO.class).apply(rows.get(2)));
  }

  @Test
  public void testRecursiveArraySetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row row1 = createSimpleRow("string1");
    Row row2 = createSimpleRow("string2");
    Row row3 = createSimpleRow("string3");
    ;

    Row row = Row.withSchema(NESTED_ARRAY_POJO_SCHEMA).addArray(row1, row2, row3).build();
    NestedArrayPOJO pojo = registry.getFromRowFunction(NestedArrayPOJO.class).apply(row);
    assertEquals(3, pojo.pojos.length);
    assertEquals("string1", pojo.pojos[0].str);
    assertEquals("string2", pojo.pojos[1].str);
    assertEquals("string3", pojo.pojos[2].str);
  }

  @Test
  public void testNestedArraysGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaTestUtils.assertSchemaEquivalent(
        NESTED_ARRAYS_POJO_SCHEMA, registry.getSchema(NestedArraysPOJO.class));

    List<List<String>> listOfLists =
        Lists.newArrayList(
            Lists.newArrayList("a", "b", "c"),
            Lists.newArrayList("d", "e", "f"),
            Lists.newArrayList("g", "h", "i"));
    NestedArraysPOJO pojo = new NestedArraysPOJO(listOfLists);
    Row row = registry.getToRowFunction(NestedArraysPOJO.class).apply(pojo);
    assertEquals(listOfLists, row.getArray("lists"));
  }

  @Test
  public void testNestedArraysSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    List<List<String>> listOfLists =
        Lists.newArrayList(
            Lists.newArrayList("a", "b", "c"),
            Lists.newArrayList("d", "e", "f"),
            Lists.newArrayList("g", "h", "i"));
    Row row = Row.withSchema(NESTED_ARRAYS_POJO_SCHEMA).addArray(listOfLists).build();
    NestedArraysPOJO pojo = registry.getFromRowFunction(NestedArraysPOJO.class).apply(row);
    assertEquals(listOfLists, pojo.lists);
  }

  @Test
  public void testMapFieldGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaTestUtils.assertSchemaEquivalent(
        NESTED_MAP_POJO_SCHEMA, registry.getSchema(NestedMapPOJO.class));

    SimplePOJO simple1 = createSimple("string1");
    SimplePOJO simple2 = createSimple("string2");
    SimplePOJO simple3 = createSimple("string3");

    NestedMapPOJO pojo =
        new NestedMapPOJO(
            ImmutableMap.of(
                "simple1", simple1,
                "simple2", simple2,
                "simple3", simple3));
    Row row = registry.getToRowFunction(NestedMapPOJO.class).apply(pojo);
    Map<String, Row> extractedMap = row.getMap("map");
    assertEquals(3, extractedMap.size());
    assertEquals("string1", extractedMap.get("simple1").getString("str"));
    assertEquals("string2", extractedMap.get("simple2").getString("str"));
    assertEquals("string3", extractedMap.get("simple3").getString("str"));
  }

  @Test
  public void testMapFieldSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row row1 = createSimpleRow("string1");
    Row row2 = createSimpleRow("string2");
    Row row3 = createSimpleRow("string3");
    Row row =
        Row.withSchema(NESTED_MAP_POJO_SCHEMA)
            .addValue(
                ImmutableMap.of(
                    "simple1", row1,
                    "simple2", row2,
                    "simple3", row3))
            .build();
    NestedMapPOJO pojo = registry.getFromRowFunction(NestedMapPOJO.class).apply(row);
    assertEquals(3, pojo.map.size());
    assertEquals("string1", pojo.map.get("simple1").str);
    assertEquals("string2", pojo.map.get("simple2").str);
    assertEquals("string3", pojo.map.get("simple3").str);
  }

  @Test
  public void testNullValuesGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row row =
        registry.getToRowFunction(POJOWithNullables.class).apply(new POJOWithNullables(null, 42));
    assertNull(row.getString("str"));
    assertEquals(42, (Object) row.getInt32("anInt"));
  }

  @Test
  public void testNullValuesSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row row = Row.withSchema(NULLABLES_SCHEMA).addValues(null, 42).build();
    POJOWithNullables pojo = registry.getFromRowFunction(POJOWithNullables.class).apply(row);
    assertNull(pojo.str);
    assertEquals(42, pojo.anInt);
  }

  @Test
  public void testNestedNullValuesGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row row =
        registry
            .getToRowFunction(POJOWithNestedNullable.class)
            .apply(new POJOWithNestedNullable(null));
    assertNull(row.getValue("nested"));
  }

  @Test
  public void testNNestedullValuesSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row row = Row.withSchema(NESTED_NULLABLE_SCHEMA).addValue(null).build();
    POJOWithNestedNullable pojo =
        registry.getFromRowFunction(POJOWithNestedNullable.class).apply(row);
    assertNull(pojo.nested);
  }
}
