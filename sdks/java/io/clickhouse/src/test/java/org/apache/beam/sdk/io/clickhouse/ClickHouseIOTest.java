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
package org.apache.beam.sdk.io.clickhouse;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Objects;
import org.apache.beam.sdk.io.clickhouse.TableSchema.ColumnType;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ClickHouseIO}. */
@RunWith(JUnit4.class)
public class ClickHouseIOTest extends BaseClickHouseTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testInt64() throws Exception {
    Schema schema =
        Schema.of(Schema.Field.of("f0", FieldType.INT64), Schema.Field.of("f1", FieldType.INT64));
    Row row1 = Row.withSchema(schema).addValue(1L).addValue(2L).build();
    Row row2 = Row.withSchema(schema).addValue(2L).addValue(4L).build();
    Row row3 = Row.withSchema(schema).addValue(3L).addValue(6L).build();

    executeSql("CREATE TABLE test_int64 (f0 Int64, f1 Int64) ENGINE=Log");

    pipeline.apply(Create.of(row1, row2, row3).withRowSchema(schema)).apply(write("test_int64"));

    pipeline.run().waitUntilFinish();

    long sum0 = executeQueryAsLong("SELECT SUM(f0) FROM test_int64");
    long sum1 = executeQueryAsLong("SELECT SUM(f1) FROM test_int64");

    assertEquals(6L, sum0);
    assertEquals(12L, sum1);
  }

  @Test
  public void testNullableInt64() throws Exception {
    Schema schema = Schema.of(Schema.Field.nullable("f0", FieldType.INT64));
    Row row1 = Row.withSchema(schema).addValue(1L).build();
    Row row2 = Row.withSchema(schema).addValue(null).build();
    Row row3 = Row.withSchema(schema).addValue(3L).build();

    executeSql("CREATE TABLE test_nullable_int64 (f0 Nullable(Int64)) ENGINE=Log");

    pipeline
        .apply(Create.of(row1, row2, row3).withRowSchema(schema))
        .apply(write("test_nullable_int64"));

    pipeline.run().waitUntilFinish();

    long sum = executeQueryAsLong("SELECT SUM(f0) FROM test_nullable_int64");
    long count0 = executeQueryAsLong("SELECT COUNT(*) FROM test_nullable_int64");
    long count1 = executeQueryAsLong("SELECT COUNT(f0) FROM test_nullable_int64");

    assertEquals(4L, sum);
    assertEquals(3L, count0);
    assertEquals(2L, count1);
  }

  @Test
  public void testInt64WithDefault() throws Exception {
    Schema schema = Schema.of(Schema.Field.nullable("f0", FieldType.INT64));
    Row row1 = Row.withSchema(schema).addValue(1L).build();
    Row row2 = Row.withSchema(schema).addValue(null).build();
    Row row3 = Row.withSchema(schema).addValue(3L).build();

    executeSql("CREATE TABLE test_int64_with_default (f0 Int64 DEFAULT -1) ENGINE=Log");

    pipeline
        .apply(Create.of(row1, row2, row3).withRowSchema(schema))
        .apply(write("test_int64_with_default"));

    pipeline.run().waitUntilFinish();

    long sum = executeQueryAsLong("SELECT SUM(f0) FROM test_int64_with_default");

    assertEquals(3L, sum);
  }

  @Test
  public void testArrayOfArrayOfInt64() throws Exception {
    Schema schema =
        Schema.of(Schema.Field.of("f0", FieldType.array(FieldType.array(FieldType.INT64))));
    Row row1 =
        Row.withSchema(schema)
            .addValue(
                Arrays.asList(Arrays.asList(1L, 2L), Arrays.asList(2L, 3L), Arrays.asList(3L, 4L)))
            .build();

    executeSql("CREATE TABLE test_array_of_array_of_int64 (f0 Array(Array(Int64))) ENGINE=Log");

    pipeline
        .apply(Create.of(row1).withRowSchema(schema))
        .apply(write("test_array_of_array_of_int64"));

    pipeline.run().waitUntilFinish();

    long sum0 =
        executeQueryAsLong(
            "SELECT SUM(arraySum(arrayMap(x -> arraySum(x), f0))) "
                + "FROM test_array_of_array_of_int64");

    assertEquals(15L, sum0);
  }

  @Test
  public void testTupleType() throws Exception {
    Schema tupleSchema =
        Schema.of(
            Schema.Field.of("f0", FieldType.STRING), Schema.Field.of("f1", FieldType.BOOLEAN));
    Schema schema = Schema.of(Schema.Field.of("t0", FieldType.row(tupleSchema)));
    Row row1Tuple = Row.withSchema(tupleSchema).addValue("tuple").addValue(true).build();

    Row row1 = Row.withSchema(schema).addValue(row1Tuple).build();

    executeSql(
        "CREATE TABLE test_named_tuples (" + "t0 Tuple(`f0` String, `f1` Bool)" + ") ENGINE=Log");

    pipeline.apply(Create.of(row1).withRowSchema(schema)).apply(write("test_named_tuples"));

    pipeline.run().waitUntilFinish();

    try (ResultSet rs = executeQuery("SELECT * FROM test_named_tuples")) {
      rs.next();
      assertEquals("[tuple, true]", rs.getString("t0"));
    }

    try (ResultSet rs = executeQuery("SELECT t0.f0 as f0, t0.f1 as f1 FROM test_named_tuples")) {
      rs.next();
      assertEquals("tuple", rs.getString("f0"));
      assertEquals("true", rs.getString("f1"));
    }
  }

  @Test
  public void testComplexTupleType() throws Exception {
    Schema sizeSchema =
        Schema.of(
            Schema.Field.of("width", FieldType.INT64.withNullable(true)),
            Schema.Field.of("height", FieldType.INT64.withNullable(true)));

    Schema browserSchema =
        Schema.of(
            Schema.Field.of("name", FieldType.STRING.withNullable(true)),
            Schema.Field.of("size", FieldType.row(sizeSchema)),
            Schema.Field.of("version", FieldType.STRING.withNullable(true)));

    Schema propSchema =
        Schema.of(
            Schema.Field.of("browser", FieldType.row(browserSchema)),
            Schema.Field.of("deviceCategory", FieldType.STRING.withNullable(true)));

    Schema schema = Schema.of(Schema.Field.of("prop", FieldType.row(propSchema)));

    Row sizeRow = Row.withSchema(sizeSchema).addValue(10L).addValue(20L).build();
    Row browserRow =
        Row.withSchema(browserSchema).addValue("test").addValue(sizeRow).addValue("1.0.0").build();
    Row propRow = Row.withSchema(propSchema).addValue(browserRow).addValue("mobile").build();
    Row row1 = Row.withSchema(schema).addValue(propRow).build();

    executeSql(
        "CREATE TABLE test_named_complex_tuples ("
            + "`prop` Tuple(`browser` Tuple(`name` Nullable(String),`size` Tuple(`width` Nullable(Int64), `height` Nullable(Int64)),`version` Nullable(String)),`deviceCategory` Nullable(String))"
            + ") ENGINE=Log");

    pipeline.apply(Create.of(row1).withRowSchema(schema)).apply(write("test_named_complex_tuples"));

    pipeline.run().waitUntilFinish();

    try (ResultSet rs = executeQuery("SELECT * FROM test_named_complex_tuples")) {
      rs.next();
      assertEquals("[[test, [10, 20], 1.0.0], mobile]", rs.getString("prop"));
    }

    try (ResultSet rs =
        executeQuery(
            "SELECT prop.browser.name as name, prop.browser.size as size FROM test_named_complex_tuples")) {
      rs.next();
      assertEquals("test", rs.getString("name"));
      assertEquals("[10, 20]", rs.getString("size"));
    }
  }

  @Test
  public void testPrimitiveTypes() throws Exception {
    Schema schema =
        Schema.of(
            Schema.Field.of("f0", FieldType.DATETIME),
            Schema.Field.of("f1", FieldType.DATETIME),
            Schema.Field.of("f2", FieldType.FLOAT),
            Schema.Field.of("f3", FieldType.DOUBLE),
            Schema.Field.of("f4", FieldType.BYTE),
            Schema.Field.of("f5", FieldType.INT16),
            Schema.Field.of("f6", FieldType.INT32),
            Schema.Field.of("f7", FieldType.INT64),
            Schema.Field.of("f8", FieldType.STRING),
            Schema.Field.of("f9", FieldType.INT16),
            Schema.Field.of("f10", FieldType.INT32),
            Schema.Field.of("f11", FieldType.INT64),
            Schema.Field.of("f12", FieldType.INT64),
            Schema.Field.of("f13", FieldType.STRING),
            Schema.Field.of("f14", FieldType.STRING),
            Schema.Field.of("f15", FieldType.STRING),
            Schema.Field.of("f16", FieldType.BYTES),
            Schema.Field.of("f17", FieldType.logicalType(FixedBytes.of(3))),
            Schema.Field.of("f18", FieldType.BOOLEAN),
            Schema.Field.of("f19", FieldType.STRING));
    Row row1 =
        Row.withSchema(schema)
            .addValue(new DateTime(2030, 10, 1, 0, 0, 0, DateTimeZone.UTC))
            .addValue(new DateTime(2030, 10, 9, 8, 7, 6, DateTimeZone.UTC))
            .addValue(2.2f)
            .addValue(3.3)
            .addValue((byte) 4)
            .addValue((short) 5)
            .addValue(6)
            .addValue(7L)
            .addValue("eight")
            .addValue((short) 9)
            .addValue(10)
            .addValue(11L)
            .addValue(12L)
            .addValue("abc")
            .addValue("cde")
            .addValue("qwe")
            .addValue(new byte[] {'a', 's', 'd'})
            .addValue(new byte[] {'z', 'x', 'c'})
            .addValue(true)
            .addValue("lowcardenality")
            .build();

    executeSql(
        "CREATE TABLE test_primitive_types ("
            + "f0  Date,"
            + "f1  DateTime,"
            + "f2  Float32,"
            + "f3  Float64,"
            + "f4  Int8,"
            + "f5  Int16,"
            + "f6  Int32,"
            + "f7  Int64,"
            + "f8  String,"
            + "f9  UInt8,"
            + "f10 UInt16,"
            + "f11 UInt32,"
            + "f12 UInt64,"
            + "f13 Enum8('abc' = 1, 'cde' = 2),"
            + "f14 Enum16('abc' = -1, 'cde' = -2),"
            + "f15 FixedString(3),"
            + "f16 FixedString(3),"
            + "f17 FixedString(3),"
            + "f18 Bool,"
            + "f19 LowCardinality(String)"
            + ") ENGINE=Log");

    pipeline.apply(Create.of(row1).withRowSchema(schema)).apply(write("test_primitive_types"));

    pipeline.run().waitUntilFinish();

    try (ResultSet rs = executeQuery("SELECT * FROM test_primitive_types")) {
      rs.next();

      assertEquals("2030-10-01", rs.getString("f0"));
      assertEquals("2030-10-09 08:07:06", rs.getString("f1"));
      assertEquals("2.2", rs.getString("f2"));
      assertEquals("3.3", rs.getString("f3"));
      assertEquals("4", rs.getString("f4"));
      assertEquals("5", rs.getString("f5"));
      assertEquals("6", rs.getString("f6"));
      assertEquals("7", rs.getString("f7"));
      assertEquals("eight", rs.getString("f8"));
      assertEquals("9", rs.getString("f9"));
      assertEquals("10", rs.getString("f10"));
      assertEquals("11", rs.getString("f11"));
      assertEquals("12", rs.getString("f12"));
      assertEquals("abc", rs.getString("f13"));
      assertEquals("cde", rs.getString("f14"));
      assertArrayEquals(new byte[] {'q', 'w', 'e'}, rs.getBytes("f15"));
      assertArrayEquals(new byte[] {'a', 's', 'd'}, rs.getBytes("f16"));
      assertArrayEquals(new byte[] {'z', 'x', 'c'}, rs.getBytes("f17"));
      assertEquals("true", rs.getString("f18"));
      assertEquals("lowcardenality", rs.getString("f19"));
    }
  }

  @Test
  public void testArrayOfPrimitiveTypes() throws Exception {
    Schema schema =
        Schema.of(
            Schema.Field.of("f0", FieldType.array(FieldType.DATETIME)),
            Schema.Field.of("f1", FieldType.array(FieldType.DATETIME)),
            Schema.Field.of("f2", FieldType.array(FieldType.FLOAT)),
            Schema.Field.of("f3", FieldType.array(FieldType.DOUBLE)),
            Schema.Field.of("f4", FieldType.array(FieldType.BYTE)),
            Schema.Field.of("f5", FieldType.array(FieldType.INT16)),
            Schema.Field.of("f6", FieldType.array(FieldType.INT32)),
            Schema.Field.of("f7", FieldType.array(FieldType.INT64)),
            Schema.Field.of("f8", FieldType.array(FieldType.STRING)),
            Schema.Field.of("f9", FieldType.array(FieldType.INT16)),
            Schema.Field.of("f10", FieldType.array(FieldType.INT32)),
            Schema.Field.of("f11", FieldType.array(FieldType.INT64)),
            Schema.Field.of("f12", FieldType.array(FieldType.INT64)),
            Schema.Field.of("f13", FieldType.array(FieldType.STRING)),
            Schema.Field.of("f14", FieldType.array(FieldType.STRING)),
            Schema.Field.of("f15", FieldType.array(FieldType.BOOLEAN)));
    Row row1 =
        Row.withSchema(schema)
            .addArray(
                new DateTime(2030, 10, 1, 0, 0, 0, DateTimeZone.UTC),
                new DateTime(2031, 10, 1, 0, 0, 0, DateTimeZone.UTC))
            .addArray(
                new DateTime(2030, 10, 9, 8, 7, 6, DateTimeZone.UTC),
                new DateTime(2031, 10, 9, 8, 7, 6, DateTimeZone.UTC))
            .addArray(2.2f, 3.3f)
            .addArray(3.3, 4.4)
            .addArray((byte) 4, (byte) 5)
            .addArray((short) 5, (short) 6)
            .addArray(6, 7)
            .addArray(7L, 8L)
            .addArray("eight", "nine")
            .addArray((short) 9, (short) 10)
            .addArray(10, 11)
            .addArray(11L, 12L)
            .addArray(12L, 13L)
            .addArray("abc", "cde")
            .addArray("cde", "abc")
            .addArray(true, false)
            .build();

    executeSql(
        "CREATE TABLE test_array_of_primitive_types ("
            + "f0  Array(Date),"
            + "f1  Array(DateTime),"
            + "f2  Array(Float32),"
            + "f3  Array(Float64),"
            + "f4  Array(Int8),"
            + "f5  Array(Int16),"
            + "f6  Array(Int32),"
            + "f7  Array(Int64),"
            + "f8  Array(String),"
            + "f9  Array(UInt8),"
            + "f10 Array(UInt16),"
            + "f11 Array(UInt32),"
            + "f12 Array(UInt64),"
            + "f13 Array(Enum8('abc' = 1, 'cde' = 2)),"
            + "f14 Array(Enum16('abc' = -1, 'cde' = -2)),"
            + "f15 Array(Bool)"
            + ") ENGINE=Log");

    pipeline
        .apply(Create.of(row1).withRowSchema(schema))
        .apply(write("test_array_of_primitive_types"));

    pipeline.run().waitUntilFinish();

    try (ResultSet rs = executeQuery("SELECT * FROM test_array_of_primitive_types")) {
      rs.next();

      assertEquals("[2030-10-01, 2031-10-01]", rs.getString("f0"));
      assertEquals("[2030-10-09T08:07:06, 2031-10-09T08:07:06]", rs.getString("f1"));
      // Since comparing float/double values is not precise, we compare the string representation
      assertEquals("[2.2,3.3]", rs.getString("f2"));
      assertEquals("[3.3,4.4]", rs.getString("f3"));
      assertArrayEquals(new byte[] {4, 5}, (byte[]) rs.getArray("f4").getArray());
      assertArrayEquals(new short[] {5, 6}, (short[]) rs.getArray("f5").getArray());
      assertArrayEquals(new int[] {6, 7}, (int[]) rs.getArray("f6").getArray());
      assertArrayEquals(new long[] {7L, 8L}, (long[]) rs.getArray("f7").getArray());
      assertArrayEquals(new String[] {"eight", "nine"}, (String[]) rs.getArray("f8").getArray());
      assertArrayEquals(new byte[] {9, 10}, (byte[]) rs.getArray("f9").getArray());
      assertArrayEquals(new short[] {10, 11}, (short[]) rs.getArray("f10").getArray());
      assertArrayEquals(new int[] {11, 12}, (int[]) rs.getArray("f11").getArray());
      assertArrayEquals(new long[] {12L, 13L}, (long[]) rs.getArray("f12").getArray());
      assertArrayEquals(new String[] {"abc", "cde"}, (String[]) rs.getArray("f13").getArray());
      assertArrayEquals(new String[] {"cde", "abc"}, (String[]) rs.getArray("f14").getArray());
      assertArrayEquals(new boolean[] {true, false}, (boolean[]) rs.getArray("f15").getArray());
    }
  }

  @Test
  public void testInsertSql() {
    TableSchema tableSchema =
        TableSchema.of(
            TableSchema.Column.of("f0", ColumnType.INT64),
            TableSchema.Column.of("f1", ColumnType.INT64));

    String expected = "INSERT INTO \"test_table\" (\"f0\", \"f1\")";

    assertEquals(expected, ClickHouseIO.WriteFn.insertSql(tableSchema, "test_table"));
  }

  /** POJO used to test . */
  @DefaultSchema(JavaFieldSchema.class)
  public static final class POJO {
    public int f0;
    public long f1;

    public POJO(int f0, long f1) {
      this.f0 = f0;
      this.f1 = f1;
    }

    public POJO() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final POJO pojo = (POJO) o;
      return f0 == pojo.f0 && f1 == pojo.f1;
    }

    @Override
    public int hashCode() {
      return Objects.hash(f0, f1);
    }
  }

  @Ignore
  // FIXME java.lang.ClassNotFoundException: javax.annotation.Nullable
  public void testPojo() throws Exception {
    POJO pojo1 = new POJO(1, 2L);
    POJO pojo2 = new POJO(2, 4L);
    POJO pojo3 = new POJO(3, 6L);

    executeSql("CREATE TABLE test_pojo(f0 Int32, f1 Int64) ENGINE=Log");

    pipeline.apply(Create.of(pojo1, pojo2, pojo3)).apply(write("test_pojo"));

    pipeline.run().waitUntilFinish();

    long sum0 = executeQueryAsLong("SELECT SUM(f0) FROM test_pojo");
    long sum1 = executeQueryAsLong("SELECT SUM(f1) FROM test_pojo");

    assertEquals(6L, sum0);
    assertEquals(12L, sum1);
  }

  private <T> ClickHouseIO.Write<T> write(String table) {
    return ClickHouseIO.<T>write(clickHouse.getJdbcUrl(), table).withMaxRetries(0);
  }
}
