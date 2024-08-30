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
package org.apache.beam.sdk.io.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.chrono.ISOChronology;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test SchemaUtils. */
@RunWith(JUnit4.class)
public class SchemaUtilTest {
  @Test
  public void testToBeamSchema() throws SQLException {
    ResultSetMetaData mockResultSetMetaData = mock(ResultSetMetaData.class);

    ImmutableList<JdbcFieldInfo> fieldInfo =
        ImmutableList.of(
            JdbcFieldInfo.of("int_array_col", Types.ARRAY, JDBCType.INTEGER.getName(), false),
            JdbcFieldInfo.of("bigint_col", Types.BIGINT),
            JdbcFieldInfo.of("binary_col", Types.BINARY, 255),
            JdbcFieldInfo.of("bit_col", Types.BIT),
            JdbcFieldInfo.of("boolean_col", Types.BOOLEAN),
            JdbcFieldInfo.of("char_col", Types.CHAR, 255),
            JdbcFieldInfo.of("date_col", Types.DATE),
            JdbcFieldInfo.of("decimal_col", Types.DECIMAL),
            JdbcFieldInfo.of("double_col", Types.DOUBLE),
            JdbcFieldInfo.of("float_col", Types.FLOAT),
            JdbcFieldInfo.of("integer_col", Types.INTEGER),
            JdbcFieldInfo.of("longnvarchar_col", Types.LONGNVARCHAR, 1024),
            JdbcFieldInfo.of("longvarchar_col", Types.LONGVARCHAR, 1024),
            JdbcFieldInfo.of("longvarbinary_col", Types.LONGVARBINARY, 1024),
            JdbcFieldInfo.of("nchar_col", Types.NCHAR, 255),
            JdbcFieldInfo.of("numeric_col", Types.NUMERIC, 12, 4),
            JdbcFieldInfo.of("nvarchar_col", Types.NVARCHAR, 255),
            JdbcFieldInfo.of("real_col", Types.REAL),
            JdbcFieldInfo.of("smallint_col", Types.SMALLINT),
            JdbcFieldInfo.of("time_col", Types.TIME),
            JdbcFieldInfo.of("timestamp_col", Types.TIMESTAMP),
            JdbcFieldInfo.of("timestamptz_col", Types.TIMESTAMP_WITH_TIMEZONE),
            JdbcFieldInfo.of("tinyint_col", Types.TINYINT),
            JdbcFieldInfo.of("varbinary_col", Types.VARBINARY, 255),
            JdbcFieldInfo.of("varchar_col", Types.VARCHAR, 255));

    when(mockResultSetMetaData.getColumnCount()).thenReturn(fieldInfo.size());
    for (int i = 0; i < fieldInfo.size(); i++) {
      JdbcFieldInfo f = fieldInfo.get(i);
      when(mockResultSetMetaData.getColumnLabel(eq(i + 1))).thenReturn(f.columnLabel);
      when(mockResultSetMetaData.getColumnType(eq(i + 1))).thenReturn(f.columnType);
      when(mockResultSetMetaData.getColumnTypeName(eq(i + 1))).thenReturn(f.columnTypeName);
      when(mockResultSetMetaData.getPrecision(eq(i + 1))).thenReturn(f.precision);
      when(mockResultSetMetaData.getScale(eq(i + 1))).thenReturn(f.scale);
      when(mockResultSetMetaData.isNullable(eq(i + 1)))
          .thenReturn(
              f.nullable ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);
    }

    Schema wantBeamSchema =
        Schema.builder()
            .addArrayField("int_array_col", Schema.FieldType.INT32)
            .addField("bigint_col", Schema.FieldType.INT64)
            .addField("binary_col", LogicalTypes.fixedLengthBytes(JDBCType.BINARY, 255))
            .addField("bit_col", LogicalTypes.JDBC_BIT_TYPE)
            .addField("boolean_col", Schema.FieldType.BOOLEAN)
            .addField("char_col", LogicalTypes.fixedLengthString(JDBCType.CHAR, 255))
            .addField("date_col", LogicalTypes.JDBC_DATE_TYPE)
            .addField("decimal_col", Schema.FieldType.DECIMAL)
            .addField("double_col", Schema.FieldType.DOUBLE)
            .addField("float_col", LogicalTypes.JDBC_FLOAT_TYPE)
            .addField("integer_col", Schema.FieldType.INT32)
            .addField(
                "longnvarchar_col", LogicalTypes.variableLengthString(JDBCType.LONGNVARCHAR, 1024))
            .addField(
                "longvarchar_col", LogicalTypes.variableLengthString(JDBCType.LONGVARCHAR, 1024))
            .addField(
                "longvarbinary_col", LogicalTypes.variableLengthBytes(JDBCType.LONGVARBINARY, 1024))
            .addField("nchar_col", LogicalTypes.fixedLengthString(JDBCType.NCHAR, 255))
            .addField("numeric_col", LogicalTypes.numeric(12, 4))
            .addField("nvarchar_col", LogicalTypes.variableLengthString(JDBCType.NVARCHAR, 255))
            .addField("real_col", Schema.FieldType.FLOAT)
            .addField("smallint_col", Schema.FieldType.INT16)
            .addField("time_col", LogicalTypes.JDBC_TIME_TYPE)
            .addField("timestamp_col", Schema.FieldType.DATETIME)
            .addField("timestamptz_col", LogicalTypes.JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE)
            .addField("tinyint_col", Schema.FieldType.BYTE)
            .addField("varbinary_col", LogicalTypes.variableLengthBytes(JDBCType.VARBINARY, 255))
            .addField("varchar_col", LogicalTypes.variableLengthString(JDBCType.VARCHAR, 255))
            .build();

    Schema haveBeamSchema = SchemaUtil.toBeamSchema(mockResultSetMetaData);
    assertEquals(wantBeamSchema, haveBeamSchema);
  }

  @Test
  public void testBeamRowMapperArray() throws Exception {
    ResultSet mockArrayElementsResultSet = mock(ResultSet.class);
    when(mockArrayElementsResultSet.next()).thenReturn(true, true, true, false);
    when(mockArrayElementsResultSet.getInt(eq(1))).thenReturn(10, 20, 30);

    Array mockArray = mock(Array.class);
    when(mockArray.getResultSet()).thenReturn(mockArrayElementsResultSet);

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.getArray(eq(1))).thenReturn(mockArray);

    Schema wantSchema =
        Schema.builder().addField("array", Schema.FieldType.array(Schema.FieldType.INT32)).build();
    Row wantRow =
        Row.withSchema(wantSchema).addValues((Object) ImmutableList.of(10, 20, 30)).build();

    SchemaUtil.BeamRowMapper beamRowMapper = SchemaUtil.BeamRowMapper.of(wantSchema);
    Row haveRow = beamRowMapper.mapRow(mockResultSet);

    assertEquals(wantRow, haveRow);
  }

  @Test
  public void testBeamRowMapperPrimitiveTypes() throws Exception {
    ResultSet mockResultSet = mock(ResultSet.class);
    AtomicBoolean isNull = new AtomicBoolean(false);
    when(mockResultSet.wasNull())
        .thenAnswer(
            x -> {
              boolean val = isNull.get();
              isNull.set(false);
              return val;
            });
    when(mockResultSet.getLong(eq(1))).thenReturn(42L);
    when(mockResultSet.getBytes(eq(2))).thenReturn("binary".getBytes(Charset.forName("UTF-8")));
    when(mockResultSet.getBoolean(eq(3))).thenReturn(true);
    when(mockResultSet.getBoolean(eq(4))).thenReturn(false);
    when(mockResultSet.getString(eq(5))).thenReturn("char");
    when(mockResultSet.getBigDecimal(eq(6))).thenReturn(BigDecimal.valueOf(25L));
    when(mockResultSet.getDouble(eq(7))).thenReturn(20.5D);
    when(mockResultSet.getFloat(eq(8))).thenReturn(15.5F);
    when(mockResultSet.getInt(eq(9))).thenReturn(10);
    when(mockResultSet.getString(eq(10))).thenReturn("longvarchar");
    when(mockResultSet.getBytes(eq(11)))
        .thenReturn("longvarbinary".getBytes(Charset.forName("UTF-8")));
    when(mockResultSet.getBigDecimal(eq(12))).thenReturn(BigDecimal.valueOf(1000L));
    when(mockResultSet.getFloat(eq(13))).thenReturn(32F);
    when(mockResultSet.getShort(eq(14))).thenReturn((short) 8);
    when(mockResultSet.getShort(eq(15))).thenReturn((short) 4);
    when(mockResultSet.getBytes(eq(16))).thenReturn("varbinary".getBytes(Charset.forName("UTF-8")));
    when(mockResultSet.getString(eq(17))).thenReturn("varchar");
    when(mockResultSet.getBoolean(eq(18)))
        .thenAnswer(
            x -> {
              isNull.set(true);
              return false;
            });
    when(mockResultSet.getInt(eq(19)))
        .thenAnswer(
            x -> {
              isNull.set(true);
              return 0;
            });

    Schema wantSchema =
        Schema.builder()
            .addField("bigint_col", Schema.FieldType.INT64)
            .addField("binary_col", Schema.FieldType.BYTES)
            .addField("bit_col", Schema.FieldType.BOOLEAN)
            .addField("boolean_col", Schema.FieldType.BOOLEAN)
            .addField("char_col", Schema.FieldType.STRING)
            .addField("decimal_col", Schema.FieldType.DECIMAL)
            .addField("double_col", Schema.FieldType.DOUBLE)
            .addField("float_col", Schema.FieldType.FLOAT)
            .addField("integer_col", Schema.FieldType.INT32)
            .addField("longvarchar_col", Schema.FieldType.STRING)
            .addField("longvarbinary_col", Schema.FieldType.BYTES)
            .addField("numeric_col", Schema.FieldType.DECIMAL)
            .addField("real_col", Schema.FieldType.FLOAT)
            .addField("smallint_col", Schema.FieldType.INT16)
            .addField("tinyint_col", Schema.FieldType.INT16)
            .addField("varbinary_col", Schema.FieldType.BYTES)
            .addField("varchar_col", Schema.FieldType.STRING)
            .addField("nullable_boolean_col", Schema.FieldType.BOOLEAN.withNullable(true))
            .addField("another_int_col", Schema.FieldType.INT32.withNullable(true))
            .build();
    Row wantRow =
        Row.withSchema(wantSchema)
            .addValues(
                42L,
                "binary".getBytes(Charset.forName("UTF-8")),
                true,
                false,
                "char",
                BigDecimal.valueOf(25L),
                20.5D,
                15.5F,
                10,
                "longvarchar",
                "longvarbinary".getBytes(Charset.forName("UTF-8")),
                BigDecimal.valueOf(1000L),
                32F,
                (short) 8,
                (short) 4,
                "varbinary".getBytes(Charset.forName("UTF-8")),
                "varchar",
                null,
                null)
            .build();

    SchemaUtil.BeamRowMapper beamRowMapper = SchemaUtil.BeamRowMapper.of(wantSchema);
    Row haveRow = beamRowMapper.mapRow(mockResultSet);

    assertEquals(wantRow, haveRow);
  }

  @Test
  public void testJdbcLogicalTypesMapValidAvroSchemaIT() {
    String expectedAvroSchema =
        "{"
            + " \"type\": \"record\","
            + " \"name\": \"topLevelRecord\","
            + " \"fields\": [{"
            + "  \"name\": \"longvarchar_col\","
            + "  \"type\": {"
            + "   \"type\": \"string\","
            + "   \"logicalType\": \"varchar\","
            + "   \"maxLength\": 50"
            + "  }"
            + " }, {"
            + "  \"name\": \"varchar_col\","
            + "  \"type\": {"
            + "   \"type\": \"string\","
            + "   \"logicalType\": \"varchar\","
            + "   \"maxLength\": 15"
            + "  }"
            + " }, {"
            + "  \"name\": \"fixedlength_char_col\","
            + "  \"type\": {"
            + "   \"type\": \"string\","
            + "   \"logicalType\": \"char\","
            + "   \"maxLength\": 25"
            + "  }"
            + " }, {"
            + "  \"name\": \"date_col\","
            + "  \"type\": {"
            + "   \"type\": \"int\","
            + "   \"logicalType\": \"date\""
            + "  }"
            + " }, {"
            + "  \"name\": \"time_col\","
            + "  \"type\": {"
            + "   \"type\": \"int\","
            + "   \"logicalType\": \"time-millis\""
            + "  }"
            + " }]"
            + "}";

    Schema jdbcRowSchema =
        Schema.builder()
            .addField(
                "longvarchar_col", LogicalTypes.variableLengthString(JDBCType.LONGVARCHAR, 50))
            .addField("varchar_col", LogicalTypes.variableLengthString(JDBCType.VARCHAR, 15))
            .addField("fixedlength_char_col", LogicalTypes.fixedLengthString(JDBCType.CHAR, 25))
            .addField("date_col", LogicalTypes.JDBC_DATE_TYPE)
            .addField("time_col", LogicalTypes.JDBC_TIME_TYPE)
            .build();

    System.out.println(AvroUtils.toAvroSchema(jdbcRowSchema));

    assertEquals(
        new org.apache.avro.Schema.Parser().parse(expectedAvroSchema),
        AvroUtils.toAvroSchema(jdbcRowSchema));
  }

  @Test
  public void testBeamRowMapperDateTime() throws Exception {
    long epochMilli = 1558719710000L;

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.getObject(eq(1), eq(java.time.LocalDate.class)))
        .thenReturn(
            java.time.Instant.ofEpochMilli(epochMilli)
                .atZone(ZoneId.systemDefault())
                .toLocalDate());
    when(mockResultSet.getTime(eq(2), any())).thenReturn(new Time(epochMilli));
    when(mockResultSet.getTimestamp(eq(3), any())).thenReturn(new Timestamp(epochMilli));
    when(mockResultSet.getTimestamp(eq(4), any())).thenReturn(new Timestamp(epochMilli));

    Schema wantSchema =
        Schema.builder()
            .addField("date_col", LogicalTypes.JDBC_DATE_TYPE)
            .addField("time_col", LogicalTypes.JDBC_TIME_TYPE)
            .addField("timestamptz_col", LogicalTypes.JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE)
            .addField("timestamp_col", Schema.FieldType.DATETIME)
            .build();

    DateTime wantDateTime = new DateTime(epochMilli, ISOChronology.getInstanceUTC());

    Row wantRow =
        Row.withSchema(wantSchema)
            .addValues(
                wantDateTime.withTimeAtStartOfDay(),
                wantDateTime.withDate(new LocalDate(0L)),
                wantDateTime,
                wantDateTime)
            .build();

    SchemaUtil.BeamRowMapper beamRowMapper = SchemaUtil.BeamRowMapper.of(wantSchema);
    Row haveRow = beamRowMapper.mapRow(mockResultSet);

    assertEquals(wantRow, haveRow);
  }

  ////////////////////////////////////////////////////////////////////////////////////////
  private static final class JdbcFieldInfo {
    private final String columnLabel;
    private final int columnType;
    private final String columnTypeName;
    private final boolean nullable;
    private final int precision;
    private final int scale;

    private JdbcFieldInfo(
        String columnLabel,
        int columnType,
        String columnTypeName,
        boolean nullable,
        int precision,
        int scale) {
      this.columnLabel = columnLabel;
      this.columnType = columnType;
      this.columnTypeName = columnTypeName;
      this.nullable = nullable;
      this.precision = precision;
      this.scale = scale;
    }

    private static JdbcFieldInfo of(
        String columnLabel, int columnType, String columnTypeName, boolean nullable) {
      return new JdbcFieldInfo(columnLabel, columnType, columnTypeName, nullable, 0, 0);
    }

    @SuppressWarnings("unused")
    private static JdbcFieldInfo of(String columnLabel, int columnType, boolean nullable) {
      return new JdbcFieldInfo(columnLabel, columnType, null, nullable, 0, 0);
    }

    private static JdbcFieldInfo of(String columnLabel, int columnType) {
      return new JdbcFieldInfo(columnLabel, columnType, null, false, 0, 0);
    }

    private static JdbcFieldInfo of(String columnLabel, int columnType, int precision) {
      return new JdbcFieldInfo(columnLabel, columnType, null, false, precision, 0);
    }

    private static JdbcFieldInfo of(String columnLabel, int columnType, int precision, int scale) {
      return new JdbcFieldInfo(columnLabel, columnType, null, false, precision, scale);
    }
  }

  @Test
  public void testSchemaFieldComparator() {
    assertTrue(
        SchemaUtil.compareSchemaField(
            Schema.Field.of("name", Schema.FieldType.STRING),
            Schema.Field.of("name", Schema.FieldType.STRING)));
    assertFalse(
        SchemaUtil.compareSchemaField(
            Schema.Field.of("name", Schema.FieldType.STRING),
            Schema.Field.of("anotherName", Schema.FieldType.STRING)));
    assertFalse(
        SchemaUtil.compareSchemaField(
            Schema.Field.of("name", Schema.FieldType.STRING),
            Schema.Field.of("name", Schema.FieldType.INT64)));
  }

  @Test
  public void testSchemaFieldTypeComparator() {
    assertTrue(SchemaUtil.compareSchemaFieldType(Schema.FieldType.STRING, Schema.FieldType.STRING));
    assertFalse(SchemaUtil.compareSchemaFieldType(Schema.FieldType.STRING, Schema.FieldType.INT16));
    assertTrue(
        SchemaUtil.compareSchemaFieldType(
            LogicalTypes.variableLengthString(JDBCType.VARCHAR, 255),
            LogicalTypes.variableLengthString(JDBCType.VARCHAR, 255)));
    assertFalse(
        SchemaUtil.compareSchemaFieldType(
            LogicalTypes.variableLengthString(JDBCType.VARCHAR, 255),
            LogicalTypes.fixedLengthBytes(JDBCType.BIT, 255)));
    assertTrue(
        SchemaUtil.compareSchemaFieldType(
            Schema.FieldType.STRING, LogicalTypes.variableLengthString(JDBCType.VARCHAR, 255)));
    assertFalse(
        SchemaUtil.compareSchemaFieldType(
            Schema.FieldType.INT16, LogicalTypes.variableLengthString(JDBCType.VARCHAR, 255)));
    assertTrue(
        SchemaUtil.compareSchemaFieldType(
            LogicalTypes.variableLengthString(JDBCType.VARCHAR, 255), Schema.FieldType.STRING));
  }
}
