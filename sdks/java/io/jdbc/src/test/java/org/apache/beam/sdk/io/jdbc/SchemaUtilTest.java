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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
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
            JdbcFieldInfo.of("binary_col", Types.BINARY),
            JdbcFieldInfo.of("bit_col", Types.BIT),
            JdbcFieldInfo.of("boolean_col", Types.BOOLEAN),
            JdbcFieldInfo.of("char_col", Types.CHAR),
            JdbcFieldInfo.of("date_col", Types.DATE),
            JdbcFieldInfo.of("decimal_col", Types.DECIMAL),
            JdbcFieldInfo.of("double_col", Types.DOUBLE),
            JdbcFieldInfo.of("float_col", Types.FLOAT),
            JdbcFieldInfo.of("integer_col", Types.INTEGER),
            JdbcFieldInfo.of("longvarchar_col", Types.LONGVARCHAR),
            JdbcFieldInfo.of("longvarbinary_col", Types.LONGVARBINARY),
            JdbcFieldInfo.of("numeric_col", Types.NUMERIC),
            JdbcFieldInfo.of("real_col", Types.REAL),
            JdbcFieldInfo.of("smallint_col", Types.SMALLINT),
            JdbcFieldInfo.of("time_col", Types.TIME),
            JdbcFieldInfo.of("timestamp_col", Types.TIMESTAMP),
            JdbcFieldInfo.of("timestamptz_col", Types.TIMESTAMP_WITH_TIMEZONE),
            JdbcFieldInfo.of("tinyint_col", Types.TINYINT),
            JdbcFieldInfo.of("varbinary_col", Types.VARBINARY),
            JdbcFieldInfo.of("varchar_col", Types.VARCHAR));

    when(mockResultSetMetaData.getColumnCount()).thenReturn(fieldInfo.size());
    for (int i = 0; i < fieldInfo.size(); i++) {
      JdbcFieldInfo f = fieldInfo.get(i);
      when(mockResultSetMetaData.getColumnLabel(eq(i + 1))).thenReturn(f.columnLabel);
      when(mockResultSetMetaData.getColumnType(eq(i + 1))).thenReturn(f.columnType);
      when(mockResultSetMetaData.getColumnTypeName(eq(i + 1))).thenReturn(f.columnTypeName);
      when(mockResultSetMetaData.isNullable(eq(i + 1)))
          .thenReturn(
              f.nullable ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);
    }

    Schema wantBeamSchema =
        Schema.builder()
            .addArrayField("int_array_col", Schema.FieldType.INT32)
            .addField("bigint_col", Schema.FieldType.INT64)
            .addField("binary_col", Schema.FieldType.BYTES)
            .addField("bit_col", Schema.FieldType.BOOLEAN)
            .addField("boolean_col", Schema.FieldType.BOOLEAN)
            .addField("char_col", Schema.FieldType.STRING)
            .addField("date_col", SchemaUtil.SQL_DATE_LOGICAL_TYPE)
            .addField("decimal_col", Schema.FieldType.DECIMAL)
            .addField("double_col", Schema.FieldType.DOUBLE)
            .addField("float_col", Schema.FieldType.FLOAT)
            .addField("integer_col", Schema.FieldType.INT32)
            .addField("longvarchar_col", Schema.FieldType.STRING)
            .addField("longvarbinary_col", Schema.FieldType.BYTES)
            .addField("numeric_col", Schema.FieldType.DECIMAL)
            .addField("real_col", Schema.FieldType.FLOAT)
            .addField("smallint_col", Schema.FieldType.INT16)
            .addField("time_col", SchemaUtil.SQL_TIME_LOGICAL_TYPE)
            .addField("timestamp_col", Schema.FieldType.DATETIME)
            .addField("timestamptz_col", SchemaUtil.SQL_TIMESTAMP_WITH_LOCAL_TZ_LOGICAL_TYPE)
            .addField("tinyint_col", Schema.FieldType.BYTE)
            .addField("varbinary_col", Schema.FieldType.BYTES)
            .addField("varchar_col", Schema.FieldType.STRING)
            .build();

    Schema haveBeamSchema = SchemaUtil.toBeamSchema(mockResultSetMetaData);
    assertEquals(wantBeamSchema, haveBeamSchema);
  }

  @Test
  public void testBeamRowMapper_array() throws Exception {
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
  public void testBeamRowMapper_primitiveTypes() throws Exception {
    ResultSet mockResultSet = mock(ResultSet.class);
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
                "varchar")
            .build();

    SchemaUtil.BeamRowMapper beamRowMapper = SchemaUtil.BeamRowMapper.of(wantSchema);
    Row haveRow = beamRowMapper.mapRow(mockResultSet);

    assertEquals(wantRow, haveRow);
  }

  @Test
  public void testBeamRowMapper_datetime() throws Exception {
    long epochMilli = 1558719710000L;

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.getDate(eq(1))).thenReturn(new Date(epochMilli));
    when(mockResultSet.getTime(eq(2))).thenReturn(new Time(epochMilli));
    when(mockResultSet.getTimestamp(eq(3))).thenReturn(new Timestamp(epochMilli));
    when(mockResultSet.getTimestamp(eq(4))).thenReturn(new Timestamp(epochMilli));

    Schema wantSchema =
        Schema.builder()
            .addField("date_col", SchemaUtil.SQL_DATE_LOGICAL_TYPE)
            .addField("time_col", SchemaUtil.SQL_TIME_LOGICAL_TYPE)
            .addField("timestamptz_col", SchemaUtil.SQL_TIMESTAMP_WITH_LOCAL_TZ_LOGICAL_TYPE)
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

    private JdbcFieldInfo(
        String columnLabel, int columnType, String columnTypeName, boolean nullable) {
      this.columnLabel = columnLabel;
      this.columnType = columnType;
      this.columnTypeName = columnTypeName;
      this.nullable = nullable;
    }

    private static JdbcFieldInfo of(
        String columnLabel, int columnType, String columnTypeName, boolean nullable) {
      return new JdbcFieldInfo(columnLabel, columnType, columnTypeName, nullable);
    }

    private static JdbcFieldInfo of(String columnLabel, int columnType, boolean nullable) {
      return new JdbcFieldInfo(columnLabel, columnType, null, nullable);
    }

    private static JdbcFieldInfo of(String columnLabel, int columnType) {
      return new JdbcFieldInfo(columnLabel, columnType, null, false);
    }
  }
}
