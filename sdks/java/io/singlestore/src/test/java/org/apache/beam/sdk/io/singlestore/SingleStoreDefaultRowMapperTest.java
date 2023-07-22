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
package org.apache.beam.sdk.io.singlestore;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.CHAR;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NULL;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.singlestore.jdbc.client.result.ResultSetMetaData;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Test DefaultRowMapper. */
@RunWith(JUnit4.class)
public class SingleStoreDefaultRowMapperTest {
  @Test
  public void testEmptyRow() throws Exception {
    ResultSetMetaData md = Mockito.mock(ResultSetMetaData.class);
    Mockito.when(md.getColumnCount()).thenReturn(0);
    ResultSet res = Mockito.mock(ResultSet.class);

    SingleStoreDefaultRowMapper mapper = new SingleStoreDefaultRowMapper();
    mapper.init(md);
    Schema s = mapper.getCoder().getSchema();
    Row r = mapper.mapRow(res);

    assertEquals(0, s.getFieldCount());
    assertEquals(0, r.getFieldCount());
  }

  @Test
  public void testAllDataTypes() throws Exception {
    ResultSetMetaData md = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(md.getColumnCount()).thenReturn(17);

    Mockito.when(md.getColumnType(1)).thenReturn(BIT);
    Mockito.when(md.getColumnType(2)).thenReturn(TINYINT);
    Mockito.when(md.getColumnType(3)).thenReturn(SMALLINT);
    Mockito.when(md.getColumnType(4)).thenReturn(INTEGER);
    Mockito.when(md.getColumnType(5)).thenReturn(BIGINT);
    Mockito.when(md.getColumnType(6)).thenReturn(REAL);
    Mockito.when(md.getColumnType(7)).thenReturn(DOUBLE);
    Mockito.when(md.getColumnType(8)).thenReturn(DECIMAL);
    Mockito.when(md.getColumnType(9)).thenReturn(TIMESTAMP);
    Mockito.when(md.getColumnType(10)).thenReturn(DATE);
    Mockito.when(md.getColumnType(11)).thenReturn(TIME);
    Mockito.when(md.getColumnType(12)).thenReturn(LONGVARBINARY);
    Mockito.when(md.getColumnType(13)).thenReturn(VARBINARY);
    Mockito.when(md.getColumnType(14)).thenReturn(BINARY);
    Mockito.when(md.getColumnType(15)).thenReturn(LONGVARCHAR);
    Mockito.when(md.getColumnType(16)).thenReturn(VARCHAR);
    Mockito.when(md.getColumnType(17)).thenReturn(CHAR);

    for (int i = 12; i <= 17; i++) {
      Mockito.when(md.getPrecision(i)).thenReturn(10);
    }

    for (int i = 1; i <= md.getColumnCount(); i++) {
      Mockito.when(md.getColumnLabel(i)).thenReturn("c" + i);
      Mockito.when(md.isNullable(i)).thenReturn(java.sql.ResultSetMetaData.columnNoNulls);
    }

    ResultSet res = Mockito.mock(ResultSet.class);

    Mockito.when(res.getBoolean(1)).thenReturn(true);
    Mockito.when(res.getByte(2)).thenReturn((byte) 10);
    Mockito.when(res.getShort(3)).thenReturn((short) 10);
    Mockito.when(res.getInt(4)).thenReturn(10);
    Mockito.when(res.getLong(5)).thenReturn((long) 10);
    Mockito.when(res.getFloat(6)).thenReturn((float) 10.1);
    Mockito.when(res.getDouble(7)).thenReturn(10.1);
    Mockito.when(res.getBigDecimal(8)).thenReturn(new BigDecimal("100.100"));
    Mockito.when(res.getTimestamp(Mockito.eq(9), Mockito.any()))
        .thenReturn(Timestamp.valueOf("2022-10-10 10:10:10"));
    Mockito.when(res.getObject(10, java.time.LocalDate.class))
        .thenReturn(LocalDate.of(2022, 10, 10));
    Mockito.when(res.getTime(Mockito.eq(11), Mockito.any())).thenReturn(Time.valueOf("10:10:10"));
    Mockito.when(res.getBytes(12)).thenReturn("asd".getBytes(StandardCharsets.UTF_8));
    Mockito.when(res.getBytes(13)).thenReturn("asd".getBytes(StandardCharsets.UTF_8));
    Mockito.when(res.getBytes(14)).thenReturn("asd\0\0\0\0\0\0\0".getBytes(StandardCharsets.UTF_8));
    Mockito.when(res.getString(15)).thenReturn("asd");
    Mockito.when(res.getString(16)).thenReturn("asd");
    Mockito.when(res.getString(17)).thenReturn("asd");

    Mockito.when(res.wasNull()).thenReturn(false);

    SingleStoreDefaultRowMapper mapper = new SingleStoreDefaultRowMapper();
    mapper.init(md);
    Schema s = mapper.getCoder().getSchema();
    Row r = mapper.mapRow(res);

    assertEquals(17, s.getFieldCount());
    for (int i = 0; i < s.getFieldCount(); i++) {
      assertEquals("c" + (i + 1), s.getField(i).getName());
    }

    assertEquals(Schema.FieldType.BOOLEAN, s.getField(0).getType());
    assertEquals(Schema.FieldType.BYTE, s.getField(1).getType());
    assertEquals(Schema.FieldType.INT16, s.getField(2).getType());
    assertEquals(Schema.FieldType.INT32, s.getField(3).getType());
    assertEquals(Schema.FieldType.INT64, s.getField(4).getType());
    assertEquals(Schema.FieldType.FLOAT, s.getField(5).getType());
    assertEquals(Schema.FieldType.DOUBLE, s.getField(6).getType());
    assertEquals(Schema.FieldType.DECIMAL, s.getField(7).getType());
    assertEquals(Schema.FieldType.DATETIME, s.getField(8).getType());
    assertEquals(Schema.FieldType.DATETIME, s.getField(9).getType());
    assertEquals(Schema.FieldType.DATETIME, s.getField(10).getType());
    assertEquals(Schema.TypeName.LOGICAL_TYPE, s.getField(11).getType().getTypeName());
    assertEquals(Schema.TypeName.LOGICAL_TYPE, s.getField(12).getType().getTypeName());
    assertEquals(Schema.TypeName.LOGICAL_TYPE, s.getField(13).getType().getTypeName());
    assertEquals(Schema.TypeName.LOGICAL_TYPE, s.getField(14).getType().getTypeName());
    assertEquals(Schema.TypeName.LOGICAL_TYPE, s.getField(15).getType().getTypeName());
    assertEquals(Schema.TypeName.LOGICAL_TYPE, s.getField(16).getType().getTypeName());
    assertEquals(Schema.FieldType.BYTES, s.getField(11).getType().getLogicalType().getBaseType());
    assertEquals(Schema.FieldType.BYTES, s.getField(12).getType().getLogicalType().getBaseType());
    assertEquals(Schema.FieldType.BYTES, s.getField(13).getType().getLogicalType().getBaseType());
    assertEquals(Schema.FieldType.STRING, s.getField(14).getType().getLogicalType().getBaseType());
    assertEquals(Schema.FieldType.STRING, s.getField(15).getType().getLogicalType().getBaseType());
    assertEquals(Schema.FieldType.STRING, s.getField(16).getType().getLogicalType().getBaseType());

    assertEquals(17, r.getFieldCount());

    assertEquals(true, r.getBoolean(0));
    assertEquals((Byte) (byte) 10, r.getByte(1));
    assertEquals((Short) (short) 10, r.getInt16(2));
    assertEquals((Integer) 10, r.getInt32(3));
    assertEquals((Long) (long) 10, r.getInt64(4));
    assertEquals((Float) (float) 10.1, r.getFloat(5));
    assertEquals((Double) 10.1, r.getDouble(6));
    assertEquals(new BigDecimal("100.100"), r.getDecimal(7));
    assertEquals(0, new DateTime("2022-10-10T10:10:10").compareTo(r.getDateTime(8)));
    assertEquals(0, new DateTime("2022-10-10T00:00:00Z").compareTo(r.getDateTime(9)));
    assertEquals(0, new DateTime("1970-01-01T10:10:10").compareTo(r.getDateTime(10)));
    assertArrayEquals("asd".getBytes(StandardCharsets.UTF_8), r.getBytes(11));
    assertArrayEquals("asd".getBytes(StandardCharsets.UTF_8), r.getBytes(12));
    assertArrayEquals("asd\0\0\0\0\0\0\0".getBytes(StandardCharsets.UTF_8), r.getBytes(13));
    assertEquals("asd", r.getString(14));
    assertEquals("asd", r.getString(15));
    assertEquals("asd", r.getString(16));
  }

  @Test
  public void testNullValues() throws Exception {
    ResultSetMetaData md = Mockito.mock(ResultSetMetaData.class);

    Mockito.when(md.getColumnCount()).thenReturn(18);

    Mockito.when(md.getColumnType(1)).thenReturn(BIT);
    Mockito.when(md.getColumnType(2)).thenReturn(TINYINT);
    Mockito.when(md.getColumnType(3)).thenReturn(SMALLINT);
    Mockito.when(md.getColumnType(4)).thenReturn(INTEGER);
    Mockito.when(md.getColumnType(5)).thenReturn(BIGINT);
    Mockito.when(md.getColumnType(6)).thenReturn(REAL);
    Mockito.when(md.getColumnType(7)).thenReturn(DOUBLE);
    Mockito.when(md.getColumnType(8)).thenReturn(DECIMAL);
    Mockito.when(md.getColumnType(9)).thenReturn(TIMESTAMP);
    Mockito.when(md.getColumnType(10)).thenReturn(DATE);
    Mockito.when(md.getColumnType(11)).thenReturn(TIME);
    Mockito.when(md.getColumnType(12)).thenReturn(LONGVARBINARY);
    Mockito.when(md.getColumnType(13)).thenReturn(VARBINARY);
    Mockito.when(md.getColumnType(14)).thenReturn(BINARY);
    Mockito.when(md.getColumnType(15)).thenReturn(LONGVARCHAR);
    Mockito.when(md.getColumnType(16)).thenReturn(VARCHAR);
    Mockito.when(md.getColumnType(17)).thenReturn(CHAR);
    Mockito.when(md.getColumnType(18)).thenReturn(NULL);

    for (int i = 1; i <= md.getColumnCount(); i++) {
      Mockito.when(md.getColumnLabel(i)).thenReturn("c" + i);
      Mockito.when(md.isNullable(i)).thenReturn(java.sql.ResultSetMetaData.columnNullable);
    }

    ResultSet res = Mockito.mock(ResultSet.class);

    Mockito.when(res.getBoolean(1)).thenReturn(false);
    Mockito.when(res.getByte(2)).thenReturn((byte) 0);
    Mockito.when(res.getShort(3)).thenReturn((short) 0);
    Mockito.when(res.getInt(4)).thenReturn(0);
    Mockito.when(res.getLong(5)).thenReturn((long) 0);
    Mockito.when(res.getFloat(6)).thenReturn((float) 0);
    Mockito.when(res.getDouble(7)).thenReturn(0.0);
    Mockito.when(res.getBigDecimal(8)).thenReturn(null);
    Mockito.when(res.getTimestamp(9)).thenReturn(null);
    Mockito.when(res.getDate(10)).thenReturn(null);
    Mockito.when(res.getTime(11)).thenReturn(null);
    Mockito.when(res.getBytes(12)).thenReturn(null);
    Mockito.when(res.getBytes(13)).thenReturn(null);
    Mockito.when(res.getBytes(14)).thenReturn(null);
    Mockito.when(res.getString(15)).thenReturn(null);
    Mockito.when(res.getString(16)).thenReturn(null);
    Mockito.when(res.getString(17)).thenReturn(null);
    Mockito.when(res.getString(18)).thenReturn(null);

    Mockito.when(res.wasNull()).thenReturn(true);

    SingleStoreDefaultRowMapper mapper = new SingleStoreDefaultRowMapper();
    mapper.init(md);
    Schema s = mapper.getCoder().getSchema();
    Row r = mapper.mapRow(res);

    assertEquals(18, s.getFieldCount());
    for (int i = 0; i < s.getFieldCount(); i++) {
      assertEquals("c" + (i + 1), s.getField(i).getName());
    }

    assertEquals(18, r.getFieldCount());
    for (int i = 0; i < r.getFieldCount(); i++) {
      assertNull(r.getValue(i));
    }
  }
}
