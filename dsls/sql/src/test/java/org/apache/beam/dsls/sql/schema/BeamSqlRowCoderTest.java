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

package org.apache.beam.dsls.sql.schema;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;

/**
 * Tests for BeamSqlRowCoder.
 */
public class BeamSqlRowCoderTest {
  private static final List<String> columnNames = Arrays.asList(
      "col_tinyint", "col_smallint", "col_integer",
      "col_bigint", "col_float", "col_double", "col_decimal",
      "col_string_varchar", "col_time", "col_timestamp"
  );

  private static final List<Integer> columnTypes = Arrays.asList(
      Types.TINYINT, Types.SMALLINT, Types.INTEGER,
      Types.BIGINT, Types.FLOAT, Types.DOUBLE,
      Types.DECIMAL, Types.VARCHAR, Types.TIME, Types.TIMESTAMP
  );

  private static final BeamSqlRecordType beamSQLRecordType =
      BeamSqlRecordType.create(columnNames, columnTypes);

  @Test
  public void encodeAndDecode() throws Exception {
    BeamSqlRow row = new BeamSqlRow(beamSQLRecordType);
    row.addField("col_tinyint", Byte.valueOf("1"));
    row.addField("col_smallint", Short.valueOf("1"));
    row.addField("col_integer", 1);
    row.addField("col_bigint", 1L);
    row.addField("col_float", 1.1F);
    row.addField("col_double", 1.1);
    row.addField("col_decimal", BigDecimal.ZERO);
    row.addField("col_string_varchar", "hello");
    GregorianCalendar calendar = new GregorianCalendar();
    calendar.setTime(new Date());
    row.addField("col_time", calendar);
    row.addField("col_timestamp", new Date());


    BeamSqlRowCoder coder = new BeamSqlRowCoder(beamSQLRecordType);
    CoderProperties.coderDecodeEncodeEqual(coder, row);
  }

  @Test
  public void encodeAndDecode_withNulls() throws Exception {
    BeamSqlRow row = new BeamSqlRow(beamSQLRecordType);
    row.addField("col_tinyint", null);
    row.addField("col_smallint", null);
    row.addField("col_integer", null);
    row.addField("col_bigint", null);
    row.addField("col_float", null);
    row.addField("col_double", 1.1);
    row.addField("col_decimal", null);
    row.addField("col_string_varchar", null);
    row.addField("col_time", null);
    row.addField("col_timestamp", null);


    BeamSqlRowCoder coder = new BeamSqlRowCoder(beamSQLRecordType);
    CoderProperties.coderDecodeEncodeEqual(coder, row);
  }

  @Test
  public void encodeAndDecode_allIntegers() throws Exception {
    List<String> columnNames = Arrays.asList(
        "i1", "i2", "i3", "i4"
    );

    List<Integer> columnTypes = Arrays.asList(
        Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER
    );

    BeamSqlRecordType beamSQLRecordType =
        BeamSqlRecordType.create(columnNames, columnTypes);

    BeamSqlRow row = new BeamSqlRow(beamSQLRecordType);
    row.addField("i1", 1);
    row.addField("i2", null);
    row.addField("i3", null);
    row.addField("i4", null);


    BeamSqlRowCoder coder = new BeamSqlRowCoder(beamSQLRecordType);
    CoderProperties.coderDecodeEncodeEqual(coder, row);
  }
}
