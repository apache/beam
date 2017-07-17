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
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.beam.dsls.sql.utils.CalciteUtils;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

/**
 * Tests for BeamSqlRowCoder.
 */
public class BeamSqlRowCoderTest {

  @Test
  public void encodeAndDecode() throws Exception {
    final RelProtoDataType protoRowType = new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder()
            .add("col_tinyint", SqlTypeName.TINYINT)
            .add("col_smallint", SqlTypeName.SMALLINT)
            .add("col_integer", SqlTypeName.INTEGER)
            .add("col_bigint", SqlTypeName.BIGINT)
            .add("col_float", SqlTypeName.FLOAT)
            .add("col_double", SqlTypeName.DOUBLE)
            .add("col_decimal", SqlTypeName.DECIMAL)
            .add("col_string_varchar", SqlTypeName.VARCHAR)
            .add("col_time", SqlTypeName.TIME)
            .add("col_timestamp", SqlTypeName.TIMESTAMP)
            .add("col_boolean", SqlTypeName.BOOLEAN)
            .build();
      }
    };

    BeamSqlRecordType beamSQLRecordType = CalciteUtils.toBeamRecordType(
        protoRowType.apply(new JavaTypeFactoryImpl(
            RelDataTypeSystem.DEFAULT)));
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
    row.addField("col_boolean", true);


    BeamSqlRowCoder coder = new BeamSqlRowCoder(beamSQLRecordType);
    CoderProperties.coderDecodeEncodeEqual(coder, row);
  }
}
