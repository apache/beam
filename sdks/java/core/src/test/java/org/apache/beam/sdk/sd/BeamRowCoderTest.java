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

package org.apache.beam.sdk.sd;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;

/**
 * Tests for BeamRowCoder.
 */
public class BeamRowCoderTest {

  @Test
  public void encodeAndDecode() throws Exception {
    BeamRowType beamSQLRowType = BeamRowType.create(
        Arrays.asList("col_tinyint", "col_smallint", "col_integer", "col_bigint", "col_float",
            "col_double", "col_decimal", "col_string_varchar", "col_time", "col_timestamp",
            "col_boolean"),
        Arrays.asList(Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.FLOAT,
            Types.DOUBLE, Types.DECIMAL, Types.VARCHAR, Types.TIME, Types.TIMESTAMP,
            Types.BOOLEAN));

    BeamRow row = new BeamRow(beamSQLRowType);
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


    BeamRowCoder coder = new BeamRowCoder(beamSQLRowType);
    CoderProperties.coderDecodeEncodeEqual(coder, row);
  }
}
