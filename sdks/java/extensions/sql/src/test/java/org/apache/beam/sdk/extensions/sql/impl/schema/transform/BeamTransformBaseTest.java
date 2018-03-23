/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.impl.schema.transform;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.BeforeClass;

/**
 * shared methods to test PTransforms which execute Beam SQL steps.
 */
public class BeamTransformBaseTest {
  static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  static Schema inputSchema;
  static List<Row> inputRows;

  @BeforeClass
  public static void prepareInput() throws NumberFormatException, ParseException {
    inputSchema =
        RowSqlType
            .builder()
            .withIntegerField("f_int")
            .withBigIntField("f_long")
            .withSmallIntField("f_short")
            .withTinyIntField("f_byte")
            .withFloatField("f_float")
            .withDoubleField("f_double")
            .withVarcharField("f_string")
            .withTimestampField("f_timestamp")
            .withIntegerField("f_int2")
            .build();

    inputRows =
        TestUtils.RowsBuilder
            .of(inputSchema)
            .addRows(
                1,
                1000L,
                Short.valueOf("1"),
                Byte.valueOf("1"),
                1.0F,
                1.0,
                "string_row1",
                format.parse("2017-01-01 01:01:03"),
                1)
            .addRows(
                1,
                2000L,
                Short.valueOf("2"),
                Byte.valueOf("2"),
                2.0F,
                2.0,
                "string_row2",
                format.parse("2017-01-01 01:02:03"),
                2)
            .addRows(
                1,
                3000L,
                Short.valueOf("3"),
                Byte.valueOf("3"),
                3.0F,
                3.0,
                "string_row3",
                format.parse("2017-01-01 01:03:03"),
                3)
            .addRows(
                1,
                4000L,
                Short.valueOf("4"),
                Byte.valueOf("4"),
                4.0F,
                4.0,
                "string_row4",
                format.parse("2017-01-01 02:04:03"),
                4)
            .getRows();
  }
}
