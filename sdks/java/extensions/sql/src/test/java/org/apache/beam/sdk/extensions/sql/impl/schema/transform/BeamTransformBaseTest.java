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
package org.apache.beam.sdk.extensions.sql.impl.schema.transform;

import java.text.ParseException;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.BeforeClass;

/** shared methods to test PTransforms which execute Beam SQL steps. */
public class BeamTransformBaseTest {
  static final DateTimeFormatter FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  static Schema inputSchema;
  static List<Row> inputRows;

  @BeforeClass
  public static void prepareInput() throws NumberFormatException, ParseException {
    inputSchema =
        Schema.builder()
            .addInt32Field("f_int")
            .addInt64Field("f_long")
            .addInt16Field("f_short")
            .addByteField("f_byte")
            .addFloatField("f_float")
            .addDoubleField("f_double")
            .addStringField("f_string")
            .addDateTimeField("f_timestamp")
            .addInt32Field("f_int2")
            .build();

    inputRows =
        TestUtils.RowsBuilder.of(inputSchema)
            .addRows(
                1,
                1000L,
                Short.valueOf("1"),
                Byte.valueOf("1"),
                1.0F,
                1.0,
                "string_row1",
                FORMAT.parseDateTime("2017-01-01 01:01:03"),
                1)
            .addRows(
                1,
                2000L,
                Short.valueOf("2"),
                Byte.valueOf("2"),
                2.0F,
                2.0,
                "string_row2",
                FORMAT.parseDateTime("2017-01-01 01:02:03"),
                2)
            .addRows(
                1,
                3000L,
                Short.valueOf("3"),
                Byte.valueOf("3"),
                3.0F,
                3.0,
                "string_row3",
                FORMAT.parseDateTime("2017-01-01 01:03:03"),
                3)
            .addRows(
                1,
                4000L,
                Short.valueOf("4"),
                Byte.valueOf("4"),
                4.0F,
                4.0,
                "string_row4",
                FORMAT.parseDateTime("2017-01-01 02:04:03"),
                4)
            .getRows();
  }
}
