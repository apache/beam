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
package org.apache.beam.sdk.extensions.sql;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

/**
 * prepare input records to test {@link BeamSql}.
 *
 * <p>Note that, any change in these records would impact tests in this package.
 *
 */
public class BeamSqlDslBase {
  public static final DateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  @Rule
  public ExpectedException exceptions = ExpectedException.none();

  static Schema schemaInTableA;
  static List<Row> rowsInTableA;

  //bounded PCollections
  PCollection<Row> boundedInput1;
  PCollection<Row> boundedInput2;

  //unbounded PCollections
  PCollection<Row> unboundedInput1;
  PCollection<Row> unboundedInput2;

  @BeforeClass
  public static void prepareClass() throws ParseException {
    schemaInTableA =
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
            .withDecimalField("f_decimal")
            .build();

    rowsInTableA =
        TestUtils.RowsBuilder.of(schemaInTableA)
            .addRows(
                1, 1000L, (short) 1, (byte) 1, 1.0f, 1.0d, "string_row1",
                FORMAT.parse("2017-01-01 01:01:03"), 0, new BigDecimal(1))
            .addRows(
                2, 2000L, (short) 2, (byte) 2, 2.0f, 2.0d, "string_row2",
                FORMAT.parse("2017-01-01 01:02:03"), 0, new BigDecimal(2))
            .addRows(
                3, 3000L, (short) 3, (byte) 3, 3.0f, 3.0d, "string_row3",
                FORMAT.parse("2017-01-01 01:06:03"), 0, new BigDecimal(3))
            .addRows(
                4, 4000L, (short) 4, (byte) 4, 4.0f, 4.0d, "第四行",
                FORMAT.parse("2017-01-01 02:04:03"), 0, new BigDecimal(4))
            .getRows();
  }

  @Before
  public void preparePCollections() {
    boundedInput1 = PBegin.in(pipeline).apply("boundedInput1",
        Create.of(rowsInTableA).withCoder(schemaInTableA.getRowCoder()));

    boundedInput2 = PBegin.in(pipeline).apply("boundedInput2",
        Create.of(rowsInTableA.get(0)).withCoder(schemaInTableA.getRowCoder()));

    unboundedInput1 = prepareUnboundedPCollection1();
    unboundedInput2 = prepareUnboundedPCollection2();
  }

  private PCollection<Row> prepareUnboundedPCollection1() {
    TestStream.Builder<Row> values = TestStream
        .create(schemaInTableA.getRowCoder());

    for (Row row : rowsInTableA) {
      values = values.advanceWatermarkTo(new Instant(row.getDate("f_timestamp")));
      values = values.addElements(row);
    }

    return PBegin
        .in(pipeline)
        .apply("unboundedInput1", values.advanceWatermarkToInfinity())
        .apply("unboundedInput1.fixedWindow1year",
               Window.into(FixedWindows.of(Duration.standardDays(365))));
  }

  private PCollection<Row> prepareUnboundedPCollection2() {
    TestStream.Builder<Row> values = TestStream
        .create(schemaInTableA.getRowCoder());

    Row row = rowsInTableA.get(0);
    values = values.advanceWatermarkTo(new Instant(row.getDate("f_timestamp")));
    values = values.addElements(row);

    return PBegin
        .in(pipeline)
        .apply("unboundedInput2", values.advanceWatermarkToInfinity())
        .apply("unboundedInput2.fixedWindow1year",
               Window.into(FixedWindows.of(Duration.standardDays(365))));
  }
}
