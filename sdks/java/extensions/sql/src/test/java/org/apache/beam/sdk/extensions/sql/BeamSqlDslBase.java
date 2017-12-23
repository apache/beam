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
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
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

  public static BeamRecordSqlType rowTypeInTableA;
  public static List<BeamRecord> recordsInTableA;

  //bounded PCollections
  public PCollection<BeamRecord> boundedInput1;
  public PCollection<BeamRecord> boundedInput2;

  //unbounded PCollections
  public PCollection<BeamRecord> unboundedInput1;
  public PCollection<BeamRecord> unboundedInput2;

  @BeforeClass
  public static void prepareClass() throws ParseException {
    rowTypeInTableA = BeamRecordSqlType.create(
        Arrays.asList("f_int", "f_long", "f_short", "f_byte", "f_float", "f_double", "f_string",
            "f_timestamp", "f_int2", "f_decimal"),
        Arrays.asList(Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.TINYINT, Types.FLOAT,
            Types.DOUBLE, Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER, Types.DECIMAL));

    recordsInTableA = prepareInputRowsInTableA();
  }

  @Before
  public void preparePCollections(){
    boundedInput1 = PBegin.in(pipeline).apply("boundedInput1",
        Create.of(recordsInTableA).withCoder(rowTypeInTableA.getRecordCoder()));

    boundedInput2 = PBegin.in(pipeline).apply("boundedInput2",
        Create.of(recordsInTableA.get(0)).withCoder(rowTypeInTableA.getRecordCoder()));

    unboundedInput1 = prepareUnboundedPCollection1();
    unboundedInput2 = prepareUnboundedPCollection2();
  }

  private PCollection<BeamRecord> prepareUnboundedPCollection1() {
    TestStream.Builder<BeamRecord> values = TestStream
        .create(rowTypeInTableA.getRecordCoder());

    for (BeamRecord row : recordsInTableA) {
      values = values.advanceWatermarkTo(new Instant(row.getDate("f_timestamp")));
      values = values.addElements(row);
    }

    return PBegin.in(pipeline).apply("unboundedInput1", values.advanceWatermarkToInfinity());
  }

  private PCollection<BeamRecord> prepareUnboundedPCollection2() {
    TestStream.Builder<BeamRecord> values = TestStream
        .create(rowTypeInTableA.getRecordCoder());

    BeamRecord row = recordsInTableA.get(0);
    values = values.advanceWatermarkTo(new Instant(row.getDate("f_timestamp")));
    values = values.addElements(row);

    return PBegin.in(pipeline).apply("unboundedInput2", values.advanceWatermarkToInfinity());
  }

  private static List<BeamRecord> prepareInputRowsInTableA() throws ParseException{
    List<BeamRecord> rows = new ArrayList<>();

    BeamRecord row1 = new BeamRecord(rowTypeInTableA
        , 1, 1000L, Short.valueOf("1"), Byte.valueOf("1"), 1.0f, 1.0, "string_row1"
        , FORMAT.parse("2017-01-01 01:01:03"), 0, new BigDecimal(1));
    rows.add(row1);

    BeamRecord row2 = new BeamRecord(rowTypeInTableA
        , 2, 2000L, Short.valueOf("2"), Byte.valueOf("2"), 2.0f, 2.0, "string_row2"
        , FORMAT.parse("2017-01-01 01:02:03"), 0, new BigDecimal(2));
    rows.add(row2);

    BeamRecord row3 = new BeamRecord(rowTypeInTableA
        , 3, 3000L, Short.valueOf("3"), Byte.valueOf("3"), 3.0f, 3.0, "string_row3"
        , FORMAT.parse("2017-01-01 01:06:03"), 0, new BigDecimal(3));
    rows.add(row3);

    BeamRecord row4 = new BeamRecord(rowTypeInTableA
        , 4, 4000L, Short.valueOf("4"), Byte.valueOf("4"), 4.0f, 4.0, "第四行"
        , FORMAT.parse("2017-01-01 02:04:03"), 0, new BigDecimal(4));
    rows.add(row4);

    return rows;
  }
}
