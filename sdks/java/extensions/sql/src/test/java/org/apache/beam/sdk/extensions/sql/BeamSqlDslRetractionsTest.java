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

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for retractions mode.
 */
public class BeamSqlDslRetractionsTest extends BeamSqlDslBase {
  public PCollection<Row> boundedInput3;

  @Before
  public void setUp() {
    RowType rowTypeInTableB =
        RowSqlType.builder()
            .withIntegerField("f_int")
            .withDoubleField("f_double")
            .withIntegerField("f_int2")
            .withDecimalField("f_decimal")
            .build();

    List<Row> rowsInTableB =
        TestUtils.RowsBuilder.of(rowTypeInTableB)
            .addRows(
                1, 1.0, 0, new BigDecimal(1),
                4, 4.0, 0, new BigDecimal(4),
                7, 7.0, 0, new BigDecimal(7),
                13, 13.0, 0, new BigDecimal(13),
                5, 5.0, 0, new BigDecimal(5),
                10, 10.0, 0, new BigDecimal(10),
                17, 17.0, 0, new BigDecimal(17)
            ).getRows();

    boundedInput3 = PBegin.in(pipeline).apply(
        "boundedInput3",
        Create.of(rowsInTableB).withCoder(rowTypeInTableB.getRowCoder()));
  }

  @Test
  public void testSupportsNonGlobalWindowWithCustomTrigger() {
    DateTime startTime = new DateTime(2017, 1, 1, 0, 0, 0, 0);

    RowType type =
        RowSqlType
            .builder()
            .withIntegerField("f_intGroupingKey")
            .withIntegerField("f_intValue")
            .withTimestampField("f_timestamp")
            .build();

    Object[] rows = new Object[]{
        0, 1, startTime.plusSeconds(0).toDate(),
        0, 2, startTime.plusSeconds(1).toDate(),
        0, 3, startTime.plusSeconds(2).toDate(),
        0, 4, startTime.plusSeconds(3).toDate(),
        0, 5, startTime.plusSeconds(4).toDate(),
        0, 6, startTime.plusSeconds(6).toDate()
    };

    PCollection<Row> input =
        createTestPCollection(type, rows, "f_timestamp")
            .apply(Window
                       .<Row>into(
                           FixedWindows.of(Duration.standardDays(10)))
                       .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(2)))
                       .accumulatingAndRetractingFiredPanes()
                       .withAllowedLateness(Duration.standardDays(365))
                       .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY));

    String sql =
        "SELECT SUM(f_intValue) AS `sum` FROM PCOLLECTION GROUP BY f_intGroupingKey";

    PCollection<Row> result = input.apply("sql", BeamSql.query(sql));

//    assertEquals(
//        FixedWindows.of(Duration.standardSeconds(3)),
//        result.getWindowingStrategy().getWindowFn());

    PAssert
        .that(result)
        .containsInAnyOrder(
            rowsWithSingleIntField("sum", Arrays.asList(3, 3, 9, 6)));

    pipeline.run();
  }

  private List<Row> rowsWithSingleIntField(String fieldName, List<Integer> values) {
    return
        TestUtils
            .rowsBuilderOf(RowSqlType.builder().withIntegerField(fieldName).build())
            .addRows(values)
            .getRows();
  }

  private PCollection<Row> createTestPCollection(
      RowType type,
      Object[] rows,
      String timestampField) {
    return
        TestUtils
            .rowsBuilderOf(type)
            .addRows(rows)
            .getPCollectionBuilder()
            .inPipeline(pipeline)
            .withTimestampField(timestampField)
            .buildUnbounded();
  }
}
