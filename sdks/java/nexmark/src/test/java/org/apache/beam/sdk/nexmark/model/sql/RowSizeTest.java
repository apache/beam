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

package org.apache.beam.sdk.nexmark.model.sql;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Iterables;
import java.math.BigDecimal;
import java.util.Date;
import java.util.GregorianCalendar;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link RowSize}.
 */
public class RowSizeTest {

  private static final Schema ROW_TYPE = RowSqlType.builder()
      .withTinyIntField("f_tinyint")
      .withSmallIntField("f_smallint")
      .withIntegerField("f_int")
      .withBigIntField("f_bigint")
      .withFloatField("f_float")
      .withDoubleField("f_double")
      .withDecimalField("f_decimal")
      .withBooleanField("f_boolean")
      .withTimeField("f_time")
      .withDateField("f_date")
      .withTimestampField("f_timestamp")
      .withCharField("f_char")
      .withVarcharField("f_varchar")
      .build();

  private static final long ROW_SIZE = 91L;

  private static final Row ROW =
      Row
          .withSchema(ROW_TYPE)
          .addValues(
              (byte) 1,
              (short) 2,
              (int) 3,
              (long) 4,
              (float) 5.12,
              (double) 6.32,
              new BigDecimal(7),
              false,
              new GregorianCalendar(2019, 03, 02),
              new Date(10L),
              new Date(11L),
              "12",
              "13")
          .build();

  @Rule public TestPipeline testPipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCalculatesCorrectSize() throws Exception {
    assertEquals(ROW_SIZE, RowSize.of(ROW).sizeInBytes());
  }

  @Test
  public void testParDoConvertsToRecordSize() throws Exception {
    PCollection<Row> rows = testPipeline.apply(
        TestStream
            .create(ROW_TYPE.getRowCoder())
            .addElements(ROW)
            .advanceWatermarkToInfinity());

    PAssert
        .that(rows)
        .satisfies(new CorrectSize());

    testPipeline.run();
  }

  static class CorrectSize implements SerializableFunction<Iterable<Row>, Void> {
    @Override
    public Void apply(Iterable<Row> input) {
      RowSize recordSize = RowSize.of(Iterables.getOnlyElement(input));
      assertThat(recordSize.sizeInBytes(), equalTo(ROW_SIZE));
      return null;
    }
  }
}
