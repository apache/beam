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

import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.DateTimeType;
import org.apache.beam.sdk.extensions.sql.utils.DateTimeUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** BeamSqlDateTimeTest. */
public class BeamSqlDateTimeTest {
  private static final Schema INPUT_ROW_TYPE =
      Schema.builder().addLogicalTypeField("f_datetime", new DateTimeType()).build();

  private static final Row TEST_ROW =
      Row.withSchema(INPUT_ROW_TYPE)
          .addValue(DateTimeUtils.parseDateTime("2019-06-01 12:23:10.123"))
          .build();

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testSelectDateTime() {
    PCollection<Row> input = pCollectionOf2Elements();

    PCollection<Row> result =
        input.apply("sqlQuery", SqlTransform.query("SELECT * FROM PCOLLECTION"));

    PAssert.that(result).containsInAnyOrder(TEST_ROW);

    pipeline.run();
  }

  private PCollection<Row> pCollectionOf2Elements() {
    return pipeline.apply(
        "boundedInput1",
        Create.of(TEST_ROW)
            .withSchema(
                INPUT_ROW_TYPE,
                SerializableFunctions.identity(),
                SerializableFunctions.identity()));
  }
}
