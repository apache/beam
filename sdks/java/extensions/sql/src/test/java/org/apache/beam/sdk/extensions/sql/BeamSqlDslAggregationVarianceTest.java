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

import static org.apache.beam.sdk.extensions.sql.utils.RowAsserts.matchesScalar;

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** Integration tests for {@code VAR_POP} and {@code VAR_SAMP}. */
public class BeamSqlDslAggregationVarianceTest {

  private static final double PRECISION = 1e-7;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  private PCollection<Row> boundedInput;

  @Before
  public void setUp() {
    Schema schema =
        Schema.builder()
            .addInt32Field("f_int")
            .addDoubleField("f_double")
            .addInt32Field("f_int2")
            .build();

    List<Row> rowsInTableB =
        TestUtils.RowsBuilder.of(schema)
            .addRows(
                1, 1.0, 0, 4, 4.0, 0, 7, 7.0, 0, 13, 13.0, 0, 5, 5.0, 0, 10, 10.0, 0, 17, 17.0, 0)
            .getRows();

    boundedInput = pipeline.apply(Create.of(rowsInTableB).withRowSchema(schema));
  }

  @Test
  public void testPopulationVarianceDouble() {
    String sql = "SELECT VAR_POP(f_double) FROM PCOLLECTION GROUP BY f_int2";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql)))
        .satisfies(matchesScalar(26.40816326, PRECISION));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPopulationVarianceInt() {
    String sql = "SELECT VAR_POP(f_int) FROM PCOLLECTION GROUP BY f_int2";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(26));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSampleVarianceDouble() {
    String sql = "SELECT VAR_SAMP(f_double) FROM PCOLLECTION GROUP BY f_int2";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql)))
        .satisfies(matchesScalar(30.80952381, PRECISION));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSampleVarianceInt() {
    String sql = "SELECT VAR_SAMP(f_int) FROM PCOLLECTION GROUP BY f_int2";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(30));

    pipeline.run().waitUntilFinish();
  }
}
