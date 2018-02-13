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
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration tests for {@code COVAR_POP} and {@code COVAR_SAMP}.
 */
public class BeamSqlDslAggregationCovarianceTest {

    private static final double PRECISION = 1e-7;

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    private PCollection<Row> boundedInput;

    @Before
    public void setUp() {
        RowType rowType = RowSqlType.builder()
                .withDoubleField("f_double1")
                .withDoubleField("f_double2")
                .withDoubleField("f_double3")
                .withIntegerField("f_int1")
                .withIntegerField("f_int2")
                .withIntegerField("f_int3")
                .build();

        List<Row> rowsInTableB =
                TestUtils.RowsBuilder
                        .of(rowType)
                        .addRows(
                                3.0, 1.0, 1.0, 3, 1, 0,
                                4.0, 2.0, 2.0, 4, 2, 0,
                                5.0, 3.0, 1.0, 5, 3, 0,
                                6.0, 4.0, 2.0, 6, 4, 0,
                                8.0, 4.0, 1.0, 8, 4, 0)
                        .getRows();

        boundedInput = PBegin
                .in(pipeline)
                .apply(Create.of(rowsInTableB).withCoder(rowType.getRowCoder()));
    }

    @Test
    public void testPopulationVarianceDouble() {
        String sql = "SELECT COVAR_POP(f_double1, f_double2) FROM PCOLLECTION GROUP BY f_int3";

        PAssert
                .that(boundedInput.apply(BeamSql.query(sql)))
                .satisfies(matchesScalar(1.84, PRECISION));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testPopulationVarianceInt() {
        String sql = "SELECT COVAR_POP(f_int1, f_int2) FROM PCOLLECTION GROUP BY f_int3";

        PAssert
                .that(boundedInput.apply(BeamSql.query(sql)))
                .satisfies(matchesScalar(1));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testSampleVarianceDouble() {
        String sql = "SELECT COVAR_SAMP(f_double1, f_double2) FROM PCOLLECTION GROUP BY f_int3";

        PAssert
                .that(boundedInput.apply(BeamSql.query(sql)))
                .satisfies(matchesScalar(2.3, PRECISION));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testSampleVarianceInt() {
        String sql = "SELECT COVAR_SAMP(f_int1, f_int2) FROM PCOLLECTION GROUP BY f_int3";

        PAssert
                .that(boundedInput.apply(BeamSql.query(sql)))
                .satisfies(matchesScalar(2));

        pipeline.run().waitUntilFinish();
    }
}
