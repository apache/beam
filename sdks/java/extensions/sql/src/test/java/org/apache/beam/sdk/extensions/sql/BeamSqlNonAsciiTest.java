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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;

/**
 * Tests for non ascii char in sql.
 */
public class BeamSqlNonAsciiTest extends BeamSqlDslBase {

    @Test
    public void testDefaultCharsetLiteral() {
        String sql = "SELECT * FROM TABLE_A WHERE f_string = '第四行'";

    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("TABLE_A"), boundedInput1)
            .apply("testCompositeFilter", BeamSql.query(sql));

        PAssert.that(result).containsInAnyOrder(rowsInTableA.get(3));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testNationalCharsetLiteral() {
        String sql = "SELECT * FROM TABLE_A WHERE f_string = N'第四行'";

    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("TABLE_A"), boundedInput1)
            .apply("testCompositeFilter", BeamSql.query(sql));

        PAssert.that(result).containsInAnyOrder(rowsInTableA.get(3));

        pipeline.run().waitUntilFinish();
    }
}
