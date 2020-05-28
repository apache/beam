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

import java.util.*;
import org.apache.beam.sdk.schemas.*;
import org.apache.beam.sdk.testing.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.junit.*;

/**
 * A simple Analytic Functions experiment for BeamSQL created in order to understand the query
 * processing workflow of BeamSQL and Calcite.
 */
public class BeamAnalyticFunctionsExperimentTest extends BeamSqlDslBase {

  /**
   * Table schema and data taken from
   * https://cloud.google.com/bigquery/docs/reference/standard-sql/analytic-function-concepts#produce_table
   *
   * <p>Basic analytic function query taken from
   * https://cloud.google.com/bigquery/docs/reference/standard-sql/analytic-function-concepts#compute_a_grand_total
   */
  @Test
  public void testSimpleOverFunction() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);
    Schema schema =
        Schema.builder()
            .addStringField("item")
            .addStringField("category")
            .addInt32Field("purchases")
            .build();
    PCollection<Row> inputRows =
        pipeline
            .apply(
                Create.of(
                    TestUtils.rowsBuilderOf(schema)
                        .addRows(
                            "kale",
                            "vegetable",
                            23,
                            "orange",
                            "fruit",
                            2,
                            "cabbage",
                            "vegetable",
                            9,
                            "apple",
                            "fruit",
                            8,
                            "leek",
                            "vegetable",
                            2,
                            "lettuce",
                            "vegetable",
                            10)
                        .getRows()))
            .setRowSchema(schema);
    String sql =
        "SELECT item, purchases, category, sum(purchases) over () as total_purchases  FROM PCOLLECTION";
    PCollection<Row> result = inputRows.apply("sql", SqlTransform.query(sql));
    PAssert.that(result)
        .satisfies(
            input -> {
              Iterator<Row> iter = input.iterator();
              while (iter.hasNext()) {
                Row row = iter.next();
                // check results
              }
              return null;
            });

    pipeline.run();
  }
}
