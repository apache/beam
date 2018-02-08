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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/**
 * {@code BeamSql} is the DSL interface of BeamSQL. It translates a SQL query as a
 * {@link PTransform}, so developers can use standard SQL queries in a Beam pipeline.
 *
 * <h1>Beam SQL DSL usage:</h1>
 * A typical pipeline with Beam SQL DSL is:
 * <pre>
 *{@code
PipelineOptions options = PipelineOptionsFactory.create();
Pipeline p = Pipeline.create(options);

//create table from TextIO;
PCollection<Row> inputTableA = p.apply(TextIO.read().from("/my/input/patha")).apply(...);
PCollection<Row> inputTableB = p.apply(TextIO.read().from("/my/input/pathb")).apply(...);

//run a simple query, and register the output as a table in BeamSql;
String sql1 = "select MY_FUNC(c1), c2 from PCOLLECTION";
PCollection<Row> outputTableA = inputTableA.apply(
    BeamSql
        .query(sql1)
        .registerUdf("MY_FUNC", MY_FUNC.class, "FUNC");

//run a JOIN with one table from TextIO, and one table from another query
PCollection<Row> outputTableB =
    PCollectionTuple
        .of(new TupleTag<>("TABLE_O_A"), outputTableA)
        .and(new TupleTag<>("TABLE_B"), inputTableB)
        .apply(BeamSql.query("select * from TABLE_O_A JOIN TABLE_B where ..."));

//output the final result with TextIO
outputTableB.apply(...).apply(TextIO.write().to("/my/output/path"));

p.run().waitUntilFinish();
 * }
 * </pre>
 *
 */
@Experimental
public class BeamSql {

  /**
   * Returns a {@link QueryTransform} representing an equivalent execution plan.
   *
   * <p>The {@link QueryTransform} can be applied to a {@link PCollection}
   * or {@link PCollectionTuple} representing all the input tables.
   *
   * <p>The {@link PTransform} outputs a {@link PCollection} of {@link Row}.
   *
   * <p>If the {@link PTransform} is applied to {@link PCollection} then it gets registered with
   * name <em>PCOLLECTION</em>.
   *
   * <p>If the {@link PTransform} is applied to {@link PCollectionTuple} then
   * {@link TupleTag#getId()} is used as the corresponding {@link PCollection}s name.
   *
   * <ul>
   * <li>If the sql query only uses a subset of tables from the upstream {@link PCollectionTuple},
   *     this is valid;</li>
   * <li>If the sql query references a table not included in the upstream {@link PCollectionTuple},
   *     an {@code IllegalStateException} is thrown during query validation;</li>
   * <li>Always, tables from the upstream {@link PCollectionTuple} are only valid in the scope
   *     of the current query call.</li>
   * </ul>
   */
  public static QueryTransform query(String sqlQuery) {
    return QueryTransform.withQueryString(sqlQuery);
  }
}
