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
package org.apache.beam.dsls.sql;

import org.apache.beam.dsls.sql.exception.BeamSqlUnsupportedException;
import org.apache.beam.dsls.sql.rel.BeamRelNode;
import org.apache.beam.dsls.sql.schema.BeamPCollectionTable;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowCoder;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

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
TableSchema tableASchema = ...;
PCollection<BeamSqlRow> inputTableA = p.apply(TextIO.read().from("/my/input/patha"))
    .apply(BeamSql.fromTextRow(tableASchema));
TableSchema tableBSchema = ...;
PCollection<BeamSqlRow> inputTableB = p.apply(TextIO.read().from("/my/input/pathb"))
    .apply(BeamSql.fromTextRow(tableBSchema));

//run a simple query, and register the output as a table in BeamSql;
String sql1 = "select MY_FUNC(c1), c2 from TABLE_A";
PCollection<BeamSqlRow> outputTableA = inputTableA.apply(BeamSql.simpleQuery(sql1))
        .withUdf("MY_FUNC", myFunc);

//run a JOIN with one table from TextIO, and one table from another query
PCollection<BeamSqlRow> outputTableB = PCollectionTuple.of(
    new TupleTag<BeamSqlRow>("TABLE_O_A"), outputTableA)
                .and(new TupleTag<BeamSqlRow>("TABLE_B"), inputTableB)
    .apply(BeamSql.query("select * from TABLE_O_A JOIN TABLE_B where ..."));

//output the final result with TextIO
outputTableB.apply(BeamSql.toTextRow()).apply(TextIO.write().to("/my/output/path"));

p.run().waitUntilFinish();
 * }
 * </pre>
 */
@Experimental
public class BeamSql {
  /**
   * Transforms a SQL query into a {@link PTransform} representing an equivalent execution plan.
   *
   * <p>The returned {@link PTransform} can be applied to a {@link PCollectionTuple} representing
   * all the input tables and results in a {@code PCollection<BeamSQLRow>} representing the output
   * table. The {@link PCollectionTuple} contains the mapping from {@code table names} to
   * {@code PCollection<BeamSQLRow>}, each representing an input table.
   *
   * <p>It is an error to apply a {@link PCollectionTuple} missing any {@code table names}
   * referenced within the query.
   */
  public static PTransform<PCollectionTuple, PCollection<BeamSQLRow>> query(String sqlQuery) {
    return new QueryTransform(sqlQuery);

  }

  /**
   * Transforms a SQL query into a {@link PTransform} representing an equivalent execution plan.
   *
   * <p>This is a simplified form of {@link #query(String)} where the query must reference
   * a single input table.
   */
  public static PTransform<PCollection<BeamSQLRow>, PCollection<BeamSQLRow>>
  simpleQuery(String sqlQuery) throws Exception {
    return new SimpleQueryTransform(sqlQuery);
  }

  /**
   * A {@link PTransform} representing an execution plan for a SQL query.
   */
  public static class QueryTransform extends PTransform<PCollectionTuple, PCollection<BeamSQLRow>> {
    private String sqlQuery;
    public QueryTransform(String sqlQuery) {
      this.sqlQuery = sqlQuery;
    }

    @Override
    public PCollection<BeamSQLRow> expand(PCollectionTuple input) {
      BeamRelNode beamRelNode = null;
      try {
        beamRelNode = BeamSqlEnv.planner.convertToBeamRel(sqlQuery);
      } catch (ValidationException | RelConversionException | SqlParseException e) {
        throw new IllegalStateException(e);
      }

      try {
        return beamRelNode.buildBeamPipeline(input);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /**
   * A {@link PTransform} representing an execution plan for a SQL query referencing
   * a single table.
   */
  public static class SimpleQueryTransform
      extends PTransform<PCollection<BeamSQLRow>, PCollection<BeamSQLRow>> {
    private String sqlQuery;
    public SimpleQueryTransform(String sqlQuery) {
      this.sqlQuery = sqlQuery;
    }

    public SimpleQueryTransform withUdf(String udfName){
      throw new BeamSqlUnsupportedException("Pending for UDF support");
    }

    @Override
    public PCollection<BeamSQLRow> expand(PCollection<BeamSQLRow> input) {
      SqlNode sqlNode;
      try {
        sqlNode = BeamSqlEnv.planner.parseQuery(sqlQuery);
        BeamSqlEnv.planner.getPlanner().close();
      } catch (SqlParseException e) {
        throw new IllegalStateException(e);
      }
      BeamSqlRowCoder inputCoder = (BeamSqlRowCoder) input.getCoder();

      if (sqlNode instanceof SqlSelect) {
        SqlSelect select = (SqlSelect) sqlNode;
        String tableName = select.getFrom().toString();
        BeamSqlEnv.registerTable(tableName,
            new BeamPCollectionTable(input, inputCoder.getTableSchema().toRelDataType()));
        return PCollectionTuple.of(new TupleTag<BeamSQLRow>(tableName), input)
            .apply(BeamSql.query(sqlQuery));
      } else {
        throw new BeamSqlUnsupportedException(sqlNode.toString());
      }
    }
  }
}
