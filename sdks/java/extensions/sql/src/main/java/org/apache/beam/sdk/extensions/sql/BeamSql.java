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
import org.apache.beam.sdk.coders.BeamRecordCoder;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.BeamRecord;
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
PCollection<BeamSqlRow> inputTableA = p.apply(TextIO.read().from("/my/input/patha"))
    .apply(...);
PCollection<BeamSqlRow> inputTableB = p.apply(TextIO.read().from("/my/input/pathb"))
    .apply(...);

//run a simple query, and register the output as a table in BeamSql;
String sql1 = "select MY_FUNC(c1), c2 from PCOLLECTION";
PCollection<BeamSqlRow> outputTableA = inputTableA.apply(
    BeamSql.query(sql1)
    .withUdf("MY_FUNC", MY_FUNC.class, "FUNC"));

//run a JOIN with one table from TextIO, and one table from another query
PCollection<BeamSqlRow> outputTableB = PCollectionTuple.of(
    new TupleTag<BeamSqlRow>("TABLE_O_A"), outputTableA)
                .and(new TupleTag<BeamSqlRow>("TABLE_B"), inputTableB)
    .apply(BeamSql.queryMulti("select * from TABLE_O_A JOIN TABLE_B where ..."));

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
   * Transforms a SQL query into a {@link PTransform} representing an equivalent execution plan.
   *
   * <p>The returned {@link PTransform} can be applied to a {@link PCollectionTuple} representing
   * all the input tables and results in a {@code PCollection<BeamSqlRow>} representing the output
   * table. The {@link PCollectionTuple} contains the mapping from {@code table names} to
   * {@code PCollection<BeamSqlRow>}, each representing an input table.
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
  public static QueryTransform queryMulti(String sqlQuery) {
    return new QueryTransform(sqlQuery);
  }

  /**
   * Transforms a SQL query into a {@link PTransform} representing an equivalent execution plan.
   *
   * <p>This is a simplified form of {@link #queryMulti(String)} where the query must reference
   * a single input table.
   *
   * <p>Make sure to query it from a static table name <em>PCOLLECTION</em>.
   */
  public static SimpleQueryTransform query(String sqlQuery) {
    return new SimpleQueryTransform(sqlQuery);
  }

  /**
   * A {@link PTransform} representing an execution plan for a SQL query.
   *
   * <p>The table names in the input {@code PCollectionTuple} are only valid during the current
   * query.
   */
  public static class QueryTransform extends
      PTransform<PCollectionTuple, PCollection<BeamRecord>> {
    private BeamSqlEnv beamSqlEnv = new BeamSqlEnv();
    private String sqlQuery;

    public QueryTransform(String sqlQuery) {
      this.sqlQuery = sqlQuery;
    }

    /**
     * register a UDF function used in this query.
     *
     * <p>Refer to {@link BeamSqlUdf} for more about how to implement a UDF in BeamSql.
     */
     public QueryTransform withUdf(String functionName, Class<? extends BeamSqlUdf> clazz){
       beamSqlEnv.registerUdf(functionName, clazz);
       return this;
     }
     /**
      * register {@link SerializableFunction} as a UDF function used in this query.
      * Note, {@link SerializableFunction} must have a constructor without arguments.
      */
      public QueryTransform withUdf(String functionName, SerializableFunction sfn){
        beamSqlEnv.registerUdf(functionName, sfn);
        return this;
      }

     /**
      * register a {@link CombineFn} as UDAF function used in this query.
      */
     public QueryTransform withUdaf(String functionName, CombineFn combineFn){
       beamSqlEnv.registerUdaf(functionName, combineFn);
       return this;
     }

    @Override
    public PCollection<BeamRecord> expand(PCollectionTuple input) {
      registerTables(input);

      BeamRelNode beamRelNode = null;
      try {
        beamRelNode = beamSqlEnv.getPlanner().convertToBeamRel(sqlQuery);
      } catch (ValidationException | RelConversionException | SqlParseException e) {
        throw new IllegalStateException(e);
      }

      try {
        return beamRelNode.buildBeamPipeline(input, beamSqlEnv);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    //register tables, related with input PCollections.
    private void registerTables(PCollectionTuple input){
      for (TupleTag<?> sourceTag : input.getAll().keySet()) {
        PCollection<BeamRecord> sourceStream = (PCollection<BeamRecord>) input.get(sourceTag);
        BeamRecordCoder sourceCoder = (BeamRecordCoder) sourceStream.getCoder();

        beamSqlEnv.registerTable(sourceTag.getId(),
            new BeamPCollectionTable(sourceStream,
                (BeamRecordSqlType) sourceCoder.getRecordType()));
      }
    }
  }

  /**
   * A {@link PTransform} representing an execution plan for a SQL query referencing
   * a single table.
   */
  public static class SimpleQueryTransform
      extends PTransform<PCollection<BeamRecord>, PCollection<BeamRecord>> {
    private static final String PCOLLECTION_TABLE_NAME = "PCOLLECTION";
    private QueryTransform delegate;

    public SimpleQueryTransform(String sqlQuery) {
      this.delegate = new QueryTransform(sqlQuery);
    }

    /**
     * register a UDF function used in this query.
     *
     * <p>Refer to {@link BeamSqlUdf} for more about how to implement a UDAF in BeamSql.
     */
    public SimpleQueryTransform withUdf(String functionName, Class<? extends BeamSqlUdf> clazz){
      delegate.withUdf(functionName, clazz);
      return this;
    }

    /**
     * register {@link SerializableFunction} as a UDF function used in this query.
     * Note, {@link SerializableFunction} must have a constructor without arguments.
     */
    public SimpleQueryTransform withUdf(String functionName, SerializableFunction sfn){
      delegate.withUdf(functionName, sfn);
      return this;
    }

    /**
     * register a {@link CombineFn} as UDAF function used in this query.
     */
    public SimpleQueryTransform withUdaf(String functionName, CombineFn combineFn){
      delegate.withUdaf(functionName, combineFn);
      return this;
    }

    private void validateQuery() {
      SqlNode sqlNode;
      try {
        sqlNode = delegate.beamSqlEnv.getPlanner().parseQuery(delegate.sqlQuery);
        delegate.beamSqlEnv.getPlanner().getPlanner().close();
      } catch (SqlParseException e) {
        throw new IllegalStateException(e);
      }

      if (sqlNode instanceof SqlSelect) {
        SqlSelect select = (SqlSelect) sqlNode;
        String tableName = select.getFrom().toString();
        if (!tableName.equalsIgnoreCase(PCOLLECTION_TABLE_NAME)) {
          throw new IllegalStateException("Use fixed table name " + PCOLLECTION_TABLE_NAME);
        }
      } else {
        throw new UnsupportedOperationException(
            "Sql operation: " + sqlNode.toString() + " is not supported!");
      }
    }

    @Override
    public PCollection<BeamRecord> expand(PCollection<BeamRecord> input) {
      validateQuery();
      return PCollectionTuple.of(new TupleTag<BeamRecord>(PCOLLECTION_TABLE_NAME), input)
          .apply(delegate);
    }
  }
}
