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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.extensions.sql.schema.BeamSqlRowCoder;
import org.apache.beam.sdk.extensions.sql.schema.BeamSqlUdaf;
import org.apache.beam.sdk.extensions.sql.schema.BeamSqlUdf;
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
PCollection<BeamSqlRow> inputTableA = p.apply(TextIO.read().from("/my/input/patha"))
    .apply(...);
PCollection<BeamSqlRow> inputTableB = p.apply(TextIO.read().from("/my/input/pathb"))
    .apply(...);

//run a simple query, and register the output as a table in BeamSql;
String sql1 = "select MY_FUNC(c1), c2 from PCOLLECTION";
PCollection<BeamSqlRow> outputTableA = inputTableA.apply(
    BeamSql.simpleQuery(sql1)
    .withUdf("MY_FUNC", MY_FUNC.class, "FUNC"));

//run a JOIN with one table from TextIO, and one table from another query
PCollection<BeamSqlRow> outputTableB = PCollectionTuple.of(
    new TupleTag<BeamSqlRow>("TABLE_O_A"), outputTableA)
                .and(new TupleTag<BeamSqlRow>("TABLE_B"), inputTableB)
    .apply(BeamSql.query("select * from TABLE_O_A JOIN TABLE_B where ..."));

//output the final result with TextIO
outputTableB.apply(...).apply(TextIO.write().to("/my/output/path"));

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
   * all the input tables and results in a {@code PCollection<BeamSqlRow>} representing the output
   * table. The {@link PCollectionTuple} contains the mapping from {@code table names} to
   * {@code PCollection<BeamSqlRow>}, each representing an input table.
   *
   * <p>It is an error to apply a {@link PCollectionTuple} missing any {@code table names}
   * referenced within the query.
   */
  public static QueryTransform query(String sqlQuery) {
    return QueryTransform.builder()
        .setSqlEnv(new BeamSqlEnv())
        .setSqlQuery(sqlQuery)
        .build();
  }

  /**
   * Transforms a SQL query into a {@link PTransform} representing an equivalent execution plan.
   *
   * <p>This is a simplified form of {@link #query(String)} where the query must reference
   * a single input table.
   *
   * <p>Make sure to query it from a static table name <em>PCOLLECTION</em>.
   */
  public static SimpleQueryTransform simpleQuery(String sqlQuery) throws Exception {
    return SimpleQueryTransform.builder()
        .setSqlEnv(new BeamSqlEnv())
        .setSqlQuery(sqlQuery)
        .build();
  }

  /**
   * A {@link PTransform} representing an execution plan for a SQL query.
   */
  @AutoValue
  public abstract static class QueryTransform extends
      PTransform<PCollectionTuple, PCollection<BeamSqlRow>> {
    abstract BeamSqlEnv getSqlEnv();
    abstract String getSqlQuery();

    static Builder builder() {
      return new AutoValue_BeamSql_QueryTransform.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSqlQuery(String sqlQuery);
      abstract Builder setSqlEnv(BeamSqlEnv sqlEnv);
      abstract QueryTransform build();
    }

    /**
     * register a UDF function used in this query.
     */
     public QueryTransform withUdf(String functionName, Class<? extends BeamSqlUdf> clazz){
       getSqlEnv().registerUdf(functionName, clazz);
       return this;
     }

     /**
      * register a UDAF function used in this query.
      */
     public QueryTransform withUdaf(String functionName, Class<? extends BeamSqlUdaf> clazz){
       getSqlEnv().registerUdaf(functionName, clazz);
       return this;
     }

    @Override
    public PCollection<BeamSqlRow> expand(PCollectionTuple input) {
      registerTables(input);

      BeamRelNode beamRelNode = null;
      try {
        beamRelNode = getSqlEnv().planner.convertToBeamRel(getSqlQuery());
      } catch (ValidationException | RelConversionException | SqlParseException e) {
        throw new IllegalStateException(e);
      }

      try {
        return beamRelNode.buildBeamPipeline(input, getSqlEnv());
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    //register tables, related with input PCollections.
    private void registerTables(PCollectionTuple input){
      for (TupleTag<?> sourceTag : input.getAll().keySet()) {
        PCollection<BeamSqlRow> sourceStream = (PCollection<BeamSqlRow>) input.get(sourceTag);
        BeamSqlRowCoder sourceCoder = (BeamSqlRowCoder) sourceStream.getCoder();

        getSqlEnv().registerTable(sourceTag.getId(),
            new BeamPCollectionTable(sourceStream, sourceCoder.getSqlRecordType()));
      }
    }
  }

  /**
   * A {@link PTransform} representing an execution plan for a SQL query referencing
   * a single table.
   */
  @AutoValue
  public abstract static class SimpleQueryTransform
      extends PTransform<PCollection<BeamSqlRow>, PCollection<BeamSqlRow>> {
    private static final String PCOLLECTION_TABLE_NAME = "PCOLLECTION";
    abstract BeamSqlEnv getSqlEnv();
    abstract String getSqlQuery();

    static Builder builder() {
      return new AutoValue_BeamSql_SimpleQueryTransform.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSqlQuery(String sqlQuery);
      abstract Builder setSqlEnv(BeamSqlEnv sqlEnv);
      abstract SimpleQueryTransform build();
    }

    /**
     * register a UDF function used in this query.
     */
     public SimpleQueryTransform withUdf(String functionName, Class<? extends BeamSqlUdf> clazz){
       getSqlEnv().registerUdf(functionName, clazz);
       return this;
     }

     /**
      * register a UDAF function used in this query.
      */
     public SimpleQueryTransform withUdaf(String functionName, Class<? extends BeamSqlUdaf> clazz){
       getSqlEnv().registerUdaf(functionName, clazz);
       return this;
     }

    private void validateQuery() {
      SqlNode sqlNode;
      try {
        sqlNode = getSqlEnv().planner.parseQuery(getSqlQuery());
        getSqlEnv().planner.getPlanner().close();
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
    public PCollection<BeamSqlRow> expand(PCollection<BeamSqlRow> input) {
      validateQuery();
      return PCollectionTuple.of(new TupleTag<BeamSqlRow>(PCOLLECTION_TABLE_NAME), input)
          .apply(QueryTransform.builder()
              .setSqlEnv(getSqlEnv())
              .setSqlQuery(getSqlQuery())
              .build());
    }
  }
}
