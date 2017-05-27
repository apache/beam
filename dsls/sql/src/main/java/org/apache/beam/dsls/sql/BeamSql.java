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

import java.io.Serializable;
import org.apache.beam.dsls.sql.exception.BeamSqlException;
import org.apache.beam.dsls.sql.exception.BeamSqlUnsupportedException;
import org.apache.beam.dsls.sql.planner.BeamQueryPlanner;
import org.apache.beam.dsls.sql.rel.BeamRelNode;
import org.apache.beam.dsls.sql.schema.BaseBeamTable;
import org.apache.beam.dsls.sql.schema.BeamPCollectionTable;
import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 * <h1>Beam SQL DSL vs CLI client</h1>
 * Beam SQL CLI client(pending) is another way to leverage SQL to process data,
 * which is built on DSL interfaces.
 *
 */
public class BeamSql implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(BeamSql.class);

  private static SchemaPlus schema;
  private static BeamQueryPlanner planner;

  static{
    //Disable Assertion in Calcite temporally, it would not ignore any grammar errors.
    ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(false);
    schema = Frameworks.createRootSchema(true);
    planner = new BeamQueryPlanner(schema);
  }

  /**
   * Add a UDF function.
   */
  public static void addUDFFunction(String functionName, Class<?> className, String methodName){
    schema.add(functionName, ScalarFunctionImpl.create(className, methodName));
  }

  /**
   * add a {@link BaseBeamTable} to schema repository.
   *
   */
  public static void registerTable(String tableName, BaseBeamTable table) {
    schema.add(tableName, table);
    planner.getSourceTables().put(tableName, table);
  }

  /**
   * Find {@link BaseBeamTable} by table name.
   */
  public static BaseBeamTable findTable(String tableName){
    return planner.getSourceTables().get(tableName);
  }
  /**
   * explain and display the logical execution plan.
   */
  public static String explainQuery(String sqlString)
      throws ValidationException, RelConversionException, SqlParseException {
    BeamRelNode exeTree = planner.convertToBeamRel(sqlString);
    String beamPlan = RelOptUtil.toString(exeTree);
    LOG.info(String.format("beamPlan>\n%s", beamPlan));

    return beamPlan;
  }

  /**
   * compile SQL, and return a {@link Pipeline}.
   *
   * <p>This may be changed with following CLI client.
   */
  @Experimental
  public static PCollection<BeamSQLRow> compilePipeline(String sqlStatement) throws Exception{
    PipelineOptions options = PipelineOptionsFactory.fromArgs(new String[] {}).withValidation()
        .as(PipelineOptions.class); // FlinkPipelineOptions.class
    options.setJobName("BeamPlanCreator");
    Pipeline pipeline = Pipeline.create(options);

    return compilePipeline(sqlStatement, pipeline);
  }

  /**
   * compile SQL, and return a {@link Pipeline}.
   *
   * <p>This may be changed with following CLI client.
   */
  @Experimental
  public static PCollection<BeamSQLRow> compilePipeline(String sqlStatement, Pipeline basePipeline)
      throws Exception{
    PCollection<BeamSQLRow> resultStream = planner.compileBeamPipeline(sqlStatement, basePipeline);
    return resultStream;
  }

  //methods for Beam SQL as DSL.
  /**
   * Translate a PCollection with type T to {@code PCollection<BeamSQLRow>}
   * which can be accepted by Beam SQL DSL.
   */
  public static <T> PTransform<PCollection<T>, PCollection<BeamSQLRow>> toBeamSqlRow(
      PTransform<PCollection<T>, PCollection<BeamSQLRow>> recordTransform,
      BeamSQLRecordType rowType) {
    return new BeamRowRecordReader<T>(recordTransform, rowType);
  }

  /**
   * {@link PTransform} used in {@link BeamSql#toBeamSqlRow(PTransform, BeamSQLRecordType)}
   * to translate row records.
   */
  public static class BeamRowRecordReader<T>
      extends PTransform<PCollection<T>, PCollection<BeamSQLRow>> {
    private PTransform<PCollection<T>, PCollection<BeamSQLRow>> recordTransform;
    private BeamSQLRecordType rowType;

    public BeamRowRecordReader(PTransform<PCollection<T>, PCollection<BeamSQLRow>> recordTransform,
        BeamSQLRecordType rowType) {
      this.recordTransform = recordTransform;
      this.rowType = rowType;
    }

    @Override
    public PCollection<BeamSQLRow> expand(PCollection<T> input) {
      return input.apply(recordTransform).setCoder(new BeamSqlRowCoder(rowType));
    }

  }

  /**
   * {@link BeamSql#query(String)} is the core method in Beam SQL DSL.
   *
   * <p>It translates a SQL query to a composite {@link PTransform}, which is applied to
   * {@link PCollectionTuple} and emit a {@link PCollection}. Here {@link PCollectionTuple}
   * contains the input tables, each is represented as a {@link PCollection}.
   */
  public static PTransform<PCollectionTuple, PCollection<BeamSQLRow>> query(String sqlQuery) {
    return new QueryTransform(sqlQuery);

  }

  /**
   * {@link BeamSql#simpleQuery(String)} is a light-weight method, with similar functions as
   * {@link BeamSql#query(String)}, but that it's applied to a single source table.
   */
  public static PTransform<PCollection<BeamSQLRow>, PCollection<BeamSQLRow>>
  simpleQuery(String sqlQuery) throws Exception {
    return new SimpleQueryTransform(sqlQuery);
  }

  /**
   * A {@link PTransform} to translate from one SQL to a Beam composite {@link PTransform}.
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
        beamRelNode = BeamSql.planner.convertToBeamRel(sqlQuery);
      } catch (ValidationException | RelConversionException | SqlParseException e) {
        throw new BeamSqlException(e);
      }

      try {
        return assemble(input, beamRelNode);
      } catch (Exception e) {
        throw new BeamSqlException(e);
      }
    }

    private PCollection<BeamSQLRow> assemble(PCollectionTuple input, BeamRelNode beamRelNode)
        throws Exception {
      return beamRelNode.buildBeamPipeline(input);
    }

  }

  /**
   * A {@link PTransform} to wrap a call of {@link BeamSql#simpleQuery(String)}
   * as {@link BeamSql#query(String)}.
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
        sqlNode = BeamSql.planner.parseQuery(sqlQuery);
        BeamSql.planner.getPlanner().close();
      } catch (SqlParseException e) {
        throw new BeamSqlException(e);
      }
      BeamSqlRowCoder inputCoder = (BeamSqlRowCoder) input.getCoder();

      if (sqlNode instanceof SqlSelect) {
        SqlSelect select = (SqlSelect) sqlNode;
        String tableName = select.getFrom().toString();
        BeamSql.registerTable(tableName,
            new BeamPCollectionTable(input, inputCoder.getTableSchema().toRelDataType()));
        return PCollectionTuple.of(new TupleTag<BeamSQLRow>(tableName), input)
            .apply(BeamSql.query(sqlQuery));
      } else {
        throw new BeamSqlUnsupportedException(sqlNode.toString());
      }
    }

  }

}
