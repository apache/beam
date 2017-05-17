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
import org.apache.beam.dsls.sql.planner.BeamQueryPlanner;
import org.apache.beam.dsls.sql.rel.BeamRelNode;
import org.apache.beam.dsls.sql.schema.BaseBeamTable;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code BeamSQLEnvironment} is the integrated environment of BeamSQL.
 * It provides runtime context to execute SQL queries as Beam pipeline,
 * including table metadata, SQL engine and a Beam pipeline translator.
 *
 * <h1>1. BeamSQL as DSL</h1>
 * <em>BeamSQL as DSL</em> enables developers to embed SQL queries when writing a Beam pipeline.
 * A typical pipeline with BeamSQL DSL is:
 * <pre>
 *{@code
PipelineOptions options =  PipelineOptionsFactory...
Pipeline pipeline = Pipeline.create(options);

//prepare environment of BeamSQL
BeamSQLEnvironment sqlEnv = BeamSQLEnvironment.create();
//register table metadata
sqlEnv.addTableMetadata(String tableName, BeamSqlTable tableMetadata);
//register UDF
sqlEnv.registerUDF(String functionName, Method udfMethod);


//explain a SQL statement, SELECT only, and return as a PCollection;
PCollection<BeamSQLRow> phase1Stream = sqlEnv.explainSQL(pipeline, String sqlStatement);
//A PCollection explained by BeamSQL can be converted into a table, and apply queries on it;
sqlEnv.registerPCollectionAsTable(String tableName, phase1Stream);

//apply more queries, even based on phase1Stream

pipeline.run().waitUntilFinish();
 * }
 * </pre>
 *
 * <h1>2. BeamSQL as CLI</h1>
 * This feature is on planning, and not ready yet.
 *
 */
public class BeamSQLEnvironment implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(BeamSQLEnvironment.class);

  public static final BeamSQLEnvironment INSTANCE = new BeamSQLEnvironment();

  private SchemaPlus schema = Frameworks.createRootSchema(true);
  private BeamQueryPlanner planner = new BeamQueryPlanner(schema);

  private BeamSQLEnvironment() {
    //disable assertions in Calcite.
    ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(false);
  }

  /**
   * Return an instance of {@code BeamSQLEnvironment}.
   */
  public static BeamSQLEnvironment create(){
    return INSTANCE;
  }

  /**
   * Add a schema.
   *
   */
  public void addSchema(String schemaName, Schema scheme) {
    schema.add(schemaName, schema);
  }

  /**
   * add a {@link BaseBeamTable} to schema repository.
   */
  public void addTableMetadata(String tableName, BaseBeamTable tableMetadata) {
    schema.add(tableName, tableMetadata);
    planner.getSourceTables().put(tableName, tableMetadata);
  }

  /* Add a UDF function.
   *
   * <p>There're two requirements for function {@code methodName}:<br>
   * 1. It must be a STATIC method;<br>
   * 2. For a primitive parameter, use its wrapper class and handle NULL properly;
   */
  public void addUDFFunction(String functionName, Class<?> className, String methodName){
    schema.add(functionName, ScalarFunctionImpl.create(className, methodName));
  }

  /**
   * explain and display the execution plan.
   */
  public String executionPlan(String sqlString)
    throws ValidationException, RelConversionException, SqlParseException {
    BeamRelNode exeTree = planner.convertToBeamRel(sqlString);
    String beamPlan = RelOptUtil.toString(exeTree);
    LOG.info(String.format("beamPlan>\n%s", beamPlan));
    return beamPlan;
  }

  /**
   * {@code compileBeamPipeline} translate a SQL statement to executed as Beam data flow,
   * which is linked with the given {@code pipeline}. The final output stream is returned as
   * {@code PCollection} so more operations can be applied.
   */
  public PCollection<BeamSQLRow> compileBeamPipeline(String sqlStatement, Pipeline basePipeline)
      throws Exception{
    PCollection<BeamSQLRow> resultStream = planner.compileBeamPipeline(sqlStatement, basePipeline);
    return resultStream;
  }

}
