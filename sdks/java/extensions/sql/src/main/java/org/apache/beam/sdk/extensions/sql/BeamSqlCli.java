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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.store.MetaStore;
import org.apache.beam.sdk.extensions.sql.parser.BeamSqlParser;
import org.apache.beam.sdk.extensions.sql.parser.ColumnConstraint;
import org.apache.beam.sdk.extensions.sql.parser.ColumnDefinition;
import org.apache.beam.sdk.extensions.sql.parser.SqlCreateTable;
import org.apache.beam.sdk.extensions.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlNode;

/**
 * {@link BeamSqlCli} provides methods to execute Beam SQL with an interactive client.
 */
@Experimental
public class BeamSqlCli {
  private BeamSqlEnv env;
  private MetaStore metaStore;
  /**
   * The default type of table(if not specified when create table).
   */
  private String defaultTableType;

  public BeamSqlCli(MetaStore metaStore) {
    this.metaStore = metaStore;
    this.env = new BeamSqlEnv();

    // dump tables in metaStore into schema
    List<Table> tables = this.metaStore.queryAllTables();
    for (Table table : tables) {
      env.registerTable(table.getName(), metaStore.buildBeamSqlTable(table.getName()));
    }
  }

  public MetaStore getMetaStore() {
    return metaStore;
  }

  /**
   * Returns a human readable representation of the query execution plan.
   */
  public static String explainQuery(String sqlString, BeamSqlEnv sqlEnv) throws Exception {
    BeamRelNode exeTree = sqlEnv.planner.convertToBeamRel(sqlString);
    String beamPlan = RelOptUtil.toString(exeTree);
    return beamPlan;
  }

  /**
   * Executes the given sql.
   */
  public void execute(String sqlString) throws Exception {
    BeamSqlParser parser = new BeamSqlParser(sqlString);
    SqlNode sqlNode = parser.impl().parseSqlStmtEof();

    if (sqlNode instanceof SqlCreateTable) {
      handleCreateTable((SqlCreateTable) sqlNode, metaStore);
    } else {
      PipelineOptions options = PipelineOptionsFactory.fromArgs(new String[] {}).withValidation()
          .as(PipelineOptions.class);
      options.setJobName("BeamPlanCreator");
      Pipeline pipeline = Pipeline.create(options);
      compilePipeline(sqlString, pipeline, env);
      pipeline.run();
    }
  }

  private void handleCreateTable(SqlCreateTable sqlNode, MetaStore store) {
    SqlCreateTable sqlCreateTable = sqlNode;
    List<Column> columns = new ArrayList<>(sqlCreateTable.fieldList().size());
    for (ColumnDefinition columnDef : sqlCreateTable.fieldList()) {
      Column column = Column.builder()
          .name(columnDef.name())
          .type(
              CalciteUtils.toJavaType(
                  columnDef.type().deriveType(BeamQueryPlanner.TYPE_FACTORY).getSqlTypeName()
              )
          )
          .comment(columnDef.comment())
          .primaryKey(columnDef.constraint() instanceof ColumnConstraint.PrimaryKey)
          .build();
      columns.add(column);
    }

    String tableType = sqlCreateTable.location() == null
        ? defaultTableType : sqlCreateTable.location().getScheme();

    if (tableType == null) {
      throw new IllegalStateException("Table type is not specified and BeamSqlCli#defaultTableType"
          + "is not configured!");
    }

    Table table = Table.builder()
        .type(tableType)
        .name(sqlCreateTable.tableName())
        .columns(columns)
        .comment(sqlCreateTable.comment())
        .location(sqlCreateTable.location())
        .properties(sqlCreateTable.properties())
        .build();
    store.createTable(table);

    // register the new table into the schema
    env.registerTable(table.getName(), metaStore.buildBeamSqlTable(table.getName()));
  }

  public BeamSqlCli defaultTableType(String type) {
    this.defaultTableType = type;
    return this;
  }

  /**
   * compile SQL, and return a {@link Pipeline}.
   */
  public static PCollection<BeamSqlRow> compilePipeline(String sqlStatement, BeamSqlEnv sqlEnv)
      throws Exception{
    PipelineOptions options = PipelineOptionsFactory.fromArgs(new String[] {}).withValidation()
        .as(PipelineOptions.class); // FlinkPipelineOptions.class
    options.setJobName("BeamPlanCreator");
    Pipeline pipeline = Pipeline.create(options);

    return compilePipeline(sqlStatement, pipeline, sqlEnv);
  }

  /**
   * compile SQL, and return a {@link Pipeline}.
   */
  public static PCollection<BeamSqlRow> compilePipeline(String sqlStatement, Pipeline basePipeline
      , BeamSqlEnv sqlEnv) throws Exception{
    PCollection<BeamSqlRow> resultStream =
        sqlEnv.planner.compileBeamPipeline(sqlStatement, basePipeline, sqlEnv);
    return resultStream;
  }
}
