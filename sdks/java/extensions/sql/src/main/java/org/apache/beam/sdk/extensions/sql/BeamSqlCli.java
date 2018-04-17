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

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.parser.BeamSqlParser;
import org.apache.beam.sdk.extensions.sql.impl.parser.SqlCreateTable;
import org.apache.beam.sdk.extensions.sql.impl.parser.SqlDropTable;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.store.MetaStore;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlNode;

/**
 * {@link BeamSqlCli} provides methods to execute Beam SQL with an interactive client.
 */
@Experimental
public class BeamSqlCli {
  private BeamSqlEnv env;
  /**
   * The store which persists all the table meta data.
   */
  private MetaStore metaStore;

  public BeamSqlCli metaStore(MetaStore metaStore) {
    this.metaStore = metaStore;
    this.env = new BeamSqlEnv();

    // dump tables in metaStore into schema
    List<Table> tables = this.metaStore.listTables();
    for (Table table : tables) {
      env.registerTable(table.getName(), metaStore.buildBeamSqlTable(table.getName()));
    }

    return this;
  }

  public MetaStore getMetaStore() {
    return metaStore;
  }

  /**
   * Returns a human readable representation of the query execution plan.
   */
  public String explainQuery(String sqlString) throws Exception {
    BeamRelNode exeTree = env.getPlanner().convertToBeamRel(sqlString);
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
    } else if (sqlNode instanceof SqlDropTable) {
      handleDropTable((SqlDropTable) sqlNode);
    } else {
      PipelineOptions options = PipelineOptionsFactory.fromArgs(new String[] {}).withValidation()
          .as(PipelineOptions.class);
      options.setJobName("BeamPlanCreator");
      Pipeline pipeline = Pipeline.create(options);
      env.getPlanner().compileBeamPipeline(sqlString, pipeline);
      pipeline.run();
    }
  }

  private void handleCreateTable(SqlCreateTable stmt, MetaStore store) {
    Table table = stmt.toTable();
    if (table.getType() == null) {
      throw new IllegalStateException("Table type is not specified and BeamSqlCli#defaultTableType"
          + "is not configured!");
    }

    store.createTable(table);

    // register the new table into the schema
    env.registerTable(table.getName(), metaStore.buildBeamSqlTable(table.getName()));
  }

  private void handleDropTable(SqlDropTable stmt) {
    metaStore.dropTable(stmt.getNameSimple());
    env.deregisterTable(stmt.getNameSimple());
  }
}
