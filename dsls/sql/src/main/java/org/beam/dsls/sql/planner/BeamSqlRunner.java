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
package org.beam.dsls.sql.planner;

import java.io.Serializable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.beam.dsls.sql.rel.BeamRelNode;
import org.beam.dsls.sql.schema.BaseBeamTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface to explain, submit a SQL query.
 *
 */
public class BeamSqlRunner implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = -4708693435115005182L;

  private static final Logger LOG = LoggerFactory.getLogger(BeamSqlRunner.class);

  private SchemaPlus schema = Frameworks.createRootSchema(true);

  private BeamQueryPlanner planner = new BeamQueryPlanner(schema);

  /**
   * Add a schema.
   *
   */
  public void addSchema(String schemaName, Schema scheme) {
    schema.add(schemaName, schema);
  }

  /**
   * add a {@link BaseBeamTable} to schema repository.
   *
   */
  public void addTable(String tableName, BaseBeamTable table) {
    schema.add(tableName, table);
    planner.getSourceTables().put(tableName, table);
  }

  /**
   * submit as a Beam pipeline.
   *
   */
  public void submitQuery(String sqlString) throws Exception {
    planner.submitToRun(sqlString);
    planner.planner.close();
  }

  /**
   * explain and display the execution plan.
   *
   */
  public String explainQuery(String sqlString)
      throws ValidationException, RelConversionException, SqlParseException {
    BeamRelNode exeTree = planner.convertToBeamRel(sqlString);
    String beamPlan = RelOptUtil.toString(exeTree);
    System.out.println(String.format("beamPlan>\n%s", beamPlan));

    planner.planner.close();
    return beamPlan;
  }

  protected BeamQueryPlanner getPlanner() {
    return planner;
  }

}
