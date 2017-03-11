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
package org.beam.sdk.java.sql.planner;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.beam.sdk.java.sql.planner.BeamQueryPlanner;
import org.beam.sdk.java.sql.rel.BeamRelNode;
import org.beam.sdk.java.sql.schema.BaseBeamTable;
import org.beam.sdk.java.sql.schema.BeamSQLRecordType;
import org.beam.sdk.java.sql.schema.kafka.BeamKafkaTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class BeamSqlRunner implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -4708693435115005182L;
  
  private static final Logger LOG = LoggerFactory.getLogger(BeamSqlRunner.class);
      
  private JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private SchemaPlus schema = Frameworks.createRootSchema(true);

  private BeamQueryPlanner planner = new BeamQueryPlanner(schema);

  /**
   * Add a schema.
   * 
   * @param schemaName
   * @param scheme
   */
  public void addSchema(String schemaName, Schema scheme) {
    schema.add(schemaName, schema);
  }

  /**
   * add a {@link BaseBeamTable} to schema repository.
   *  
   * @param tableName
   * @param table
   */
  public void addTable(String tableName, BaseBeamTable table) {
    schema.add(tableName, table);
    planner.getSourceTables().put(tableName, table);
  }

  /**
   * submit as a Beam pipeline.
   * 
   * @param sqlString
   * @throws Exception
   */
  public void submitQuery(String sqlString) throws Exception{
    planner.submitToRun(sqlString);
    planner.planner.close();
  }
  
  /**
   * explain and display the execution plan.
   * 
   * @param sqlString
   * @throws ValidationException
   * @throws RelConversionException
   * @throws SqlParseException
   */
  public void explainQuery(String sqlString) throws ValidationException, RelConversionException, SqlParseException {
    BeamRelNode exeTree = planner.convertToBeamRel(sqlString);
    String beamPlan = RelOptUtil.toString(exeTree);
    LOG.info("beamPlan>");
    LOG.info(beamPlan);
    planner.planner.close();
  }
}
