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
import org.apache.beam.dsls.sql.schema.BaseBeamTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.utils.CalciteUtils;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.tools.Frameworks;

/**
 * {@link BeamSqlEnv} prepares the execution context for {@link BeamSql} and {@link BeamSqlCli}.
 *
 * <p>It contains a {@link SchemaPlus} which holds the metadata of tables/UDF functions, and
 * a {@link BeamQueryPlanner} which parse/validate/optimize/translate input SQL queries.
 */
public class BeamSqlEnv {
//<<<<<<< HEAD
//  static SchemaPlus schema;
//  static BeamQueryPlanner planner;
//=======
  SchemaPlus schema;
  BeamQueryPlanner planner;
//>>>>>>> eb5852b... restrict the scope of BeamSqlEnv

  public BeamSqlEnv() {
    schema = Frameworks.createRootSchema(true);
    planner = new BeamQueryPlanner(schema);
  }

  /**
   * Register a UDF function which can be used in SQL expression.
   */
  public void registerUdf(String functionName, Class<?> clazz, String methodName) {
    schema.add(functionName, ScalarFunctionImpl.create(clazz, methodName));
  }

  /**
   * Registers a {@link BaseBeamTable} which can be used for all subsequent queries.
   *
   */
//<<<<<<< HEAD
  public void registerTable(String tableName, BaseBeamTable table) {
    schema.add(tableName, new BeamCalciteTable(table.getRecordType()));
//=======
//  public void registerTable(String tableName, BaseBeamTable table) {
//    schema.add(tableName, table);
//>>>>>>> eb5852b... restrict the scope of BeamSqlEnv
    planner.getSourceTables().put(tableName, table);
  }

  /**
   * Find {@link BaseBeamTable} by table name.
   */
  public BaseBeamTable findTable(String tableName){
    return planner.getSourceTables().get(tableName);
  }

  private static class BeamCalciteTable implements ScannableTable, Serializable {
    private BeamSqlRecordType beamSqlRecordType;
    public BeamCalciteTable(BeamSqlRecordType beamSqlRecordType) {
      this.beamSqlRecordType = beamSqlRecordType;
    }
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return CalciteUtils.toCalciteRecordType(this.beamSqlRecordType)
          .apply(BeamQueryPlanner.TYPE_FACTORY);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
      // not used as Beam SQL uses its own execution engine
      return null;
    }

    /**
     * Not used {@link Statistic} to optimize the plan.
     */
    @Override
    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    /**
     * all sources are treated as TABLE in Beam SQL.
     */
    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }
  }
}
