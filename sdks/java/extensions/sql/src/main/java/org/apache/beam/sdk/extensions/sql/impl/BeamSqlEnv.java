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
package org.apache.beam.sdk.extensions.sql.impl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.RowType;
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
 * {@link BeamSqlEnv} prepares the execution context for {@link BeamSql} and
 * {@link BeamSqlCli}.
 *
 * <p>It contains a {@link SchemaPlus} which holds the metadata of tables/UDF functions,
 * and a {@link BeamQueryPlanner} which parse/validate/optimize/translate input SQL queries.
 */
public class BeamSqlEnv implements Serializable {
  transient SchemaPlus schema;
  transient BeamQueryPlanner planner;
  transient Map<String, BeamSqlTable> tables;

  public BeamSqlEnv() {
    tables = new HashMap<String, BeamSqlTable>(16);
    schema = Frameworks.createRootSchema(true);
    planner = new BeamQueryPlanner(schema);
  }

  /**
   * Register a UDF function which can be used in SQL expression.
   */
  public void registerUdf(String functionName, Class<? extends BeamSqlUdf> clazz) {
    schema.add(functionName, ScalarFunctionImpl.create(clazz, BeamSqlUdf.UDF_METHOD));
  }

  /**
   * Register {@link SerializableFunction} as a UDF function which can be used in SQL expression.
   * Note, {@link SerializableFunction} must have a constructor without arguments.
   */
  public void registerUdf(String functionName, SerializableFunction sfn) {
    schema.add(functionName, ScalarFunctionImpl.create(sfn.getClass(), "apply"));
  }

  /**
   * Register a UDAF function which can be used in GROUP-BY expression.
   * See {@link org.apache.beam.sdk.transforms.Combine.CombineFn} on how to implement a UDAF.
   */
  public void registerUdaf(String functionName, Combine.CombineFn combineFn) {
    schema.add(functionName, new UdafImpl(combineFn));
  }

  /**
   * Registers a {@link BaseBeamTable} which can be used for all subsequent queries.
   *
   */
  public void registerTable(String tableName, BeamSqlTable table) {
    tables.put(tableName, table);
    schema.add(tableName, new BeamCalciteTable(table.getRowType()));
    planner.getSourceTables().put(tableName, table);
  }

  public void deregisterTable(String targetTableName) {
    // reconstruct the schema
    schema = Frameworks.createRootSchema(true);
    for (Map.Entry<String, BeamSqlTable> entry : tables.entrySet()) {
      String tableName = entry.getKey();
      BeamSqlTable table = entry.getValue();
      if (!tableName.equals(targetTableName)) {
        schema.add(tableName, new BeamCalciteTable(table.getRowType()));
      }
    }
    planner = new BeamQueryPlanner(schema);
  }

  /**
   * Find {@link BaseBeamTable} by table name.
   */
  public BeamSqlTable findTable(String tableName){
    return planner.getSourceTables().get(tableName);
  }

  private static class BeamCalciteTable implements ScannableTable, Serializable {
    private RowType beamRowType;
    public BeamCalciteTable(RowType beamRowType) {
      this.beamRowType = beamRowType;
    }
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return CalciteUtils.toCalciteRowType(this.beamRowType)
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

  public BeamQueryPlanner getPlanner() {
    return planner;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();

    tables = new HashMap<String, BeamSqlTable>(16);
    schema = Frameworks.createRootSchema(true);
    planner = new BeamQueryPlanner(schema);
  }
}
