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
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
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
    tables = new HashMap<>(16);
    schema = Frameworks.createRootSchema(true);
    planner = new BeamQueryPlanner(schema);
  }

  /**
   * Register a UDF function which can be used in SQL expression.
   */
  public void registerUdf(String functionName, Class<?> clazz, String method) {
    schema.add(functionName, ScalarFunctionImpl.create(clazz, method));
  }

  /**
   * Register a UDF function which can be used in SQL expression.
   */
  public void registerUdf(String functionName, Class<? extends BeamSqlUdf> clazz) {
    registerUdf(functionName, clazz, BeamSqlUdf.UDF_METHOD);
  }

  /**
   * Register {@link SerializableFunction} as a UDF function which can be used in SQL expression.
   * Note, {@link SerializableFunction} must have a constructor without arguments.
   */
  public void registerUdf(String functionName, SerializableFunction sfn) {
    registerUdf(functionName, sfn.getClass(), "apply");
  }

  /**
   * Register a UDAF function which can be used in GROUP-BY expression.
   * See {@link org.apache.beam.sdk.transforms.Combine.CombineFn} on how to implement a UDAF.
   */
  public void registerUdaf(String functionName, Combine.CombineFn combineFn) {
    schema.add(functionName, new UdafImpl(combineFn));
  }

  /**
   * Registers {@link PCollection}s in {@link PCollectionTuple} as a tables.
   *
   * <p>Assumes that {@link PCollection} elements are {@link Row}s.
   *
   * <p>{@link TupleTag#getId()}s are used as table names.
   */
  public void registerPCollectionTuple(PCollectionTuple pCollectionTuple) {
    pCollectionTuple
        .getAll()
        .forEach((tag, pCollection) ->
                registerPCollection(tag.getId(), (PCollection<Row>) pCollection));
  }

  /**
   * Registers {@link PCollection} of {@link Row}s as a table.
   *
   * <p>Assumes that {@link PCollection#getCoder()} returns an instance of {@link RowCoder}.
   */
  public void registerPCollection(String name, PCollection<Row> pCollection) {
    registerTable(name, pCollection, ((RowCoder) pCollection.getCoder()).getRowType());
  }

  /**
   * Registers {@link PCollection} as a table.
   */
  public void registerTable(String tableName, PCollection<Row> pCollection, RowType rowType) {
    registerTable(tableName, new BeamPCollectionTable(pCollection, rowType));
  }

  /**
   * Registers a {@link BaseBeamTable} which can be used for all subsequent queries.
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

    @Override public boolean isRolledUp(String column) {
      return false;
    }

    @Override public boolean rolledUpColumnValidInsideAgg(String column,
                                                          SqlCall call, SqlNode parent,
                                                          CalciteConnectionConfig config) {
      return false;
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
