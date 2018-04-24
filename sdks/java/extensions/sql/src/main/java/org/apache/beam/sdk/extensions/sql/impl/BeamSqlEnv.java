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
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.UdafImpl;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSinkRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

/**
 * {@link BeamSqlEnv} prepares the execution context for {@link BeamSql} and {@link BeamSqlCli}.
 *
 * <p>It contains a {@link SchemaPlus} which holds the metadata of tables/UDF functions, and a
 * {@link BeamQueryPlanner} which parse/validate/optimize/translate input SQL queries.
 */
public class BeamSqlEnv implements Serializable {
  transient CalciteSchema schema;
  transient BeamQueryPlanner planner;

  public BeamSqlEnv() {
    schema = CalciteSchema.createRootSchema(true);
    planner = new BeamQueryPlanner(this, schema.plus());
  }

  /**
   * Register a UDF function which can be used in SQL expression.
   */
  public void registerUdf(String functionName, Class<?> clazz, String method) {
    schema.plus().add(functionName, ScalarFunctionImpl.create(clazz, method));
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
    schema.plus().add(functionName, new UdafImpl(combineFn));
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
    registerTable(name, pCollection, ((RowCoder) pCollection.getCoder()).getSchema());
  }

  /**
   * Registers {@link PCollection} as a table.
   */
  public void registerTable(String tableName, PCollection<Row> pCollection, Schema schema) {
    registerTable(tableName, new BeamPCollectionTable(pCollection, schema));
  }

  /**
   * Registers a {@link BaseBeamTable} which can be used for all subsequent queries.
   */
  public void registerTable(String tableName, BeamSqlTable table) {
    schema.add(tableName, new BeamCalciteTable(table));
  }

  public void deregisterTable(String targetTableName) {
    schema.removeTable(targetTableName);
  }

  private static class BeamCalciteTable extends AbstractQueryableTable
      implements ModifiableTable, TranslatableTable {
    private BeamSqlTable beamTable;

    public BeamCalciteTable(BeamSqlTable beamTable) {
      super(Object[].class);
      this.beamTable = beamTable;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return CalciteUtils.toCalciteRowType(this.beamTable.getSchema(),
          BeamQueryPlanner.TYPE_FACTORY);
    }

    @Override
    public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
      return new BeamIOSourceRel(
          context.getCluster(), relOptTable, beamTable);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
        SchemaPlus schema, String tableName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection getModifiableCollection() {
      return null;
    }

    @Override
    public TableModify toModificationRel(
        RelOptCluster cluster,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode child,
        TableModify.Operation operation,
        List<String> updateColumnList,
        List<RexNode> sourceExpressionList,
        boolean flattened) {
      return new BeamIOSinkRel(
          cluster, table, catalogReader, child, operation, updateColumnList,
          sourceExpressionList, flattened, beamTable);
    }
  }

  public BeamQueryPlanner getPlanner() {
    return planner;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();

    schema = CalciteSchema.createRootSchema(true);
    planner = new BeamQueryPlanner(this, schema.plus());
  }
}
