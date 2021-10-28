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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamEnumerableConverter;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSinkRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.QueryProvider;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.Queryable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptTable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.prepare.Prepare;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.TableModify;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.ModifiableTable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.TranslatableTable;

/** Adapter from {@link BeamSqlTable} to a calcite Table. */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BeamCalciteTable extends AbstractQueryableTable
    implements ModifiableTable, TranslatableTable {
  private final BeamSqlTable beamTable;
  // These two options should be unified.
  // https://issues.apache.org/jira/projects/BEAM/issues/BEAM-7590
  private final Map<String, String> pipelineOptionsMap;
  private PipelineOptions pipelineOptions;

  BeamCalciteTable(
      BeamSqlTable beamTable,
      Map<String, String> pipelineOptionsMap,
      PipelineOptions pipelineOptions) {
    super(Object[].class);
    this.beamTable = beamTable;
    this.pipelineOptionsMap = pipelineOptionsMap;
    this.pipelineOptions = pipelineOptions;
  }

  public static BeamCalciteTable of(BeamSqlTable table) {
    return new BeamCalciteTable(table, ImmutableMap.of(), null);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return CalciteUtils.toCalciteRowType(this.beamTable.getSchema(), typeFactory);
  }

  private PipelineOptions getPipelineOptions() {
    if (pipelineOptions != null) {
      return pipelineOptions;
    }

    pipelineOptions = BeamEnumerableConverter.createPipelineOptions(pipelineOptionsMap);
    return pipelineOptions;
  }

  @Override
  public BeamTableStatistics getStatistic() {
    /*
     Changing class loader is required for the JDBC path. It is similar to what done in
     {@link BeamEnumerableConverter#toRowList} and {@link BeamEnumerableConverter#toEnumerable }.
    */
    final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(BeamEnumerableConverter.class.getClassLoader());
      return beamTable.getTableStatistics(getPipelineOptions());
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new BeamIOSourceRel(
        context.getCluster(),
        context.getCluster().traitSetOf(BeamLogicalConvention.INSTANCE),
        relOptTable,
        beamTable,
        pipelineOptionsMap,
        this);
  }

  @Override
  public <T> Queryable<T> asQueryable(
      QueryProvider queryProvider, SchemaPlus schema, String tableName) {
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
        cluster,
        table,
        catalogReader,
        child,
        operation,
        updateColumnList,
        sourceExpressionList,
        flattened,
        beamTable,
        pipelineOptionsMap);
  }
}
