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
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamEnumerableConverter;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSinkRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
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
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;

/** Adapter from {@link BeamSqlTable} to a calcite Table. */
public class BeamCalciteTable extends AbstractQueryableTable
    implements ModifiableTable, TranslatableTable {
  private final BeamSqlTable beamTable;
  private final Map<String, String> pipelineOptions;

  BeamCalciteTable(BeamSqlTable beamTable, Map<String, String> pipelineOptions) {
    super(Object[].class);
    this.beamTable = beamTable;
    this.pipelineOptions = pipelineOptions;
  }

  public static BeamCalciteTable of(BeamSqlTable table) {
    return new BeamCalciteTable(table, ImmutableMap.of());
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return CalciteUtils.toCalciteRowType(this.beamTable.getSchema(), typeFactory);
  }

  @Override
  public Statistic getStatistic() {
    /*
     Changing class loader is required for the JDBC path. It is similar to what done in
     {@link BeamEnumerableConverter#toRowList} and {@link BeamEnumerableConverter#toEnumerable }.
    */
    final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(BeamEnumerableConverter.class.getClassLoader());
      BeamRowCountStatistics beamStatistics =
          beamTable.getRowCount(BeamEnumerableConverter.createPipelineOptions(pipelineOptions));
      return beamStatistics.isUnknown()
          ? Statistics.UNKNOWN
          : Statistics.of(beamStatistics.getRowCount().doubleValue(), ImmutableList.of());
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new BeamIOSourceRel(context.getCluster(), relOptTable, beamTable, pipelineOptions);
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
        pipelineOptions);
  }
}
