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
package org.apache.beam.sdk.extensions.sql.zetasql.translation;

import static org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedTableScan;
import java.util.List;
import java.util.Properties;
import org.apache.beam.sdk.extensions.sql.zetasql.TableResolution;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptTable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelRoot;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.hint.RelHint;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Table;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.TranslatableTable;

/** Converts table scan. */
class TableScanConverter extends RelConverter<ResolvedTableScan> {

  TableScanConverter(ConversionContext context) {
    super(context);
  }

  @Override
  public RelNode convert(ResolvedTableScan zetaNode, List<RelNode> inputs) {

    List<String> tablePath = getTablePath(zetaNode.getTable());

    SchemaPlus defaultSchemaPlus = getConfig().getDefaultSchema();
    // TODO: reject incorrect top-level schema

    Table calciteTable = TableResolution.resolveCalciteTable(defaultSchemaPlus, tablePath);

    // we already resolved the table before passing the query to Analyzer, so it should be there
    checkNotNull(
        calciteTable,
        "Unable to resolve the table path %s in schema %s",
        tablePath,
        defaultSchemaPlus.getName());

    String defaultSchemaName = defaultSchemaPlus.getName();

    final CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            CalciteSchema.from(defaultSchemaPlus),
            ImmutableList.of(defaultSchemaName),
            getCluster().getTypeFactory(),
            new CalciteConnectionConfigImpl(new Properties()));

    RelOptTableImpl relOptTable =
        RelOptTableImpl.create(
            catalogReader,
            calciteTable.getRowType(getCluster().getTypeFactory()),
            calciteTable,
            ImmutableList.<String>builder().add(defaultSchemaName).addAll(tablePath).build());

    if (calciteTable instanceof TranslatableTable) {
      return ((TranslatableTable) calciteTable).toRel(createToRelContext(), relOptTable);
    } else {
      throw new UnsupportedOperationException("Does not support non TranslatableTable type table!");
    }
  }

  private List<String> getTablePath(com.google.zetasql.Table table) {
    if (!getTrait().isTableResolved(table)) {
      throw new IllegalArgumentException(
          "Unexpected table found when converting to Calcite rel node: " + table);
    }

    return getTrait().getTablePath(table);
  }

  private RelOptTable.ToRelContext createToRelContext() {
    return new RelOptTable.ToRelContext() {
      @Override
      public RelRoot expandView(
          RelDataType relDataType, String s, List<String> list, List<String> list1) {
        throw new UnsupportedOperationException("This RelContext does not support expandView");
      }

      @Override
      public RelOptCluster getCluster() {
        return TableScanConverter.this.getCluster();
      }

      @Override
      public List<RelHint> getTableHints() {
        return ImmutableList.of();
      }
    };
  }
}
