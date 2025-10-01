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
package org.apache.beam.sdk.extensions.sql.impl.parser;

import static org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Static.RESOURCE;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchema;
import org.apache.beam.sdk.extensions.sql.meta.catalog.CatalogManager;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSetOption;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlUseCatalog extends SqlSetOption implements BeamSqlParser.ExecutableStatement {
  private static final Logger LOG = LoggerFactory.getLogger(SqlUseCatalog.class);
  private final SqlIdentifier catalogName;

  private static final SqlOperator OPERATOR = new SqlSpecialOperator("USE CATALOG", SqlKind.OTHER);

  public SqlUseCatalog(SqlParserPos pos, String scope, SqlNode catalogName) {
    super(pos, scope, SqlDdlNodes.getIdentifier(catalogName, pos), null);
    this.catalogName = SqlDdlNodes.getIdentifier(catalogName, pos);
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Collections.singletonList(catalogName);
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair = SqlDdlNodes.schema(context, true, catalogName);
    Schema schema = pair.left.schema;
    String name = pair.right;

    if (!(schema instanceof BeamCalciteSchema)) {
      throw SqlUtil.newContextException(
          catalogName.getParserPosition(),
          RESOURCE.internal("Schema is not of instance BeamCalciteSchema"));
    }

    BeamCalciteSchema beamCalciteSchema = (BeamCalciteSchema) schema;
    @Nullable CatalogManager catalogManager = beamCalciteSchema.getCatalogManager();
    if (catalogManager == null) {
      throw SqlUtil.newContextException(
          catalogName.getParserPosition(),
          RESOURCE.internal(
              String.format(
                  "Unexpected 'USE CATALOG' call for Schema '%s' that is not a Catalog.", name)));
    }

    if (catalogManager.getCatalog(name) == null) {
      throw SqlUtil.newContextException(
          catalogName.getParserPosition(),
          RESOURCE.internal(String.format("Cannot use catalog: '%s' not found.", name)));
    }

    if (catalogManager.currentCatalog().name().equals(name)) {
      LOG.info("Catalog '{}' is already in use.", name);
      return;
    }

    catalogManager.useCatalog(name);
    LOG.info("Switched to catalog '{}' (type: {})", name, catalogManager.currentCatalog().type());
  }
}
