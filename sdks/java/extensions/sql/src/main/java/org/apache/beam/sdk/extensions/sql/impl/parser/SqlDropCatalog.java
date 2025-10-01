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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchema;
import org.apache.beam.sdk.extensions.sql.meta.catalog.CatalogManager;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlDrop;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Pair;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlDropCatalog extends SqlDrop implements BeamSqlParser.ExecutableStatement {
  private static final Logger LOG = LoggerFactory.getLogger(SqlDropCatalog.class);
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("DROP CATALOG", SqlKind.OTHER_DDL);
  private final SqlIdentifier catalogName;

  public SqlDropCatalog(SqlParserPos pos, boolean ifExists, SqlNode catalogName) {
    super(OPERATOR, pos, ifExists);
    this.catalogName = SqlDdlNodes.getIdentifier(catalogName, pos);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getOperator().getName());
    if (ifExists) {
      writer.keyword("IF EXISTS");
    }
    catalogName.unparse(writer, leftPrec, rightPrec);
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
                  "Unexpected 'DROP CATALOG' call for Schema '%s' that is not a Catalog.", name)));
    }

    if (catalogManager.getCatalog(name) == null) {
      if (!ifExists) {
        throw SqlUtil.newContextException(
            catalogName.getParserPosition(),
            RESOURCE.internal(String.format("Cannot drop catalog: '%s' not found.", name)));
      }
      LOG.info("Ignoring 'DROP CATALOG` call for non-existent catalog: {}", name);
      return;
    }

    if (catalogManager.currentCatalog().name().equals(name)) {
      throw SqlUtil.newContextException(
          catalogName.getParserPosition(),
          RESOURCE.internal(
              String.format(
                  "Unable to drop active catalog '%s'. Please switch to another catalog first.",
                  name)));
    }

    catalogManager.dropCatalog(name);
    LOG.info("Successfully dropped catalog '{}'", name);
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(catalogName);
  }
}
