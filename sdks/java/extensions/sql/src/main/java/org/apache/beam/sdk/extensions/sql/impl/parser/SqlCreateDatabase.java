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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Static.RESOURCE;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.client.util.Lists;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.CatalogManagerSchema;
import org.apache.beam.sdk.extensions.sql.impl.CatalogSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlCreate;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Pair;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SqlCreateDatabase extends SqlCreate implements BeamSqlParser.ExecutableStatement {
  private final SqlIdentifier databaseName;
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE DATABASE", SqlKind.OTHER_DDL);

  public SqlCreateDatabase(
      SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier databaseName) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.databaseName = databaseName;
  }

  @Override
  public List<SqlNode> getOperandList() {
    ImmutableList.Builder<SqlNode> operands = ImmutableList.builder();
    operands.add(databaseName);
    return operands.build();
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (getReplace()) {
      writer.keyword("OR REPLACE");
    }
    writer.keyword("DATABASE");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    databaseName.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair = SqlDdlNodes.schema(context, true, databaseName);
    Schema schema = pair.left.schema;

    List<String> components = Lists.newArrayList(Splitter.on('.').split(databaseName.toString()));
    @Nullable
    String catalogName = components.size() > 1 ? components.get(components.size() - 2) : null;

    if (!(schema instanceof CatalogManagerSchema)) {
      throw SqlUtil.newContextException(
          databaseName.getParserPosition(),
          RESOURCE.internal(
              "Attempting to create database '"
                  + databaseName
                  + "' with unexpected Calcite Schema of type "
                  + schema.getClass()));
    }

    CatalogManagerSchema catalogManagerSchema = (CatalogManagerSchema) schema;
    CatalogSchema catalogSchema = catalogManagerSchema.getCurrentCatalogSchema();
    // override if a catalog name is present
    if (catalogName != null) {
      Schema overridden =
          checkStateNotNull(
              catalogManagerSchema.getSubSchema(catalogName),
              "Could not find Calcite Schema for catalog '%s'.",
              catalogName);
      checkState(
          overridden instanceof CatalogSchema,
          "Catalog '%s' had unexpected Calcite Schema of type %s. Expected type: %s.",
          catalogName,
          overridden.getClass(),
          CatalogSchema.class.getSimpleName());
      catalogSchema = (CatalogSchema) overridden;
    }

    catalogSchema.createDatabase(databaseName, ifNotExists);
  }
}
