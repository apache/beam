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

import static org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.Static.RESOURCE;

import com.google.api.client.util.Lists;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.CatalogManagerSchema;
import org.apache.beam.sdk.extensions.sql.impl.CatalogSchema;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlSetOption;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.Pair;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;

public class SqlUseDatabase extends SqlSetOption implements BeamSqlParser.ExecutableStatement {
  private final SqlIdentifier databaseName;

  private static final SqlOperator OPERATOR = new SqlSpecialOperator("USE DATABASE", SqlKind.OTHER);

  public SqlUseDatabase(SqlParserPos pos, String scope, SqlIdentifier databaseName) {
    super(pos, scope, SqlDdlNodes.getIdentifier(databaseName, pos), null);
    this.databaseName = databaseName;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Collections.singletonList(databaseName);
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair = SqlDdlNodes.schema(context, true, databaseName);
    Schema schema = pair.left.schema;
    String path = databaseName.toString();
    List<String> components = Lists.newArrayList(Splitter.on(".").split(path));
    TableName pathOverride = TableName.create(components, "");

    if (!(schema instanceof CatalogManagerSchema)) {
      throw SqlUtil.newContextException(
          databaseName.getParserPosition(),
          RESOURCE.internal(
              "Attempting to create database '"
                  + path
                  + "' with unexpected Calcite Schema of type "
                  + schema.getClass()));
    }

    CatalogManagerSchema catalogManagerSchema = (CatalogManagerSchema) schema;
    CatalogSchema catalogSchema = catalogManagerSchema.getCatalogSchema(pathOverride);
    // if database exists in a different catalog, we need to also switch to that catalog
    if (pathOverride.catalog() != null
        && !pathOverride
            .catalog()
            .equals(catalogManagerSchema.getCurrentCatalogSchema().getCatalog().name())) {
      SqlIdentifier catalogIdentifier =
          new SqlIdentifier(pathOverride.catalog(), databaseName.getParserPosition());
      catalogManagerSchema.useCatalog(catalogIdentifier);
    }

    catalogSchema.useDatabase(databaseName);
  }
}
