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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.CatalogManagerSchema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlDrop;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.Pair;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class SqlDropCatalog extends SqlDrop implements BeamSqlParser.ExecutableStatement {
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

    if (!(schema instanceof CatalogManagerSchema)) {
      throw SqlUtil.newContextException(
          catalogName.getParserPosition(),
          RESOURCE.internal(
              "Attempting to drop a catalog '"
                  + SqlDdlNodes.name(catalogName)
                  + "' with unexpected Calcite Schema of type "
                  + schema.getClass()));
    }

    ((CatalogManagerSchema) schema).dropCatalog(catalogName, ifExists);
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(catalogName);
  }
}
