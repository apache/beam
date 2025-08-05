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
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.CatalogManagerSchema;
import org.apache.beam.sdk.extensions.sql.impl.CatalogSchema;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class SqlDropDatabase extends SqlDrop implements BeamSqlParser.ExecutableStatement {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("DROP DATABASE", SqlKind.OTHER_DDL);
  private final SqlIdentifier databaseName;
  private final boolean cascade;

  public SqlDropDatabase(
      SqlParserPos pos, boolean ifExists, SqlIdentifier databaseName, boolean cascade) {
    super(OPERATOR, pos, ifExists);
    this.databaseName = databaseName;
    this.cascade = cascade;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getOperator().getName());
    if (ifExists) {
      writer.keyword("IF EXISTS");
    }
    databaseName.unparse(writer, leftPrec, rightPrec);
    if (cascade) {
      writer.keyword("CASCADE");
    } else {
      writer.keyword("RESTRICT");
    }
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair = SqlDdlNodes.schema(context, true, databaseName);
    Schema schema = pair.left.schema;

    if (!(schema instanceof CatalogManagerSchema)) {
      throw SqlUtil.newContextException(
          databaseName.getParserPosition(),
          RESOURCE.internal(
              "Attempting to drop database '"
                  + databaseName
                  + "' with unexpected Calcite Schema of type "
                  + schema.getClass()));
    }

    List<String> components = Lists.newArrayList(Splitter.on(".").split(databaseName.toString()));
    TableName pathOverride = TableName.create(components, "");
    CatalogSchema catalogSchema = ((CatalogManagerSchema) schema).getCatalogSchema(pathOverride);
    catalogSchema.dropDatabase(databaseName, cascade, ifExists);
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(databaseName);
  }
}
