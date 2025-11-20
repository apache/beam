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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchema;
import org.apache.beam.sdk.extensions.sql.impl.CatalogManagerSchema;
import org.apache.beam.sdk.extensions.sql.impl.CatalogSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.SqlFunctions;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SqlShowDatabases extends SqlCall implements BeamSqlParser.ExecutableStatement {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("SHOW DATABASES", SqlKind.OTHER_DDL);

  private final boolean showCurrentOnly;
  private final @Nullable SqlIdentifier catalogName;
  private final @Nullable SqlNode regex;

  public SqlShowDatabases(
      SqlParserPos pos,
      boolean showCurrentOnly,
      @Nullable SqlIdentifier catalogName,
      @Nullable SqlNode regex) {
    super(pos);
    this.showCurrentOnly = showCurrentOnly;
    this.catalogName = catalogName;
    this.regex = regex;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Collections.emptyList();
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    Schema schema = SqlDdlNodes.schema(context, true).schema;

    if (!(schema instanceof CatalogManagerSchema)) {
      throw SqlUtil.newContextException(
          pos,
          RESOURCE.internal(
              "Attempting to execute 'SHOW DATABASES' with unexpected Calcite Schema of type "
                  + schema.getClass()));
    }

    CatalogSchema catalogSchema;
    if (catalogName != null) {
      String catalog = catalogName.toString();
      catalogSchema = ((CatalogManagerSchema) schema).getCatalogSchema(catalog);
    } else {
      catalogSchema = ((CatalogManagerSchema) schema).getCurrentCatalogSchema();
    }

    if (showCurrentOnly) {
      @Nullable BeamCalciteSchema currentDatabase = catalogSchema.getCurrentDatabaseSchema();
      String output =
          currentDatabase == null ? "No database is currently set" : currentDatabase.name();
      System.out.println(output);
      return;
    }
    Collection<String> databases = catalogSchema.databases();
    print(databases, catalogSchema.getCatalog().name(), SqlDdlNodes.getString(regex));
  }

  private static void print(Collection<String> databases, String path, @Nullable String pattern) {
    SqlFunctions.LikeFunction calciteLike = new SqlFunctions.LikeFunction();

    final String headerName = "Databases in " + path;
    final String separatorChar = "-";

    int nameWidth = headerName.length();

    for (String dbName : databases) {
      if (pattern == null || calciteLike.like(dbName, pattern)) {
        nameWidth = Math.max(nameWidth, dbName.length());
      }
    }

    nameWidth += 2;
    String format = "| %-" + nameWidth + "s |%n";

    int separatorWidth = nameWidth + 2;
    String separator =
        String.format(
            "+" + String.join("", Collections.nCopies(separatorWidth, separatorChar)) + "+%n");

    System.out.printf(separator);
    System.out.printf(format, headerName);
    System.out.printf(separator);
    for (String dbName : databases.stream().sorted().collect(Collectors.toList())) {
      if (pattern == null || calciteLike.like(dbName, pattern)) {
        System.out.printf(format, dbName);
      }
    }
    System.out.printf(separator);
  }
}
