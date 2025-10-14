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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchema;
import org.apache.beam.sdk.extensions.sql.impl.CatalogManagerSchema;
import org.apache.beam.sdk.extensions.sql.impl.CatalogSchema;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.SqlFunctions;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSetOption;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SqlShowTables extends SqlSetOption implements BeamSqlParser.ExecutableStatement {
  private static final SqlOperator OPERATOR = new SqlSpecialOperator("SHOW TABLES", SqlKind.OTHER);
  private final @Nullable SqlIdentifier databaseName;
  private final @Nullable SqlNode regex;

  public SqlShowTables(
      SqlParserPos pos,
      String scope,
      @Nullable SqlIdentifier databaseName,
      @Nullable SqlNode regex) {
    super(pos, scope, new SqlIdentifier("", pos), null);
    this.databaseName = databaseName;
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
              "Attempting to execute 'SHOW TABLES' with unexpected Calcite Schema of type "
                  + schema.getClass()));
    }

    CatalogSchema catalogSchema;
    @Nullable BeamCalciteSchema databaseSchema;
    if (databaseName != null) {
      List<String> components = Lists.newArrayList(Splitter.on(".").split(databaseName.toString()));
      TableName pathOverride = TableName.create(components, "");
      catalogSchema =
          pathOverride.catalog() != null
              ? ((CatalogManagerSchema) schema).getCatalogSchema(pathOverride)
              : ((CatalogManagerSchema) schema).getCurrentCatalogSchema();

      databaseSchema = catalogSchema.getDatabaseSchema(pathOverride);
    } else {
      catalogSchema = ((CatalogManagerSchema) schema).getCurrentCatalogSchema();
      databaseSchema = catalogSchema.getCurrentDatabaseSchema();
    }

    if (databaseSchema == null) {
      throw SqlUtil.newContextException(
          pos,
          RESOURCE.internal(
              "Attempting to execute 'SHOW TABLES' with no Database used. Please set a Database first then re-run."));
    }

    String path = catalogSchema.getCatalog().name() + "." + databaseSchema.name();
    Collection<Table> tables = databaseSchema.getTables();
    print(tables, path, SqlDdlNodes.getString(regex));
  }

  private static void print(
      @Nullable Collection<Table> tables, String path, @Nullable String pattern) {
    SqlFunctions.LikeFunction calciteLike = new SqlFunctions.LikeFunction();

    final String HEADER_NAME = "Tables in " + path;
    final String HEADER_TYPE = "Type";
    final String SEPARATOR_CHAR = "-";

    int nameWidth = HEADER_NAME.length();
    int typeWidth = HEADER_TYPE.length();

    if (tables != null) {
      for (Table table : tables) {
        nameWidth = Math.max(nameWidth, table.getName().length());
        typeWidth = Math.max(typeWidth, table.getType().length());
      }
    }

    nameWidth += 2;
    typeWidth += 2;
    String format = "| %-" + nameWidth + "s | %-" + typeWidth + "s |%n";

    int separatorWidth = nameWidth + typeWidth + 5;
    String separator =
        String.format(
            "+" + new String(new char[separatorWidth]).replace("\0", SEPARATOR_CHAR) + "+%n");

    System.out.printf(separator);
    System.out.printf(format, HEADER_NAME, HEADER_TYPE);
    System.out.printf(separator);
    if (tables != null) {
      for (Table table :
          tables.stream()
              .sorted(Comparator.comparing(Table::getName))
              .collect(Collectors.toList())) {
        if (pattern == null || calciteLike.like(table.getName(), pattern)) {
          System.out.printf(format, table.getName(), table.getType());
        }
      }
      System.out.printf(separator);
    }
  }
}
