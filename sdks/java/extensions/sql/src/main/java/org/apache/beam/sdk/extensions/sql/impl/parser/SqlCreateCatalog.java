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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Static.RESOURCE;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchema;
import org.apache.beam.sdk.extensions.sql.meta.catalog.CatalogManager;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlCreate;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNodeList;
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

public class SqlCreateCatalog extends SqlCreate implements BeamSqlParser.ExecutableStatement {
  private static final Logger LOG = LoggerFactory.getLogger(SqlCreateCatalog.class);
  private final SqlIdentifier catalogName;
  private final SqlNode type;
  private final SqlNodeList properties;
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE CATALOG", SqlKind.OTHER_DDL);

  public SqlCreateCatalog(
      SqlParserPos pos,
      boolean replace,
      boolean ifNotExists,
      SqlNode catalogName,
      SqlNode type,
      SqlNodeList properties) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.catalogName = SqlDdlNodes.getIdentifier(catalogName, pos);
    this.type = checkArgumentNotNull(type, "Need to specify a TYPE for catalog '%s'", catalogName);
    this.properties = properties;
  }

  @Override
  public List<SqlNode> getOperandList() {
    ImmutableList.Builder<SqlNode> operands = ImmutableList.builder();
    operands.add(catalogName);
    operands.add(type);
    if (properties != null) {
      operands.add(properties);
    }
    return operands.build();
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (getReplace()) {
      writer.keyword("OR REPLACE");
    }
    writer.keyword("CATALOG");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    catalogName.unparse(writer, leftPrec, rightPrec);
    writer.keyword("TYPE");
    type.unparse(writer, leftPrec, rightPrec);
    if (properties != null && !properties.isEmpty()) {
      writer.keyword("PROPERTIES");
      writer.keyword("(");
      for (int i = 0; i < properties.size(); i++) {
        if (i > 0) {
          writer.keyword(",");
        }
        SqlNode property = properties.get(i);
        checkState(
            property instanceof SqlNodeList,
            String.format(
                "Unexpected properties entry '%s' of class '%s'", property, property.getClass()));
        SqlNodeList kv = ((SqlNodeList) property);

        kv.get(0).unparse(writer, leftPrec, rightPrec); // key
        writer.keyword("=");
        kv.get(1).unparse(writer, leftPrec, rightPrec); // value
      }
      writer.keyword(")");
    }
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair = SqlDdlNodes.schema(context, true, catalogName);
    Schema schema = pair.left.schema;
    String name = pair.right;
    String typeStr = checkArgumentNotNull(SqlDdlNodes.getString(type));

    if (!(schema instanceof BeamCalciteSchema)) {
      throw SqlUtil.newContextException(
          catalogName.getParserPosition(),
          RESOURCE.internal("Schema is not of instance BeamCalciteSchema"));
    }

    @Nullable CatalogManager catalogManager = ((BeamCalciteSchema) schema).getCatalogManager();
    if (catalogManager == null) {
      throw SqlUtil.newContextException(
          catalogName.getParserPosition(),
          RESOURCE.internal(
              String.format(
                  "Unexpected 'CREATE CATALOG' call for Schema '%s' that is not a Catalog.",
                  name)));
    }

    // check if catalog already exists
    if (catalogManager.getCatalog(name) != null) {
      if (getReplace()) {
        LOG.info("Replacing existing catalog '{}'", name);
        catalogManager.dropCatalog(name);
      } else if (!ifNotExists) {
        throw SqlUtil.newContextException(
            catalogName.getParserPosition(),
            RESOURCE.internal(String.format("Catalog '%s' already exists.", name)));
      } else {
        return;
      }
    }

    // create the catalog
    catalogManager.createCatalog(name, typeStr, parseProperties());
    LOG.info("Catalog '{}' (type: {}) successfully created", name, typeStr);
  }

  private Map<String, String> parseProperties() {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, String> props = new HashMap<>();

    for (SqlNode property : properties) {
      checkState(
          property instanceof SqlNodeList,
          String.format(
              "Unexpected properties entry '%s' of class '%s'", property, property.getClass()));
      SqlNodeList kv = ((SqlNodeList) property);
      checkState(kv.size() == 2, "Expected 2 items in properties entry, but got " + kv.size());
      String key = checkStateNotNull(SqlDdlNodes.getString(kv.get(0)));
      String value = checkStateNotNull(SqlDdlNodes.getString(kv.get(1)));
      props.put(key, value);
    }

    return props;
  }
}
