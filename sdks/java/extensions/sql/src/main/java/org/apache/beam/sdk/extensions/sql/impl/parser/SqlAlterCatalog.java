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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.CatalogManagerSchema;
import org.apache.beam.sdk.extensions.sql.impl.CatalogSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlAlter;
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

public class SqlAlterCatalog extends SqlAlter implements BeamSqlParser.ExecutableStatement {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("ALTER CATALOG", SqlKind.OTHER_DDL);
  private final SqlIdentifier name;
  private final @Nullable SqlNodeList setProps;
  private final @Nullable SqlNodeList resetProps;

  public SqlAlterCatalog(
      SqlParserPos pos,
      @Nullable String scope,
      SqlNode name,
      @Nullable SqlNodeList setProps,
      @Nullable SqlNodeList resetProps) {
    super(pos, scope);
    this.name = SqlDdlNodes.getIdentifier(name, pos);
    this.setProps = setProps;
    this.resetProps = resetProps;
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair = SqlDdlNodes.schema(context, true, name);
    Schema schema = pair.left.schema;

    if (!(schema instanceof CatalogManagerSchema)) {
      throw SqlUtil.newContextException(
          name.getParserPosition(),
          RESOURCE.internal(
              "Attempting to alter catalog '"
                  + SqlDdlNodes.name(name)
                  + "' with unexpected Calcite Schema of type "
                  + schema.getClass()));
    }

    CatalogSchema catalogSchema =
        ((CatalogManagerSchema) schema).getCatalogSchema(SqlDdlNodes.getString(name));

    Map<String, String> setPropsMap = SqlDdlNodes.getStringMap(setProps);
    Collection<String> resetPropsList = SqlDdlNodes.getStringList(resetProps);

    ImmutableList.Builder<String> overlappingPropsBuilder = ImmutableList.builder();
    resetPropsList.stream().filter(setPropsMap::containsKey).forEach(overlappingPropsBuilder::add);
    List<String> overlappingProps = overlappingPropsBuilder.build();
    checkState(
        overlappingProps.isEmpty(),
        "Invalid %s call: Found overlapping properties between SET and RESET: %s.",
        OPERATOR,
        overlappingProps);

    catalogSchema.updateProperties(setPropsMap, resetPropsList);
  }

  @Override
  public void unparseAlterOperation(SqlWriter writer, int left, int right) {
    writer.keyword("CATALOG");
    name.unparse(writer, left, right);
    if (setProps != null && !setProps.isEmpty()) {
      writer.keyword("SET");
      writer.keyword("(");
      for (int i = 0; i < setProps.size(); i++) {
        if (i > 0) {
          writer.keyword(",");
        }
        SqlNode property = setProps.get(i);
        checkState(
            property instanceof SqlNodeList,
            String.format(
                "Unexpected properties entry '%s' of class '%s'", property, property.getClass()));
        SqlNodeList kv = ((SqlNodeList) property);

        kv.get(0).unparse(writer, left, right); // key
        writer.keyword("=");
        kv.get(1).unparse(writer, left, right); // value
      }
      writer.keyword(")");
    }

    if (resetProps != null) {
      writer.keyword("RESET");
      writer.sep("(");
      for (int i = 0; i < resetProps.size(); i++) {
        if (i > 0) {
          writer.sep(",");
        }
        SqlNode field = resetProps.get(i);
        field.unparse(writer, 0, 0);
      }
      writer.sep(")");
    }
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    ImmutableList.Builder<SqlNode> operands = ImmutableList.builder();
    operands.add(name);
    if (setProps != null) {
      operands.add(setProps);
    }
    if (resetProps != null) {
      operands.add(resetProps);
    }
    return operands.build();
  }
}
