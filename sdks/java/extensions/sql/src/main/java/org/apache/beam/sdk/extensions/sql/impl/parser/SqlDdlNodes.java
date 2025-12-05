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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNodeList;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.NlsString;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Pair;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Util;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utilities concerning {@link SqlNode} for DDL. */
public class SqlDdlNodes {
  private SqlDdlNodes() {}

  /** Creates a DROP TABLE. */
  public static SqlDropTable dropTable(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
    return new SqlDropTable(pos, ifExists, name);
  }

  /** Creates a column declaration. */
  public static SqlNode column(
      SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, SqlNode comment) {
    return new SqlColumnDeclaration(pos, name, dataType, comment);
  }

  /** Returns the schema in which to create an object. */
  static Pair<CalciteSchema, String> schema(
      CalcitePrepare.Context context, boolean mutable, SqlIdentifier id) {
    CalciteSchema rootSchema = mutable ? context.getMutableRootSchema() : context.getRootSchema();
    @Nullable CalciteSchema schema = null;
    List<String> path = null;
    if (!id.isSimple()) {
      path = Util.skipLast(id.names);
      schema = childSchema(rootSchema, path);
    }
    // if id isSimple or if the above returned a null schema, use default schema path
    if (schema == null) {
      path = context.getDefaultSchemaPath();
      schema = childSchema(rootSchema, path);
    }
    return Pair.of(checkStateNotNull(schema, "Got null sub-schema for path '%s'", path), name(id));
  }

  private static @Nullable CalciteSchema childSchema(CalciteSchema rootSchema, List<String> path) {
    @Nullable CalciteSchema schema = rootSchema;
    for (String p : path) {
      if (schema == null) {
        break;
      }
      schema = schema.getSubSchema(p, true);
    }
    return schema;
  }

  public static String name(SqlIdentifier id) {
    if (id.isSimple()) {
      return id.getSimple();
    } else {
      return Util.last(id.names);
    }
  }

  static @Nullable String getString(SqlNode n) {
    if (n == null) {
      return null;
    }
    if (n instanceof SqlIdentifier) {
      return ((SqlIdentifier) n).toString();
    }

    NlsString literalValue = (NlsString) SqlLiteral.value(n);
    return literalValue == null ? null : literalValue.getValue();
  }

  static List<String> getStringList(@Nullable SqlNodeList l) {
    if (l == null || l.isEmpty()) {
      return Collections.emptyList();
    }
    ImmutableList.Builder<String> resetPropsList = ImmutableList.builder();
    for (SqlNode propNode : l) {
      @Nullable String prop = SqlDdlNodes.getString(propNode);
      if (prop != null) {
        resetPropsList.add(prop);
      }
    }
    return resetPropsList.build();
  }

  static Map<String, String> getStringMap(@Nullable SqlNodeList nodeList) {
    if (nodeList == null || nodeList.isEmpty()) {
      return Collections.emptyMap();
    }

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (SqlNode property : nodeList) {
      checkState(
          property instanceof SqlNodeList,
          String.format(
              "Unexpected properties entry '%s' of class '%s'", property, property.getClass()));
      SqlNodeList kv = ((SqlNodeList) property);
      checkState(kv.size() == 2, "Expected 2 items in properties entry, but got %s", kv.size());
      String key = checkStateNotNull(SqlDdlNodes.getString(kv.get(0)));
      String value = checkStateNotNull(SqlDdlNodes.getString(kv.get(1)));
      builder.put(key, value);
    }

    return builder.build();
  }

  static SqlIdentifier getIdentifier(SqlNode n, SqlParserPos pos) {
    if (n instanceof SqlIdentifier) {
      return (SqlIdentifier) n;
    }

    return new SqlIdentifier(checkArgumentNotNull(getString(n)), pos);
  }
}

// End SqlDdlNodes.java
