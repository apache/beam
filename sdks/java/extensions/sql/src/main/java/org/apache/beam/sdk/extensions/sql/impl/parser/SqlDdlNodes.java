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

import java.util.List;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.NlsString;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.Pair;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.Util;
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
    final List<String> path;
    if (id.isSimple()) {
      path = context.getDefaultSchemaPath();
    } else {
      path = Util.skipLast(id.names);
    }
    CalciteSchema schema = mutable ? context.getMutableRootSchema() : context.getRootSchema();
    for (String p : path) {
      schema = schema.getSubSchema(p, true);
    }
    return Pair.of(schema, name(id));
  }

  static String name(SqlIdentifier id) {
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
    return ((NlsString) SqlLiteral.value(n)).getValue();
  }
}

// End SqlDdlNodes.java
