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
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.parser.SqlParserPos;

/** Parse tree for column. */
public class SqlColumnDeclaration extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

  final SqlIdentifier name;
  final SqlDataTypeSpec dataType;
  final SqlNode comment;

  /** Creates a SqlColumnDeclaration; use {@link SqlDdlNodes#column}. */
  SqlColumnDeclaration(
      SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, SqlNode comment) {
    super(pos);
    this.name = name;
    this.dataType = dataType;
    this.comment = comment;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, dataType);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, 0, 0);
    dataType.unparse(writer, 0, 0);
    if (dataType.getNullable() != null && !dataType.getNullable()) {
      writer.keyword("NOT NULL");
    }
    if (comment != null) {
      writer.keyword("COMMENT");
      comment.unparse(writer, 0, 0);
    }
  }
}

// End SqlColumnDeclaration.java
