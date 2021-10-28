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
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlWriter;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree for {@code UNIQUE}, {@code PRIMARY KEY} constraints.
 *
 * <p>And {@code FOREIGN KEY}, when we support it.
 */
public class SqlCheckConstraint extends SqlCall {
  private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CHECK", SqlKind.CHECK);

  private final SqlIdentifier name;
  private final SqlNode expression;

  /** Creates a SqlCheckConstraint; use {@link SqlDdlNodes#check}. */
  SqlCheckConstraint(SqlParserPos pos, SqlIdentifier name, SqlNode expression) {
    super(pos);
    this.name = name; // may be null
    this.expression = expression;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, expression);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (name != null) {
      writer.keyword("CONSTRAINT");
      name.unparse(writer, 0, 0);
    }
    writer.keyword("CHECK");
    if (writer.isAlwaysUseParentheses()) {
      expression.unparse(writer, 0, 0);
    } else {
      writer.sep("(");
      expression.unparse(writer, 0, 0);
      writer.sep(")");
    }
  }
}

// End SqlCheckConstraint.java
