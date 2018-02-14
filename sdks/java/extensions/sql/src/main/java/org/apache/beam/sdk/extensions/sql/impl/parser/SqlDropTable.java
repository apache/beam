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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * A Calcite {@code SqlCall} which represents a drop table statement.
 */
public class SqlDropTable extends SqlCall {
  private final SqlIdentifier tableName;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator(
      "DROP_TABLE", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... o) {
      assert functionQualifier == null;
      return new SqlDropTable(pos, (SqlIdentifier) o[0]);
    }

    @Override
    public void unparse(
        SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      SqlDropTable t = (SqlDropTable) call;
      UnparseUtil u = new UnparseUtil(writer, leftPrec, rightPrec);
      u.keyword("DROP", "TABLE").node(t.tableName);
    }
  };

  public SqlDropTable(SqlParserPos pos, SqlIdentifier tableName) {
    super(pos);
    this.tableName = tableName;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    getOperator().unparse(writer, this, leftPrec, rightPrec);
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.<SqlNode>of(tableName);
  }

  public String tableName() {
    return tableName.toString().toLowerCase();
  }
}
