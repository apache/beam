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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import java.net.URI;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

/**
 * A Calcite {@code SqlCall} which represents a create table statement.
 */
public class SqlCreateTable extends SqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator(
      "CREATE_TABLE", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... o) {
      assert functionQualifier == null;
      return new SqlCreateTable(pos, (SqlIdentifier) o[0], (SqlNodeList) o[1],
                                o[2], o[3], o[4], o[5], o[6]);
    }

    @Override
    public void unparse(
        SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      SqlCreateTable t = (SqlCreateTable) call;
      UnparseUtil u = new UnparseUtil(writer, leftPrec, rightPrec);
      u.keyword("CREATE", "TABLE").node(t.tblName).nodeList(
          t.fieldList);
      u.keyword("TYPE").node(t.type);
      u.keyword("COMMENT").node(t.comment);
      u.keyword("LOCATION").node(t.location);
      if (t.properties != null) {
        u.keyword("TBLPROPERTIES").node(t.properties);
      }
      if (t.query != null) {
        u.keyword("AS").node(t.query);
      }
    }
  };

  private final SqlIdentifier tblName;
  private final SqlNodeList fieldList;
  private final SqlNode type;
  private final SqlNode comment;
  private final SqlNode location;
  private final SqlNode properties;
  private final SqlNode query;

  public SqlCreateTable(
          SqlParserPos pos, SqlIdentifier tblName, SqlNodeList fieldList, SqlNode type,
          SqlNode comment, SqlNode location, SqlNode properties, SqlNode query) {
    super(pos);
    this.tblName = tblName;
    this.fieldList = fieldList;
    this.type = type;
    this.comment = comment;
    this.location = location;
    this.properties = properties;
    this.query = query;
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
    return ImmutableNullableList.of(tblName, fieldList, location, properties,
                                    query);
  }

  public String tableName() {
    return tblName.toString();
  }

  public URI location() {
    return location == null ? null : URI.create(getString(location));
  }

  public String type() {
    return type == null ? null : getString(type);
  }

  public String comment() {
    return comment == null ? null : getString(comment);
  }

  public JSONObject properties() {
    String propertiesStr = getString(properties);
    if (Strings.isNullOrEmpty(propertiesStr)) {
      return new JSONObject();
    } else {
      return JSON.parseObject(propertiesStr);
    }
  }

  private String getString(SqlNode n) {
    return n == null ? null : ((NlsString) SqlLiteral.value(n)).getValue();
  }

  @SuppressWarnings("unchecked")
  public List<ColumnDefinition> fieldList() {
    return (List<ColumnDefinition>) ((List<? extends SqlNode>) fieldList.getList());
  }
}
