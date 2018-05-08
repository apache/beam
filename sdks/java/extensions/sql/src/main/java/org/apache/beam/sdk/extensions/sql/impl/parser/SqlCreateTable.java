/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.impl.parser;

import static com.alibaba.fastjson.JSON.parseObject;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.calcite.util.Static.RESOURCE;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchema;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTable extends SqlCreate
    implements SqlExecutableStatement {
  private final SqlIdentifier name;
  private final SqlNodeList columnList;
  private final SqlNode type;
  private final SqlNode comment;
  private final SqlNode location;
  private final SqlNode tblProperties;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

  /** Creates a SqlCreateTable. */
  SqlCreateTable(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name, SqlNodeList columnList, SqlNode type,
      SqlNode comment, SqlNode location, SqlNode tblProperties) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = checkNotNull(name);
    this.columnList = columnList; // may be null
    this.type = checkNotNull(type);
    this.comment = comment; // may be null
    this.location = location; // may be null
    this.tblProperties = tblProperties; // may be null
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, type, comment, location, tblProperties);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    writer.keyword("TYPE");
    type.unparse(writer, 0, 0);
    if (comment != null) {
      writer.keyword("COMMENT");
      comment.unparse(writer, 0, 0);
    }
    if (location != null) {
      writer.keyword("LOCATION");
      location.unparse(writer, 0, 0);
    }
    if (tblProperties != null) {
      writer.keyword("TBLPROPERTIES");
      tblProperties.unparse(writer, 0, 0);
    }
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair =
        SqlDdlNodes.schema(context, true, name);
    if (pair.left.plus().getTable(pair.right) != null) {
      // Table exists.
      if (!ifNotExists) {
        // They did not specify IF NOT EXISTS, so give error.
        throw SqlUtil.newContextException(name.getParserPosition(),
            RESOURCE.tableExists(pair.right));
      }
      return;
    }
    // Table does not exist. Create it.
    if (!(pair.left.schema instanceof BeamCalciteSchema)) {
      throw SqlUtil.newContextException(name.getParserPosition(),
          RESOURCE.internal("Schema is not instanceof BeamCalciteSchema"));
    }
    BeamCalciteSchema schema = (BeamCalciteSchema) pair.left.schema;
    schema.getTableProvider().createTable(toTable());
  }

  private String getString(SqlNode n) {
    return n == null ? null : ((NlsString) SqlLiteral.value(n)).getValue();
  }

  public Table toTable() {
    List<Column> columns = new ArrayList<>(columnList.size());
    for (Ord<SqlNode> c : Ord.zip(columnList)) {
      if (c.e instanceof SqlColumnDeclaration) {
        final SqlColumnDeclaration d = (SqlColumnDeclaration) c.e;
        Column column = Column.builder()
            .name(d.name.getSimple().toLowerCase())
            .fieldType(CalciteUtils.toFieldType(
                d.dataType.deriveType(BeamQueryPlanner.TYPE_FACTORY).getSqlTypeName()))
            .nullable(d.dataType.getNullable())
            .comment(getString(d.comment))
            .build();
        columns.add(column);
      } else {
        throw new AssertionError(c.e.getClass());
      }
    }

    Table.Builder tb = Table.builder()
        .type(getString(type).toLowerCase())
        .name(name.getSimple().toLowerCase())
        .columns(columns);
    if (comment != null) {
      tb.comment(getString(comment));
    }
    if (location != null) {
      tb.location(getString(location));
    }
    if (tblProperties != null) {
      tb.properties(parseObject(getString(tblProperties)));
    } else {
      tb.properties(new JSONObject());
    }
    return tb.build();
  }
}

// End SqlCreateTable.java
