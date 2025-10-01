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

import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Static.RESOURCE;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.TableUtils;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchema;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
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
import org.checkerframework.checker.nullness.qual.Nullable;

/** Parse tree for {@code CREATE EXTERNAL TABLE} statement. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SqlCreateExternalTable extends SqlCreate implements BeamSqlParser.ExecutableStatement {
  private final SqlIdentifier name;
  private final List<Schema.Field> columnList;
  private final SqlNode type;
  private final SqlNode comment;
  private final SqlNode location;
  private final SqlNode tblProperties;
  private final @Nullable SqlNodeList partitionFields;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE EXTERNAL TABLE", SqlKind.OTHER_DDL);

  /** Creates a SqlCreateExternalTable. */
  public SqlCreateExternalTable(
      SqlParserPos pos,
      boolean replace,
      boolean ifNotExists,
      SqlIdentifier name,
      List<Schema.Field> columnList,
      SqlNode type,
      SqlNodeList partitionFields,
      SqlNode comment,
      SqlNode location,
      SqlNode tblProperties) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = checkNotNull(name);
    this.columnList = columnList; // may be null
    this.type = checkNotNull(type);
    this.partitionFields = partitionFields;
    this.comment = comment; // may be null
    this.location = location; // may be null
    this.tblProperties = tblProperties; // may be null
  }

  @Override
  public List<SqlNode> getOperandList() {
    throw new UnsupportedOperationException(
        "Getting operands CREATE TABLE is unsupported at the moment");
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("EXTERNAL");
    writer.keyword("TABLE");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);

    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      columnList.forEach(column -> unparseColumn(writer, column));
      writer.endList(frame);
    }
    writer.keyword("TYPE");
    type.unparse(writer, 0, 0);
    if (partitionFields != null) {
      writer.keyword("PARTITIONED");
      writer.keyword("BY");
      writer.sep("(");
      for (int i = 0; i < partitionFields.size(); i++) {
        if (i > 0) {
          writer.sep(",");
        }
        SqlNode field = partitionFields.get(i);
        field.unparse(writer, 0, 0);
      }
      writer.sep(")");
    }
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
    final Pair<CalciteSchema, String> pair = SqlDdlNodes.schema(context, true, name);
    if (pair.left.plus().getTable(pair.right) != null) {
      // Table exists.
      if (!ifNotExists) {
        // They did not specify IF NOT EXISTS, so give error.
        throw SqlUtil.newContextException(
            name.getParserPosition(), RESOURCE.tableExists(pair.right));
      }
      return;
    }
    // Table does not exist. Create it.
    if (!(pair.left.schema instanceof BeamCalciteSchema)) {
      throw SqlUtil.newContextException(
          name.getParserPosition(),
          RESOURCE.internal("Schema is not instanceof BeamCalciteSchema"));
    }

    BeamCalciteSchema schema = (BeamCalciteSchema) pair.left.schema;
    Table table = toTable();
    if (partitionFields != null) {
      checkArgument(
          schema.resolveMetastore().supportsPartitioning(table),
          "Invalid use of 'PARTITIONED BY()': Table '%s' of type '%s' "
              + "does not support partitioning.",
          SqlDdlNodes.name(name),
          SqlDdlNodes.getString(type));
    }

    schema.resolveMetastore().createTable(table);
  }

  private void unparseColumn(SqlWriter writer, Schema.Field column) {
    writer.sep(",");
    writer.identifier(column.getName(), false);
    writer.identifier(CalciteUtils.toSqlTypeName(column.getType()).name(), false);

    if (column.getType().getNullable() != null && !column.getType().getNullable()) {
      writer.keyword("NOT NULL");
    }

    if (column.getDescription() != null) {
      writer.keyword("COMMENT");
      writer.literal(column.getDescription());
    }
  }

  private @Nullable List<String> parsePartitionFields() {
    if (partitionFields == null) {
      return null;
    }
    return partitionFields.stream().map(SqlDdlNodes::getString).collect(Collectors.toList());
  }

  private Table toTable() {
    return Table.builder()
        .type(SqlDdlNodes.getString(type))
        .name(SqlDdlNodes.name(name))
        .schema(columnList.stream().collect(toSchema()))
        .partitionFields(parsePartitionFields())
        .comment(SqlDdlNodes.getString(comment))
        .location(SqlDdlNodes.getString(location))
        .properties(
            (tblProperties == null)
                ? TableUtils.emptyProperties()
                : TableUtils.parseProperties(SqlDdlNodes.getString(tblProperties)))
        .build();
  }
}

// End SqlCreateExternalTable.java
