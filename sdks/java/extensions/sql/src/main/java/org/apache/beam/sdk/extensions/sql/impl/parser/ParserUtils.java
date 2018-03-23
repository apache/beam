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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;

/**
 * Util method for parser.
 */
public class ParserUtils {

  /**
   * Convert a create table statement to a {@code Table} object.
   * @param stmt
   * @return the table
   */
  public static Table convertCreateTableStmtToTable(SqlCreateTable stmt) {
    List<Column> columns = new ArrayList<>(stmt.fieldList().size());
    for (ColumnDefinition columnDef : stmt.fieldList()) {
      Column column = Column.builder()
          .name(columnDef.name().toLowerCase())
          .typeDescriptor(CalciteUtils.toFieldTypeDescriptor(
              columnDef.type().deriveType(BeamQueryPlanner.TYPE_FACTORY)))
          .comment(columnDef.comment())
          .primaryKey(columnDef.constraint() instanceof ColumnConstraint.PrimaryKey)
          .build();
      columns.add(column);
    }

    Table table = Table.builder()
        .type(stmt.type().toLowerCase())
        .name(stmt.tableName().toLowerCase())
        .columns(columns)
        .comment(stmt.comment())
        .location(stmt.location())
        .properties(stmt.properties())
        .build();

    return table;
  }
}
