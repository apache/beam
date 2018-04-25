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

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Utilities concerning {@link SqlNode} for DDL.
 */
public class SqlDdlNodes {
  private SqlDdlNodes() {}

  /** Creates a CREATE TABLE. */
  public static SqlCreateTable createTable(SqlParserPos pos, boolean replace,
      boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList,
      SqlNode type, SqlNode comment, SqlNode location, SqlNode tblProperties) {
    return new SqlCreateTable(pos, replace, ifNotExists, name, columnList,
        type, comment, location, tblProperties);
  }

  /** Creates a DROP TABLE. */
  public static SqlDropTable dropTable(SqlParserPos pos, boolean ifExists,
      SqlIdentifier name) {
    return new SqlDropTable(pos, ifExists, name);
  }

  /** Creates a column declaration. */
  public static SqlNode column(SqlParserPos pos, SqlIdentifier name,
      SqlDataTypeSpec dataType, SqlNode comment) {
    return new SqlColumnDeclaration(pos, name, dataType, comment);
  }
}

// End SqlDdlNodes.java
