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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchemaFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

/** Parse tree for {@code REGISTER EXTERNAL SCHEMA} statement. */
public class SqlRegisterExternalSchema extends SqlDdl implements SqlExecutableStatement {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("REGISTER EXTERNAL SCHEMA", SqlKind.OTHER_DDL);
  private final SqlNode type;
  private final SqlNode location;
  private final SqlNode properties;

  private SqlIdentifier name;

  /** Creates a SqlRegisterExternalSchema. */
  public SqlRegisterExternalSchema(
      SqlParserPos pos, SqlIdentifier name, SqlNode type, SqlNode location, SqlNode properties) {
    super(OPERATOR, pos);
    this.name = name;
    this.type = type;
    this.location = location;
    this.properties = properties;
  }

  @Override
  public List<SqlNode> getOperandList() {
    throw new UnsupportedOperationException(
        "Getting operands of REGISTER EXTERNAL SCHEMA is unsupported at the moment");
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("REGISTER");
    writer.keyword("EXTERNAL");
    writer.keyword("SCHEMA");

    writer.keyword("TYPE");
    type.unparse(writer, 0, 0);

    if (location != null) {
      writer.keyword("LOCATION");
      location.unparse(writer, 0, 0);
    }

    if (properties != null) {
      writer.keyword("PROPERTIES");
      properties.unparse(writer, 0, 0);
    }
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair = SqlDdlNodes.schema(context, true, name);
    final SchemaPlus parentSchema = pair.left.plus();
    final Schema subSchema =
        BeamCalciteSchemaFactory.INSTANCE.create(parentSchema, pair.right, null);

    SchemaPlus addedSchema = parentSchema.add(pair.right, subSchema);
    addedSchema.setCacheEnabled(false);
  }
}
