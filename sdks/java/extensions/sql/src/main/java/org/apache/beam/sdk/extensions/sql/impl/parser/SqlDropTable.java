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

import static org.apache.beam.sdk.extensions.sql.impl.parser.SqlDdlNodes.name;
import static org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Static.RESOURCE;

import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchema;
import org.apache.beam.sdk.extensions.sql.impl.CatalogManagerSchema;
import org.apache.beam.sdk.extensions.sql.impl.CatalogSchema;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Pair;

/** Parse tree for {@code DROP TABLE} statement. */
public class SqlDropTable extends SqlDropObject {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE);

  /** Creates a SqlDropTable. */
  SqlDropTable(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
    super(OPERATOR, pos, ifExists, name);
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair = SqlDdlNodes.schema(context, true, name);
    TableName pathOverride = TableName.create(name.toString());
    Schema schema = pair.left.schema;

    BeamCalciteSchema beamCalciteSchema;
    if (schema instanceof CatalogManagerSchema) {
      CatalogSchema catalogSchema = ((CatalogManagerSchema) schema).getCatalogSchema(pathOverride);
      beamCalciteSchema = catalogSchema.getDatabaseSchema(pathOverride);
    } else if (schema instanceof BeamCalciteSchema) {
      beamCalciteSchema = (BeamCalciteSchema) schema;
    } else {
      throw SqlUtil.newContextException(
          name.getParserPosition(),
          RESOURCE.internal(
              "Attempting to drop a table using unexpected Calcite Schema of type "
                  + schema.getClass()));
    }

    if (beamCalciteSchema.getTable(pair.right) == null) {
      // Table does not exist.
      if (!ifExists) {
        // They did not specify IF EXISTS, so give error.
        throw SqlUtil.newContextException(
            name.getParserPosition(), RESOURCE.tableNotFound(name.toString()));
      }
      return;
    }

    beamCalciteSchema.getTableProvider().dropTable(pair.right);
  }
}

// End SqlDropTable.java
