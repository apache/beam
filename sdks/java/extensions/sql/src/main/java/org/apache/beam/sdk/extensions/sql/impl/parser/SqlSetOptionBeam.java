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

import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.Static.RESOURCE;

import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchema;
import org.apache.beam.sdk.extensions.sql.impl.SqlExecutableStatement;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlSetOption;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.Pair;

/** SQL parse tree node to represent {@code SET} and {@code RESET} statements. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class SqlSetOptionBeam extends SqlSetOption implements SqlExecutableStatement {

  public SqlSetOptionBeam(SqlParserPos pos, String scope, SqlIdentifier name, SqlNode value) {
    super(pos, scope, name, value);
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    final SqlIdentifier name = getName();
    final SqlNode value = getValue();
    final Pair<CalciteSchema, String> pair = SqlDdlNodes.schema(context, true, name);
    if (!(pair.left.schema instanceof BeamCalciteSchema)) {
      throw SqlUtil.newContextException(
          name.getParserPosition(),
          RESOURCE.internal("Schema is not instanceof BeamCalciteSchema"));
    }

    BeamCalciteSchema schema = (BeamCalciteSchema) pair.left.schema;

    if (value != null) {
      schema.setPipelineOption(pair.right, SqlDdlNodes.getString(value));
    } else if ("ALL".equals(pair.right)) {
      schema.removeAllPipelineOptions();
    } else {
      schema.removePipelineOption(pair.right);
    }
  }
}

// End SqlDropObject.java
