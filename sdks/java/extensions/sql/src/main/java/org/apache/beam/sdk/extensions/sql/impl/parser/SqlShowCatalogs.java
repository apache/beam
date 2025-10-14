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

import static org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Static.RESOURCE;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.CatalogManagerSchema;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSetOption;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;

public class SqlShowCatalogs extends SqlSetOption implements BeamSqlParser.ExecutableStatement {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("SHOW CATALOGS", SqlKind.OTHER);

  public SqlShowCatalogs(SqlParserPos pos, String scope) {
    super(pos, scope, new SqlIdentifier("", pos), null);
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Collections.emptyList();
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public void execute(CalcitePrepare.Context context) {
    Schema schema = SqlDdlNodes.schema(context, true).schema;

    if (!(schema instanceof CatalogManagerSchema)) {
      throw SqlUtil.newContextException(
          pos,
          RESOURCE.internal(
              "Attempting execute 'SHOW CATALOGS' with unexpected Calcite Schema of type "
                  + schema.getClass()));
    }

    Collection<Catalog> catalogs = ((CatalogManagerSchema) schema).catalogs();
    print(catalogs);
  }

  private static void print(Collection<Catalog> catalogs) {
    final String HEADER_NAME = "Catalog Name";
    final String HEADER_TYPE = "Type";
    final String SEPARATOR_CHAR = "-";

    int nameWidth = HEADER_NAME.length();
    int typeWidth = HEADER_TYPE.length();

    // find the longest string in each column
    for (Catalog catalog : catalogs) {
      nameWidth = Math.max(nameWidth, catalog.name().length());
      typeWidth = Math.max(typeWidth, catalog.type().length());
    }

    // add a small padding
    nameWidth += 2;
    typeWidth += 2;

    // format string with calculated widths for left-justification (%-Ns)
    String format = "| %-" + nameWidth + "s | %-" + typeWidth + "s |%n";

    // separator width = column widths + padding + separators - corners ('+')
    int separatorWidth = nameWidth + typeWidth + 5;
    String separator =
        String.format(
            "+" + new String(new char[separatorWidth]).replace("\0", SEPARATOR_CHAR) + "+%n");

    // printing the table
    System.out.printf(separator);
    System.out.printf(format, HEADER_NAME, HEADER_TYPE);
    System.out.printf(separator);
    for (Catalog catalog :
        catalogs.stream()
            .sorted(Comparator.comparing(Catalog::name))
            .collect(Collectors.toList())) {
      System.out.printf(format, catalog.name(), catalog.type());
    }
    System.out.printf(separator);
  }
}
