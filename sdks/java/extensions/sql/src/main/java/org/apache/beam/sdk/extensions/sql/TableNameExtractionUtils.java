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
package org.apache.beam.sdk.extensions.sql;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOperator;

/**
 * Helper class to extract table identifiers from the query.
 *
 * <p>Supports queries:
 *
 * <pre>
 *   ... FROM table...
 *   ... FROM table1, table2 AS x...
 *   ... FROM table1 JOIN (LEFT, INNER, OUTER etc) table2 JOIN table3 ...
 *   ... FROM table1 UNION (INTERSECT etc) SELECT ...
 * </pre>
 */
public class TableNameExtractionUtils {

  public static List<TableName> extractTableNamesFromNode(SqlNode node) {
    if (node instanceof SqlSelect) {
      return extractTableFromSelect((SqlSelect) node);
    }

    if (node instanceof SqlIdentifier) {
      return extractFromIdentifier((SqlIdentifier) node);
    }

    if (node instanceof SqlJoin) {
      return extractFromJoin((SqlJoin) node);
    }

    if (node instanceof SqlCall) {
      return extractFromCall((SqlCall) node);
    }

    return Collections.emptyList();
  }

  private static List<TableName> extractTableFromSelect(SqlSelect node) {
    return extractTableNamesFromNode(node.getFrom());
  }

  private static List<TableName> extractFromCall(SqlCall node) {
    if (node.getOperator() instanceof SqlAsOperator) {
      return extractTableNamesFromNode(node.getOperandList().get(0));
    }

    if (node.getOperator() instanceof SqlSetOperator) {
      return node.getOperandList().stream()
          .map(TableNameExtractionUtils::extractTableNamesFromNode)
          .flatMap(Collection::stream)
          .collect(toList());
    }

    return Collections.emptyList();
  }

  private static List<TableName> extractFromJoin(SqlJoin join) {
    return ImmutableList.<TableName>builder()
        .addAll(extractTableNamesFromNode(join.getLeft()))
        .addAll(extractTableNamesFromNode(join.getRight()))
        .build();
  }

  private static List<TableName> extractFromIdentifier(SqlIdentifier identifier) {
    return ImmutableList.of(TableName.create(identifier.names));
  }
}
