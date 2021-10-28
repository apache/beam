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
package org.apache.beam.sdk.extensions.sql.meta.provider.test;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.apache.beam.sdk.values.Row.toRow;

import java.util.List;
import java.util.stream.Stream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.Lists;

/** Utility functions for mock classes. */
@Experimental
@SuppressWarnings({"keyfor", "nullness"}) // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
public class TestTableUtils {

  /**
   * Create a RowsBuilder with the specified row type info.
   *
   * <p>For example:
   *
   * <pre>{@code
   * TestUtils.RowsBuilder.of(
   *   Types.INTEGER, "order_id",
   *   Types.INTEGER, "sum_site_id",
   *   Types.VARCHAR, "buyer"
   * )
   * }</pre>
   *
   * @param args pairs of column type and column names.
   */
  public static Schema buildBeamSqlSchema(Object... args) {
    return Stream.iterate(0, i -> i + 2)
        .limit(args.length / 2)
        .map(i -> toRecordField(args, i))
        .collect(toSchema());
  }

  public static Schema buildBeamSqlNullableSchema(Object... args) {
    return Stream.iterate(0, i -> i + 3)
        .limit(args.length / 3)
        .map(i -> toNullableRecordField(args, i))
        .collect(toSchema());
  }

  // TODO: support nested.
  public static Schema.Field toRecordField(Object[] args, int i) {
    return Schema.Field.of((String) args[i + 1], (FieldType) args[i]);
  }

  public static Schema.Field toNullableRecordField(Object[] args, int i) {
    if ((boolean) args[i + 2]) {
      return Schema.Field.nullable((String) args[i + 1], (FieldType) args[i]);
    }
    return Schema.Field.of((String) args[i + 1], (FieldType) args[i]);
  }

  /**
   * Convenient way to build a {@code BeamSqlRow}s.
   *
   * <p>e.g.
   *
   * <pre>{@code
   * buildRows(
   *     schema,
   *     1, 1, 1, // the first row
   *     2, 2, 2, // the second row
   *     ...
   * )
   * }</pre>
   */
  public static List<Row> buildRows(Schema type, List<?> rowsValues) {
    return Lists.partition(rowsValues, type.getFieldCount()).stream()
        .map(values -> values.stream().collect(toRow(type)))
        .collect(toList());
  }
}
