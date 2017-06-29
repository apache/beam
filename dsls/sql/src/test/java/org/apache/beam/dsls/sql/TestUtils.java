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

package org.apache.beam.dsls.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Test utilities.
 */
public class TestUtils {

  /**
   * A {@code DoFn} to convert a {@code BeamSqlRow} to a comparable {@code String}.
   */
  public static class BeamSqlRow2StringDoFn extends DoFn<BeamSqlRow, String> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(ctx.element().valueInString());
    }
  }

  /**
   * Convert list of {@code BeamSqlRow} to list of {@code String}.
   */
  public static List<String> beamSqlRows2Strings(List<BeamSqlRow> rows) {
    List<String> strs = new ArrayList<>();
    for (BeamSqlRow row : rows) {
      strs.add(row.valueInString());
    }

    return strs;
  }

  /**
   * Convenient way to build a list of {@code BeamSqlRow}s.
   *
   * <p>You can use it like this:
   *
   * <pre>{@code
   * TestUtils.RowsBuilder.of(
   *   Types.INTEGER, "order_id",
   *   Types.INTEGER, "sum_site_id",
   *   Types.VARCHAR, "buyer"
   * ).values(
   *   1, 3, "james",
   *   2, 5, "bond"
   *   ).getStringRows()
   * }</pre>
   * {@code}
   */
  public static class RowsBuilder {
    private BeamSqlRecordType type;
    private List<BeamSqlRow> rows = new ArrayList<>();

    /**
     * Create a RowsBuilder with the specified row type info.
     *
     * <p>Note: check the class javadoc for for detailed example.
     *
     * @args pairs of column type and column names.
     */
    public static RowsBuilder of(final Object... args) {
      List<Integer> types = new ArrayList<>();
      List<String> names = new ArrayList<>();
      int lastTypeIndex = 0;
      for (; lastTypeIndex < args.length; lastTypeIndex += 2) {
        types.add((int) args[lastTypeIndex]);
        names.add((String) args[lastTypeIndex + 1]);
      }

      BeamSqlRecordType beamSQLRecordType = BeamSqlRecordType.create(names, types);
      RowsBuilder builder = new RowsBuilder();
      builder.type = beamSQLRecordType;

      return builder;
    }

    /**
     * Add values to the builder.
     *
     * <p>Note: check the class javadoc for for detailed example.
     */
    public RowsBuilder values(final Object... args) {
      int fieldCount = type.size();
      for (int i = 0; i < args.length; i += fieldCount) {
        BeamSqlRow row = new BeamSqlRow(type);
        for (int j = 0; j < fieldCount; j++) {
          row.addField(j, args[i + j]);
        }
        this.rows.add(row);
      }

      return this;
    }

    public List<BeamSqlRow> getRows() {
      return rows;
    }

    public List<String> getStringRows() {
      return beamSqlRows2Strings(rows);
    }
  }
}
