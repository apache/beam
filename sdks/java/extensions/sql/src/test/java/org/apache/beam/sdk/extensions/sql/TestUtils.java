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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.values.Row.toRow;
import static org.apache.beam.sdk.values.RowType.toRowType;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * Test utilities.
 */
public class TestUtils {
  /**
   * A {@code DoFn} to convert a {@code BeamSqlRow} to a comparable {@code String}.
   */
  public static class BeamSqlRow2StringDoFn extends DoFn<Row, String> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(ctx.element().toString());
    }
  }

  /**
   * Convert list of {@code BeamSqlRow} to list of {@code String}.
   */
  public static List<String> beamSqlRows2Strings(List<Row> rows) {
    List<String> strs = new ArrayList<>();
    for (Row row : rows) {
      strs.add(row.toString());
    }

    return strs;
  }

  public static RowsBuilder rowsBuilderOf(RowType type) {
    return RowsBuilder.of(type);
  }

  /**
   * Convenient way to build a list of {@code BeamSqlRow}s.
   *
   * <p>You can use it like this:

   * <pre>{@code
   * TestUtils.RowsBuilder.of(
   *   Types.INTEGER, "order_id",
   *   Types.INTEGER, "sum_site_id",
   *   Types.VARCHAR, "buyer"
   * ).addRows(
   *   1, 3, "james",
   *   2, 5, "bond"
   *   ).getStringRows()
   * }</pre>
   * {@code}
   */
  public static class RowsBuilder {
    private RowType type;
    private List<Row> rows = new ArrayList<>();

    /**
     * Create a RowsBuilder with the specified row type info.
     *
     * <p>For example:
     * <pre>{@code
     * TestUtils.RowsBuilder.of(
     *   Types.INTEGER, "order_id",
     *   Types.INTEGER, "sum_site_id",
     *   Types.VARCHAR, "buyer"
     * )}</pre>
     *
     * @args pairs of column type and column names.
     */
    public static RowsBuilder of(final Object... args) {
      RowType beamSQLRowType = buildBeamSqlRowType(args);
      RowsBuilder builder = new RowsBuilder();
      builder.type = beamSQLRowType;

      return builder;
    }

    /**
     * Create a RowsBuilder with the specified row type info.
     *
     * <p>For example:
     * <pre>{@code
     * TestUtils.RowsBuilder.of(
     *   rowType
     * )}</pre>
     *
     * @beamSQLRowType the row type.
     */
    public static RowsBuilder of(final RowType rowType) {
      RowsBuilder builder = new RowsBuilder();
      builder.type = rowType;

      return builder;
    }

    /**
     * Add rows to the builder.
     *
     * <p>Note: check the class javadoc for for detailed example.
     */
    public RowsBuilder addRows(final Object... args) {
      this.rows.addAll(buildRows(type, Arrays.asList(args)));
      return this;
    }

    /**
     * Add rows to the builder.
     *
     * <p>Note: check the class javadoc for for detailed example.
     */
    public RowsBuilder addRows(final List args) {
      this.rows.addAll(buildRows(type, args));
      return this;
    }

    public List<Row> getRows() {
      return rows;
    }

    public List<String> getStringRows() {
      return beamSqlRows2Strings(rows);
    }

    public PCollectionBuilder getPCollectionBuilder() {
      return
          pCollectionBuilder()
              .withRowType(type)
              .withRows(rows);
    }
  }

  public static PCollectionBuilder pCollectionBuilder() {
    return new PCollectionBuilder();
  }

  static class PCollectionBuilder {
    private RowType type;
    private List<Row> rows;
    private String timestampField;
    private Pipeline pipeline;

    public PCollectionBuilder withRowType(RowType type) {
      this.type = type;
      return this;
    }

    public PCollectionBuilder withRows(List<Row> rows) {
      this.rows = rows;
      return this;
    }

    /**
     * Event time field, defines watermark.
     */
    public PCollectionBuilder withTimestampField(String timestampField) {
      this.timestampField = timestampField;
      return this;
    }

    public PCollectionBuilder inPipeline(Pipeline pipeline) {
      this.pipeline = pipeline;
      return this;
    }

    /**
     * Builds an unbounded {@link PCollection} in {@link Pipeline}
     * set by {@link #inPipeline(Pipeline)}.
     *
     * <p>If timestamp field was set with {@link #withTimestampField(String)} then
     * watermark will be advanced to the values from that field.
     */
    public PCollection<Row> buildUnbounded() {
      checkArgument(pipeline != null);
      checkArgument(rows.size() > 0);

      if (type == null) {
        type = rows.get(0).getRowType();
      }

      TestStream.Builder<Row> values = TestStream.create(type.getRowCoder());

      for (Row row : rows) {
        if (timestampField != null) {
          values = values.advanceWatermarkTo(new Instant(row.getDate(timestampField)));
        }

        values = values.addElements(row);
      }

      return PBegin
          .in(pipeline)
          .apply("unboundedPCollection", values.advanceWatermarkToInfinity());
    }
  }

  /**
   * Convenient way to build a {@code BeamSqlRowType}.
   *
   * <p>e.g.
   *
   * <pre>{@code
   *   buildBeamSqlRowType(
   *       SqlCoders.BIGINT, "order_id",
   *       SqlCoders.INTEGER, "site_id",
   *       SqlCoders.DOUBLE, "price",
   *       SqlCoders.TIMESTAMP, "order_time"
   *   )
   * }</pre>
   */
  public static RowType buildBeamSqlRowType(Object... args) {
    return
        Stream
            .iterate(0, i -> i + 2)
            .limit(args.length / 2)
            .map(i -> toRecordField(args, i))
            .collect(toRowType());
  }

  private static RowType.Field toRecordField(Object[] args, int i) {
    return RowType.newField((String) args[i + 1], (Coder) args[i]);
  }

  /**
   * Convenient way to build a {@code BeamSqlRow}s.
   *
   * <p>e.g.
   *
   * <pre>{@code
   *   buildRows(
   *       rowType,
   *       1, 1, 1, // the first row
   *       2, 2, 2, // the second row
   *       ...
   *   )
   * }</pre>
   */
  public static List<Row> buildRows(RowType type, List<?> rowsValues) {
    return
        Lists
            .partition(rowsValues, type.getFieldCount())
            .stream()
            .map(values -> values.stream().collect(toRow(type)))
            .collect(toList());
  }

  public static PCollectionTuple tuple(String tag, PCollection<Row> pCollection) {
    return PCollectionTuple.of(new TupleTag<>(tag), pCollection);
  }

  public static PCollectionTuple tuple(String tag1, PCollection<Row> pCollection1,
                                       String tag2, PCollection<Row> pCollection2) {
    return tuple(tag1, pCollection1).and(new TupleTag<>(tag2), pCollection2);
  }

  public static PCollectionTuple tuple(String tag1, PCollection<Row> pCollection1,
                                       String tag2, PCollection<Row> pCollection2,
                                       String tag3, PCollection<Row> pCollection3) {
    return tuple(
        tag1, pCollection1,
        tag2, pCollection2).and(new TupleTag<>(tag3), pCollection3);
  }
}
