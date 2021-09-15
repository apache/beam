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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.Pair;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** A mocked unbounded table. */
@Experimental
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class TestUnboundedTable extends TestTable {
  /** rows flow out from this table with the specified watermark instant. */
  private final List<Pair<Duration, List<Row>>> timestampedRows = new ArrayList<>();
  /** specify the index of column in the row which stands for the event time field. */
  private int timestampField;

  private BeamTableStatistics statistics = BeamTableStatistics.UNBOUNDED_UNKNOWN;

  private TestUnboundedTable(Schema beamSchema) {
    super(beamSchema);
  }

  /**
   * Convenient way to build a mocked unbounded table.
   *
   * <p>e.g.
   *
   * <pre>{@code
   * TestUnboundedTable
   *   .of(Types.BIGINT, "order_id",
   *       Types.INTEGER, "site_id",
   *       Types.DOUBLE, "price",
   *       Types.TIMESTAMP, "order_time")
   * }</pre>
   */
  public static TestUnboundedTable of(final Object... args) {
    return new TestUnboundedTable(TestTableUtils.buildBeamSqlSchema(args));
  }

  public TestUnboundedTable setStatistics(BeamTableStatistics statistics) {
    this.statistics = statistics;
    return this;
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    return this.statistics;
  }

  public TestUnboundedTable timestampColumnIndex(int idx) {
    this.timestampField = idx;
    return this;
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.UNBOUNDED;
  }

  /**
   * Add rows to the builder.
   *
   * <p>Sample usage:
   *
   * <pre>{@code
   * addRows(
   *   duration,      -- duration which stands for the corresponding watermark instant
   *   1, 3, "james", -- first row
   *   2, 5, "bond"   -- second row
   *   ...
   * )
   * }</pre>
   */
  public TestUnboundedTable addRows(Duration duration, Object... args) {
    List<Row> rows = TestTableUtils.buildRows(getSchema(), Arrays.asList(args));
    // record the watermark + rows
    this.timestampedRows.add(Pair.of(duration, rows));
    return this;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    TestStream.Builder<Row> values = TestStream.create(schema);

    for (Pair<Duration, List<Row>> pair : timestampedRows) {
      values = values.advanceWatermarkTo(new Instant(0).plus(pair.getKey()));
      for (int i = 0; i < pair.getValue().size(); i++) {
        values =
            values.addElements(
                TimestampedValue.of(
                    pair.getValue().get(i),
                    new Instant(pair.getValue().get(i).getDateTime(timestampField))));
      }
    }

    return begin
        .apply(
            "MockedUnboundedTable_" + COUNTER.incrementAndGet(),
            values.advanceWatermarkToInfinity())
        .setRowSchema(getSchema());
  }
}
