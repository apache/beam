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

package org.apache.beam.dsls.sql.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.dsls.sql.schema.BeamIOType;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.calcite.util.Pair;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A mocked unbounded table.
 */
public class MockedUnboundedTable extends MockedTable {
  private List<Pair<Duration, List<BeamSqlRow>>> timestampedRows = new ArrayList<>();
  private int timestampField;
  private MockedUnboundedTable(BeamSqlRecordType beamSqlRecordType) {
    super(beamSqlRecordType);
  }

  /**
   * Convenient way to build a mocked table.
   *
   * <p>e.g.
   *
   * <pre>{@code
   * MockedUnboundedTable
   *   .of(Types.BIGINT, "order_id",
   *       Types.INTEGER, "site_id",
   *       Types.DOUBLE, "price",
   *       Types.TIMESTAMP, "order_time")
   * }</pre>
   */
  public static MockedUnboundedTable of(final Object... args){
    List<Integer> types = new ArrayList<>();
    List<String> names = new ArrayList<>();
    int lastTypeIndex = 0;
    for (; lastTypeIndex < args.length; lastTypeIndex += 2) {
      types.add((int) args[lastTypeIndex]);
      names.add((String) args[lastTypeIndex + 1]);
    }

    return new MockedUnboundedTable(
        BeamSqlRecordType.create(names, types)
    );
  }

  public MockedUnboundedTable timestampColumnIndex(int idx) {
    this.timestampField = idx;
    return this;
  }

  public MockedUnboundedTable addRows(Duration duration, Object... args) {
    List<BeamSqlRow> rows = new ArrayList<>();
    int fieldCount = getRecordType().size();

    for (int i = 0; i < args.length; i += fieldCount) {
      BeamSqlRow row = new BeamSqlRow(getRecordType());
      for (int j = 0; j < fieldCount; j++) {
        row.addField(j, args[i + j]);
      }
      rows.add(row);
    }

    // record the watermark + rows
    this.timestampedRows.add(Pair.of(duration, rows));
    return this;
  }

  @Override public BeamIOType getSourceType() {
    return BeamIOType.UNBOUNDED;
  }

  @Override public PCollection<BeamSqlRow> buildIOReader(Pipeline pipeline) {
    TestStream.Builder<BeamSqlRow> values = TestStream.create(
        new BeamSqlRowCoder(beamSqlRecordType));

    for (Pair<Duration, List<BeamSqlRow>> pair : timestampedRows) {
      values = values.advanceWatermarkTo(new Instant(0).plus(pair.getKey()));
      for (int i = 0; i < pair.getValue().size(); i++) {
        values = values.addElements(TimestampedValue.of(pair.getValue().get(i),
            new Instant(pair.getValue().get(i).getDate(timestampField))));
      }
    }

    return pipeline.begin().apply(
        "MockedUnboundedTable_" + COUNTER.incrementAndGet(),
        values.advanceWatermarkToInfinity());
  }

  @Override public PTransform<? super PCollection<BeamSqlRow>, PDone> buildIOWriter() {
    throw new UnsupportedOperationException("MockedUnboundedTable#buildIOWriter unsupported!");
  }
}
