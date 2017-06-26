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
import org.apache.beam.dsls.sql.utils.CalciteUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A mocked unbounded table.
 */
public class MockedUnboundedTable extends MockedTable {
  private List<Pair<Duration, List<BeamSqlRow>>> timestampedRows = new ArrayList<>();
  private int timestampField;
  public MockedUnboundedTable(BeamSqlRecordType beamSqlRecordType, int timestampField) {
    super(beamSqlRecordType);
    this.timestampField = timestampField;
  }

  /**
   * Convenient way to build a mocked table with mock data:
   *
   * <p>e.g.
   *
   * <pre>{@code
   * MockedUnboundedTable
   *   .of(SqlTypeName.BIGINT, "order_id",
   *       SqlTypeName.INTEGER, "site_id",
   *       SqlTypeName.DOUBLE, "price",
   *       SqlTypeName.TIMESTAMP, "order_time",
   *
   *       1L, 2, 1.0, new Date(),
   *       1L, 1, 2.0, new Date(),
   *       2L, 4, 3.0, new Date(),
   *       2L, 1, 4.0, new Date(),
   *       5L, 5, 5.0, new Date(),
   *       6L, 6, 6.0, new Date(),
   *       7L, 7, 7.0, new Date(),
   *       8L, 8888, 8.0, new Date(),
   *       8L, 999, 9.0, new Date(),
   *       10L, 100, 10.0, new Date())
   * }</pre>
   */
  public static MockedUnboundedTable of(final Object... args){
    final RelProtoDataType protoRowType = new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a0) {
        RelDataTypeFactory.FieldInfoBuilder builder = a0.builder();

        int lastTypeIndex = 0;
        for (; lastTypeIndex < args.length; lastTypeIndex += 2) {
          if (args[lastTypeIndex] instanceof SqlTypeName) {
            builder.add(args[lastTypeIndex + 1].toString(),
                (SqlTypeName) args[lastTypeIndex]);
          } else {
            break;
          }
        }
        return builder.build();
      }
    };


    List<BeamSqlRow> rows = new ArrayList<>();
    BeamSqlRecordType beamSQLRecordType = CalciteUtils
        .toBeamRecordType(protoRowType.apply(BeamQueryPlanner.TYPE_FACTORY));
    int fieldCount = beamSQLRecordType.size();



    for (int i = fieldCount * 2 + 1; i < args.length; i += fieldCount) {
      BeamSqlRow row = new BeamSqlRow(beamSQLRecordType);
      for (int j = 0; j < fieldCount; j++) {
        row.addField(j, args[i + j]);
      }
      rows.add(row);
    }
    MockedUnboundedTable table = new MockedUnboundedTable(
        beamSQLRecordType, (int) args[fieldCount * 2]);
    table.addInputRecords(rows);

    return table;
  }

  public MockedUnboundedTable addInputRecords(List<BeamSqlRow> rows) {
    this.timestampedRows.add(Pair.of(Duration.standardMinutes(0), rows));

    return this;
  }

  public MockedUnboundedTable addInputRecords(Duration duration, Object... args) {
    List<BeamSqlRow> rows = new ArrayList<>();
    int fieldCount = getRecordType().size();

    for (int i = 0; i < args.length; i += fieldCount) {
      BeamSqlRow row = new BeamSqlRow(getRecordType());
      for (int j = 0; j < fieldCount; j++) {
        row.addField(j, args[i + j]);
      }
      rows.add(row);
    }

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
