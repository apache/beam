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
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.beam.dsls.sql.schema.BaseBeamTable;
import org.apache.beam.dsls.sql.schema.BeamIOType;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A mock table use to check input/output.
 *
 */
public class MockedBeamSqlTable extends BaseBeamTable {

  public static final ConcurrentLinkedQueue<BeamSqlRow> CONTENT = new ConcurrentLinkedQueue<>();

  private List<BeamSqlRow> inputRecords;

  public MockedBeamSqlTable(RelProtoDataType protoRowType) {
    super(protoRowType);
  }

  public MockedBeamSqlTable withInputRecords(List<BeamSqlRow> inputRecords){
    this.inputRecords = inputRecords;
    return this;
  }

  /**
   * Convenient way to build a mocked table with mock data:
   *
   * <p>e.g.
   *
   * <pre>{@code
   * MockedBeamSQLTable
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
  public static MockedBeamSqlTable of(final Object... args){
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
    BeamSqlRecordType beamSQLRecordType = BeamSqlRecordType.from(
        protoRowType.apply(BeamQueryPlanner.TYPE_FACTORY));
    int fieldCount = beamSQLRecordType.size();

    for (int i = fieldCount * 2; i < args.length; i += fieldCount) {
      BeamSqlRow row = new BeamSqlRow(beamSQLRecordType);
      for (int j = 0; j < fieldCount; j++) {
        row.addField(j, args[i + j]);
      }
      rows.add(row);
    }
    return new MockedBeamSqlTable(protoRowType).withInputRecords(rows);
  }

  @Override
  public BeamIOType getSourceType() {
    return BeamIOType.UNBOUNDED;
  }

  @Override
  public PCollection<BeamSqlRow> buildIOReader(Pipeline pipeline) {
    return PBegin.in(pipeline).apply(Create.of(inputRecords));
  }

  @Override
  public PTransform<? super PCollection<BeamSqlRow>, PDone> buildIOWriter() {
    return new OutputStore();
  }

  public List<BeamSqlRow> getInputRecords() {
    return inputRecords;
  }

  /**
   * Keep output in {@code CONTENT} for validation.
   *
   */
  public static class OutputStore extends PTransform<PCollection<BeamSqlRow>, PDone> {

    @Override
    public PDone expand(PCollection<BeamSqlRow> input) {
      input.apply(ParDo.of(new DoFn<BeamSqlRow, Void>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          CONTENT.add(c.element());
        }

        @Teardown
        public void close() {

        }

      }));
      return PDone.in(input.getPipeline());
    }
  }

}
