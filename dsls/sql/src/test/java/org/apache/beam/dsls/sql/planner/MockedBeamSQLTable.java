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
import java.util.Date;
import java.util.List;

import org.apache.beam.dsls.sql.schema.BaseBeamTable;
import org.apache.beam.dsls.sql.schema.BeamIOType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.calcite.rel.type.RelProtoDataType;

/**
 * A mock table use to check input/output.
 *
 */
public class MockedBeamSQLTable extends BaseBeamTable {

  /**
   *
   */
  private static final long serialVersionUID = 1373168368414036932L;

  public static final List<String> CONTENT = new ArrayList<>();

  public MockedBeamSQLTable(RelProtoDataType protoRowType) {
    super(protoRowType);
  }

  @Override
  public BeamIOType getSourceType() {
    return BeamIOType.UNBOUNDED;
  }

  @Override
  public PTransform<? super PBegin, PCollection<BeamSQLRow>> buildIOReader() {
    BeamSQLRow row1 = new BeamSQLRow(beamSqlRecordType);
    row1.addField(0, 12345L);
    row1.addField(1, 0);
    row1.addField(2, 10.5);
    row1.addField(3, new Date());

    BeamSQLRow row2 = new BeamSQLRow(beamSqlRecordType);
    row2.addField(0, 12345L);
    row2.addField(1, 1);
    row2.addField(2, 20.5);
    row2.addField(3, new Date());

    BeamSQLRow row3 = new BeamSQLRow(beamSqlRecordType);
    row3.addField(0, 12345L);
    row3.addField(1, 0);
    row3.addField(2, 20.5);
    row3.addField(3, new Date());

    BeamSQLRow row4 = new BeamSQLRow(beamSqlRecordType);
    row4.addField(0, null);
    row4.addField(1, null);
    row4.addField(2, 20.5);
    row4.addField(3, new Date());

    return Create.of(row1, row2, row3);
  }

  @Override
  public PTransform<? super PCollection<BeamSQLRow>, PDone> buildIOWriter() {
    return new OutputStore();
  }

  /**
   * Keep output in {@code CONTENT} for validation.
   *
   */
  public static class OutputStore extends PTransform<PCollection<BeamSQLRow>, PDone> {

    @Override
    public PDone expand(PCollection<BeamSQLRow> input) {
      input.apply(ParDo.of(new DoFn<BeamSQLRow, Void>() {

        @Setup
        public void setup() {
          CONTENT.clear();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
          CONTENT.add(c.element().valueInString());
        }

        @Teardown
        public void close() {

        }

      }));
      return PDone.in(input.getPipeline());
    }

  }

}
