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

  public static final List<BeamSQLRow> CONTENT = new ArrayList<>();

  private List<BeamSQLRow> inputRecords;

  public MockedBeamSQLTable(RelProtoDataType protoRowType) {
    super(protoRowType);
  }

  public MockedBeamSQLTable withInputRecords(List<BeamSQLRow> inputRecords){
    this.inputRecords = inputRecords;
    return this;
  }

  @Override
  public BeamIOType getSourceType() {
    return BeamIOType.UNBOUNDED;
  }

  @Override
  public PTransform<? super PBegin, PCollection<BeamSQLRow>> buildIOReader() {
    return Create.of(inputRecords);
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
