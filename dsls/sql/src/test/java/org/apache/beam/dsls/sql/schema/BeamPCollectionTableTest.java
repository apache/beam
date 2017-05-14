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
package org.apache.beam.dsls.sql.schema;

import org.apache.beam.dsls.sql.planner.BasePlanner;
import org.apache.beam.dsls.sql.planner.BeamQueryPlanner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test case for BeamPCollectionTable.
 */
public class BeamPCollectionTableTest extends BasePlanner{
  public static TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void prepareTable(){
    RelProtoDataType protoRowType = new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder().add("c1", SqlTypeName.INTEGER)
            .add("c2", SqlTypeName.VARCHAR).build();
      }
    };

    BeamSQLRow row = new BeamSQLRow(BeamSQLRecordType.from(
        protoRowType.apply(BeamQueryPlanner.TYPE_FACTORY)));
    row.addField(0, 1);
    row.addField(1, "hello world.");
    PCollection<BeamSQLRow> inputStream = PBegin.in(pipeline).apply(Create.of(row));
    runner.addTableMetadata("COLLECTION_TABLE",
        new BeamPCollectionTable(inputStream, protoRowType));
  }

  @Test
  public void testSelectFromPCollectionTable() throws Exception{
    String sql = "select c1, c2 from COLLECTION_TABLE";
    runner.executionPlan(sql);
  }

}
