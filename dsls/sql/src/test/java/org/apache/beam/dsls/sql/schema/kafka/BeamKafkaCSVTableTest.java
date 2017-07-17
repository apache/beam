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

package org.apache.beam.dsls.sql.schema.kafka;

import java.io.Serializable;
import org.apache.beam.dsls.sql.planner.BeamQueryPlanner;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowType;
import org.apache.beam.dsls.sql.utils.CalciteUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.csv.CSVFormat;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for BeamKafkaCSVTable.
 */
public class BeamKafkaCSVTableTest {
  @Rule
  public TestPipeline pipeline = TestPipeline.create();
  public static BeamSqlRow row1 = new BeamSqlRow(genRowType());
  public static BeamSqlRow row2 = new BeamSqlRow(genRowType());

  @BeforeClass
  public static void setUp() {
    row1.addField(0, 1L);
    row1.addField(1, 1);
    row1.addField(2, 1.0);

    row2.addField(0, 2L);
    row2.addField(1, 2);
    row2.addField(2, 2.0);
  }

  @Test public void testCsvRecorderDecoder() throws Exception {
    PCollection<BeamSqlRow> result = pipeline
        .apply(
            Create.of("1,\"1\",1.0", "2,2,2.0")
        )
        .apply(ParDo.of(new String2KvBytes()))
        .apply(
            new BeamKafkaCSVTable.CsvRecorderDecoder(genRowType(), CSVFormat.DEFAULT)
        );

    PAssert.that(result).containsInAnyOrder(row1, row2);

    pipeline.run();
  }

  @Test public void testCsvRecorderEncoder() throws Exception {
    PCollection<BeamSqlRow> result = pipeline
        .apply(
            Create.of(row1, row2)
        )
        .apply(
            new BeamKafkaCSVTable.CsvRecorderEncoder(genRowType(), CSVFormat.DEFAULT)
        ).apply(
            new BeamKafkaCSVTable.CsvRecorderDecoder(genRowType(), CSVFormat.DEFAULT)
        );

    PAssert.that(result).containsInAnyOrder(row1, row2);

    pipeline.run();
  }

  private static BeamSqlRowType genRowType() {
    return CalciteUtils.toBeamRowType(new RelProtoDataType() {

      @Override public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder().add("order_id", SqlTypeName.BIGINT)
            .add("site_id", SqlTypeName.INTEGER)
            .add("price", SqlTypeName.DOUBLE).build();
      }
    }.apply(BeamQueryPlanner.TYPE_FACTORY));
  }

  private static class String2KvBytes extends DoFn<String, KV<byte[], byte[]>>
      implements Serializable {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(KV.of(new byte[] {}, ctx.element().getBytes()));
    }
  }
}
