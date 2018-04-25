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

package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import java.io.Serializable;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.csv.CSVFormat;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for BeamKafkaCSVTable.
 */
public class BeamKafkaCSVTableTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static final Row ROW1 =
      Row
          .withSchema(genRowType())
          .addValues(1L, 1, 1.0)
          .build();

  private static final Row ROW2 =
      Row.withSchema(genRowType())
          .addValues(2L, 2, 2.0)
          .build();

  @Test public void testCsvRecorderDecoder() throws Exception {
    PCollection<Row> result = pipeline
        .apply(
            Create.of("1,\"1\",1.0", "2,2,2.0")
        )
        .apply(ParDo.of(new String2KvBytes()))
        .apply(
            new BeamKafkaCSVTable.CsvRecorderDecoder(genRowType(), CSVFormat.DEFAULT)
        );

    PAssert.that(result).containsInAnyOrder(ROW1, ROW2);

    pipeline.run();
  }

  @Test public void testCsvRecorderEncoder() throws Exception {
    PCollection<Row> result = pipeline
        .apply(
            Create.of(ROW1, ROW2)
        )
        .apply(
            new BeamKafkaCSVTable.CsvRecorderEncoder(genRowType(), CSVFormat.DEFAULT)
        ).apply(
            new BeamKafkaCSVTable.CsvRecorderDecoder(genRowType(), CSVFormat.DEFAULT)
        );

    PAssert.that(result).containsInAnyOrder(ROW1, ROW2);

    pipeline.run();
  }

  private static Schema genRowType() {
    return CalciteUtils.toBeamSchema(
        BeamQueryPlanner.TYPE_FACTORY.builder()
            .add("order_id", SqlTypeName.BIGINT)
            .add("site_id", SqlTypeName.INTEGER)
            .add("price", SqlTypeName.DOUBLE)
            .build());
  }

  private static class String2KvBytes extends DoFn<String, KV<byte[], byte[]>>
      implements Serializable {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(KV.of(new byte[] {}, ctx.element().getBytes()));
    }
  }
}
