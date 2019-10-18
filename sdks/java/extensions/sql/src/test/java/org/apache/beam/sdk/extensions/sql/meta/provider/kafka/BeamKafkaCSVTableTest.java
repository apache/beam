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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.csv.CSVFormat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/** Test for BeamKafkaCSVTable. */
public class BeamKafkaCSVTableTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static final Row ROW1 = Row.withSchema(genSchema()).addValues(1L, 1, 1.0).build();

  private static final Row ROW2 = Row.withSchema(genSchema()).addValues(2L, 2, 2.0).build();

  private static Map<String, BeamSqlTable> tables = new HashMap<>();
  protected static BeamSqlEnv env = BeamSqlEnv.readOnly("test", tables);

  @Test
  public void testOrderedArrivalSinglePartitionRate() {
    KafkaCSVTestTable table = getTable(1);
    for (int i = 0; i < 100; i++) {
      table.addRecord(KafkaTestRecord.create("key1", i + ",1,2", "topic1", 500 * i));
    }

    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(2d, stats.getRate(), 0.001);
  }

  @Test
  public void testOrderedArrivalMultiplePartitionsRate() {
    KafkaCSVTestTable table = getTable(3);
    for (int i = 0; i < 100; i++) {
      table.addRecord(KafkaTestRecord.create("key" + i, i + ",1,2", "topic1", 500 * i));
    }

    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(2d, stats.getRate(), 0.001);
  }

  @Test
  public void testOnePartitionAheadRate() {
    KafkaCSVTestTable table = getTable(3);
    for (int i = 0; i < 100; i++) {
      table.addRecord(KafkaTestRecord.create("1", i + ",1,2", "topic1", 1000 * i));
      table.addRecord(KafkaTestRecord.create("2", i + ",1,2", "topic1", 500 * i));
    }

    table.setNumberOfRecordsForRate(20);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(1d, stats.getRate(), 0.001);
  }

  @Test
  public void testLateRecords() {
    KafkaCSVTestTable table = getTable(3);

    table.addRecord(KafkaTestRecord.create("1", 132 + ",1,2", "topic1", 1000));
    for (int i = 0; i < 98; i++) {
      table.addRecord(KafkaTestRecord.create("1", i + ",1,2", "topic1", 500));
    }
    table.addRecord(KafkaTestRecord.create("1", 133 + ",1,2", "topic1", 2000));

    table.setNumberOfRecordsForRate(200);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(1d, stats.getRate(), 0.001);
  }

  @Test
  public void testAllLate() {
    KafkaCSVTestTable table = getTable(3);

    table.addRecord(KafkaTestRecord.create("1", 132 + ",1,2", "topic1", 1000));
    for (int i = 0; i < 98; i++) {
      table.addRecord(KafkaTestRecord.create("1", i + ",1,2", "topic1", 500));
    }

    table.setNumberOfRecordsForRate(200);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertTrue(stats.isUnknown());
  }

  @Test
  public void testEmptyPartitionsRate() {
    KafkaCSVTestTable table = getTable(3);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertTrue(stats.isUnknown());
  }

  @Test
  public void allTheRecordsSameTimeRate() {
    KafkaCSVTestTable table = getTable(3);
    for (int i = 0; i < 100; i++) {
      table.addRecord(KafkaTestRecord.create("key" + i, i + ",1,2", "topic1", 1000));
    }
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertTrue(stats.isUnknown());
  }

  private static class PrintDoFn extends DoFn<Row, Row> {

    @ProcessElement
    public void process(ProcessContext c) {
      System.out.println("we are here");
      System.out.println(c.element().getValues());
    }
  }

  @Test
  public void testCsvRecorderDecoder() {
    PCollection<Row> result =
        pipeline
            .apply(Create.of("1,\"1\",1.0", "2,2,2.0"))
            .apply(ParDo.of(new String2KvBytes()))
            .apply(new BeamKafkaCSVTable.CsvRecorderDecoder(genSchema(), CSVFormat.DEFAULT));

    PAssert.that(result).containsInAnyOrder(ROW1, ROW2);

    pipeline.run();
  }

  @Test
  public void testCsvRecorderEncoder() {
    PCollection<Row> result =
        pipeline
            .apply(Create.of(ROW1, ROW2))
            .apply(new BeamKafkaCSVTable.CsvRecorderEncoder(genSchema(), CSVFormat.DEFAULT))
            .apply(new BeamKafkaCSVTable.CsvRecorderDecoder(genSchema(), CSVFormat.DEFAULT));

    PAssert.that(result).containsInAnyOrder(ROW1, ROW2);

    pipeline.run();
  }

  private static Schema genSchema() {
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    return CalciteUtils.toSchema(
        typeFactory
            .builder()
            .add("order_id", SqlTypeName.BIGINT)
            .add("site_id", SqlTypeName.INTEGER)
            .add("price", SqlTypeName.DOUBLE)
            .build());
  }

  private static class String2KvBytes extends DoFn<String, KV<byte[], byte[]>>
      implements Serializable {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(KV.of(new byte[] {}, ctx.element().getBytes(UTF_8)));
    }
  }

  private KafkaCSVTestTable getTable(int numberOfPartitions) {
    return new KafkaCSVTestTable(
        TestTableUtils.buildBeamSqlSchema(
            Schema.FieldType.INT32,
            "order_id",
            Schema.FieldType.INT32,
            "site_id",
            Schema.FieldType.INT32,
            "price"),
        ImmutableList.of("topic1", "topic2"),
        numberOfPartitions);
  }
}
