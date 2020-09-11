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
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/** Test for BeamKafkaCSVTable. */
public abstract class BeamKafkaTableTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  protected static final Schema BEAM_SQL_SCHEMA =
      TestTableUtils.buildBeamSqlSchema(
          Schema.FieldType.INT32,
          "order_id",
          Schema.FieldType.INT32,
          "site_id",
          Schema.FieldType.INT32,
          "price");

  protected static final List<String> TOPICS = ImmutableList.of("topic1", "topic2");

  protected static final Schema SCHEMA = genSchema();

  protected static final Row ROW1 = Row.withSchema(SCHEMA).addValues(1L, 1, 1.0).build();

  protected static final Row ROW2 = Row.withSchema(SCHEMA).addValues(2L, 2, 2.0).build();

  private static final Map<String, BeamSqlTable> tables = new HashMap<>();

  protected static BeamSqlEnv env = BeamSqlEnv.readOnly("test", tables);

  protected abstract KafkaTestRecord<?> createKafkaTestRecord(
      String key, boolean useFixedKey, int i, int timestamp, boolean useFixedTimestamp);

  protected abstract KafkaTestTable getTable(int numberOfPartitions);

  protected abstract PCollection<Row> createRecorderDecoder(TestPipeline pipeline);

  protected abstract PCollection<Row> createRecorderEncoder(TestPipeline pipeline);

  @Test
  public void testOrderedArrivalSinglePartitionRate() {
    KafkaTestTable table = getTable(1);
    for (int i = 0; i < 100; i++) {
      table.addRecord(createKafkaTestRecord("k", false, i, 500, false));
    }

    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(2d, stats.getRate(), 0.001);
  }

  @Test
  public void testOrderedArrivalMultiplePartitionsRate() {
    KafkaTestTable table = getTable(3);
    for (int i = 0; i < 100; i++) {
      table.addRecord(createKafkaTestRecord("k", false, i, 500, false));
    }

    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(2d, stats.getRate(), 0.001);
  }

  @Test
  public void testOnePartitionAheadRate() {
    KafkaTestTable table = getTable(3);
    for (int i = 0; i < 100; i++) {
      table.addRecord(createKafkaTestRecord("1", true, i, 1000, false));
      table.addRecord(createKafkaTestRecord("2", true, i, 500, false));
    }

    table.setNumberOfRecordsForRate(20);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(1d, stats.getRate(), 0.001);
  }

  @Test
  public void testLateRecords() {
    KafkaTestTable table = getTable(3);

    table.addRecord(createKafkaTestRecord("1", true, 132, 1000, true));
    for (int i = 0; i < 98; i++) {
      table.addRecord(createKafkaTestRecord("1", true, i, 500, true));
    }
    table.addRecord(createKafkaTestRecord("1", true, 133, 2000, true));

    table.setNumberOfRecordsForRate(200);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(1d, stats.getRate(), 0.001);
  }

  @Test
  public void testAllLate() {
    KafkaTestTable table = getTable(3);

    table.addRecord(createKafkaTestRecord("1", true, 132, 1000, true));
    for (int i = 0; i < 98; i++) {
      table.addRecord(createKafkaTestRecord("1", true, i, 500, true));
    }

    table.setNumberOfRecordsForRate(200);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertTrue(stats.isUnknown());
  }

  @Test
  public void testEmptyPartitionsRate() {
    KafkaTestTable table = getTable(3);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertTrue(stats.isUnknown());
  }

  @Test
  public void allTheRecordsSameTimeRate() {
    KafkaTestTable table = getTable(3);
    for (int i = 0; i < 100; i++) {
      table.addRecord(createKafkaTestRecord("key", false, i, 1000, true));
    }
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertTrue(stats.isUnknown());
  }

  @Test
  public void testRecorderDecoder() {
    PCollection<Row> result = createRecorderDecoder(pipeline);
    PAssert.that(result).containsInAnyOrder(ROW1, ROW2);

    pipeline.run();
  }

  @Test
  public void testRecorderEncoder() {
    PCollection<Row> result = createRecorderDecoder(pipeline);
    PAssert.that(result).containsInAnyOrder(ROW1, ROW2);
    pipeline.run();
  }

  protected static Schema genSchema() {
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    return CalciteUtils.toSchema(
        typeFactory
            .builder()
            .add("order_id", SqlTypeName.BIGINT)
            .add("site_id", SqlTypeName.INTEGER)
            .add("price", SqlTypeName.DOUBLE)
            .build());
  }

  protected static class String2KvBytes extends DoFn<String, KV<byte[], byte[]>>
      implements Serializable {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(KV.of(new byte[] {}, ctx.element().getBytes(UTF_8)));
    }
  }
}
