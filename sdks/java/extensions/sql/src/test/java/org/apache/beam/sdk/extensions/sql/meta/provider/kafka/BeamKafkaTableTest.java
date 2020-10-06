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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/** Test utility for BeamKafkaTable implementations. */
public abstract class BeamKafkaTableTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  protected static final List<String> TOPICS = ImmutableList.of("topic1", "topic2");

  /** Returns proper implementation of KafkaTestTable for the tested format */
  protected abstract KafkaTestTable getTestTable(int numberOfPartitions);

  /** Returns proper implementation of BeamKafkaTable for the tested format */
  protected abstract BeamKafkaTable getBeamKafkaTable();

  /** Returns encoded payload for the tested format. */
  protected abstract byte[] generateEncodedPayload(int i);

  /** Provides a deterministic row from the given integer. */
  protected abstract Row generateRow(int i);

  @Test
  public void testOrderedArrivalSinglePartitionRate() {
    KafkaTestTable table = getTestTable(1);
    for (int i = 0; i < 100; i++) {
      table.addRecord(createKafkaTestRecord("k" + i, i, 500L * i));
    }

    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(2d, stats.getRate(), 0.001);
  }

  @Test
  public void testOrderedArrivalMultiplePartitionsRate() {
    KafkaTestTable table = getTestTable(3);
    for (int i = 0; i < 100; i++) {
      table.addRecord(createKafkaTestRecord("k" + i, i, 500L * i));
    }

    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(2d, stats.getRate(), 0.001);
  }

  @Test
  public void testOnePartitionAheadRate() {
    KafkaTestTable table = getTestTable(3);
    for (int i = 0; i < 100; i++) {
      table.addRecord(createKafkaTestRecord("1", i, 1000L * i));
      table.addRecord(createKafkaTestRecord("2", i, 500L * i));
    }

    table.setNumberOfRecordsForRate(20);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(1d, stats.getRate(), 0.001);
  }

  @Test
  public void testLateRecords() {
    KafkaTestTable table = getTestTable(3);

    table.addRecord(createKafkaTestRecord("1", 132, 1000L));
    for (int i = 0; i < 98; i++) {
      table.addRecord(createKafkaTestRecord("1", i, 500L));
    }
    table.addRecord(createKafkaTestRecord("1", 133, 2000L));

    table.setNumberOfRecordsForRate(200);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(1d, stats.getRate(), 0.001);
  }

  @Test
  public void testAllLate() {
    KafkaTestTable table = getTestTable(3);

    table.addRecord(createKafkaTestRecord("1", 132, 1000L));
    for (int i = 0; i < 98; i++) {
      table.addRecord(createKafkaTestRecord("1", i, 500L));
    }

    table.setNumberOfRecordsForRate(200);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertTrue(stats.isUnknown());
  }

  @Test
  public void testEmptyPartitionsRate() {
    KafkaTestTable table = getTestTable(3);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertTrue(stats.isUnknown());
  }

  @Test
  public void allTheRecordsSameTimeRate() {
    KafkaTestTable table = getTestTable(3);
    for (int i = 0; i < 100; i++) {
      table.addRecord(createKafkaTestRecord("key" + i, i, 1000L));
    }
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertTrue(stats.isUnknown());
  }

  @Test
  public void testRecorderDecoder() {
    BeamKafkaTable kafkaTable = getBeamKafkaTable();

    PCollection<Row> result =
        pipeline
            .apply(Create.of(generateEncodedPayload(1), generateEncodedPayload(2)))
            .apply(MapElements.via(new ToKV()))
            .apply(kafkaTable.getPTransformForInput());

    PAssert.that(result).containsInAnyOrder(generateRow(1), generateRow(2));
    pipeline.run();
  }

  @Test
  public void testRecorderEncoder() {
    BeamKafkaTable kafkaTable = getBeamKafkaTable();
    PCollection<Row> result =
        pipeline
            .apply(Create.of(generateRow(1), generateRow(2)))
            .apply(kafkaTable.getPTransformForOutput())
            .apply(kafkaTable.getPTransformForInput());
    PAssert.that(result).containsInAnyOrder(generateRow(1), generateRow(2));
    pipeline.run();
  }

  private static class ToKV extends SimpleFunction<byte[], KV<byte[], byte[]>> {
    @Override
    public KV<byte[], byte[]> apply(byte[] bytes) {
      return KV.of(new byte[] {}, bytes);
    }
  }

  private KafkaTestRecord createKafkaTestRecord(String key, int i, long timestamp) {
    return KafkaTestRecord.create(key, generateEncodedPayload(i), "topic1", timestamp);
  }
}
