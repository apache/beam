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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class BeamKafkaTableStatisticsTest {
  @Test
  public void testOrderedArrivalSinglePartitionRate() {
    KafkaTestTable table = testTable(1);
    for (int i = 0; i < 100; i++) {
      table.addRecord(createKafkaTestRecord("k" + i, i, 500L * i));
    }

    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(2d, stats.getRate(), 0.001);
  }

  @Test
  public void testOrderedArrivalMultiplePartitionsRate() {
    KafkaTestTable table = testTable(3);
    for (int i = 0; i < 100; i++) {
      table.addRecord(createKafkaTestRecord("k" + i, i, 500L * i));
    }

    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertEquals(2d, stats.getRate(), 0.001);
  }

  @Test
  public void testOnePartitionAheadRate() {
    KafkaTestTable table = testTable(3);
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
    KafkaTestTable table = testTable(3);

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
    KafkaTestTable table = testTable(3);

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
    KafkaTestTable table = testTable(3);
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertTrue(stats.isUnknown());
  }

  @Test
  public void allTheRecordsSameTimeRate() {
    KafkaTestTable table = testTable(3);
    for (int i = 0; i < 100; i++) {
      table.addRecord(createKafkaTestRecord("key" + i, i, 1000L));
    }
    BeamTableStatistics stats = table.getTableStatistics(null);
    Assert.assertTrue(stats.isUnknown());
  }

  private KafkaTestTable testTable(int partitionsPerTopic) {
    List<String> topics = ImmutableList.of("topic1", "topic2");
    Schema schema = Schema.builder().addInt32Field("f_int").build();
    return new KafkaTestTable(schema, topics, partitionsPerTopic);
  }

  private KafkaTestRecord createKafkaTestRecord(String key, Integer i, long timestamp) {
    return KafkaTestRecord.create(key, i.toString().getBytes(UTF_8), "topic1", timestamp);
  }
}
