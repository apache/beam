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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WriterFactory}. */
@RunWith(JUnit4.class)
public class WriterFactoryTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  private static final Schema SHARDED_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "shard", Types.IntegerType.get()));

  private WriterFactory factoryFor(Table table, PartitionSpec spec, long targetFileSize) {
    return factoryFor(table, spec, targetFileSize, ImmutableMap.of());
  }

  private WriterFactory factoryFor(
      Table table, PartitionSpec spec, long targetFileSize, java.util.Map<String, String> props) {
    WriterFactory wf =
        new WriterFactory(
            FileFormat.PARQUET,
            targetFileSize,
            ThreadLocalRandom.current().nextLong(),
            1,
            "op-test",
            spec,
            props,
            false);
    wf.init(table);
    return wf;
  }

  private static Record shardedRow(long id, int shard) {
    Record r = GenericRecord.create(SHARDED_SCHEMA);
    r.setField("id", id);
    r.setField("shard", shard);
    return r;
  }

  @Test
  public void unpartitionedWriteProducesOneFileWithAllRecords() throws Exception {
    TableIdentifier id = TableIdentifier.of("default", "wf_unp_" + System.nanoTime());
    Table table = warehouse.createTable(id, TestFixtures.SCHEMA);

    TaskWriter<Record> writer = factoryFor(table, table.spec(), Long.MAX_VALUE).create();
    for (int i = 0; i < 5; i++) {
      Record r = GenericRecord.create(TestFixtures.SCHEMA);
      r.setField("id", (long) i);
      r.setField("data", "row-" + i);
      writer.write(r);
    }
    DataFile[] files = writer.dataFiles();

    assertEquals("an unpartitioned write under target rolls into one file", 1, files.length);
    assertEquals(5L, files[0].recordCount());
  }

  @Test
  public void partitionedWriteRoutesEachRecordToItsOwnPartition() throws Exception {
    // The fanout writer keys every record individually, so rows of different partitions must land
    // in different files, each registered under the partition its rows belong to.
    TableIdentifier id = TableIdentifier.of("default", "wf_part_" + System.nanoTime());
    PartitionSpec spec = PartitionSpec.builderFor(SHARDED_SCHEMA).identity("shard").build();
    Table table = warehouse.createTable(id, SHARDED_SCHEMA, spec);

    TaskWriter<Record> writer = factoryFor(table, spec, Long.MAX_VALUE).create();
    for (int shard = 0; shard < 3; shard++) {
      for (int i = 0; i < 2; i++) {
        writer.write(shardedRow(shard * 10L + i, shard));
      }
    }
    DataFile[] files = writer.dataFiles();

    assertEquals("one output file per partition", 3, files.length);
    Set<Integer> registeredShards = new HashSet<>();
    for (DataFile f : files) {
      registeredShards.add(f.partition().get(0, Integer.class));
      assertEquals("each partition's rows stay together", 2L, f.recordCount());
    }
    assertEquals(new HashSet<>(java.util.Arrays.asList(0, 1, 2)), registeredShards);
  }

  @Test
  public void fanoutBeyondOpenWriterCapFailsWithGuidance() throws Exception {
    // A repartitioning subgroup can fan out to many partitions, each holding an open appender with
    // its own row-group buffers. The cap turns that OOM into a diagnosable failure.
    TableIdentifier id = TableIdentifier.of("default", "wf_cap_" + System.nanoTime());
    PartitionSpec spec = PartitionSpec.builderFor(SHARDED_SCHEMA).identity("shard").build();
    Table table = warehouse.createTable(id, SHARDED_SCHEMA, spec);

    int originalCap = WriterFactory.maxOpenFanoutWriters;
    WriterFactory.maxOpenFanoutWriters = 1;
    try {
      TaskWriter<Record> writer = factoryFor(table, spec, Long.MAX_VALUE).create();
      writer.write(shardedRow(0L, 0));
      IllegalStateException ex =
          assertThrows(IllegalStateException.class, () -> writer.write(shardedRow(1L, 1)));
      assertTrue(
          "the message must guide the operator: " + ex.getMessage(),
          ex.getMessage().contains("simultaneously-open writers"));
      writer.abort();
    } finally {
      WriterFactory.maxOpenFanoutWriters = originalCap;
    }
  }

  @Test
  public void writesRollOnceTheTargetFileSizeIsExceeded() throws Exception {
    TableIdentifier id = TableIdentifier.of("default", "wf_roll_" + System.nanoTime());
    Table table = warehouse.createTable(id, TestFixtures.SCHEMA);

    TaskWriter<Record> writer = factoryFor(table, table.spec(), 1L).create();
    List<Record> rows = new ArrayList<>();
    // Iceberg only evaluates the size every 1000th row
    for (int i = 0; i < 2500; i++) {
      Record r = GenericRecord.create(TestFixtures.SCHEMA);
      r.setField("id", (long) i);
      r.setField("data", "row-" + i);
      rows.add(r);
    }
    for (Record r : rows) {
      writer.write(r);
    }
    DataFile[] files = writer.dataFiles();

    assertTrue("a 1-byte target must roll into several files", files.length > 1);
    long total = 0;
    for (DataFile f : files) {
      total += f.recordCount();
    }
    assertEquals("no record may be lost across the roll", 2500L, total);
  }
}
