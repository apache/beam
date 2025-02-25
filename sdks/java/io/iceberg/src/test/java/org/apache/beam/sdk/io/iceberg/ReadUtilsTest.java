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
package org.apache.beam.sdk.io.iceberg;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.ParquetReader;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test class for {@link ReadUtils}. */
public class ReadUtilsTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Test
  public void testCreateReader() throws IOException {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    Map<String, List<Record>> data =
        ImmutableMap.<String, List<Record>>builder()
            .put("files1s1.parquet", TestFixtures.FILE1SNAPSHOT1)
            .put("file2s1.parquet", TestFixtures.FILE2SNAPSHOT1)
            .put("file3s1.parquet", TestFixtures.FILE3SNAPSHOT1)
            .build();

    for (Map.Entry<String, List<Record>> entry : data.entrySet()) {
      simpleTable
          .newFastAppend()
          .appendFile(
              warehouse.writeRecords(entry.getKey(), simpleTable.schema(), entry.getValue()))
          .commit();
    }

    int numFiles = 0;
    try (CloseableIterable<CombinedScanTask> iterable = simpleTable.newScan().planTasks()) {
      for (CombinedScanTask combinedScanTask : iterable) {
        for (FileScanTask fileScanTask : combinedScanTask.tasks()) {
          String fileName = Iterables.getLast(Splitter.on("/").split(fileScanTask.file().path()));
          List<Record> recordsRead = new ArrayList<>();
          try (ParquetReader<Record> reader = ReadUtils.createReader(fileScanTask, simpleTable)) {
            reader.forEach(recordsRead::add);
          }

          assertEquals(data.get(fileName), recordsRead);
          numFiles++;
        }
      }
    }
    assertEquals(data.size(), numFiles);
  }
}
