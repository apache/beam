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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;

/** Test fixtures and data generators for testing {@link ExpireSnapshots}. */
public class ExpireSnapshotsTestFixtures {

  public static Record createRecord(long id, String data) {
    Record r = GenericRecord.create(TestFixtures.SCHEMA);
    r.setField("id", id);
    r.setField("data", data);
    return r;
  }

  /**
   * Builds a table with 3 successive append snapshots.
   *
   * <ul>
   *   <li>Snapshot 1: File 1 (rows 0, 1)
   *   <li>Snapshot 2: File 2 (rows 2, 3)
   *   <li>Snapshot 3: File 3 (rows 4, 5)
   * </ul>
   */
  public static Table createTableWithMultiSnapshots(
      TestDataWarehouse warehouse, TableIdentifier tableId) throws IOException {
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    for (int i = 1; i <= 3; i++) {
      List<Record> records = new ArrayList<>();
      records.add(createRecord((i - 1) * 2L, "val-" + ((i - 1) * 2L)));
      records.add(createRecord((i - 1) * 2L + 1L, "val-" + ((i - 1) * 2L + 1L)));

      DataFile file =
          warehouse.writeRecords(
              "file_" + i + "_" + System.nanoTime() + ".parquet", table.schema(), records);

      if (table.currentSnapshot() != null) {
        waitUntilAfter(table.currentSnapshot().timestampMillis());
      }
      AppendFiles append = table.newAppend();
      append.appendFile(file);
      append.commit();
      table.refresh();
    }

    return table;
  }

  /**
   * Builds a table with overwriting snapshots.
   *
   * <ul>
   *   <li>Snapshot 1 adds File A (id 0)
   *   <li>Snapshot 2 adds File B (id 1)
   *   <li>Snapshot 3 overwrites File A with File C (id 2)
   * </ul>
   *
   * Active files in Snapshot 3: File B and File C. File A is obsolete.
   */
  public static Table createTableWithOverwrites(
      TestDataWarehouse warehouse, TableIdentifier tableId) throws IOException {
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    // Snapshot 1: File A
    DataFile fileA =
        warehouse.writeRecords(
            "file_a_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(createRecord(0L, "val-0")));
    table.newAppend().appendFile(fileA).commit();
    table.refresh();

    // Snapshot 2: File B
    DataFile fileB =
        warehouse.writeRecords(
            "file_b_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(createRecord(1L, "val-1")));
    if (table.currentSnapshot() != null) {
      waitUntilAfter(table.currentSnapshot().timestampMillis());
    }
    table.newAppend().appendFile(fileB).commit();
    table.refresh();

    // Snapshot 3: Overwrite File A with File C
    DataFile fileC =
        warehouse.writeRecords(
            "file_c_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(createRecord(2L, "val-2")));
    if (table.currentSnapshot() != null) {
      waitUntilAfter(table.currentSnapshot().timestampMillis());
    }
    OverwriteFiles overwrite = table.newOverwrite();
    overwrite.deleteFile(fileA);
    overwrite.addFile(fileC);
    overwrite.commit();
    table.refresh();

    return table;
  }

  /**
   * Builds a table with a branch.
   *
   * <ul>
   *   <li>Snapshot 1 on main (File A)
   *   <li>Branch "branch_a" created from Snapshot 1
   *   <li>Snapshot 2 appended to main (File B)
   *   <li>Snapshot 3 appended to "branch_a" (File C)
   * </ul>
   */
  public static Table createTableWithBranch(
      TestDataWarehouse warehouse, TableIdentifier tableId, String branchName) throws IOException {
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    // Snapshot 1: File A
    DataFile fileA =
        warehouse.writeRecords(
            "file_main_1_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(createRecord(0L, "main-0")));
    table.newAppend().appendFile(fileA).commit();
    table.refresh();
    long snapshot1Id = table.currentSnapshot().snapshotId();

    // Create branch from snapshot 1
    table.manageSnapshots().createBranch(branchName, snapshot1Id).commit();
    table.refresh();

    // Snapshot 2: File B on main
    DataFile fileB =
        warehouse.writeRecords(
            "file_main_2_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(createRecord(1L, "main-1")));
    if (table.currentSnapshot() != null) {
      waitUntilAfter(table.currentSnapshot().timestampMillis());
    }
    table.newAppend().appendFile(fileB).commit();
    table.refresh();

    // Snapshot 3: File C on branch
    DataFile fileC =
        warehouse.writeRecords(
            "file_branch_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(createRecord(2L, "branch-0")));
    waitUntilAfter(table.currentSnapshot().timestampMillis());
    table.newAppend().toBranch(branchName).appendFile(fileC).commit();
    table.refresh();

    return table;
  }

  public static void waitUntilAfter(long timestampMillis) {
    long current = System.currentTimeMillis();
    while (current <= timestampMillis) {
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      current = System.currentTimeMillis();
    }
  }
}
