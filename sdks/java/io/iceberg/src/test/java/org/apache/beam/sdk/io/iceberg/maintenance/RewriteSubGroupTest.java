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
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RewriteSubGroupTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Test
  public void derivesRewrittenFilesAndStartingSnapshot() throws Exception {
    TableIdentifier tableId = TableIdentifier.of("default", "rfg_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    AppendFiles append = table.newAppend();
    append.appendFile(
        warehouse.writeRecords("f1.parquet", table.schema(), TestFixtures.FILE1SNAPSHOT1));
    append.appendFile(
        warehouse.writeRecords("f2.parquet", table.schema(), TestFixtures.FILE2SNAPSHOT1));
    append.appendFile(
        warehouse.writeRecords("f3.parquet", table.schema(), TestFixtures.FILE3SNAPSHOT1));
    append.commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    assertEquals(3, tasks.size());
    Set<String> inputPaths =
        tasks.stream().map(t -> t.file().location()).collect(Collectors.toSet());

    RewriteSubGroup group =
        RewriteSubGroup.builder()
            .setGlobalIndex(1)
            .setFileScanTasks(tasks, table.specs())
            .setOutputSpecId(table.spec().specId())
            .setWriteMaxFileSize(Long.MAX_VALUE)
            .setStartingSnapshotId(snapshotId)
            .setStartingSequenceNumber(table.snapshot(snapshotId).sequenceNumber())
            .setOperationId("op-test")
            .setParentGroupIndex(0)
            .setParentSubgroupCount(1)
            .build();

    assertEquals(snapshotId, group.getStartingSnapshotId());
    assertEquals(3, RewriteGroupTestHelpers.rewrittenDataFiles(group, table).size());
    assertEquals(
        inputPaths,
        RewriteGroupTestHelpers.rewrittenDataFiles(group, table).stream()
            .map(d -> d.location())
            .collect(Collectors.toSet()));
    // COW table: no delete files.
    assertTrue(RewriteGroupTestHelpers.danglingDeleteFiles(group, table).isEmpty());
  }

  @Test
  public void schemaFieldNumbersArePinned() throws Exception {
    // A6: RewriteFileGroup pins every field with @SchemaFieldNumber (AutoValueSchema field order is
    // not guaranteed across Beam releases; a reorder would break Dataflow in-place --update). Lock
    // the shape so any future edit that adds/reorders/renames a field trips this test in review.
    Schema schema = SchemaRegistry.createDefault().getSchema(RewriteSubGroup.class);
    assertEquals(10, schema.getFieldCount());
    assertEquals("globalIndex", schema.getField(0).getName());
    assertEquals("parentGroupIndex", schema.getField(1).getName());
    assertEquals("parentSubgroupCount", schema.getField(2).getName());
    assertEquals("taskDescriptors", schema.getField(3).getName());
    assertEquals("outputSpecId", schema.getField(4).getName());
    assertEquals("writeMaxFileSize", schema.getField(5).getName());
    assertEquals("totalInputFileByteSize", schema.getField(6).getName());
    assertEquals("startingSnapshotId", schema.getField(7).getName());
    assertEquals("operationId", schema.getField(8).getName());
    assertEquals("startingSequenceNumber", schema.getField(9).getName());
  }

  @Test
  public void taskDescriptorSchemaFieldNumbersArePinned() throws Exception {
    // C1: the new per-range carrier pins every field too (nested inside RewriteFileGroup's coder).
    Schema schema = SchemaRegistry.createDefault().getSchema(TaskDescriptor.class);
    assertEquals(6, schema.getFieldCount());
    assertEquals("dataFileJson", schema.getField(0).getName());
    assertEquals("specId", schema.getField(1).getName());
    assertEquals("start", schema.getField(2).getName());
    assertEquals("length", schema.getField(3).getName());
    assertEquals("dataSequenceNumber", schema.getField(4).getName());
    assertEquals("deleteFileJsons", schema.getField(5).getName());
  }

  @Test
  public void executedGroupSchemaFieldNumbersArePinned() throws Exception {
    // A6: ExecutedGroup pins every field with @SchemaFieldNumber for the same reason (see above).
    Schema schema = SchemaRegistry.createDefault().getSchema(ExecutedGroup.class);
    assertEquals(9, schema.getFieldCount());
    assertEquals("startingSnapshotId", schema.getField(0).getName());
    assertEquals("operationId", schema.getField(1).getName());
    assertEquals("parentGroupIndex", schema.getField(2).getName());
    assertEquals("parentSubgroupCount", schema.getField(3).getName());
    assertEquals("totalInputByteSize", schema.getField(4).getName());
    assertEquals("newFiles", schema.getField(5).getName());
    assertEquals("rewrittenDataFiles", schema.getField(6).getName());
    assertEquals("danglingDeleteFileJsons", schema.getField(7).getName());
    assertEquals("startingSequenceNumber", schema.getField(8).getName());
  }
}
