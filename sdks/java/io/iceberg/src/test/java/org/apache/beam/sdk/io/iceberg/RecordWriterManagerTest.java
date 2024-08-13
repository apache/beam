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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link RecordWriterManager}. */
@RunWith(JUnit4.class)
public class RecordWriterManagerTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public TestName testName = new TestName();
  private static final Schema BEAM_SCHEMA =
      Schema.builder().addInt32Field("id").addStringField("name").addBooleanField("bool").build();
  private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      IcebergUtils.beamSchemaToIcebergSchema(BEAM_SCHEMA);
  private static final PartitionSpec PARTITION_SPEC =
      PartitionSpec.builderFor(ICEBERG_SCHEMA).truncate("name", 3).identity("bool").build();

  private TableIdentifier tableIdentifier;
  private WindowedValue<IcebergDestination> windowedDestination;
  private HadoopCatalog catalog;

  @Before
  public void setUp() {
    tableIdentifier = TableIdentifier.of("default", "table_" + testName.getMethodName());
    IcebergDestination icebergDestination =
        IcebergDestination.builder()
            .setFileFormat(FileFormat.PARQUET)
            .setTableIdentifier(tableIdentifier)
            .build();
    windowedDestination =
        WindowedValue.of(
            icebergDestination,
            GlobalWindow.TIMESTAMP_MAX_VALUE,
            GlobalWindow.INSTANCE,
            PaneInfo.NO_FIRING);
    warehouse.createTable(tableIdentifier, ICEBERG_SCHEMA, PARTITION_SPEC);
    catalog = new HadoopCatalog(new Configuration(), warehouse.location);
  }

  @Test
  public void testCreateNewWriterForEachPartition() throws IOException {
    // New writer manager with a maximum limit of 3 writers
    RecordWriterManager writerManager = new RecordWriterManager(catalog, "test_file_name", 100, 3);
    assertEquals(0, writerManager.openWriters);

    boolean writeSuccess;

    // The following row will have new partition: [aaa, true].
    // This is a new partition so a new writer will be created.
    Row row = Row.withSchema(BEAM_SCHEMA).addValues(1, "aaa", true).build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertTrue(writeSuccess);
    assertEquals(1, writerManager.openWriters);

    // The following row will have new partition: [bbb, false].
    // This is a new partition so a new writer will be created.
    row = Row.withSchema(BEAM_SCHEMA).addValues(2, "bbb", false).build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertTrue(writeSuccess);
    assertEquals(2, writerManager.openWriters);

    // The following row will have existing partition: [bbb, false].
    // A writer already exists for this partition, so no new writers are created.
    row = Row.withSchema(BEAM_SCHEMA).addValues(3, "bbbaaa", false).build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertTrue(writeSuccess);
    assertEquals(2, writerManager.openWriters);

    // The following row will have new partition: [bbb, true].
    // This is a new partition so a new writer will be created.
    row = Row.withSchema(BEAM_SCHEMA).addValues(4, "bbb123", true).build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertTrue(writeSuccess);
    assertEquals(3, writerManager.openWriters);

    // The following row will have new partition: [aaa, false].
    // The writerManager is already saturated with three writers. This record is rejected.
    row = Row.withSchema(BEAM_SCHEMA).addValues(5, "aaa123", false).build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertFalse(writeSuccess);
    assertEquals(3, writerManager.openWriters);

    // Closing PartitionRecordWriter will close all writers.
    writerManager.close();
    assertEquals(0, writerManager.openWriters);
  }
}
