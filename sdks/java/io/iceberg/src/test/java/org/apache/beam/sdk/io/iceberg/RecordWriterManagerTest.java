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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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

  private WindowedValue<IcebergDestination> windowedDestination;
  private HadoopCatalog catalog;

  @Before
  public void setUp() {
    windowedDestination =
        getWindowedDestination("table_" + testName.getMethodName(), PARTITION_SPEC);
    catalog = new HadoopCatalog(new Configuration(), warehouse.location);
    RecordWriterManager.TABLE_CACHE.invalidateAll();
  }

  private WindowedValue<IcebergDestination> getWindowedDestination(
      String tableName, @Nullable PartitionSpec partitionSpec) {
    TableIdentifier tableIdentifier = TableIdentifier.of("default", tableName);

    warehouse.createTable(tableIdentifier, ICEBERG_SCHEMA, partitionSpec);

    IcebergDestination icebergDestination =
        IcebergDestination.builder()
            .setFileFormat(FileFormat.PARQUET)
            .setTableIdentifier(tableIdentifier)
            .build();
    return WindowedValue.of(
        icebergDestination,
        GlobalWindow.TIMESTAMP_MAX_VALUE,
        GlobalWindow.INSTANCE,
        PaneInfo.NO_FIRING);
  }

  @Test
  public void testCreateNewWriterForEachDestination() throws IOException {
    // Writer manager with a maximum limit of 3 writers
    RecordWriterManager writerManager = new RecordWriterManager(catalog, "test_file_name", 1000, 3);
    assertEquals(0, writerManager.openWriters);

    boolean writeSuccess;

    WindowedValue<IcebergDestination> dest1 = getWindowedDestination("dest1", null);
    WindowedValue<IcebergDestination> dest2 = getWindowedDestination("dest2", null);
    WindowedValue<IcebergDestination> dest3 = getWindowedDestination("dest3", PARTITION_SPEC);
    WindowedValue<IcebergDestination> dest4 = getWindowedDestination("dest4", null);

    // dest1
    // This is a new destination so a new writer will be created.
    Row row = Row.withSchema(BEAM_SCHEMA).addValues(1, "aaa", true).build();
    writeSuccess = writerManager.write(dest1, row);
    assertTrue(writeSuccess);
    assertEquals(1, writerManager.openWriters);

    // dest2
    // This is a new destination so a new writer will be created.
    row = Row.withSchema(BEAM_SCHEMA).addValues(1, "aaa", true).build();
    writeSuccess = writerManager.write(dest2, row);
    assertTrue(writeSuccess);
    assertEquals(2, writerManager.openWriters);

    // dest3, partition: [aaa, true]
    // This is a new destination so a new writer will be created.
    row = Row.withSchema(BEAM_SCHEMA).addValues(1, "aaa", true).build();
    writeSuccess = writerManager.write(dest3, row);
    assertTrue(writeSuccess);
    assertEquals(3, writerManager.openWriters);

    // dest4
    // This is a new destination, but the writer manager is saturated with 3 writers. reject the
    // record
    row = Row.withSchema(BEAM_SCHEMA).addValues(1, "aaa", true).build();
    writeSuccess = writerManager.write(dest4, row);
    assertFalse(writeSuccess);
    assertEquals(3, writerManager.openWriters);

    // dest3, partition: [aaa, false]
    // new partition, but the writer manager is saturated with 3 writers. reject the record
    row = Row.withSchema(BEAM_SCHEMA).addValues(1, "aaa", false).build();
    writeSuccess = writerManager.write(dest3, row);
    assertFalse(writeSuccess);
    assertEquals(3, writerManager.openWriters);

    // Closing PartitionRecordWriter will close all writers.
    writerManager.close();
    assertEquals(0, writerManager.openWriters);

    // We should only have 3 data files (one for each destination we wrote to)
    assertEquals(3, writerManager.getSerializableDataFiles().keySet().size());
    assertThat(
        writerManager.getSerializableDataFiles().keySet(), containsInAnyOrder(dest1, dest2, dest3));
  }

  @Test
  public void testCreateNewWriterForEachPartition() throws IOException {
    // Writer manager with a maximum limit of 3 writers
    RecordWriterManager writerManager = new RecordWriterManager(catalog, "test_file_name", 1000, 3);
    assertEquals(0, writerManager.openWriters);

    boolean writeSuccess;

    // partition: [aaa, true].
    // This is a new partition so a new writer will be created.
    Row row = Row.withSchema(BEAM_SCHEMA).addValues(1, "aaa", true).build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertTrue(writeSuccess);
    assertEquals(1, writerManager.openWriters);

    // partition: [bbb, false].
    // This is a new partition so a new writer will be created.
    row = Row.withSchema(BEAM_SCHEMA).addValues(2, "bbb", false).build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertTrue(writeSuccess);
    assertEquals(2, writerManager.openWriters);

    // partition: [bbb, false].
    // A writer already exists for this partition, so no new writers are created.
    row = Row.withSchema(BEAM_SCHEMA).addValues(3, "bbbaaa", false).build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertTrue(writeSuccess);
    assertEquals(2, writerManager.openWriters);

    // partition: [bbb, true].
    // This is a new partition so a new writer will be created.
    row = Row.withSchema(BEAM_SCHEMA).addValues(4, "bbb123", true).build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertTrue(writeSuccess);
    assertEquals(3, writerManager.openWriters);

    // partition: [aaa, false].
    // The writerManager is already saturated with three writers. This record is rejected.
    row = Row.withSchema(BEAM_SCHEMA).addValues(5, "aaa123", false).build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertFalse(writeSuccess);
    assertEquals(3, writerManager.openWriters);

    // Closing RecordWriterManager will close all writers.
    writerManager.close();
    assertEquals(0, writerManager.openWriters);

    // We should have only one destination
    assertEquals(1, writerManager.getSerializableDataFiles().size());
    assertTrue(writerManager.getSerializableDataFiles().containsKey(windowedDestination));
    // We should have 3 data files (one for each partition we wrote to)
    assertEquals(3, writerManager.getSerializableDataFiles().get(windowedDestination).size());
    long totalRows = 0;
    for (SerializableDataFile dataFile :
        writerManager.getSerializableDataFiles().get(windowedDestination)) {
      totalRows += dataFile.getRecordCount();
    }
    assertEquals(4L, totalRows);
  }

  @Test
  public void testRespectMaxFileSize() throws IOException {
    // Writer manager with a maximum file size of 100 bytes
    RecordWriterManager writerManager = new RecordWriterManager(catalog, "test_file_name", 100, 2);
    assertEquals(0, writerManager.openWriters);
    boolean writeSuccess;

    PartitionKey partitionKey = new PartitionKey(PARTITION_SPEC, ICEBERG_SCHEMA);
    // row partition:: [aaa, true].
    // This is a new partition so a new writer will be created.
    Row row = Row.withSchema(BEAM_SCHEMA).addValues(1, "aaa", true).build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertTrue(writeSuccess);
    assertEquals(1, writerManager.openWriters);

    partitionKey.partition(IcebergUtils.beamRowToIcebergRecord(ICEBERG_SCHEMA, row));
    Map<PartitionKey, Integer> writerCounts =
        writerManager.destinations.get(windowedDestination).writerCounts;
    // this is our first writer
    assertEquals(1, writerCounts.get(partitionKey).intValue());

    // row partition:: [aaa, true].
    // existing partition. use existing writer
    row =
        Row.withSchema(BEAM_SCHEMA)
            .addValues(2, "aaa" + RandomStringUtils.randomAlphanumeric(1000), true)
            .build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertTrue(writeSuccess);
    assertEquals(1, writerManager.openWriters);
    // check that we still use our first writer
    assertEquals(1, writerCounts.get(partitionKey).intValue());

    // row partition:: [aaa, true].
    // writer has reached max file size. create a new writer
    row = Row.withSchema(BEAM_SCHEMA).addValues(2, "aaabb", true).build();
    writeSuccess = writerManager.write(windowedDestination, row);
    assertTrue(writeSuccess);
    // check that we have opened and are using a second writer
    assertEquals(2, writerCounts.get(partitionKey).intValue());
    // check that only one writer is open (we have closed the first writer)
    assertEquals(1, writerManager.openWriters);

    writerManager.close();
    assertEquals(0, writerManager.openWriters);
  }

  @Test
  public void testRequireClosingBeforeFetchingDataFiles() {
    RecordWriterManager writerManager = new RecordWriterManager(catalog, "test_file_name", 100, 2);
    Row row = Row.withSchema(BEAM_SCHEMA).addValues(1, "aaa", true).build();
    writerManager.write(windowedDestination, row);
    assertEquals(1, writerManager.openWriters);

    assertThrows(IllegalStateException.class, writerManager::getSerializableDataFiles);
  }

  /** DataFile doesn't implement a .equals() method. Check equality manually. */
  private static void checkDataFileEquality(DataFile d1, DataFile d2) {
    assertEquals(d1.path(), d2.path());
    assertEquals(d1.format(), d2.format());
    assertEquals(d1.recordCount(), d2.recordCount());
    assertEquals(d1.partition(), d2.partition());
    assertEquals(d1.specId(), d2.specId());
    assertEquals(d1.keyMetadata(), d2.keyMetadata());
    assertEquals(d1.splitOffsets(), d2.splitOffsets());
    assertEquals(d1.columnSizes(), d2.columnSizes());
    assertEquals(d1.valueCounts(), d2.valueCounts());
    assertEquals(d1.nullValueCounts(), d2.nullValueCounts());
    assertEquals(d1.nanValueCounts(), d2.nanValueCounts());
    assertEquals(d1.equalityFieldIds(), d2.equalityFieldIds());
    assertEquals(d1.fileSequenceNumber(), d2.fileSequenceNumber());
    assertEquals(d1.dataSequenceNumber(), d2.dataSequenceNumber());
    assertEquals(d1.pos(), d2.pos());
  }

  @Test
  public void testSerializableDataFileRoundTripEquality() throws IOException {
    PartitionKey partitionKey = new PartitionKey(PARTITION_SPEC, ICEBERG_SCHEMA);

    Row row = Row.withSchema(BEAM_SCHEMA).addValues(1, "abcdef", true).build();
    Row row2 = Row.withSchema(BEAM_SCHEMA).addValues(2, "abcxyz", true).build();
    // same partition for both records (name_trunc=abc, bool=true)
    partitionKey.partition(IcebergUtils.beamRowToIcebergRecord(ICEBERG_SCHEMA, row));

    RecordWriter writer =
        new RecordWriter(catalog, windowedDestination.getValue(), "test_file_name", partitionKey);
    writer.write(IcebergUtils.beamRowToIcebergRecord(ICEBERG_SCHEMA, row));
    writer.write(IcebergUtils.beamRowToIcebergRecord(ICEBERG_SCHEMA, row2));

    writer.close();
    DataFile datafile = writer.getDataFile();
    assertEquals(2L, datafile.recordCount());

    DataFile roundTripDataFile =
        SerializableDataFile.from(datafile, partitionKey)
            .createDataFile(ImmutableMap.of(PARTITION_SPEC.specId(), PARTITION_SPEC));

    checkDataFileEquality(datafile, roundTripDataFile);
  }

  /**
   * Users may update the table's spec while a write pipeline is running. Sometimes, this can happen
   * after converting {@link DataFile} to {@link SerializableDataFile}s. When converting back to
   * {@link DataFile} to commit in the {@link AppendFilesToTables} step, we need to make sure to use
   * the same {@link PartitionSpec} it was originally created with.
   *
   * <p>This test checks that we're preserving the right {@link PartitionSpec} when such an update
   * happens.
   */
  @Test
  public void testRecreateSerializableDataAfterUpdatingPartitionSpec() throws IOException {
    PartitionKey partitionKey = new PartitionKey(PARTITION_SPEC, ICEBERG_SCHEMA);

    Row row = Row.withSchema(BEAM_SCHEMA).addValues(1, "abcdef", true).build();
    Row row2 = Row.withSchema(BEAM_SCHEMA).addValues(2, "abcxyz", true).build();
    // same partition for both records (name_trunc=abc, bool=true)
    partitionKey.partition(IcebergUtils.beamRowToIcebergRecord(ICEBERG_SCHEMA, row));

    // write some rows
    RecordWriter writer =
        new RecordWriter(catalog, windowedDestination.getValue(), "test_file_name", partitionKey);
    writer.write(IcebergUtils.beamRowToIcebergRecord(ICEBERG_SCHEMA, row));
    writer.write(IcebergUtils.beamRowToIcebergRecord(ICEBERG_SCHEMA, row2));
    writer.close();

    // fetch data file and its serializable version
    DataFile datafile = writer.getDataFile();
    SerializableDataFile serializableDataFile = SerializableDataFile.from(datafile, partitionKey);

    assertEquals(2L, datafile.recordCount());
    assertEquals(serializableDataFile.getPartitionSpecId(), datafile.specId());

    // update spec
    Table table = catalog.loadTable(windowedDestination.getValue().getTableIdentifier());
    table.updateSpec().addField("id").removeField("bool").commit();

    Map<Integer, PartitionSpec> updatedSpecs = table.specs();
    DataFile roundTripDataFile = serializableDataFile.createDataFile(updatedSpecs);

    checkDataFileEquality(datafile, roundTripDataFile);
  }

  @Test
  public void testWriterKeepsUpWithUpdatingPartitionSpec() throws IOException {
    Table table = catalog.loadTable(windowedDestination.getValue().getTableIdentifier());
    Row row = Row.withSchema(BEAM_SCHEMA).addValues(1, "abcdef", true).build();
    Row row2 = Row.withSchema(BEAM_SCHEMA).addValues(2, "abcxyz", true).build();

    // write some rows
    RecordWriterManager writer =
        new RecordWriterManager(catalog, "test_prefix", Long.MAX_VALUE, Integer.MAX_VALUE);
    writer.write(windowedDestination, row);
    writer.write(windowedDestination, row2);
    writer.close();
    DataFile dataFile =
        writer
            .getSerializableDataFiles()
            .get(windowedDestination)
            .get(0)
            .createDataFile(table.specs());

    // check data file path contains the correct partition components
    assertEquals(2L, dataFile.recordCount());
    assertEquals(dataFile.specId(), PARTITION_SPEC.specId());
    assertThat(dataFile.path().toString(), containsString("name_trunc=abc"));
    assertThat(dataFile.path().toString(), containsString("bool=true"));

    // table is cached
    assertEquals(1, RecordWriterManager.TABLE_CACHE.size());

    // update spec
    table.updateSpec().addField("id").removeField("bool").commit();

    // write a second data file
    // should refresh the table and use the new partition spec
    RecordWriterManager writer2 =
        new RecordWriterManager(catalog, "test_prefix_2", Long.MAX_VALUE, Integer.MAX_VALUE);
    writer2.write(windowedDestination, row);
    writer2.write(windowedDestination, row2);
    writer2.close();

    List<SerializableDataFile> serializableDataFiles =
        writer2.getSerializableDataFiles().get(windowedDestination);
    assertEquals(2, serializableDataFiles.size());
    for (SerializableDataFile serializableDataFile : serializableDataFiles) {
      assertEquals(table.spec().specId(), serializableDataFile.getPartitionSpecId());
      dataFile = serializableDataFile.createDataFile(table.specs());
      assertEquals(1L, dataFile.recordCount());
      assertThat(dataFile.path().toString(), containsString("name_trunc=abc"));
      assertThat(
          dataFile.path().toString(), either(containsString("id=1")).or(containsString("id=2")));
    }
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testWriterExceptionGetsCaught() throws IOException {
    RecordWriterManager writerManager = new RecordWriterManager(catalog, "test_file_name", 100, 2);
    Row row = Row.withSchema(BEAM_SCHEMA).addValues(1, "abcdef", true).build();
    PartitionKey partitionKey = new PartitionKey(PARTITION_SPEC, ICEBERG_SCHEMA);
    partitionKey.partition(IcebergUtils.beamRowToIcebergRecord(ICEBERG_SCHEMA, row));

    writerManager.write(windowedDestination, row);

    RecordWriterManager.DestinationState state =
        writerManager.destinations.get(windowedDestination);
    // replace with a failing record writer
    FailingRecordWriter failingWriter =
        new FailingRecordWriter(
            catalog, windowedDestination.getValue(), "test_failing_writer", partitionKey);
    state.writers.put(partitionKey, failingWriter);
    writerManager.write(windowedDestination, row);

    // this tests that we indeed enter the catch block
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Encountered 1 failed writer(s)");
    try {
      writerManager.close();
    } catch (IllegalStateException e) {
      // fetch underlying exceptions and validate
      Throwable[] underlyingExceptions = e.getSuppressed();
      assertEquals(1, underlyingExceptions.length);
      for (Throwable t : underlyingExceptions) {
        assertThat(
            t.getMessage(),
            containsString("Encountered an error when closing data writer for table"));
        assertThat(
            t.getMessage(),
            containsString(windowedDestination.getValue().getTableIdentifier().toString()));
        assertThat(t.getMessage(), containsString(failingWriter.path()));
        Throwable realCause = t.getCause();
        assertEquals("I am failing!", realCause.getMessage());
      }

      throw e;
    }
  }

  static class FailingRecordWriter extends RecordWriter {
    FailingRecordWriter(
        Catalog catalog, IcebergDestination destination, String filename, PartitionKey partitionKey)
        throws IOException {
      super(catalog, destination, filename, partitionKey);
    }

    @Override
    public void close() throws IOException {
      throw new IOException("I am failing!");
    }
  }
}
