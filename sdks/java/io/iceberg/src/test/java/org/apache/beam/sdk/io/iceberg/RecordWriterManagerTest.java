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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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
    return getWindowedDestination(tableName, ICEBERG_SCHEMA, partitionSpec);
  }

  private WindowedValue<IcebergDestination> getWindowedDestination(
      String tableName, org.apache.iceberg.Schema schema, @Nullable PartitionSpec partitionSpec) {
    TableIdentifier tableIdentifier = TableIdentifier.of("default", tableName);

    warehouse.createTable(tableIdentifier, schema, partitionSpec);

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

    Map<String, PartitionField> partitionFieldMap = new HashMap<>();
    for (PartitionField partitionField : PARTITION_SPEC.fields()) {
      partitionFieldMap.put(partitionField.name(), partitionField);
    }

    String partitionPath =
        RecordWriterManager.getPartitionDataPath(partitionKey.toPath(), partitionFieldMap);
    DataFile roundTripDataFile =
        SerializableDataFile.from(datafile, partitionPath)
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
    Map<String, PartitionField> partitionFieldMap = new HashMap<>();
    for (PartitionField partitionField : PARTITION_SPEC.fields()) {
      partitionFieldMap.put(partitionField.name(), partitionField);
    }
    String partitionPath =
        RecordWriterManager.getPartitionDataPath(partitionKey.toPath(), partitionFieldMap);
    DataFile datafile = writer.getDataFile();
    SerializableDataFile serializableDataFile = SerializableDataFile.from(datafile, partitionPath);

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

  @Test
  public void testIdentityPartitioning() throws IOException {
    Schema primitiveTypeSchema =
        Schema.builder()
            .addBooleanField("bool")
            .addInt32Field("int")
            .addInt64Field("long")
            .addFloatField("float")
            .addDoubleField("double")
            .addStringField("str")
            .build();

    Row row =
        Row.withSchema(primitiveTypeSchema).addValues(true, 1, 1L, 1.23f, 4.56, "str").build();
    org.apache.iceberg.Schema icebergSchema =
        IcebergUtils.beamSchemaToIcebergSchema(primitiveTypeSchema);
    PartitionSpec spec =
        PartitionSpec.builderFor(icebergSchema)
            .identity("bool")
            .identity("int")
            .identity("long")
            .identity("float")
            .identity("double")
            .identity("str")
            .build();
    WindowedValue<IcebergDestination> dest =
        getWindowedDestination("identity_partitioning", icebergSchema, spec);

    RecordWriterManager writer =
        new RecordWriterManager(catalog, "test_prefix", Long.MAX_VALUE, Integer.MAX_VALUE);
    writer.write(dest, row);
    writer.close();
    List<SerializableDataFile> files = writer.getSerializableDataFiles().get(dest);
    assertEquals(1, files.size());
    SerializableDataFile dataFile = files.get(0);
    assertEquals(1, dataFile.getRecordCount());
    // build this string: bool=true/int=1/long=1/float=1.0/double=1.0/str=str
    List<String> expectedPartitions = new ArrayList<>();
    for (Schema.Field field : primitiveTypeSchema.getFields()) {
      Object val = row.getValue(field.getName());
      expectedPartitions.add(field.getName() + "=" + val);
    }
    String expectedPartitionPath = String.join("/", expectedPartitions);
    assertEquals(expectedPartitionPath, dataFile.getPartitionPath());
    assertThat(dataFile.getPath(), containsString(expectedPartitionPath));
  }

  @Test
  public void testBucketPartitioning() throws IOException {
    Schema bucketSchema =
        Schema.builder()
            .addInt32Field("int")
            .addInt64Field("long")
            .addStringField("str")
            .addLogicalTypeField("date", SqlTypes.DATE)
            .addLogicalTypeField("time", SqlTypes.TIME)
            .addLogicalTypeField("datetime", SqlTypes.DATETIME)
            .addDateTimeField("datetime_tz")
            .build();

    String timestamp = "2024-10-08T13:18:20.053";
    LocalDateTime localDateTime = LocalDateTime.parse(timestamp);

    Row row =
        Row.withSchema(bucketSchema)
            .addValues(
                1,
                1L,
                "str",
                localDateTime.toLocalDate(),
                localDateTime.toLocalTime(),
                localDateTime,
                DateTime.parse(timestamp))
            .build();
    org.apache.iceberg.Schema icebergSchema = IcebergUtils.beamSchemaToIcebergSchema(bucketSchema);
    PartitionSpec spec =
        PartitionSpec.builderFor(icebergSchema)
            .bucket("int", 2)
            .bucket("long", 2)
            .bucket("str", 2)
            .bucket("date", 2)
            .bucket("time", 2)
            .bucket("datetime", 2)
            .bucket("datetime_tz", 2)
            .build();
    WindowedValue<IcebergDestination> dest =
        getWindowedDestination("bucket_partitioning", icebergSchema, spec);

    RecordWriterManager writer =
        new RecordWriterManager(catalog, "test_prefix", Long.MAX_VALUE, Integer.MAX_VALUE);
    writer.write(dest, row);
    writer.close();
    List<SerializableDataFile> files = writer.getSerializableDataFiles().get(dest);
    assertEquals(1, files.size());
    SerializableDataFile dataFile = files.get(0);
    assertEquals(1, dataFile.getRecordCount());
    for (Schema.Field field : bucketSchema.getFields()) {
      String expectedPartition = field.getName() + "_bucket";
      assertThat(dataFile.getPartitionPath(), containsString(expectedPartition));
      assertThat(dataFile.getPath(), containsString(expectedPartition));
    }
  }

  @Test
  public void testTimePartitioning() throws IOException {
    Schema timePartitioningSchema =
        Schema.builder()
            .addLogicalTypeField("y_date", SqlTypes.DATE)
            .addLogicalTypeField("y_datetime", SqlTypes.DATETIME)
            .addDateTimeField("y_datetime_tz")
            .addLogicalTypeField("m_date", SqlTypes.DATE)
            .addLogicalTypeField("m_datetime", SqlTypes.DATETIME)
            .addDateTimeField("m_datetime_tz")
            .addLogicalTypeField("d_date", SqlTypes.DATE)
            .addLogicalTypeField("d_datetime", SqlTypes.DATETIME)
            .addDateTimeField("d_datetime_tz")
            .addLogicalTypeField("h_datetime", SqlTypes.DATETIME)
            .addDateTimeField("h_datetime_tz")
            .build();
    org.apache.iceberg.Schema icebergSchema =
        IcebergUtils.beamSchemaToIcebergSchema(timePartitioningSchema);
    PartitionSpec spec =
        PartitionSpec.builderFor(icebergSchema)
            .year("y_date")
            .year("y_datetime")
            .year("y_datetime_tz")
            .month("m_date")
            .month("m_datetime")
            .month("m_datetime_tz")
            .day("d_date")
            .day("d_datetime")
            .day("d_datetime_tz")
            .hour("h_datetime")
            .hour("h_datetime_tz")
            .build();

    WindowedValue<IcebergDestination> dest =
        getWindowedDestination("time_partitioning", icebergSchema, spec);

    String timestamp = "2024-10-08T13:18:20.053";
    LocalDateTime localDateTime = LocalDateTime.parse(timestamp);
    LocalDate localDate = localDateTime.toLocalDate();
    String timestamptz = "2024-10-08T13:18:20.053+03:27";
    DateTime dateTime = DateTime.parse(timestamptz);

    Row row =
        Row.withSchema(timePartitioningSchema)
            .addValues(localDate, localDateTime, dateTime) // year
            .addValues(localDate, localDateTime, dateTime) // month
            .addValues(localDate, localDateTime, dateTime) // day
            .addValues(localDateTime, dateTime) // hour
            .build();

    // write some rows
    RecordWriterManager writer =
        new RecordWriterManager(catalog, "test_prefix", Long.MAX_VALUE, Integer.MAX_VALUE);
    writer.write(dest, row);
    writer.close();
    List<SerializableDataFile> files = writer.getSerializableDataFiles().get(dest);
    assertEquals(1, files.size());
    SerializableDataFile serializableDataFile = files.get(0);
    assertEquals(1, serializableDataFile.getRecordCount());

    int year = localDateTime.getYear();
    int month = localDateTime.getMonthValue();
    int day = localDateTime.getDayOfMonth();
    int hour = localDateTime.getHour();
    List<String> expectedPartitions = new ArrayList<>();
    for (Schema.Field field : timePartitioningSchema.getFields()) {
      String name = field.getName();
      String expected = "";
      if (name.startsWith("y_")) {
        expected = String.format("%s_year=%s", name, year);
      } else if (name.startsWith("m_")) {
        expected = String.format("%s_month=%s-%02d", name, year, month);
      } else if (name.startsWith("d_")) {
        expected = String.format("%s_day=%s-%02d-%02d", name, year, month, day);
      } else if (name.startsWith("h_")) {
        if (name.contains("tz")) {
          hour = dateTime.withZone(DateTimeZone.UTC).getHourOfDay();
        }
        expected = String.format("%s_hour=%s-%02d-%02d-%02d", name, year, month, day, hour);
      }
      expectedPartitions.add(expected);
    }
    String expectedPartition = String.join("/", expectedPartitions);
    DataFile dataFile =
        serializableDataFile.createDataFile(
            catalog.loadTable(dest.getValue().getTableIdentifier()).specs());
    assertThat(dataFile.path().toString(), containsString(expectedPartition));
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
