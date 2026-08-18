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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.cdc.CdcReadUtils;
import org.apache.beam.sdk.io.iceberg.cdc.TestChangelogTasks;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.NativeEncryptionInputFile;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Verifies that IcebergIO honors Iceberg v3 table encryption.
 *
 * <p>These are self-contained: {@link TestKms} stands in for a KMS service, so no cloud credentials
 * are needed.
 *
 * <p>What the spec requires of an engine working with a table that has {@code encryption.key-id}
 * set:
 *
 * <ul>
 *   <li>data files are encrypted, and each manifest entry carries the file's {@code key_metadata}
 *   <li>manifests and manifest lists are encrypted; the snapshot records the manifest list's {@code
 *       key-id}
 *   <li>the keys minted along the way are tracked in the table metadata's {@code encryption-keys}
 *   <li>the encrypted files are readable again
 * </ul>
 */
@RunWith(JUnit4.class)
public class IcebergEncryptionTest implements Serializable {
  /** Same shape as {@link TestFixtures#SCHEMA}, plus the identifier field CDC needs. */
  private static final Schema CDC_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get())),
          ImmutableSet.of(1));

  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public transient TestPipeline writePipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();
  @Rule public transient TestName testName = new TestName();

  private String warehouse;
  private IcebergCatalogConfig catalogConfig;
  private TableIdentifier tableId;

  @Before
  public void setUp() throws IOException {
    warehouse = "file:" + temporaryFolder.newFolder("warehouse");
    catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName("encryption-test")
            .setCatalogProperties(
                ImmutableMap.<String, String>builder()
                    .put(CatalogProperties.CATALOG_IMPL, EncryptedTestCatalog.class.getName())
                    .put(CatalogProperties.WAREHOUSE_LOCATION, warehouse)
                    .put(CatalogProperties.ENCRYPTION_KMS_IMPL, TestKms.class.getName())
                    .build())
            .build();
    tableId =
        TableIdentifier.of(
            "default",
            testName.getMethodName() + "_" + UUID.randomUUID().toString().substring(0, 8));
  }

  /**
   * Checks the fixture itself: the test catalog really does hand out a {@link
   * StandardEncryptionManager}. Without this, every other test here could pass vacuously against a
   * plaintext table.
   */
  @Test
  public void testFixtureProducesAnEncryptedTable() {
    Table table = createEncryptedTable(TestFixtures.SCHEMA);

    assertThat(table.encryption(), Matchers.instanceOf(StandardEncryptionManager.class));
    assertEquals(
        TestKms.MASTER_KEY_ID, table.properties().get(TableProperties.ENCRYPTION_TABLE_KEY));
    assertEquals(3, ((HasTableOperations) table).operations().current().formatVersion());
  }

  /**
   * Spec: data files in an encrypted table are encrypted, and the manifest entry carries the
   * per-file {@code key_metadata} needed to decrypt them.
   */
  @Test
  public void testWrittenDataFilesAreEncryptedAndCarryKeyMetadata() throws IOException {
    createEncryptedTable(TestFixtures.SCHEMA);
    runWritePipeline();

    Table table = loadTable();
    List<DataFile> dataFiles =
        ImmutableList.copyOf(table.currentSnapshot().addedDataFiles(table.io()));
    assertFalse("expected the write to produce data files", dataFiles.isEmpty());

    for (DataFile dataFile : dataFiles) {
      assertNotNull(
          "data file " + dataFile.location() + " is missing key_metadata", dataFile.keyMetadata());
      assertDataFileIsEncrypted(table, dataFile);
    }
  }

  /**
   * Spec: manifests and manifest lists in an encrypted table are encrypted, and the snapshot
   * records the key ID used for its manifest list.
   */
  @Test
  public void testManifestsAndManifestListAreEncrypted() throws IOException {
    createEncryptedTable(TestFixtures.SCHEMA);
    runWritePipeline();

    Table table = loadTable();
    Snapshot snapshot = table.currentSnapshot();

    assertNotNull("snapshot is missing the manifest list key-id", snapshot.keyId());
    // Iceberg refuses to open an encrypted manifest list through a plain FileIO
    assertThrows(
        "manifest list is readable without the encryption manager",
        IllegalArgumentException.class,
        () -> snapshot.allManifests(plaintextIo()));

    List<ManifestFile> manifests = snapshot.allManifests(table.io());
    assertFalse("expected the write to produce manifests", manifests.isEmpty());
    assertManifestsAreEncrypted(table, manifests);
  }

  /**
   * Covers the manifest-writing path in {@link AppendFilesToTables}, which only runs when a batch
   * of files spans more than one partition spec.
   */
  @Test
  public void testManifestsWrittenForMultiSpecBatchAreEncrypted() throws IOException {
    Table table = createEncryptedTable(TestFixtures.SCHEMA);

    // one file under the initial (unpartitioned) spec...
    FileWriteResult first = writeDataFileWithBeam(table, TestFixtures.FILE1SNAPSHOT1);

    // ...and one under a second spec, so the batch spans two specs
    table.updateSpec().addField("data").commit();
    table.refresh();
    FileWriteResult second = writeDataFileWithBeam(table, TestFixtures.FILE1SNAPSHOT2);

    writePipeline
        .apply(Create.of(first, second))
        .apply(new AppendFilesToTables(catalogConfig, "test-manifest"));
    writePipeline.run().waitUntilFinish();

    Table committed = loadTable();
    List<ManifestFile> manifests = committed.currentSnapshot().allManifests(committed.io());

    // sanity check: we really did exercise the manifest path, one manifest per spec
    assertEquals("expected one manifest per partition spec", 2, manifests.size());
    assertManifestsAreEncrypted(committed, manifests);
  }

  @Test
  public void testEncryptionKeysAreTrackedInTableMetadata() {
    createEncryptedTable(TestFixtures.SCHEMA);
    runWritePipeline();

    Table table = loadTable();
    TableMetadata metadata = ((HasTableOperations) table).operations().current();

    assertFalse(
        "table metadata has no encryption-keys after a write", metadata.encryptionKeys().isEmpty());
    for (EncryptedKey key : metadata.encryptionKeys()) {
      assertNotNull(key.keyId());
      ByteBuffer encryptedKeyMetadata = key.encryptedKeyMetadata();
      assertNotNull(encryptedKeyMetadata);
      assertTrue(encryptedKeyMetadata.remaining() > 0);
    }

    // the snapshot's key-id must resolve to one of the tracked keys
    String snapshotKeyId = table.currentSnapshot().keyId();
    assertThat(
        "snapshot key-id is not tracked in encryption-keys",
        metadata.encryptionKeys().stream().map(EncryptedKey::keyId).collect(Collectors.toList()),
        Matchers.hasItem(snapshotKeyId));
  }

  @Test
  public void testEncryptedFilesAreReadableByIceberg() {
    createEncryptedTable(TestFixtures.SCHEMA);
    runWritePipeline();

    List<Record> records = ImmutableList.copyOf(IcebergGenerics.read(loadTable()).build());

    assertThat(records, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
  }

  @Test
  public void testEncryptedTableRoundTripsThroughBeam() {
    createEncryptedTable(TestFixtures.SCHEMA);
    runWritePipeline();

    PCollection<Row> rows =
        readPipeline.apply("Read encrypted table", IcebergIO.readRows(catalogConfig).from(tableId));

    PAssert.that(rows).containsInAnyOrder(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1));
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadsFilesWrittenByIcebergLibraries() throws IOException {
    Table table = createEncryptedTable(TestFixtures.SCHEMA);
    appendWithIceberg(table, TestFixtures.FILE1SNAPSHOT1);

    PCollection<Row> rows =
        readPipeline.apply("Read encrypted table", IcebergIO.readRows(catalogConfig).from(tableId));

    PAssert.that(rows).containsInAnyOrder(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1));
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testTableIsUnreadableWithoutTheKms() {
    createEncryptedTable(TestFixtures.SCHEMA);
    runWritePipeline();

    Table table = loadTableWithoutKms();

    assertThrows(
        "encrypted table was readable without the KMS",
        IllegalArgumentException.class,
        () -> ImmutableList.copyOf(IcebergGenerics.read(table).build()));
  }

  @Test
  public void testCdcReadsEncryptedDataFile() throws IOException {
    Table table = createEncryptedTable(CDC_SCHEMA);
    DataFile dataFile =
        writeDataFileWithIceberg(table, cdcRecord(0L, "alpha"), cdcRecord(1L, "beta"));
    assertNotNull("data file should carry key metadata", dataFile.keyMetadata());

    CloseableIterable<Record> records =
        CdcReadUtils.changelogRecordsForTask(
            TestChangelogTasks.addedRows(table, dataFile, ImmutableList.of()),
            table,
            cdcScanConfig(table),
            true);

    assertEquals(ImmutableList.of(0L, 1L), idsOf(records));
  }

  @Test
  public void testCdcAppliesEncryptedDeleteFiles() throws IOException {
    Table table = createEncryptedTable(CDC_SCHEMA);
    DataFile dataFile =
        writeDataFileWithIceberg(
            table,
            cdcRecord(0L, "keep-0"),
            cdcRecord(1L, "drop-by-pos"),
            cdcRecord(2L, "drop-by-data"),
            cdcRecord(3L, "keep-3"));

    DeleteFile positionDelete = writePositionDelete(table, dataFile, 1L);
    DeleteFile equalityDelete = writeEqualityDelete(table, dataFile, "drop-by-data");
    assertNotNull("position delete should carry key metadata", positionDelete.keyMetadata());
    assertNotNull("equality delete should carry key metadata", equalityDelete.keyMetadata());

    CloseableIterable<Record> records =
        CdcReadUtils.changelogRecordsForTask(
            TestChangelogTasks.addedRows(
                table, dataFile, ImmutableList.of(positionDelete, equalityDelete)),
            table,
            cdcScanConfig(table),
            true);

    assertEquals(ImmutableList.of(0L, 3L), idsOf(records));
  }

  /** Creates a v3 table with a table encryption key, i.e. an encrypted table. */
  private Table createEncryptedTable(Schema schema) {
    return catalog()
        .buildTable(tableId, schema)
        .withProperty(TableProperties.FORMAT_VERSION, "3")
        .withProperty(TableProperties.ENCRYPTION_TABLE_KEY, TestKms.MASTER_KEY_ID)
        .create();
  }

  private Catalog catalog() {
    return catalogConfig.catalog();
  }

  private Table loadTable() {
    return catalog().loadTable(tableId);
  }

  private void runWritePipeline() {
    writePipeline
        .apply("Records to add", Create.of(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1)))
        .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
        .apply("Append to table", IcebergIO.writeRows(catalogConfig).to(tableId));
    writePipeline.run().waitUntilFinish();
  }

  /**
   * Asserts that each manifest is encrypted: it carries {@code key_metadata}, Iceberg refuses to
   * open it without an encryption manager.
   */
  private void assertManifestsAreEncrypted(Table table, List<ManifestFile> manifests)
      throws IOException {
    for (ManifestFile manifest : manifests) {
      assertNotNull(
          "manifest " + manifest.path() + " is missing key_metadata", manifest.keyMetadata());
      assertThrows(
          "manifest " + manifest.path() + " is readable without the encryption manager",
          IllegalArgumentException.class,
          () -> plaintextIo().newInputFile(manifest));

      List<String> dataFilePaths =
          ImmutableList.copyOf(ManifestFiles.readPaths(manifest, table.io(), table.specs()));
      assertFalse(
          "manifest " + manifest.path() + " decrypted to no entries", dataFilePaths.isEmpty());
    }
  }

  /** Writes a data file through Beam's {@link RecordWriter}. */
  private FileWriteResult writeDataFileWithBeam(Table table, List<Record> records)
      throws IOException {
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    if (!table.spec().isUnpartitioned()) {
      partitionKey.partition(
          new InternalRecordWrapper(table.schema().asStruct()).wrap(records.get(0)));
    }

    RecordWriter writer =
        new RecordWriter(table, FileFormat.PARQUET, UUID.randomUUID().toString(), partitionKey);
    for (Record record : records) {
      writer.write(record);
    }
    writer.close();

    return FileWriteResult.builder()
        .setTableIdentifier(tableId)
        .setSerializableDataFile(SerializableDataFile.from(writer.getDataFile(), table.spec()))
        .build();
  }

  /** Writes a data file through Iceberg's own writer stack. */
  private static DataFile writeDataFileWithIceberg(Table table, Record... records)
      throws IOException {
    return writeDataFileWithIceberg(table, ImmutableList.copyOf(records));
  }

  private static DataFile writeDataFileWithIceberg(Table table, List<Record> records)
      throws IOException {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec());

    DataWriter<Record> writer =
        appenderFactory.newDataWriter(fileFactory.newOutputFile(), FileFormat.PARQUET, null);
    try (DataWriter<Record> toClose = writer) {
      for (Record record : records) {
        toClose.write(record);
      }
    }
    return writer.toDataFile();
  }

  /** Appends records to a table using Iceberg's own writer stack, and commits. */
  private static void appendWithIceberg(Table table, List<Record> records) throws IOException {
    table.newAppend().appendFile(writeDataFileWithIceberg(table, records)).commit();
  }

  private static DeleteFile writePositionDelete(Table table, DataFile dataFile, long... positions)
      throws IOException {
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec());
    PositionDeleteWriter<Record> writer =
        appenderFactory.newPosDeleteWriter(
            encryptedFile(table, dataFile.location() + ".pos-delete.parquet"),
            FileFormat.PARQUET,
            null);
    try (PositionDeleteWriter<Record> toClose = writer) {
      for (long position : positions) {
        toClose.write(PositionDelete.<Record>create().set(dataFile.location(), position));
      }
    }
    return writer.toDeleteFile();
  }

  private static DeleteFile writeEqualityDelete(
      Table table, DataFile dataFile, @Nullable String data) throws IOException {
    Schema deleteSchema = table.schema().select("data");
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec(), new int[] {2}, deleteSchema, null);
    EqualityDeleteWriter<Record> writer =
        appenderFactory.newEqDeleteWriter(
            encryptedFile(table, dataFile.location() + ".eq-delete.parquet"),
            FileFormat.PARQUET,
            null);
    try (EqualityDeleteWriter<Record> toClose = writer) {
      GenericRecord deleteRecord = GenericRecord.create(deleteSchema);
      deleteRecord.setField("data", data);
      toClose.write(deleteRecord);
    }
    return writer.toDeleteFile();
  }

  private static EncryptedOutputFile encryptedFile(Table table, String location) {
    return EncryptingFileIO.combine(table.io(), table.encryption())
        .newEncryptingOutputFile(location);
  }

  private static Record cdcRecord(long id, String data) {
    GenericRecord record = GenericRecord.create(CDC_SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private IcebergScanConfig cdcScanConfig(Table table) {
    return IcebergScanConfig.builder()
        .setCatalogConfig(catalogConfig)
        .setTableIdentifier(tableId)
        .setSchema(IcebergUtils.icebergSchemaToBeamSchema(table.schema()))
        .setKeepFields(ImmutableList.of("id"))
        .build();
  }

  private static List<Long> idsOf(CloseableIterable<Record> records) {
    return ImmutableList.copyOf(records).stream()
        .map(record -> (Long) record.getField("id"))
        .collect(Collectors.toList());
  }

  /**
   * Asserts that {@code dataFile} uses Parquet modular encryption, which is the layout every
   * Iceberg engine expects of an encrypted Parquet file.
   *
   * <p>The footer is unreadable without a key, and reading it with the key from {@code
   * key_metadata} reports an encrypted footer.
   */
  private static void assertDataFileIsEncrypted(Table table, ContentFile<?> dataFile)
      throws IOException {
    assertThrows(
        "data file " + dataFile.location() + " is readable without a key",
        ParquetCryptoRuntimeException.class,
        () -> ParquetFileReader.open(parquetInputFile(dataFile.location())).close());

    EncryptedInputFile encrypted =
        EncryptedFiles.encryptedInput(
            table.io().newInputFile(dataFile.location()), dataFile.keyMetadata());
    InputFile decrypted = table.encryption().decrypt(encrypted);
    assertThat(decrypted, Matchers.instanceOf(NativeEncryptionInputFile.class));
    NativeEncryptionInputFile nativeFile = (NativeEncryptionInputFile) decrypted;

    ParquetReadOptions options =
        ParquetReadOptions.builder()
            .withDecryption(
                FileDecryptionProperties.builder()
                    .withFooterKey(
                        ByteBuffers.toByteArray(nativeFile.keyMetadata().encryptionKey()))
                    .withAADPrefix(ByteBuffers.toByteArray(nativeFile.keyMetadata().aadPrefix()))
                    .build())
            .build();
    try (ParquetFileReader reader =
        ParquetFileReader.open(parquetInputFile(dataFile.location()), options)) {
      assertEquals(
          "data file " + dataFile.location() + " does not use Parquet modular encryption",
          FileMetaData.EncryptionType.ENCRYPTED_FOOTER,
          reader.getFooter().getFileMetaData().getEncryptionType());
    }
  }

  /**
   * The same warehouse seen through a catalog with no KMS configured, i.e. what an engine without
   * access to the encryption keys has.
   */
  private Table loadTableWithoutKms() {
    return IcebergCatalogConfig.builder()
        .setCatalogName("no-kms")
        .setCatalogProperties(
            ImmutableMap.of(
                "type",
                CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
                CatalogProperties.WAREHOUSE_LOCATION,
                warehouse))
        .build()
        .catalog()
        .loadTable(tableId);
  }

  /** A {@link FileIO} with no encryption manager attached. */
  private FileIO plaintextIo() {
    return loadTableWithoutKms().io();
  }

  private static org.apache.parquet.io.InputFile parquetInputFile(String location)
      throws IOException {
    return HadoopInputFile.fromPath(new Path(URI.create(location)), new Configuration());
  }
}
