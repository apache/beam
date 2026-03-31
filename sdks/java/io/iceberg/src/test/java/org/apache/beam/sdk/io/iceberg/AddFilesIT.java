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

import static java.lang.String.format;
import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;
import static org.apache.beam.sdk.io.iceberg.IcebergUtils.beamSchemaToIcebergSchema;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Notification;
import com.google.cloud.storage.NotificationInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubGrpcClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsubOptions;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Deduplicate;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.hadoop.util.Lists;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.util.Pair;
import org.awaitility.Awaitility;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for {@link AddFiles}, using Pubsub notifications to pass newly created file
 * paths downstream to add to an Iceberg table.
 */
public class AddFilesIT {
  private static final Logger LOG = LoggerFactory.getLogger(AddFilesIT.class);

  private static final String WAREHOUSE = "gs://managed-iceberg-biglake-its";
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  @Rule public TestName testName = new TestName();
  private final String notificationsTopic =
      format(
          "projects/%s/topics/%s-%s-%s",
          PROJECT,
          AddFilesIT.class.getSimpleName(),
          testName.getMethodName(),
          System.currentTimeMillis());
  private static final Schema NOTIFICATION_SCHEMA =
      Schema.builder()
          .addStringField("bucket")
          .addStringField("name")
          .addStringField("kind")
          .build();
  private static final Map<String, String> BIGLAKE_PROPS =
      Map.of(
          "type", "rest",
          "uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog",
          "warehouse", WAREHOUSE,
          "header.x-goog-user-project", PROJECT,
          "rest.auth.type", "google",
          "io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO",
          "rest-metrics-reporting-enabled", "false");
  private Storage storage;
  private PubsubClient pubsub;
  private Notification notification;
  private final String namespace = getClass().getSimpleName();
  private String srcTableName;
  private String destTableName;
  private TableIdentifier srcTableId;
  private TableIdentifier destTableId;
  private long salt;
  private String dirName;
  private static final Schema ROW_SCHEMA =
      Schema.builder().addInt64Field("id").addStringField("name").addInt32Field("age").build();
  private static final List<String> PARTITION_FIELDS =
      Arrays.asList("bucket(id, 16)", "truncate(name, 8)", "age");
  private static final PartitionSpec SPEC =
      PartitionUtils.toPartitionSpec(PARTITION_FIELDS, ROW_SCHEMA);
  private static final Map<String, String> TABLE_PROPS = ImmutableMap.of("foo", "bar");
  private static final List<Row> TEST_ROWS =
      IntStream.range(0, 20)
          .mapToObj(
              i -> Row.withSchema(ROW_SCHEMA).addValues((long) i, "name_" + i, i + 30).build())
          .collect(Collectors.toList());
  private final RESTCatalog catalog = new RESTCatalog();

  @Before
  public void setup() throws IOException {
    storage = StorageOptions.newBuilder().build().getService();
    pubsub =
        PubsubGrpcClient.FACTORY.newClient(
            null, null, TestPipeline.testingPipelineOptions().as(TestPubsubOptions.class));

    pubsub.createTopic(PubsubClient.topicPathFromPath(notificationsTopic));

    NotificationInfo notificationInfo =
        NotificationInfo.newBuilder(notificationsTopic)
            .setEventTypes(NotificationInfo.EventType.OBJECT_FINALIZE)
            .setPayloadFormat(NotificationInfo.PayloadFormat.JSON_API_V1)
            .build();
    try {
      notification = storage.createNotification(WAREHOUSE.replace("gs://", ""), notificationInfo);
    } catch (StorageException e) {
      if (e.getMessage().contains("Too many overlapping notifications")) {
        List<Notification> existing = storage.listNotifications(WAREHOUSE.replace("gs://", ""));
        LOG.warn(
            "Too many notifications on bucket {}: {}. Deleting existing notifications to make room: {}",
            WAREHOUSE,
            e,
            existing.stream()
                .map(NotificationInfo::getNotificationId)
                .collect(Collectors.toList()));
        existing.forEach(
            n -> storage.deleteNotification(WAREHOUSE.replace("gs://", ""), n.getNotificationId()));

        // try creating it again
        notification = storage.createNotification(WAREHOUSE.replace("gs://", ""), notificationInfo);
      } else {
        throw e;
      }
    }

    salt = System.currentTimeMillis();
    dirName = format("%s-%s/%s", getClass().getSimpleName(), salt, testName.getMethodName());
    srcTableName = "src_" + testName.getMethodName() + "_" + salt;
    destTableName = "dest_" + testName.getMethodName() + "_" + salt;
    srcTableId = TableIdentifier.of(namespace, srcTableName);
    destTableId = TableIdentifier.of(namespace, destTableName);

    catalog.initialize("test_catalog", BIGLAKE_PROPS);
    cleanupCatalog();
    catalog.createNamespace(Namespace.of(namespace));
  }

  private void cleanupCatalog() {
    Namespace ns = Namespace.of(namespace);
    if (catalog.namespaceExists(ns)) {
      catalog.listTables(ns).forEach(catalog::dropTable);
      catalog.dropNamespace(ns);
    }
  }

  @After
  public void cleanup() {
    try {
      pubsub.deleteTopic(PubsubClient.topicPathFromPath(notificationsTopic));
      pubsub.close();
    } catch (Exception e) {
      LOG.warn("Failed to clean up PubSub", e);
    }

    try {
      storage.deleteNotification(WAREHOUSE.replace("gs://", ""), notification.getNotificationId());
      storage.close();
    } catch (Exception e) {
      LOG.warn("Failed to clean up GCS notifications", e);
    }

    try {
      cleanupCatalog();
    } catch (Exception e) {
      LOG.warn("Failed to clean up Iceberg catalog", e);
    }

    try {
      Iterable<Blob> blobs =
          storage
              .list(WAREHOUSE.replace("gs://", ""), Storage.BlobListOption.prefix(dirName))
              .getValues();
      blobs.forEach(b -> storage.delete(b.getBlobId()));
    } catch (Exception e) {
      LOG.warn("Failed to clean up GCS bucket", e);
    }
  }

  @Test
  public void testStreamingImportFromExistingIcebergTable()
      throws IOException, InterruptedException {
    // first create a source iceberg table
    catalog.createTable(srcTableId, beamSchemaToIcebergSchema(ROW_SCHEMA), SPEC);

    String filter = format("%s/%s/data/", namespace, srcTableName);

    // build AddFiles pipeline and let it run in the background
    PipelineResult addFilesPipeline = startAddFilesListener(filter);

    // before writing, confirm the destination table still does not exist
    assertFalse(catalog.tableExists(destTableId));

    // write some rows to the source table
    LOG.info("Writing records to the source table");
    Pipeline q = Pipeline.create();
    q.apply(Create.of(TEST_ROWS))
        .setRowSchema(ROW_SCHEMA)
        .apply(
            Managed.write(Managed.ICEBERG)
                .withConfig(
                    ImmutableMap.of(
                        "table", srcTableId.toString(), "catalog_properties", BIGLAKE_PROPS)));
    q.run().waitUntilFinish();

    // check that the destination table has been created
    Awaitility.await()
        .atMost(java.time.Duration.ofMinutes(5))
        .pollInterval(java.time.Duration.ofSeconds(10))
        .until(() -> catalog.tableExists(destTableId));
    LOG.info("Destination table has been created");

    LOG.info("Checking if all source files have been registered in the destination table");
    Awaitility.await()
        .atMost(java.time.Duration.ofMinutes(2))
        .pollInterval(java.time.Duration.ofSeconds(5))
        .until(() -> checkTableFiles() != null);
    LOG.info("Destination table has registered all source files.");
    Pair<Map<String, DataFile>, Map<String, DataFile>> srcAndDestfiles =
        checkStateNotNull(checkTableFiles());

    for (Map.Entry<String, DataFile> srcFile : srcAndDestfiles.first().entrySet()) {
      String location = srcFile.getKey();
      DataFile destFile =
          checkStateNotNull(
              srcAndDestfiles.second().get(location),
              "Source file '%s' was not registered in the destination table",
              location);

      // check that partition metadata was preserved
      assertEquals(destFile.partition(), srcFile.getValue().partition());
    }

    // safe to cancel the AddFiles pipeline now
    LOG.info("Canceling AddFiles listener.");
    addFilesPipeline.cancel();

    // check all records are there
    checkRecordsInDestinationTable();
  }

  /**
   * Fetch all added files in both tables. Return null if some files have not yet propagated to
   * destination table
   */
  private @Nullable Pair<Map<String, DataFile>, Map<String, DataFile>> checkTableFiles() {
    Table destTable = catalog.loadTable(destTableId);
    Table srcTable = catalog.loadTable(srcTableId);

    Map<String, DataFile> srcFiles = new HashMap<>();
    Map<String, DataFile> destFiles = new HashMap<>();
    for (Snapshot snapshot : srcTable.snapshots()) {
      snapshot.addedDataFiles(srcTable.io()).forEach(df -> srcFiles.put(df.location(), df));
    }
    for (Snapshot snapshot : destTable.snapshots()) {
      snapshot.addedDataFiles(destTable.io()).forEach(df -> destFiles.put(df.location(), df));
    }

    LOG.info(
        "Number of source files: {}, Number of registered destination files: {}",
        srcFiles.size(),
        destFiles.size());

    if (srcFiles.size() != destFiles.size()) {
      Set<String> onlyInSrc = new HashSet<>(srcFiles.keySet());
      onlyInSrc.removeAll(destFiles.keySet());
      LOG.info("Missing source files: {}", onlyInSrc);
      return null;
    }

    return Pair.of(srcFiles, destFiles);
  }

  @Test
  public void testStreamingParquetImport()
      throws InterruptedException, TimeoutException, IOException {
    // start with a table that does not exist

    String parquetDir = format("%s/%s/", WAREHOUSE, dirName);
    String tempDir = format("%s/%s-tmp/", WAREHOUSE, dirName);

    // let the add files pipeline run in the background
    PipelineResult addFilesPipeline = startAddFilesListener(dirName);

    // before writing, confirm the destination table still does not exist
    assertFalse(catalog.tableExists(destTableId));

    // write some parquet files
    LOG.info("Writing records to the parquet dir");
    Pipeline q = Pipeline.create();
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(ROW_SCHEMA);
    q.apply(Create.of(TEST_ROWS))
        .setRowSchema(ROW_SCHEMA)
        .apply(
            MapElements.into(TypeDescriptor.of(GenericRecord.class))
                .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
        .setCoder(AvroCoder.of(avroSchema))
        .apply(
            FileIO.<String, GenericRecord>writeDynamic()
                .by(
                    record ->
                        format("%s-%s-%s", record.get("id"), record.get("name"), record.get("age")))
                .via(ParquetIO.sink(avroSchema))
                .withNaming(name -> defaultNaming(name, ".parquet"))
                .withTempDirectory(tempDir)
                .to(parquetDir)
                .withDestinationCoder(StringUtf8Coder.of()));
    q.run().waitUntilFinish();

    GcsUtil gcsUtil = TestPipeline.testingPipelineOptions().as(GcsOptions.class).getGcsUtil();

    Iterable<StorageObject> objects =
        gcsUtil.listObjects(WAREHOUSE.replace("gs://", ""), dirName, null).getItems();
    List<String> writtenFilePaths =
        Lists.newArrayList(objects).stream()
            .map(o -> format("gs://%s/%s", o.getBucket(), o.getName()))
            .collect(Collectors.toList());
    LOG.info("Written file paths: {}", writtenFilePaths);

    // check that the destination table has been created
    Awaitility.await()
        .atMost(java.time.Duration.ofMinutes(1))
        .pollInterval(java.time.Duration.ofSeconds(5))
        .until(() -> catalog.tableExists(destTableId));
    LOG.info("Destination table has been created");

    LOG.info("Checking if all source files have been registered in the destination table");
    Awaitility.await()
        .atMost(java.time.Duration.ofMinutes(2))
        .pollInterval(java.time.Duration.ofSeconds(5))
        .until(() -> checkTableHasRegisteredParquetFiles(writtenFilePaths));
    LOG.info(
        "Destination table has registered all source files ({} files).", writtenFilePaths.size());

    // safe to cancel the AddFiles pipeline now
    LOG.info("Canceling AddFiles listener.");
    addFilesPipeline.cancel();

    // check all records are there
    checkRecordsInDestinationTable();
  }

  @Test
  public void testBatchParquetImport() throws IOException {
    // start with a table that does not exist

    String parquetDir = format("%s/%s/", WAREHOUSE, dirName);
    String tempDir = format("%s/%s-tmp/", WAREHOUSE, dirName);

    // write some parquet files
    LOG.info("Writing records to the parquet dir");
    Pipeline q = Pipeline.create();
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(ROW_SCHEMA);
    q.apply(Create.of(TEST_ROWS))
        .setRowSchema(ROW_SCHEMA)
        .apply(
            MapElements.into(TypeDescriptor.of(GenericRecord.class))
                .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
        .setCoder(AvroCoder.of(avroSchema))
        .apply(
            FileIO.<String, GenericRecord>writeDynamic()
                .by(
                    record ->
                        format("%s-%s-%s", record.get("id"), record.get("name"), record.get("age")))
                .via(ParquetIO.sink(avroSchema))
                .withNaming(name -> defaultNaming(name, ".parquet"))
                .withTempDirectory(tempDir)
                .to(parquetDir)
                .withDestinationCoder(StringUtf8Coder.of()));
    q.run().waitUntilFinish();

    GcsUtil gcsUtil = TestPipeline.testingPipelineOptions().as(GcsOptions.class).getGcsUtil();

    Iterable<StorageObject> objects =
        gcsUtil.listObjects(WAREHOUSE.replace("gs://", ""), dirName, null).getItems();
    List<String> writtenFilePaths =
        Lists.newArrayList(objects).stream()
            .map(o -> format("gs://%s/%s", o.getBucket(), o.getName()))
            .collect(Collectors.toList());
    LOG.info("Written file paths: {}", writtenFilePaths);

    // before adding, confirm the destination table still does not exist
    assertFalse(catalog.tableExists(destTableId));

    // run batch AddFiles
    Pipeline p = Pipeline.create();
    PCollectionRowTuple tuple =
        p.apply(Create.of(writtenFilePaths))
            .apply(
                new AddFiles(
                    IcebergCatalogConfig.builder().setCatalogProperties(BIGLAKE_PROPS).build(),
                    namespace + "." + destTableName,
                    null,
                    PARTITION_FIELDS,
                    TABLE_PROPS,
                    10,
                    Duration.standardSeconds(10)));
    PAssert.that(tuple.get("errors")).empty();
    p.run().waitUntilFinish();

    // check that the destination table has been created
    assertTrue(catalog.tableExists(destTableId));
    LOG.info("Destination table has been created");

    LOG.info("Checking if all source files have been registered in the destination table");
    assertTrue(checkTableHasRegisteredParquetFiles(writtenFilePaths));
    LOG.info(
        "Destination table has registered all source files ({} files).", writtenFilePaths.size());

    // check all records are there
    checkRecordsInDestinationTable();
  }

  private void checkRecordsInDestinationTable() {
    Pipeline s = Pipeline.create();
    PCollection<Row> destRows =
        s.apply(
                Managed.read(Managed.ICEBERG)
                    .withConfig(
                        ImmutableMap.of(
                            "table", destTableId.toString(), "catalog_properties", BIGLAKE_PROPS)))
            .getSinglePCollection();
    PAssert.that(destRows).containsInAnyOrder(TEST_ROWS);
    s.run().waitUntilFinish();
  }

  private boolean checkTableHasRegisteredParquetFiles(List<String> parquetFiles) {
    Table destTable = catalog.loadTable(destTableId);

    int numRegisteredFiles = 0;
    for (Snapshot snapshot : destTable.snapshots()) {
      numRegisteredFiles += Iterables.size(snapshot.addedDataFiles(destTable.io()));
    }
    LOG.info(
        "Number of source files: {}, Number of registered destination files: {}",
        parquetFiles.size(),
        numRegisteredFiles);
    return numRegisteredFiles == parquetFiles.size();
  }

  private PipelineResult startAddFilesListener(String filter) throws InterruptedException {
    DirectOptions options = TestPipeline.testingPipelineOptions().as(DirectOptions.class);
    options.setBlockOnRun(false);
    Pipeline p = Pipeline.create(options);

    PCollectionRowTuple tuple =
        p.apply(PubsubIO.readStrings().fromTopic(notificationsTopic))
            .apply(JsonToRow.withSchema(NOTIFICATION_SCHEMA))
            .apply(Filter.by(row -> row.getString("name").contains(filter)))
            .apply(
                MapElements.into(strings())
                    .via(
                        row ->
                            format("gs://%s/%s", row.getString("bucket"), row.getString("name"))))
            .apply(Deduplicate.values())
            .apply(
                new AddFiles(
                    IcebergCatalogConfig.builder().setCatalogProperties(BIGLAKE_PROPS).build(),
                    namespace + "." + destTableName,
                    null,
                    PARTITION_FIELDS,
                    TABLE_PROPS,
                    10,
                    Duration.standardSeconds(10)));
    PAssert.that(tuple.get("errors")).empty();
    PipelineResult result = p.run();

    LOG.info(
        "Started running the AddFiles listener pipeline. Waiting for 10s to allow it enough time to setup");
    Thread.sleep(10_000);
    return result;
  }
}
