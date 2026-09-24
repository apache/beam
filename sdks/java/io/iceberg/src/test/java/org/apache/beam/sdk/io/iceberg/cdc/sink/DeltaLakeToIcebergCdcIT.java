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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.api.services.storage.model.StorageObject;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.delta.DeltaIO;
import org.apache.beam.sdk.io.delta.DeltaWriteTestUtils;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.Timestamp;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads a Delta Lake change data feed with {@link DeltaIO#readChanges} and applies it to an Iceberg
 * table with {@link IcebergIO#writeCdcRows}, on a Hadoop catalog.
 *
 * <p>The Delta table is built with the Kernel test utilities: version 0 is a plain append and
 * versions 1 and 2 carry hand-written change files, so the feed covers all four change types. The
 * Delta reader sets each row's native {@link ValueKind} and its {@code _commit_version} column
 * orders a key's changes, so the sink needs no change-type column.
 */
@RunWith(JUnit4.class)
public class DeltaLakeToIcebergCdcIT {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaLakeToIcebergCdcIT.class);

  private static final GcpOptions OPTIONS =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class);

  /** Everything this run writes, Delta tables and the Iceberg warehouse, lives under here. */
  private static final String ROOT =
      String.format(
          "%s/%s/%s",
          OPTIONS.getTempLocation(),
          DeltaLakeToIcebergCdcIT.class.getSimpleName(),
          UUID.randomUUID());

  private static final String WAREHOUSE = ROOT + "/warehouse";
  private static final String CATALOG_NAME = "delta_cdc_it";

  private static final StructType DELTA_SCHEMA =
      new StructType().add("id", IntegerType.INTEGER, false).add("name", StringType.STRING);

  /** The change-file schema: the table columns plus the change data feed columns. */
  private static final StructType DELTA_CDC_SCHEMA =
      new StructType()
          .add("id", IntegerType.INTEGER, false)
          .add("name", StringType.STRING)
          .add(DeltaIO.CHANGE_TYPE_COLUMN, StringType.STRING)
          .add(DeltaIO.COMMIT_VERSION_COLUMN, LongType.LONG)
          .add(DeltaIO.COMMIT_TIMESTAMP_COLUMN, TimestampType.TIMESTAMP);

  private static final Schema ROW_SCHEMA =
      Schema.builder().addInt32Field("id").addNullableStringField("name").build();

  private static final Schema CDC_ROW_SCHEMA =
      Schema.builder()
          .addFields(ROW_SCHEMA.getFields())
          .addStringField(DeltaIO.CHANGE_TYPE_COLUMN)
          .addInt64Field(DeltaIO.COMMIT_VERSION_COLUMN)
          .addLogicalTypeField(DeltaIO.COMMIT_TIMESTAMP_COLUMN, Timestamp.MICROS)
          .build();

  private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.IntegerType.get()),
              Types.NestedField.optional(2, "name", Types.StringType.get())),
          ImmutableSet.of(1));

  @Rule public final TestPipeline p = TestPipeline.create();
  @Rule public final TestName testName = new TestName();
  @Rule public final Timeout globalTimeout = Timeout.seconds(10 * 60);

  private HadoopCatalog catalog;
  private Engine engine;

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    hadoopConfig().forEach(conf::set);
    catalog = new HadoopCatalog();
    catalog.setConf(conf);
    catalog.initialize(CATALOG_NAME, ImmutableMap.of("warehouse", WAREHOUSE));
    catalog.createNamespace(namespace());
    engine = DefaultEngine.create(conf);
  }

  @After
  public void cleanUp() throws Exception {
    for (TableIdentifier identifier : catalog.listTables(namespace())) {
      catalog.dropTable(identifier);
    }
    catalog.dropNamespace(namespace());
    if (!ROOT.startsWith("gs://")) {
      return;
    }
    try {
      GcsUtil gcsUtil = OPTIONS.as(GcsOptions.class).getGcsUtil();
      GcsPath root = GcsPath.fromUri(ROOT);
      @Nullable List<StorageObject> objects =
          gcsUtil.listObjects(root.getBucket(), root.getObject(), null).getItems();
      if (objects != null) {
        gcsUtil.remove(
            objects.stream()
                .map(obj -> "gs://" + root.getBucket() + "/" + obj.getName())
                .collect(Collectors.toList()));
      }
    } catch (Exception e) {
      LOG.warn("Failed to clean up {}", ROOT, e);
    }
  }

  private Namespace namespace() {
    return Namespace.of(testName.getMethodName());
  }

  /** GCS needs the connector wired up; a local temp location needs nothing. */
  private static Map<String, String> hadoopConfig() {
    if (!ROOT.startsWith("gs://")) {
      return ImmutableMap.of();
    }
    return ImmutableMap.of(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        "fs.gs.auth.type", "APPLICATION_DEFAULT",
        "fs.gs.project.id", OPTIONS.getProject());
  }

  private static IcebergCatalogConfig catalogConfig() {
    String ioImpl =
        ROOT.startsWith("gs://")
            ? "org.apache.iceberg.gcp.gcs.GCSFileIO"
            : "org.apache.iceberg.hadoop.HadoopFileIO";
    return IcebergCatalogConfig.builder()
        .setCatalogName(CATALOG_NAME)
        .setCatalogProperties(
            ImmutableMap.<String, String>builder()
                .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
                .put("warehouse", WAREHOUSE)
                .put("io-impl", ioImpl)
                .build())
        .build();
  }

  private static Row row(int id, String name) {
    return Row.withSchema(ROW_SCHEMA).addValues(id, name).build();
  }

  /** A change-file row; the commit columns must match the commit the file is registered in. */
  private static Row change(String changeType, long version, int id, String name) {
    return Row.withSchema(CDC_ROW_SCHEMA)
        .addValues(id, name, changeType, version, Instant.ofEpochMilli(version * 1000L))
        .build();
  }

  /**
   * Three Delta commits: an append, then two change-file commits carrying updates, deletes, an
   * insert, a delete after an update and a re-insert after a delete.
   */
  private String writeDeltaTable() throws Exception {
    String path = ROOT + "/delta/" + testName.getMethodName();
    DeltaWriteTestUtils.writeAppendCommit(
        engine,
        path,
        0L,
        0L,
        DELTA_SCHEMA,
        ImmutableList.of(
            row(1, "a"), row(2, "b"), row(3, "c"), row(4, "d"), row(5, "e"), row(6, "f")));
    DeltaWriteTestUtils.writeCdcCommit(
        engine,
        path,
        1L,
        1000L,
        DELTA_SCHEMA,
        null,
        null,
        ImmutableList.of(
            change("update_preimage", 1L, 1, "a"),
            change("update_postimage", 1L, 1, "a2"),
            change("delete", 1L, 2, "b"),
            change("update_preimage", 1L, 3, "c"),
            change("update_postimage", 1L, 3, "c2"),
            change("insert", 1L, 7, "g")),
        DELTA_CDC_SCHEMA);
    DeltaWriteTestUtils.writeCdcCommit(
        engine,
        path,
        2L,
        2000L,
        DELTA_SCHEMA,
        null,
        null,
        ImmutableList.of(
            change("delete", 2L, 1, "a2"),
            change("insert", 2L, 2, "b2"),
            change("delete", 2L, 6, "f"),
            change("update_preimage", 2L, 7, "g"),
            change("update_postimage", 2L, 7, "g2")),
        DELTA_CDC_SCHEMA);
    return path;
  }

  /** Every live row as sorted {@code "id:name"} strings. */
  private static List<String> readRows(Table table) {
    table.refresh();
    return ImmutableList.copyOf(IcebergGenerics.read(table).build()).stream()
        .map(record -> record.getField("id") + ":" + record.getField("name"))
        .sorted()
        .collect(ImmutableList.toImmutableList());
  }

  @Test
  public void changeFeedAppliesToIcebergTable() throws Exception {
    applyChangeFeed(/* upsert= */ false);
  }

  /** The same feed in upsert mode, which drops the update before-images. */
  @Test
  public void changeFeedAppliesToIcebergTableWithUpsert() throws Exception {
    applyChangeFeed(/* upsert= */ true);
  }

  private void applyChangeFeed(boolean upsert) throws Exception {
    String deltaTable = writeDeltaTable();
    TableIdentifier targetId = TableIdentifier.of(namespace(), "target");
    catalog.createTable(
        targetId,
        ICEBERG_SCHEMA,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of("format-version", "2"));

    PCollection<Row> changes =
        p.apply(
            DeltaIO.readChanges()
                .from(deltaTable)
                .withStartVersion(0L)
                .withMetadataColumns(DeltaIO.COMMIT_VERSION_COLUMN)
                .withConfig(hadoopConfig()));
    changes.apply(
        IcebergIO.writeCdcRows(catalogConfig())
            .to(targetId)
            .withSequenceNumberColumn(DeltaIO.COMMIT_VERSION_COLUMN)
            .withUpsert(upsert));
    p.run().waitUntilFinish();

    assertThat(
        readRows(catalog.loadTable(targetId)),
        equalTo(ImmutableList.of("2:b2", "3:c2", "4:d", "5:e", "7:g2")));
  }
}
