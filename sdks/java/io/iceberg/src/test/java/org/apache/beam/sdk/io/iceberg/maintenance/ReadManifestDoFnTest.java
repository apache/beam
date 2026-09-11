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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReadManifestDoFnTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private IcebergCatalogConfig getCatalogConfig() {
    return IcebergCatalogConfig.builder()
        .setCatalogProperties(ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
        .build();
  }

  @Test
  public void testReadsDataFilesFromManifest() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "read_manifest_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    DataFile file =
        warehouse.writeRecords(
            "data_1_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(ExpireSnapshotsTestFixtures.createRecord(1L, "val-1")));
    AppendFiles append = table.newAppend();
    append.appendFile(file);
    append.commit();
    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertEquals(1, manifests.size());
    ManifestFile manifest = manifests.get(0);

    PCollection<KV<String, FileInfo>> output =
        pipeline
            .apply(
                Create.of(
                        KV.of(
                            tableId.toString(), ManifestFileBean.fromManifestFile(manifest, true)))
                    .withCoder(
                        KvCoder.of(
                            StringUtf8Coder.of(), SerializableCoder.of(ManifestFileBean.class))))
            .apply(ParDo.of(new ReadManifestDoFn(getCatalogConfig())));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of(
                file.path().toString(),
                FileInfo.of(file.path().toString(), FileCategory.DATA, true, tableId.toString())));

    pipeline.run();
  }

  @Test
  public void testReadsDeleteFilesFromManifest() {
    TableIdentifier tableId =
        TableIdentifier.of("default", "read_delete_manifest_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();
    table.refresh();

    DeleteFile posDelete =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath(warehouse.location + "/pos_delete_" + System.nanoTime() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(1L)
            .build();

    table.newRowDelta().addDeletes(posDelete).commit();
    table.refresh();

    List<ManifestFile> deleteManifests = table.currentSnapshot().deleteManifests(table.io());
    assertEquals(1, deleteManifests.size());
    ManifestFile deleteManifest = deleteManifests.get(0);

    PCollection<KV<String, FileInfo>> output =
        pipeline
            .apply(
                Create.of(
                        KV.of(
                            tableId.toString(),
                            ManifestFileBean.fromManifestFile(deleteManifest, true)))
                    .withCoder(
                        KvCoder.of(
                            StringUtf8Coder.of(), SerializableCoder.of(ManifestFileBean.class))))
            .apply(ParDo.of(new ReadManifestDoFn(getCatalogConfig())));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of(
                posDelete.path().toString(),
                FileInfo.of(
                    posDelete.path().toString(),
                    FileCategory.POSITION_DELETES,
                    true,
                    tableId.toString())));

    pipeline.run();
  }
}
