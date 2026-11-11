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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ManifestDescriptorTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  private static final String TABLE_ID = "default.manifest_descriptor";

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(TestFixtures.SCHEMA).withSpecId(3).identity("data").build();

  private static SchemaCoder<ManifestDescriptor> coder() throws Exception {
    return SchemaRegistry.createDefault().getSchemaCoder(ManifestDescriptor.class);
  }

  private Table createTable(String name) {
    return warehouse.createTable(
        TableIdentifier.of("default", name),
        TestFixtures.SCHEMA,
        null,
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));
  }

  private static void assertDescribes(
      ManifestFile manifest, ManifestDescriptor descriptor, boolean reachable) {
    assertEquals(TABLE_ID, descriptor.getTableIdentifier());
    assertEquals(manifest.path(), descriptor.getPath());
    assertEquals(manifest.length(), descriptor.getLength());
    assertEquals(manifest.partitionSpecId(), descriptor.getPartitionSpecId());
    assertEquals(manifest.content(), descriptor.getContent());
    assertEquals(manifest.snapshotId(), (Long) descriptor.getSnapshotId());
    assertEquals(manifest.sequenceNumber(), descriptor.getSequenceNumber());
    assertEquals(manifest.firstRowId(), descriptor.getFirstRowId());
    assertEquals(manifest.keyMetadata(), descriptor.getKeyMetadata());
    assertEquals(reachable, descriptor.getReachable());
    assertNull(descriptor.partitionSpec());
  }

  private static ManifestFile mockManifest(@Nullable Long snapshotId) {
    ManifestFile manifest = mock(ManifestFile.class);
    when(manifest.path()).thenReturn("/tmp/manifest.avro");
    when(manifest.length()).thenReturn(1024L);
    when(manifest.partitionSpecId()).thenReturn(SPEC.specId());
    when(manifest.content()).thenReturn(ManifestContent.DATA);
    when(manifest.snapshotId()).thenReturn(snapshotId);
    when(manifest.sequenceNumber()).thenReturn(5L);
    return manifest;
  }

  @Test
  public void testFromDataManifest() throws Exception {
    Table table = createTable("data_manifest");
    DataFile file =
        warehouse.writeData("data.parquet", table.schema(), TestFixtures.FILE1SNAPSHOT1_DATA);
    table.newAppend().appendFile(file).commit();

    List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
    assertEquals(1, manifests.size());
    ManifestFile manifest = manifests.get(0);

    ManifestDescriptor descriptor = ManifestDescriptor.from(TABLE_ID, manifest, true);
    assertDescribes(manifest, descriptor, true);
    assertEquals(ManifestContent.DATA, descriptor.getContent());
    assertEquals(descriptor, CoderUtils.clone(coder(), descriptor));
  }

  @Test
  public void testFromDeleteManifest() throws Exception {
    Table table = createTable("delete_manifest");
    DataFile file =
        warehouse.writeData("data.parquet", table.schema(), TestFixtures.FILE1SNAPSHOT1_DATA);
    table.newAppend().appendFile(file).commit();
    DeleteFile deletes =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath(warehouse.location + "/pos-deletes.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(10L)
            .withRecordCount(1L)
            .build();
    table.newRowDelta().addDeletes(deletes).commit();

    List<ManifestFile> manifests = table.currentSnapshot().deleteManifests(table.io());
    assertEquals(1, manifests.size());
    ManifestFile manifest = manifests.get(0);

    ManifestDescriptor descriptor = ManifestDescriptor.from(TABLE_ID, manifest, false);
    assertDescribes(manifest, descriptor, false);
    assertEquals(ManifestContent.DELETES, descriptor.getContent());
    assertEquals(descriptor, CoderUtils.clone(coder(), descriptor));
  }

  @Test
  public void testRejectsManifestWithoutSnapshotId() {
    ManifestFile manifest = mockManifest(null);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> ManifestDescriptor.from(TABLE_ID, manifest, true));
    assertTrue(e.getMessage().contains("/tmp/manifest.avro"));
  }

  @Test
  public void testCopiesFirstRowId() throws Exception {
    ManifestFile manifest = mockManifest(1L);
    when(manifest.firstRowId()).thenReturn(42L);

    ManifestDescriptor descriptor = ManifestDescriptor.from(TABLE_ID, manifest, true);
    assertEquals(Long.valueOf(42L), descriptor.getFirstRowId());
    assertEquals(descriptor, CoderUtils.clone(coder(), descriptor));
  }

  @Test
  public void testCopiesKeyMetadataFromBufferView() throws Exception {
    // A read-only view into the middle of a larger array, which Beam cannot encode directly.
    byte[] backing = new byte[] {9, 1, 2, 3, 9};
    ByteBuffer view = ByteBuffer.wrap(backing, 1, 3).slice().asReadOnlyBuffer();
    ManifestFile manifest = mockManifest(1L);
    when(manifest.keyMetadata()).thenReturn(view);

    ManifestDescriptor descriptor = ManifestDescriptor.from(TABLE_ID, manifest, true);
    backing[2] = 7; // Changes to the source must not affect the descriptor.

    ByteBuffer keyMetadata = descriptor.getKeyMetadata();
    assertNotNull(keyMetadata);
    assertArrayEquals(new byte[] {1, 2, 3}, encodableBytes(keyMetadata));

    ManifestDescriptor decoded = CoderUtils.clone(coder(), descriptor);
    ByteBuffer decodedKeyMetadata = decoded.getKeyMetadata();
    assertNotNull(decodedKeyMetadata);
    assertArrayEquals(new byte[] {1, 2, 3}, encodableBytes(decodedKeyMetadata));
    assertEquals(descriptor, decoded);
  }

  /**
   * Returns the backing array of {@code buffer}, asserting that the array holds exactly the
   * buffer's contents. Beam encodes {@link ByteBuffer} schema fields from {@link
   * ByteBuffer#array()}, so this is the property that makes the field encodable.
   */
  private static byte[] encodableBytes(ByteBuffer buffer) {
    assertTrue(buffer.hasArray());
    assertEquals(0, buffer.arrayOffset());
    assertEquals(0, buffer.position());
    byte[] array = buffer.array();
    assertEquals(array.length, buffer.remaining());
    return array;
  }

  @Test
  public void testCarriedPartitionSpecSurvivesEncoding() throws Exception {
    ManifestDescriptor descriptor =
        ManifestDescriptor.from(TABLE_ID, mockManifest(1L), true).withPartitionSpec(SPEC);

    ManifestDescriptor decoded = CoderUtils.clone(coder(), descriptor);
    PartitionSpec carried = decoded.partitionSpec();
    assertNotNull(carried);
    assertEquals(SPEC.specId(), carried.specId());
    assertEquals(SPEC.fields(), carried.fields());
    assertEquals(SPEC.partitionType(), carried.partitionType());
    assertTrue(SPEC.compatibleWith(carried));
    // Only the spec's source field is carried.
    assertEquals(TestFixtures.SCHEMA.select("data").asStruct(), carried.schema().asStruct());
  }

  @Test
  public void testCarriesHistoricalSpecAfterSourceColumnIsDropped() throws Exception {
    org.apache.iceberg.Schema wideSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "category", Types.StringType.get()),
            Types.NestedField.optional(4, "extra", Types.StringType.get()));
    Table table =
        warehouse.createTable(
            TableIdentifier.of("default", "evolved_spec"),
            wideSchema,
            PartitionSpec.builderFor(wideSchema).identity("category").bucket("id", 4).build(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));
    String dataPath = warehouse.location + "/category=a/id_bucket=1/data.parquet";
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath(dataPath)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(10L)
                .withRecordCount(1L)
                .withPartitionPath("category=a/id_bucket=1")
                .build())
        .commit();
    ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
    int specId = manifest.partitionSpecId();

    // Evolve the spec away from "category", then drop the column.
    table.updateSpec().removeField("category").commit();
    table.updateSchema().deleteColumn("category").commit();
    table.refresh();
    PartitionSpec historical = table.specs().get(specId);
    assertNotNull(historical);

    ManifestDescriptor decoded =
        CoderUtils.clone(
            coder(),
            ManifestDescriptor.from(TABLE_ID, manifest, false).withPartitionSpec(historical));
    PartitionSpec carried = decoded.partitionSpec();
    assertNotNull(carried);
    assertEquals(historical.specId(), carried.specId());
    assertEquals(historical.fields(), carried.fields());
    assertEquals(historical.partitionType(), carried.partitionType());
    // Wide schema fields that the spec does not use are not carried.
    assertNull(carried.schema().findField("data"));
    assertNull(carried.schema().findField("extra"));

    // The manifest can be read using only the carried spec.
    List<String> paths = new ArrayList<>();
    try (CloseableIterable<DataFile> files =
        ManifestFiles.read(manifest, table.io(), ImmutableMap.of(specId, carried))) {
      for (DataFile file : files) {
        paths.add(file.location());
      }
    }
    assertEquals(ImmutableList.of(dataPath), paths);
  }

  @Test
  public void testWithPartitionSpecRejectsMismatchedSpecId() {
    ManifestDescriptor descriptor = ManifestDescriptor.from(TABLE_ID, mockManifest(1L), true);
    PartitionSpec other =
        PartitionSpec.builderFor(TestFixtures.SCHEMA).withSpecId(4).identity("data").build();
    assertThrows(IllegalArgumentException.class, () -> descriptor.withPartitionSpec(other));
  }

  @Test
  public void testContentIsEncodedAsEnumeration() throws Exception {
    Schema schema = SchemaRegistry.createDefault().getSchema(ManifestDescriptor.class);
    assertTrue(schema.getField("content").getType().getLogicalType() instanceof EnumerationType);
  }
}
