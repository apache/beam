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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ManifestFileAdapterTest {

  private static ManifestDescriptor.Builder descriptorBuilder() {
    return ManifestDescriptor.builder()
        .setTableIdentifier("default.adapter")
        .setPath("/tmp/manifest.avro")
        .setLength(1024L)
        .setPartitionSpecId(3)
        .setContent(ManifestContent.DELETES)
        .setSnapshotId(11L)
        .setSequenceNumber(5L)
        .setReachable(true);
  }

  @Test
  public void testReturnsDescriptorValuesForReadPath() {
    ManifestFile adapter =
        new ManifestFileAdapter(
            descriptorBuilder()
                .setFirstRowId(42L)
                .setKeyMetadata(ByteBuffer.wrap(new byte[] {1, 2, 3}))
                .build());

    assertEquals("/tmp/manifest.avro", adapter.path());
    assertEquals(1024L, adapter.length());
    assertEquals(3, adapter.partitionSpecId());
    assertEquals(ManifestContent.DELETES, adapter.content());
    assertEquals(Long.valueOf(11L), adapter.snapshotId());
    assertEquals(5L, adapter.sequenceNumber());
    assertEquals(Long.valueOf(42L), adapter.firstRowId());
    assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 3}), adapter.keyMetadata());
  }

  @Test
  public void testNullableValuesAreNull() {
    ManifestFile adapter = new ManifestFileAdapter(descriptorBuilder().build());
    assertNull(adapter.firstRowId());
    assertNull(adapter.keyMetadata());
  }

  @Test
  public void testKeyMetadataReadsDoNotAffectDescriptor() {
    ManifestDescriptor descriptor =
        descriptorBuilder().setKeyMetadata(ByteBuffer.wrap(new byte[] {1, 2, 3})).build();
    ManifestDescriptor copy = descriptor.toBuilder().build();
    ManifestFile adapter = new ManifestFileAdapter(descriptor);

    ByteBuffer keyMetadata = adapter.keyMetadata();
    assertNotNull(keyMetadata);
    keyMetadata.get(new byte[3]); // Consume the buffer, as a reader would.

    ByteBuffer descriptorKeyMetadata = descriptor.getKeyMetadata();
    assertNotNull(descriptorKeyMetadata);
    assertEquals(0, descriptorKeyMetadata.position());
    assertEquals(copy, descriptor);
    assertEquals(copy.hashCode(), descriptor.hashCode());
    assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 3}), adapter.keyMetadata());
  }

  @Test
  public void testUnsupportedMethodsThrow() {
    ManifestFile adapter = new ManifestFileAdapter(descriptorBuilder().build());
    assertUnsupported(adapter::minSequenceNumber);
    assertUnsupported(adapter::addedFilesCount);
    assertUnsupported(adapter::addedRowsCount);
    assertUnsupported(adapter::existingFilesCount);
    assertUnsupported(adapter::existingRowsCount);
    assertUnsupported(adapter::deletedFilesCount);
    assertUnsupported(adapter::deletedRowsCount);
    assertUnsupported(adapter::partitions);
  }

  private static void assertUnsupported(ThrowingRunnable call) {
    assertThrows(UnsupportedOperationException.class, call);
  }

  @Test
  public void testCopyReturnsSelf() {
    ManifestFile adapter = new ManifestFileAdapter(descriptorBuilder().build());
    assertSame(adapter, adapter.copy());
  }

  @Test
  public void testToStringIdentifiesManifest() {
    String string = new ManifestFileAdapter(descriptorBuilder().build()).toString();
    assertTrue(string.contains("/tmp/manifest.avro"));
    assertTrue(string.contains("default.adapter"));
  }
}
