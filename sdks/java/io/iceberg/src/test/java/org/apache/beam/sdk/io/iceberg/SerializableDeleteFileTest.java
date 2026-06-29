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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.Test;

/** Tests for {@link SerializableDeleteFile}. */
public class SerializableDeleteFileTest {
  private static final org.apache.iceberg.Schema SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "category", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("category").build();

  @Test
  public void testPositionDeleteRoundTripPreservesMetadataUsedByCdcReads() throws Exception {
    Map<Integer, Long> columnSizes = new HashMap<>();
    columnSizes.put(1, 11L);
    Map<Integer, Long> valueCounts = new HashMap<>();
    valueCounts.put(1, 3L);
    Map<Integer, Long> nullValueCounts = new HashMap<>();
    nullValueCounts.put(1, 0L);
    Map<Integer, Long> nanValueCounts = new HashMap<>();
    nanValueCounts.put(1, 0L);
    Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
    lowerBounds.put(1, ByteBuffer.wrap(new byte[] {0x01}));
    Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
    upperBounds.put(1, ByteBuffer.wrap(new byte[] {0x05}));
    Metrics metrics =
        new Metrics(
            3L,
            columnSizes,
            valueCounts,
            nullValueCounts,
            nanValueCounts,
            lowerBounds,
            upperBounds);
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("gs://bucket/deletes/category=A/pos.parquet")
            .withFormat(FileFormat.PARQUET)
            .withPartitionPath("category=A")
            .withFileSizeInBytes(256L)
            .withMetrics(metrics)
            .withSplitOffsets(Arrays.asList(4L, 128L))
            .withEncryptionKeyMetadata(ByteBuffer.wrap(new byte[] {0x0A, 0x0B}))
            .build();
    setSequenceNumbers(deleteFile, 44L, 45L);

    SerializableDeleteFile serialized = SerializableDeleteFile.from(deleteFile, "category=A", true);
    DeleteFile reconstructed =
        serialized.createDeleteFile(
            singletonMap(SPEC.specId(), SPEC), singletonMap(0, SortOrder.unsorted()));

    assertEquals(deleteFile.content(), reconstructed.content());
    assertEquals(deleteFile.location(), reconstructed.location());
    assertEquals(deleteFile.format(), reconstructed.format());
    assertEquals(deleteFile.recordCount(), reconstructed.recordCount());
    assertEquals(deleteFile.fileSizeInBytes(), reconstructed.fileSizeInBytes());
    assertEquals(deleteFile.partition(), reconstructed.partition());
    assertEquals(deleteFile.specId(), reconstructed.specId());
    assertEquals(deleteFile.keyMetadata(), reconstructed.keyMetadata());
    assertEquals(deleteFile.splitOffsets(), reconstructed.splitOffsets());
    assertEquals(deleteFile.columnSizes(), reconstructed.columnSizes());
    assertEquals(deleteFile.valueCounts(), reconstructed.valueCounts());
    assertEquals(deleteFile.nullValueCounts(), reconstructed.nullValueCounts());
    assertEquals(deleteFile.nanValueCounts(), reconstructed.nanValueCounts());
    assertEquals(deleteFile.lowerBounds(), reconstructed.lowerBounds());
    assertEquals(deleteFile.upperBounds(), reconstructed.upperBounds());
    assertEquals(Long.valueOf(44L), serialized.getDataSequenceNumber());
    assertEquals(Long.valueOf(45L), serialized.getFileSequenceNumber());
    assertNull(reconstructed.dataSequenceNumber());
    assertNull(reconstructed.fileSequenceNumber());
  }

  @Test
  public void testEqualityDeleteRoundTripPreservesFieldIdsAndSortOrder() {
    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).asc("id").withOrderId(7).build();
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(1, 2)
            .withSortOrder(sortOrder)
            .withPath("gs://bucket/deletes/category=A/eq.parquet")
            .withFormat(FileFormat.PARQUET)
            .withPartitionPath("category=A")
            .withFileSizeInBytes(256L)
            .withRecordCount(2L)
            .build();

    SerializableDeleteFile serialized = SerializableDeleteFile.from(deleteFile, "category=A", true);
    DeleteFile reconstructed =
        serialized.createDeleteFile(singletonMap(SPEC.specId(), SPEC), singletonMap(7, sortOrder));

    assertEquals(FileContent.EQUALITY_DELETES, reconstructed.content());
    assertEquals(Arrays.asList(1, 2), reconstructed.equalityFieldIds());
    assertEquals(Integer.valueOf(7), reconstructed.sortOrderId());
  }

  @Test
  public void testPuffinDeleteRoundTripPreservesDeletionVectorMetadata() {
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("gs://bucket/deletes/category=A/dv.puffin")
            .withFormat(FileFormat.PUFFIN)
            .withPartitionPath("category=A")
            .withFileSizeInBytes(512L)
            .withRecordCount(1L)
            .withContentOffset(64L)
            .withContentSizeInBytes(128L)
            .withReferencedDataFile("gs://bucket/data/category=A/data.parquet")
            .build();

    SerializableDeleteFile serialized = SerializableDeleteFile.from(deleteFile, "category=A", true);
    DeleteFile reconstructed =
        serialized.createDeleteFile(
            singletonMap(SPEC.specId(), SPEC), singletonMap(0, SortOrder.unsorted()));

    assertEquals(FileFormat.PUFFIN, reconstructed.format());
    assertEquals(Long.valueOf(64L), reconstructed.contentOffset());
    assertEquals(Long.valueOf(128L), reconstructed.contentSizeInBytes());
    assertEquals("gs://bucket/data/category=A/data.parquet", reconstructed.referencedDataFile());
  }

  @Test
  public void testCreateDeleteFileFailsClearlyForMissingPartitionSpec() {
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("gs://bucket/deletes/category=A/pos.parquet")
            .withFormat(FileFormat.PARQUET)
            .withPartitionPath("category=A")
            .withFileSizeInBytes(256L)
            .withRecordCount(2L)
            .build();
    SerializableDeleteFile serialized = SerializableDeleteFile.from(deleteFile, "category=A", true);

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> serialized.createDeleteFile(emptyMap(), null));

    assertTrue(thrown.getMessage().contains("created with spec id '" + SPEC.specId() + "'"));
  }

  @Test
  public void testCreateEqualityDeleteFileFailsClearlyForMissingSortOrder() {
    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).asc("id").withOrderId(7).build();
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(1)
            .withSortOrder(sortOrder)
            .withPath("gs://bucket/deletes/category=A/eq.parquet")
            .withFormat(FileFormat.PARQUET)
            .withPartitionPath("category=A")
            .withFileSizeInBytes(256L)
            .withRecordCount(2L)
            .build();
    SerializableDeleteFile serialized = SerializableDeleteFile.from(deleteFile, "category=A", true);

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> serialized.createDeleteFile(singletonMap(SPEC.specId(), SPEC), emptyMap()));

    assertTrue(thrown.getMessage().contains("sort order id '7'"));
  }

  private static void setSequenceNumbers(
      DeleteFile deleteFile, long dataSequenceNumber, long fileSequenceNumber) throws Exception {
    invoke(deleteFile, "setDataSequenceNumber", dataSequenceNumber);
    invoke(deleteFile, "setFileSequenceNumber", fileSequenceNumber);
  }

  private static void invoke(DeleteFile deleteFile, String methodName, Long value)
      throws Exception {
    Method method = deleteFile.getClass().getMethod(methodName, Long.class);
    method.setAccessible(true);
    try {
      method.invoke(deleteFile, value);
    } catch (InvocationTargetException e) {
      throw (Exception) e.getCause();
    }
  }
}
