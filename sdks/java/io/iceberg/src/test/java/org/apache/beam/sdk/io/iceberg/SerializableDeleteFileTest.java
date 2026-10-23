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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
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
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SerializableDeleteFile}. */
@RunWith(JUnit4.class)
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

    SerializableDeleteFile serialized = SerializableDeleteFile.from(deleteFile, SPEC, true);
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

    SerializableDeleteFile serialized = SerializableDeleteFile.from(deleteFile, SPEC, true);
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

    SerializableDeleteFile serialized = SerializableDeleteFile.from(deleteFile, SPEC, true);
    DeleteFile reconstructed =
        serialized.createDeleteFile(
            singletonMap(SPEC.specId(), SPEC), singletonMap(0, SortOrder.unsorted()));

    assertEquals(FileFormat.PUFFIN, reconstructed.format());
    assertEquals(Long.valueOf(64L), reconstructed.contentOffset());
    assertEquals(Long.valueOf(128L), reconstructed.contentSizeInBytes());
    assertEquals("gs://bucket/data/category=A/data.parquet", reconstructed.referencedDataFile());
  }

  /** Reconstruction fails clearly when the spec map or the sort-order map lacks the file's id. */
  @Test
  public void testCreateDeleteFileFailsClearlyForMissingSpecOrSortOrder() {
    // facet: missing partition spec.
    DeleteFile positionDelete =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("gs://bucket/deletes/category=A/pos.parquet")
            .withFormat(FileFormat.PARQUET)
            .withPartitionPath("category=A")
            .withFileSizeInBytes(256L)
            .withRecordCount(2L)
            .build();
    SerializableDeleteFile serializedPosition =
        SerializableDeleteFile.from(positionDelete, SPEC, true);
    IllegalStateException missingSpec =
        assertThrows(
            IllegalStateException.class,
            () -> serializedPosition.createDeleteFile(emptyMap(), null));
    assertTrue(missingSpec.getMessage().contains("created with spec id '" + SPEC.specId() + "'"));

    // facet: missing sort order (equality delete).
    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).asc("id").withOrderId(7).build();
    DeleteFile equalityDelete =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(1)
            .withSortOrder(sortOrder)
            .withPath("gs://bucket/deletes/category=A/eq.parquet")
            .withFormat(FileFormat.PARQUET)
            .withPartitionPath("category=A")
            .withFileSizeInBytes(256L)
            .withRecordCount(2L)
            .build();
    SerializableDeleteFile serializedEquality =
        SerializableDeleteFile.from(equalityDelete, SPEC, true);
    IllegalStateException missingOrder =
        assertThrows(
            IllegalStateException.class,
            () ->
                serializedEquality.createDeleteFile(singletonMap(SPEC.specId(), SPEC), emptyMap()));
    assertTrue(missingOrder.getMessage().contains("sort order id '7'"));
  }

  /**
   * A {@link DeleteFile} must be serialized with the EXACT spec it was written with: a mismatched
   * spec id and a spec id absent from the map each fail loudly.
   */
  @Test
  public void fromRejectsMismatchedOrUnknownSpec() {
    // facet: single-spec overload, wrong spec.
    DeleteFile unpartitioned =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath("gs://bucket/deletes/pos.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(1L)
            .withRecordCount(1L)
            .build();
    PartitionSpec otherSpec =
        PartitionSpec.builderFor(SCHEMA).identity("category").withSpecId(1).build();
    IllegalArgumentException mismatch =
        assertThrows(
            IllegalArgumentException.class,
            () -> SerializableDeleteFile.from(unpartitioned, otherSpec));
    assertThat(mismatch.getMessage(), containsString("does not match"));

    // facet: spec-map overload, id missing from the map.
    DeleteFile deleteFile = positionDeletes(SPEC, partition(SPEC, "A"));
    IllegalStateException unknown =
        assertThrows(
            IllegalStateException.class,
            () -> SerializableDeleteFile.from(deleteFile, emptyMap(), true));
    assertThat(unknown.getMessage(), containsString("partition spec id '0'"));
  }

  /**
   * All three delete-file kinds — equality, position, and a V3 deletion vector — round-trip through
   * the schema coder with their partition tuples intact.
   */
  @Test
  public void allDeleteFileKindsRoundTripThroughSchemaCoderWithPartition() throws Exception {
    // facet: equality delete.
    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).asc("id").withOrderId(7).build();
    DeleteFile equalityDelete =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(1, 2)
            .withSortOrder(sortOrder)
            .withPath("gs://bucket/deletes/category=A/eq.parquet")
            .withFormat(FileFormat.PARQUET)
            .withPartition(partition(SPEC, "A"))
            .withFileSizeInBytes(256L)
            .withRecordCount(2L)
            .build();
    DeleteFile equalityReconstructed =
        encodeDecode(SerializableDeleteFile.from(equalityDelete, SPEC))
            .createDeleteFile(singletonMap(SPEC.specId(), SPEC), singletonMap(7, sortOrder));
    assertEquals(FileContent.EQUALITY_DELETES, equalityReconstructed.content());
    assertEquals(equalityDelete.partition(), equalityReconstructed.partition());
    assertEquals("category=A", SPEC.partitionToPath(equalityReconstructed.partition()));

    // facet: position delete.
    DeleteFile positionDelete = positionDeletes(SPEC, partition(SPEC, "A"));
    DeleteFile positionReconstructed =
        encodeDecode(SerializableDeleteFile.from(positionDelete, SPEC))
            .createDeleteFile(singletonMap(SPEC.specId(), SPEC), null);
    assertEquals(FileContent.POSITION_DELETES, positionReconstructed.content());
    assertEquals(positionDelete.partition(), positionReconstructed.partition());
    assertEquals("category=A", SPEC.partitionToPath(positionReconstructed.partition()));

    // facet: V3 deletion vector (a Puffin blob with offset/size/referenced-data-file).
    DeleteFile dv =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("gs://bucket/deletes/category=A/dv.puffin")
            .withFormat(FileFormat.PUFFIN)
            .withPartition(partition(SPEC, "A"))
            .withFileSizeInBytes(512L)
            .withRecordCount(1L)
            .withContentOffset(64L)
            .withContentSizeInBytes(128L)
            .withReferencedDataFile("gs://bucket/data/category=A/data.parquet")
            .build();
    DeleteFile dvReconstructed =
        encodeDecode(SerializableDeleteFile.from(dv, SPEC))
            .createDeleteFile(singletonMap(SPEC.specId(), SPEC), null);
    assertEquals(FileFormat.PUFFIN, dvReconstructed.format());
    assertEquals(Long.valueOf(64L), dvReconstructed.contentOffset());
    assertEquals(Long.valueOf(128L), dvReconstructed.contentSizeInBytes());
    assertEquals("gs://bucket/data/category=A/data.parquet", dvReconstructed.referencedDataFile());
    assertEquals(dv.partition(), dvReconstructed.partition());
    assertEquals("category=A", SPEC.partitionToPath(dvReconstructed.partition()));
  }

  /**
   * An identity partition on a {@code timestamptz} column: {@link
   * org.apache.iceberg.types.Conversions#fromPartitionString} has no case for TIMESTAMP at all, so
   * the old partition-path round-trip blew up at reconstruct time. The JSON representation carries
   * the raw micros and round-trips exactly.
   */
  @Test
  public void timestampPartitionRoundTripsThroughJsonButNotThroughPartitionPath() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("event_time").build();
    long micros = 1_709_618_828_000_009L;
    GenericRecord partition = GenericRecord.create(spec.partitionType());
    partition.setField("event_time", micros);
    DeleteFile deleteFile = positionDeletes(spec, partition);

    SerializableDeleteFile serialized = SerializableDeleteFile.from(deleteFile, spec);
    DeleteFile reconstructed = serialized.createDeleteFile(singletonMap(spec.specId(), spec), null);

    assertEquals(Long.valueOf(micros), reconstructed.partition().get(0, Long.class));
    assertEquals(deleteFile.partition(), reconstructed.partition());

    // The pre-change wire shape (partition path only) cannot reconstruct this partition at all.
    SerializableDeleteFile legacy = withoutJsonPartition(serialized);
    assertThrows(
        UnsupportedOperationException.class,
        () -> legacy.createDeleteFile(singletonMap(spec.specId(), spec), null));
  }

  /**
   * {@code PartitionSpec.partitionToPath} URL-encodes each value but {@code DataFiles.fillFromPath}
   * never decodes it, so a string partition containing {@code / }, {@code &} or {@code =} used to
   * come back SILENTLY WRONG — the delete would be registered under a {@code (specId, partition)}
   * that no data file lives in, and would simply never apply. The JSON representation is exact.
   */
  @Test
  public void stringPartitionWithSpecialCharactersRoundTripsExactly() {
    String value = "a/b c&d=e";
    DeleteFile deleteFile = positionDeletes(SPEC, partition(SPEC, value));

    SerializableDeleteFile serialized = SerializableDeleteFile.from(deleteFile, SPEC);
    DeleteFile reconstructed = serialized.createDeleteFile(singletonMap(SPEC.specId(), SPEC), null);

    assertEquals(value, reconstructed.partition().get(0, CharSequence.class).toString());
    assertEquals(deleteFile.partition(), reconstructed.partition());

    // Prove the old representation was silently lossy rather than merely throwing.
    DeleteFile viaLegacyPath =
        withoutJsonPartition(serialized).createDeleteFile(singletonMap(SPEC.specId(), SPEC), null);
    assertThat(
        viaLegacyPath.partition().get(0, CharSequence.class).toString(), not(equalTo(value)));
  }

  /**
   * A null partition value is rendered as the literal text {@code null} in a partition path, which
   * {@code fromPartitionString} hands back as the four-character string "null" for a string column
   * — again silently wrong. JSON omits the field and it decodes back to a real null.
   */
  @Test
  public void nullPartitionValueRoundTripsAsNull() {
    DeleteFile deleteFile = positionDeletes(SPEC, partition(SPEC, null));

    SerializableDeleteFile serialized = SerializableDeleteFile.from(deleteFile, SPEC);
    DeleteFile reconstructed = serialized.createDeleteFile(singletonMap(SPEC.specId(), SPEC), null);

    assertThat(reconstructed.partition().get(0, CharSequence.class), nullValue());
    assertEquals(deleteFile.partition(), reconstructed.partition());

    // The old path turns the null into the literal string "null".
    DeleteFile viaLegacyPath =
        withoutJsonPartition(serialized).createDeleteFile(singletonMap(SPEC.specId(), SPEC), null);
    assertEquals("null", viaLegacyPath.partition().get(0, CharSequence.class).toString());
  }

  /**
   * NaN / Infinity floating-point partition values don't round-trip through the JSON partition
   * representation ({@code SingleValueParser.fromJson} rejects the non-standard {@code NaN} token).
   * Reconstruct must fall back to the partition-path string, which handles them, rather than
   * crash-looping the sink at commit time.
   */
  @Test
  public void nanFloatPartitionReconstructsViaPathFallback() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "f", Types.FloatType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("f").build();
    GenericRecord partition = GenericRecord.create(spec.partitionType());
    partition.setField("f", Float.NaN);
    DeleteFile deleteFile = positionDeletes(spec, partition);

    SerializableDeleteFile serialized = SerializableDeleteFile.from(deleteFile, spec);
    // Must reconstruct without throwing (JSON decode of NaN fails -> partition-path fallback).
    DeleteFile reconstructed = serialized.createDeleteFile(singletonMap(spec.specId(), spec), null);

    Object value = reconstructed.partition().get(0, Object.class);
    assertThat(value, instanceOf(Float.class));
    assertTrue("partition value must round-trip as NaN", Float.isNaN((Float) value));
  }

  private static GenericRecord partition(PartitionSpec spec, @Nullable Object value) {
    GenericRecord record = GenericRecord.create(spec.partitionType());
    record.set(0, value);
    return record;
  }

  private static DeleteFile positionDeletes(PartitionSpec spec, StructLike partition) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withPath("gs://bucket/deletes/pos.parquet")
        .withFormat(FileFormat.PARQUET)
        .withPartition(partition)
        .withFileSizeInBytes(256L)
        .withRecordCount(2L)
        .build();
  }

  /** Rebuilds the element as a pre-jsonPartition release would have encoded it. */
  private static SerializableDeleteFile withoutJsonPartition(SerializableDeleteFile file) {
    return SerializableDeleteFile.builder()
        .setContentType(file.getContentType())
        .setLocation(file.getLocation())
        .setFileFormat(file.getFileFormat())
        .setRecordCount(file.getRecordCount())
        .setFileSizeInBytes(file.getFileSizeInBytes())
        .setPartitionPath(file.getPartitionPath())
        .setPartitionSpecId(file.getPartitionSpecId())
        .build();
  }

  private static SerializableDeleteFile encodeDecode(SerializableDeleteFile file) throws Exception {
    SchemaCoder<SerializableDeleteFile> coder =
        SchemaRegistry.createDefault().getSchemaCoder(SerializableDeleteFile.class);
    return CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, file));
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
