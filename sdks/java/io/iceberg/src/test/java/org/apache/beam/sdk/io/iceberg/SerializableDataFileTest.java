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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.Test;

/**
 * Test for {@link SerializableDataFile}. More tests can be found in {@link
 * org.apache.beam.sdk.io.iceberg.RecordWriterManagerTest}.
 */
public class SerializableDataFileTest {
  static final Set<String> FIELDS_SET =
      ImmutableSet.<String>builder()
          .add("path")
          .add("fileFormat")
          .add("recordCount")
          .add("fileSizeInBytes")
          .add("partitionPath")
          .add("jsonPartition")
          .add("partitionSpecId")
          .add("keyMetadata")
          .add("splitOffsets")
          .add("columnSizes")
          .add("valueCounts")
          .add("nullValueCounts")
          .add("nanValueCounts")
          .add("lowerBounds")
          .add("upperBounds")
          .add("dataSequenceNumber")
          .add("fileSequenceNumber")
          .add("firstRowId")
          .build();

  @Test
  public void testFieldsInEqualsMethodInSyncWithGetterFields() {
    List<String> getMethodNames =
        Arrays.stream(SerializableDataFile.class.getDeclaredMethods())
            .map(Method::getName)
            .filter(methodName -> methodName.startsWith("get"))
            .collect(Collectors.toList());

    List<String> lowerCaseFields =
        FIELDS_SET.stream().map(String::toLowerCase).collect(Collectors.toList());
    List<String> extras = new ArrayList<>();
    for (String field : getMethodNames) {
      if (!lowerCaseFields.contains(field.substring(3).toLowerCase())) {
        extras.add(field);
      }
    }
    if (!extras.isEmpty()) {
      throw new IllegalStateException(
          "Detected new field(s) added to SerializableDataFile: "
              + extras
              + "\nPlease include the new field(s) in SerializableDataFile's equals() and hashCode() methods, then add them "
              + "to this test class's FIELDS_SET.");
    }
  }

  /**
   * B13/A6: every field is pinned with {@code @SchemaFieldNumber} because Dataflow's in-place
   * {@code --update} rejects a reordered schema or a changed field nullability. Lock the field
   * count, order/names, and the two nullability-sensitive fields so any future edit trips review.
   */
  @Test
  public void schemaFieldNumbersArePinned() throws Exception {
    org.apache.beam.sdk.schemas.Schema schema =
        SchemaRegistry.createDefault().getSchema(SerializableDataFile.class);
    assertEquals(18, schema.getFieldCount());
    assertEquals("path", schema.getField(0).getName());
    assertEquals("fileFormat", schema.getField(1).getName());
    assertEquals("recordCount", schema.getField(2).getName());
    assertEquals("fileSizeInBytes", schema.getField(3).getName());
    assertEquals("partitionPath", schema.getField(4).getName());
    assertEquals("partitionSpecId", schema.getField(5).getName());
    assertEquals("keyMetadata", schema.getField(6).getName());
    assertEquals("splitOffsets", schema.getField(7).getName());
    assertEquals("columnSizes", schema.getField(8).getName());
    assertEquals("valueCounts", schema.getField(9).getName());
    assertEquals("nullValueCounts", schema.getField(10).getName());
    assertEquals("nanValueCounts", schema.getField(11).getName());
    assertEquals("lowerBounds", schema.getField(12).getName());
    assertEquals("upperBounds", schema.getField(13).getName());
    assertEquals("dataSequenceNumber", schema.getField(14).getName());
    assertEquals("fileSequenceNumber", schema.getField(15).getName());
    assertEquals("firstRowId", schema.getField(16).getName());
    assertEquals("jsonPartition", schema.getField(17).getName());
  }

  /**
   * Bounds with {@code capacity > limit} must be copied by {@code [position, limit)}, not by {@link
   * ByteBuffer#array()}. Otherwise trailing 0x00 bytes leak into the manifest bounds and break
   * equality predicate pushdown in some query engines.
   */
  @Test
  public void testBoundByteBufferIsCopiedByLimitNotBackingArrayLength() {
    // Encode bounds the same way iceberg-parquet does in the wild — via
    // Conversions.toByteBuffer(STRING, value). For UTF-8 strings of 10+
    // characters the underlying JDK CharsetEncoder over-allocates by ~10%
    // and flips, producing a ByteBuffer with capacity > limit.
    int columnId = 3;
    String lowerValue = "lower_bound_str";
    String upperValue = "upper_bound_str";
    byte[] expectedLower = lowerValue.getBytes(StandardCharsets.UTF_8);
    byte[] expectedUpper = upperValue.getBytes(StandardCharsets.UTF_8);

    ByteBuffer lower = Conversions.toByteBuffer(Types.StringType.get(), lowerValue);
    ByteBuffer upper = Conversions.toByteBuffer(Types.StringType.get(), upperValue);

    Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
    lowerBounds.put(columnId, lower);
    Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
    upperBounds.put(columnId, upper);

    Metrics metrics = new Metrics(1L, null, null, null, null, lowerBounds, upperBounds);

    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withFormat(FileFormat.PARQUET)
            .withPath("gs://test-bucket/data/test-file.parquet")
            .withFileSizeInBytes(1024L)
            .withMetrics(metrics)
            .build();

    SerializableDataFile serialized =
        SerializableDataFile.from(dataFile, PartitionSpec.unpartitioned());

    byte[] serializedLower = serialized.getLowerBounds().get(columnId);
    byte[] serializedUpper = serialized.getUpperBounds().get(columnId);
    assertEquals(
        "lower bound length must match content, not backing array",
        expectedLower.length,
        serializedLower.length);
    assertEquals(
        "upper bound length must match content, not backing array",
        expectedUpper.length,
        serializedUpper.length);
    assertArrayEquals(expectedLower, serializedLower);
    assertArrayEquals(expectedUpper, serializedUpper);
  }

  /**
   * F8: {@code from(DataFile, spec)} must populate BOTH the JSON partition (primary) and the
   * partition path (fallback), so the deprecated {@code partitionPath} schema field stays non-null
   * across releases (Dataflow's in-place pipeline update rejects a changed field nullability).
   */
  @Test
  public void fromPopulatesBothPartitionRepresentations() {
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withFormat(FileFormat.PARQUET)
            .withPath("gs://test-bucket/data/f.parquet")
            .withFileSizeInBytes(1L)
            .withRecordCount(1L)
            .build();

    SerializableDataFile sdf = SerializableDataFile.from(dataFile, PartitionSpec.unpartitioned());

    assertEquals(
        "partition path must be populated (unpartitioned -> empty string), not null",
        "",
        sdf.getPartitionPath());
    assertNotNull("json partition must also be populated", sdf.getJsonPartition());
  }

  /**
   * F6: a {@link DataFile} must be serialized with the EXACT spec it was written with — a
   * mismatched spec id (usually a spec evolution on a shared/refreshed table between writing and
   * serializing) must fail loudly rather than silently encode the partition under the wrong field
   * ids.
   */
  @Test
  public void fromRejectsSpecIdMismatch() {
    DataFile unpartitioned =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withFormat(FileFormat.PARQUET)
            .withPath("gs://test-bucket/data/f.parquet")
            .withFileSizeInBytes(1L)
            .withRecordCount(1L)
            .build();
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    PartitionSpec otherSpec = PartitionSpec.builderFor(schema).identity("id").withSpecId(1).build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SerializableDataFile.from(unpartitioned, otherSpec));
    assertTrue(
        "message should explain the spec-id mismatch: " + ex.getMessage(),
        ex.getMessage().contains("does not match"));
  }

  /**
   * B13/F8: elements encoded by a PRE-jsonPartition pipeline carry only {@code partitionPath}
   * (field 4), with {@code jsonPartition} (field 14) null. {@code createDataFile} must reconstruct
   * the partition via {@code withPartitionPath} rather than crash on the missing JSON.
   */
  @Test
  public void legacyElementWithoutJsonPartitionReconstructsViaPartitionPath() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "shard", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("shard").build();

    SerializableDataFile legacy =
        SerializableDataFile.builder()
            .setPath("gs://test-bucket/data/legacy.parquet")
            .setFileFormat("PARQUET")
            .setRecordCount(1L)
            .setFileSizeInBytes(1L)
            .setPartitionPath("shard=5")
            .setPartitionSpecId(spec.specId())
            .build(); // no setJsonPartition -> jsonPartition is null (pre-upgrade encoding)

    DataFile reconstructed = legacy.createDataFile(ImmutableMap.of(spec.specId(), spec));
    assertEquals("shard=5", spec.partitionToPath(reconstructed.partition()));
  }

  /**
   * F7: NaN / Infinity floating-point partition values don't round-trip through the JSON partition
   * representation ({@code SingleValueParser.fromJson} rejects the quoted {@code "NaN"}).
   * Reconstruct must fall back to the partition-path string, which handles them, rather than
   * crash-looping the sink at commit time.
   */
  @Test
  public void nanFloatPartitionReconstructsViaPathFallback() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "f", Types.FloatType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("f").build();
    GenericRecord partition = GenericRecord.create(spec.partitionType());
    partition.setField("f", Float.NaN);
    DataFile dataFile =
        DataFiles.builder(spec)
            .withFormat(FileFormat.PARQUET)
            .withPath("gs://test-bucket/data/nan.parquet")
            .withFileSizeInBytes(1L)
            .withRecordCount(1L)
            .withPartition(partition)
            .build();

    SerializableDataFile sdf = SerializableDataFile.from(dataFile, spec);
    // Must reconstruct without throwing (JSON decode of NaN fails -> partition-path fallback).
    DataFile reconstructed = sdf.createDataFile(ImmutableMap.of(spec.specId(), spec));

    Object value = reconstructed.partition().get(0, Object.class);
    assertTrue(
        "partition value must round-trip as NaN",
        value instanceof Float && Float.isNaN((Float) value));
  }
}
