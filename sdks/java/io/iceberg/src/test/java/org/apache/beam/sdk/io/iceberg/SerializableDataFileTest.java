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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
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
          .add("partitionSpecId")
          .add("keyMetadata")
          .add("splitOffsets")
          .add("columnSizes")
          .add("valueCounts")
          .add("nullValueCounts")
          .add("nanValueCounts")
          .add("lowerBounds")
          .add("upperBounds")
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

    SerializableDataFile serialized = SerializableDataFile.from(dataFile, "");

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
}
