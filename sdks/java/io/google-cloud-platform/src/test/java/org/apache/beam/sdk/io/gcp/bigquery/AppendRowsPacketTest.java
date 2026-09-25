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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link AppendRowsPacket}, focused on partitioning matched vs. mismatched rows. */
@RunWith(JUnit4.class)
public class AppendRowsPacketTest {

  private static boolean hasHash(int i) {
    return i % 2 == 0;
  }

  private static boolean hasUnknown(int i) {
    return i % 3 == 0;
  }

  // Some rows have no failsafe row, matching the @Nullable element type in production.
  private static boolean hasFailsafe(int i) {
    return i % 4 != 3;
  }

  private static ByteString serializedFor(int i) {
    return ByteString.copyFromUtf8("row" + i);
  }

  private static byte[] hashFor(int i) {
    return ("hash" + i).getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] originalFor(int i) {
    return ("original" + i).getBytes(StandardCharsets.UTF_8);
  }

  private static TableRow unknownFor(int i) {
    return new TableRow().set("unknown", "u" + i);
  }

  private static @Nullable TableRow failsafeFor(int i) {
    return hasFailsafe(i) ? new TableRow().set("failsafe", "f" + i) : null;
  }

  private static Instant timestampFor(int i) {
    return Instant.ofEpochMilli(1_000 + i);
  }

  private static Instant deadlineFor(int i) {
    return Instant.ofEpochMilli(2_000 + i);
  }

  private static StoragePayloadWithDeadline payloadFor(int i) {
    try {
      StorageApiWritePayload payload =
          StorageApiWritePayload.of(
                  hasUnknown(i) ? originalFor(i) : serializedFor(i).toByteArray(),
                  hasUnknown(i) ? unknownFor(i) : null,
                  failsafeFor(i))
              .withTimestamp(timestampFor(i));
      if (hasHash(i)) {
        payload = payload.withSchemaHash(hashFor(i));
      }
      return StoragePayloadWithDeadline.of(payload, deadlineFor(i));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Builds a packet of {@code numRows} rows, with the given indices marked schema-mismatched. */
  private static AppendRowsPacket packetOf(int numRows, int... mismatchedIndices) {
    BitSet mismatchedSet = new BitSet();
    for (int i : mismatchedIndices) {
      mismatchedSet.set(i);
    }

    ProtoRows.Builder rows = ProtoRows.newBuilder();
    List<Instant> timestamps = Lists.newArrayList();
    List<@Nullable TableRow> failsafeRows = Lists.newArrayList();
    List<StoragePayloadWithDeadline> mismatchedRows = Lists.newArrayList();

    for (int i = 0; i < numRows; i++) {
      if (mismatchedSet.get(i)) {
        mismatchedRows.add(payloadFor(i));
      } else {
        rows.addSerializedRows(serializedFor(i));
        timestamps.add(timestampFor(i));
        failsafeRows.add(failsafeFor(i));
      }
    }

    return AppendRowsPacket.create(rows.build(), timestamps, failsafeRows, mismatchedRows);
  }

  private static List<String> serializedRowsOf(AppendRowsPacket packet) {
    return packet.getProtoRows().getSerializedRowsList().stream()
        .map(ByteString::toStringUtf8)
        .collect(Collectors.toList());
  }

  @Test
  public void testGetSchemaMatchedRowsOnly_preservesMatchedRowsAndClearsMismatches() {
    AppendRowsPacket original = packetOf(7, 1, 3, 6);
    AppendRowsPacket matched = original.getSchemaMatchedRowsOnly();

    assertEquals(4, matched.getProtoRows().getSerializedRowsCount());
    assertTrue(matched.getSchemaMismatchedRows().isEmpty());

    int[] expectedIndices = {0, 2, 4, 5};
    for (int newIndex = 0; newIndex < expectedIndices.length; newIndex++) {
      int originalIndex = expectedIndices[newIndex];
      assertEquals(
          serializedFor(originalIndex), matched.getProtoRows().getSerializedRows(newIndex));
      assertEquals(timestampFor(originalIndex), matched.getTimestamps().get(newIndex));
      assertEquals(failsafeFor(originalIndex), matched.getFailsafeTableRows().get(newIndex));
    }
  }

  @Test
  public void testGetSchemaMismatchedRows_preservesExactOriginalPayloads() throws Exception {
    AppendRowsPacket original = packetOf(7, 1, 3, 6);
    List<StoragePayloadWithDeadline> payloads = original.getSchemaMismatchedRows();

    assertEquals(3, payloads.size());
    int[] originalIndices = {1, 3, 6};
    for (int newIndex = 0; newIndex < originalIndices.length; newIndex++) {
      int originalIndex = originalIndices[newIndex];
      StoragePayloadWithDeadline withDeadline = payloads.get(newIndex);
      StorageApiWritePayload payload = withDeadline.getStoragePayload();

      assertEquals(deadlineFor(originalIndex), withDeadline.getDeadline());
      assertEquals(timestampFor(originalIndex), payload.getTimestamp());
      assertEquals(failsafeFor(originalIndex), payload.getFailsafeTableRow());

      if (hasHash(originalIndex)) {
        assertArrayEquals(hashFor(originalIndex), payload.getSchemaHash());
      } else {
        assertNull(payload.getSchemaHash());
      }
      if (hasUnknown(originalIndex)) {
        assertEquals(unknownFor(originalIndex), payload.getUnknownFields());
        assertArrayEquals(originalFor(originalIndex), payload.getPayload());
      } else {
        assertNull(payload.getUnknownFields());
        assertArrayEquals(serializedFor(originalIndex).toByteArray(), payload.getPayload());
      }
    }
  }

  @Test
  public void testPartitionIsLossless() {
    int[][] patterns = {
      {}, {0}, {6}, {1, 3, 6}, {0, 1, 2, 3, 4, 5, 6}, {0, 6}, {2, 3, 4},
    };

    for (int[] pattern : patterns) {
      AppendRowsPacket original = packetOf(7, pattern);
      List<String> matched = serializedRowsOf(original.getSchemaMatchedRowsOnly());
      List<StoragePayloadWithDeadline> mismatched = original.getSchemaMismatchedRows();

      assertEquals(
          "row count must be conserved for pattern " + java.util.Arrays.toString(pattern),
          7,
          matched.size() + mismatched.size());
    }
  }

  @Test
  public void testGetSchemaMatchedRowsOnly_noMismatchesReturnsSameInstance() {
    AppendRowsPacket original = packetOf(4);
    assertSame(original, original.getSchemaMatchedRowsOnly());
  }

  @Test
  public void testPartitionOfEmptyPacket() {
    AppendRowsPacket empty = packetOf(0);

    assertTrue(empty.getSchemaMismatchedRows().isEmpty());
    assertEquals(0, empty.getSchemaMatchedRowsOnly().getProtoRows().getSerializedRowsCount());
  }

  @Test
  public void testGetSchemaMatchedRowsOnly_allMismatchedYieldsEmptyPacket() {
    AppendRowsPacket matched = packetOf(3, 0, 1, 2).getSchemaMatchedRowsOnly();

    assertEquals(0, matched.getProtoRows().getSerializedRowsCount());
    assertTrue(matched.getSchemaMismatchedRows().isEmpty());
  }

  @Test
  public void testFromStorageApiWritePayload_partitionsMatchedAndMismatchedRowsDirectly()
      throws Exception {
    TableSchema tableSchema = TableSchema.newBuilder().build();
    DescriptorProtos.DescriptorProto descriptor =
        DescriptorProtos.DescriptorProto.newBuilder().setName("test").build();
    AppendClientInfo appendClientInfo = AppendClientInfo.of(tableSchema, descriptor, client -> {});
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(false, true, new TableReference(), true);

    List<StoragePayloadWithDeadline> inputs = Lists.newArrayList();
    for (int i = 0; i < 4; i++) {
      StorageApiWritePayload payload =
          StorageApiWritePayload.of(
                  hasUnknown(i) ? originalFor(i) : serializedFor(i).toByteArray(),
                  hasUnknown(i) ? unknownFor(i) : null,
                  failsafeFor(i))
              .withTimestamp(timestampFor(i))
              .withSchemaHash(appendClientInfo.getTableSchemaHash());
      inputs.add(StoragePayloadWithDeadline.of(payload, deadlineFor(i)));
    }

    AppendRowsPacket packet =
        AppendRowsPacket.fromStorageApiWritePayload(
            Iterators.peekingIterator(inputs.iterator()),
            Long.MAX_VALUE,
            helper,
            Instant.ofEpochMilli(5_000),
            appendClientInfo,
            e -> false);

    // Rows 0 and 3 have unknown fields (hasUnknown(0) and hasUnknown(3)) so they are mismatched;
    // rows 1 and 2 have no unknown fields and match the empty schema.
    assertEquals(2, packet.getProtoRows().getSerializedRowsCount());
    assertEquals(serializedFor(1), packet.getProtoRows().getSerializedRows(0));
    assertEquals(serializedFor(2), packet.getProtoRows().getSerializedRows(1));

    assertEquals(2, packet.getSchemaMismatchedRows().size());
    assertEquals(inputs.get(0), packet.getSchemaMismatchedRows().get(0));
    assertEquals(inputs.get(3), packet.getSchemaMismatchedRows().get(1));
  }
}
