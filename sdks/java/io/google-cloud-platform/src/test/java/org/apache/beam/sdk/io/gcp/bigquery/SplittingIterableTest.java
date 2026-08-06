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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.DescriptorProtos;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SplittingIterableTest {

  private AppendClientInfo createAppendClientInfo() throws Exception {
    TableSchema tableSchema = TableSchema.newBuilder().build();
    DescriptorProtos.DescriptorProto descriptor =
        DescriptorProtos.DescriptorProto.newBuilder().setName("test").build();
    return AppendClientInfo.of(tableSchema, descriptor, client -> {});
  }

  @Test
  public void testBatchingBySplitSize() throws Exception {
    List<StoragePayloadWithDeadline> payloads = new ArrayList<>();
    // Payload of 10 bytes each
    for (int i = 0; i < 5; i++) {
      StorageApiWritePayload payload =
          StorageApiWritePayload.of(new byte[10], null, null)
              .withTimestamp(Instant.ofEpochMilli(i));
      payloads.add(StoragePayloadWithDeadline.of(payload, null));
    }

    List<TimestampedValue<BigQueryStorageApiInsertError>> failedRows = new ArrayList<>();
    SchemaChangeDetectorHelper schemaChangeDetectorHelper =
        new SchemaChangeDetectorHelper(false, false, new TableReference(), false);
    AppendClientInfo appendClientInfo = createAppendClientInfo();

    // Split size 25 means 2 payloads (20 bytes) per batch, then 2, then 1.
    SplittingIterable iterable =
        new SplittingIterable(
            payloads,
            25,
            failedRows::add,
            () -> new byte[0],
            () -> TableRowToStorageApiProto.wrapDescriptorProto(appendClientInfo.getDescriptor()),
            Instant.now(),
            () -> appendClientInfo,
            schemaChangeDetectorHelper);

    Iterator<AppendRowsPacket> it = iterable.iterator();

    assertTrue(it.hasNext());
    AppendRowsPacket batch1 = it.next();
    assertEquals(2, batch1.getProtoRows().getSerializedRowsCount());
    assertEquals(Instant.ofEpochMilli(0), batch1.getTimestamps().get(0));
    assertEquals(Instant.ofEpochMilli(1), batch1.getTimestamps().get(1));

    assertTrue(it.hasNext());
    AppendRowsPacket batch2 = it.next();
    assertEquals(2, batch2.getProtoRows().getSerializedRowsCount());

    assertTrue(it.hasNext());
    AppendRowsPacket batch3 = it.next();
    assertEquals(1, batch3.getProtoRows().getSerializedRowsCount());

    assertFalse(it.hasNext());
    assertTrue(failedRows.isEmpty());
  }

  @Test
  public void testLargeElementExceedingSplitSize() throws Exception {
    List<StoragePayloadWithDeadline> payloads = new ArrayList<>();
    // Payload of 10 bytes, 100 bytes, 10 bytes
    payloads.add(
        StoragePayloadWithDeadline.of(StorageApiWritePayload.of(new byte[10], null, null), null));
    payloads.add(
        StoragePayloadWithDeadline.of(StorageApiWritePayload.of(new byte[100], null, null), null));
    payloads.add(
        StoragePayloadWithDeadline.of(StorageApiWritePayload.of(new byte[10], null, null), null));

    List<TimestampedValue<BigQueryStorageApiInsertError>> failedRows = new ArrayList<>();
    SchemaChangeDetectorHelper schemaChangeDetectorHelper =
        new SchemaChangeDetectorHelper(false, false, new TableReference(), false);
    AppendClientInfo appendClientInfo = createAppendClientInfo();

    // Split size 25
    SplittingIterable iterable =
        new SplittingIterable(
            payloads,
            25,
            failedRows::add,
            () -> new byte[0],
            () -> TableRowToStorageApiProto.wrapDescriptorProto(appendClientInfo.getDescriptor()),
            Instant.now(),
            () -> appendClientInfo,
            schemaChangeDetectorHelper);

    Iterator<AppendRowsPacket> it = iterable.iterator();

    assertTrue(it.hasNext());
    AppendRowsPacket batch1 = it.next();
    assertEquals(1, batch1.getProtoRows().getSerializedRowsCount());

    assertTrue(it.hasNext());
    AppendRowsPacket batch2 = it.next();
    assertEquals(1, batch2.getProtoRows().getSerializedRowsCount());
    assertEquals(100, batch2.getProtoRows().getSerializedRows(0).size());

    assertTrue(it.hasNext());
    AppendRowsPacket batch3 = it.next();
    assertEquals(1, batch3.getProtoRows().getSerializedRowsCount());

    assertFalse(it.hasNext());
  }

  @Test
  public void testSchemaMismatchedAndMatchedRows() throws Exception {
    List<StoragePayloadWithDeadline> payloads = new ArrayList<>();
    byte[] hash1 = "currentHash".getBytes(StandardCharsets.UTF_8);
    byte[] hash2 = "oldHash".getBytes(StandardCharsets.UTF_8);

    TableRow unknownFieldsRow = new TableRow().set("foo", "bar");

    payloads.add(
        StoragePayloadWithDeadline.of(
            StorageApiWritePayload.of(new byte[0], null, null).withSchemaHash(hash1), null));
    payloads.add(
        StoragePayloadWithDeadline.of(
            StorageApiWritePayload.of(new byte[0], unknownFieldsRow, null).withSchemaHash(hash2),
            null));
    payloads.add(
        StoragePayloadWithDeadline.of(
            StorageApiWritePayload.of(new byte[0], null, null).withSchemaHash(hash1), null));

    List<TimestampedValue<BigQueryStorageApiInsertError>> failedRows = new ArrayList<>();
    SchemaChangeDetectorHelper schemaChangeDetectorHelper =
        new SchemaChangeDetectorHelper(false, true, new TableReference(), true);
    AppendClientInfo appendClientInfo = createAppendClientInfo();

    SplittingIterable iterable =
        new SplittingIterable(
            payloads,
            100,
            failedRows::add,
            () -> "currentHash".getBytes(StandardCharsets.UTF_8), // Matches hash1
            () -> TableRowToStorageApiProto.wrapDescriptorProto(appendClientInfo.getDescriptor()),
            Instant.now(),
            () -> appendClientInfo,
            schemaChangeDetectorHelper);

    Iterator<AppendRowsPacket> it = iterable.iterator();
    assertTrue(it.hasNext());
    AppendRowsPacket batch = it.next();

    assertEquals(3, batch.getProtoRows().getSerializedRowsCount());

    // Check getting only mismatched rows
    AppendRowsPacket mismatched = batch.getSchemaMismatchedRowsOnly();
    assertEquals(1, mismatched.getProtoRows().getSerializedRowsCount());
    assertArrayEquals(new byte[0], mismatched.getProtoRows().getSerializedRows(0).toByteArray());
    assertEquals(unknownFieldsRow, mismatched.getUnknownFields().get(0));
    assertArrayEquals(new byte[0], mismatched.getOriginalPayloads().get(0));
    assertArrayEquals(hash2, mismatched.getSchemaHashes().get(0));

    // Check getting only matched rows
    AppendRowsPacket matched = batch.getSchemaMatchedRowsOnly();
    assertEquals(2, matched.getProtoRows().getSerializedRowsCount());
    assertArrayEquals(new byte[0], matched.getProtoRows().getSerializedRows(0).toByteArray());
    assertArrayEquals(new byte[0], matched.getProtoRows().getSerializedRows(1).toByteArray());
    assertArrayEquals(hash1, matched.getSchemaHashes().get(0));
    assertArrayEquals(hash1, matched.getSchemaHashes().get(1));
    assertNull(matched.getUnknownFields().get(0));
    assertNull(matched.getOriginalPayloads().get(0));

    // Reconstruct stream
    List<StorageApiWritePayload> reconstructed =
        batch
            .toPayloadStream()
            .map(StoragePayloadWithDeadline::getStoragePayload)
            .collect(Collectors.toList());
    assertEquals(3, reconstructed.size());
    assertArrayEquals(new byte[0], reconstructed.get(0).getPayload());
    assertArrayEquals(new byte[0], reconstructed.get(1).getPayload());
    assertArrayEquals(new byte[0], reconstructed.get(2).getPayload());
  }

  @Test
  public void testFailedRows() throws Exception {
    List<StoragePayloadWithDeadline> payloads = new ArrayList<>();
    payloads.add(
        StoragePayloadWithDeadline.of(StorageApiWritePayload.of(new byte[0], null, null), null));
    // Provide unknown fields, so auto update schema tries to merge and fails
    TableRow unknownFieldsRow = new TableRow().set("foo", "bar");
    payloads.add(
        StoragePayloadWithDeadline.of(
            StorageApiWritePayload.of(new byte[0], unknownFieldsRow, null), null));

    List<TimestampedValue<BigQueryStorageApiInsertError>> failedRows = new ArrayList<>();
    SchemaChangeDetectorHelper schemaChangeDetectorHelper =
        new SchemaChangeDetectorHelper(
            true, // autoUpdateSchema
            false, // ignoreUnknownValues (this will trigger SchemaConversionException)
            new TableReference().setTableId("test_table"),
            false);
    AppendClientInfo appendClientInfo = createAppendClientInfo();

    SplittingIterable iterable =
        new SplittingIterable(
            payloads,
            100,
            failedRows::add,
            () -> new byte[0],
            () -> TableRowToStorageApiProto.wrapDescriptorProto(appendClientInfo.getDescriptor()),
            Instant.now(),
            () -> appendClientInfo,
            schemaChangeDetectorHelper);

    Iterator<AppendRowsPacket> it = iterable.iterator();
    assertTrue(it.hasNext());
    AppendRowsPacket batch = it.next();

    // Only 1 row successfully added to batch
    assertEquals(1, batch.getProtoRows().getSerializedRowsCount());
    assertArrayEquals(new byte[0], batch.getProtoRows().getSerializedRows(0).toByteArray());

    // 1 row failed
    assertEquals(1, failedRows.size());
    BigQueryStorageApiInsertError error = failedRows.get(0).getValue();
    assertEquals("test_table", error.getTable().getTableId());
  }
}
