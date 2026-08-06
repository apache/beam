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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaChangeDetectorHelperTest {

  private TableReference tableReference;
  private BigQueryServices.WriteStreamService mockWriteStreamService;
  private BigQueryServices.StreamAppendClient mockStreamAppendClient;
  private AppendClientInfo mockAppendClientInfo;

  @Before
  public void setUp() {
    tableReference = new TableReference().setProjectId("p").setDatasetId("d").setTableId("t");
    mockWriteStreamService = mock(BigQueryServices.WriteStreamService.class);
    mockStreamAppendClient = mock(BigQueryServices.StreamAppendClient.class);
    mockAppendClientInfo = mock(AppendClientInfo.class);
  }

  @Test
  public void testCheckUpdatedSchema_autoUpdateTrue_hasUpdate() throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(true, false, tableReference, false);
    TableSchema currentSchema = TableSchema.newBuilder().build();
    TableSchema newSchema =
        TableSchema.newBuilder()
            .addFields(TableFieldSchema.newBuilder().setName("new_field").build())
            .build();

    when(mockWriteStreamService.getWriteStreamSchema("test_stream")).thenReturn(newSchema);

    Optional<TableSchema> updated =
        helper.checkUpdatedSchema(currentSchema, "test_stream", mockWriteStreamService);

    assertTrue(updated.isPresent());
    assertEquals(1, updated.get().getFieldsCount());
    assertEquals("new_field", updated.get().getFields(0).getName());
  }

  @Test
  public void testCheckUpdatedSchema_autoUpdateFalse() throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(false, false, tableReference, false);
    TableSchema currentSchema = TableSchema.newBuilder().build();

    Optional<TableSchema> updated =
        helper.checkUpdatedSchema(currentSchema, "test_stream", mockWriteStreamService);

    assertFalse(updated.isPresent());
  }

  @Test
  public void testCheckUpdatedSchema_autoUpdateTrue_noUpdate() throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(true, false, tableReference, false);
    TableSchema currentSchema = TableSchema.newBuilder().build();

    when(mockWriteStreamService.getWriteStreamSchema("test_stream")).thenReturn(null);

    Optional<TableSchema> updated =
        helper.checkUpdatedSchema(currentSchema, "test_stream", mockWriteStreamService);

    assertFalse(updated.isPresent());
  }

  @Test
  public void testCheckResponseForUpdatedSchema_hasUpdate() throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(true, false, tableReference, false);
    TableSchema currentSchema = TableSchema.newBuilder().build();
    TableSchema newSchema =
        TableSchema.newBuilder()
            .addFields(TableFieldSchema.newBuilder().setName("new_field").build())
            .build();

    when(mockStreamAppendClient.getUpdatedSchema()).thenReturn(newSchema);

    Optional<TableSchema> updated =
        helper.checkResponseForUpdatedSchema(currentSchema, mockStreamAppendClient);

    assertTrue(updated.isPresent());
    assertEquals(1, updated.get().getFieldsCount());
    assertEquals("new_field", updated.get().getFields(0).getName());
  }

  @Test
  public void testCheckResponseForUpdatedSchema_noUpdate() throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(true, false, tableReference, false);
    TableSchema currentSchema = TableSchema.newBuilder().build();

    when(mockStreamAppendClient.getUpdatedSchema()).thenReturn(null);

    Optional<TableSchema> updated =
        helper.checkResponseForUpdatedSchema(currentSchema, mockStreamAppendClient);

    assertFalse(updated.isPresent());
  }

  @Test
  public void testGetMergedPayload_autoUpdateFalse() throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(false, false, tableReference, false);
    StorageApiWritePayload payload =
        StorageApiWritePayload.of(new byte[] {1, 2, 3}, new TableRow().set("foo", "bar"), null);

    SchemaChangeDetectorHelper.MergePayloadResult result =
        helper.getMergedPayload(payload, Instant.now(), null, mockAppendClientInfo);

    assertEquals(SchemaChangeDetectorHelper.MergePayloadResult.Kind.MERGED, result.getKind());
    assertArrayEquals(new byte[] {1, 2, 3}, result.getMerged().toByteArray());
  }

  @Test
  public void testGetMergedPayload_autoUpdateTrue_emptyUnknownFields() throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(true, false, tableReference, false);
    StorageApiWritePayload payload = StorageApiWritePayload.of(new byte[] {1, 2, 3}, null, null);

    SchemaChangeDetectorHelper.MergePayloadResult result =
        helper.getMergedPayload(payload, Instant.now(), null, mockAppendClientInfo);

    assertEquals(SchemaChangeDetectorHelper.MergePayloadResult.Kind.MERGED, result.getKind());
    assertArrayEquals(new byte[] {1, 2, 3}, result.getMerged().toByteArray());
  }

  @Test
  public void testGetMergedPayload_autoUpdateTrue_mergeSuccess() throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(true, false, tableReference, false);
    TableRow unknownFields = new TableRow().set("foo", "bar");
    StorageApiWritePayload payload =
        StorageApiWritePayload.of(new byte[] {1, 2, 3}, unknownFields, null);

    ByteString mergedBytes = ByteString.copyFrom(new byte[] {4, 5, 6});
    when(mockAppendClientInfo.mergeNewFields(any(ByteString.class), eq(unknownFields), eq(false)))
        .thenReturn(mergedBytes);

    SchemaChangeDetectorHelper.MergePayloadResult result =
        helper.getMergedPayload(payload, Instant.now(), null, mockAppendClientInfo);

    assertEquals(SchemaChangeDetectorHelper.MergePayloadResult.Kind.MERGED, result.getKind());
    assertArrayEquals(new byte[] {4, 5, 6}, result.getMerged().toByteArray());
  }

  @Test
  public void testGetMergedPayload_autoUpdateTrue_mergeFailure() throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(true, false, tableReference, false);
    TableRow unknownFields = new TableRow().set("foo", "bar");
    StorageApiWritePayload payload =
        StorageApiWritePayload.of(new byte[] {1, 2, 3}, unknownFields, null);

    when(mockAppendClientInfo.mergeNewFields(any(ByteString.class), eq(unknownFields), eq(false)))
        .thenThrow(new TableRowToStorageApiProto.SchemaDoesntMatchException("conversion error"));
    TableRow expectedFailsafe = new TableRow().set("failsafe", "true");

    SchemaChangeDetectorHelper.MergePayloadResult result =
        helper.getMergedPayload(payload, Instant.now(), expectedFailsafe, mockAppendClientInfo);

    assertEquals(SchemaChangeDetectorHelper.MergePayloadResult.Kind.FAILED, result.getKind());
    TimestampedValue<BigQueryStorageApiInsertError> failed = result.getFailed();
    assertEquals(
        "org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto$SchemaDoesntMatchException: conversion error",
        failed.getValue().getErrorMessage());
    assertEquals(expectedFailsafe, failed.getValue().getRow());
  }

  @Test
  public void testIsPayloadSchemaOutOfDate_ignoreSchemaHashesTrue_hasMissingUnknownField()
      throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(false, false, tableReference, true);
    TableRow unknownFields = new TableRow().set("foo", "bar");
    StorageApiWritePayload payload =
        StorageApiWritePayload.of(new byte[] {1, 2, 3}, unknownFields, null);

    DescriptorProtos.DescriptorProto descriptorProto =
        DescriptorProtos.DescriptorProto.newBuilder().setName("test").build();
    Descriptors.Descriptor descriptor =
        TableRowToStorageApiProto.wrapDescriptorProto(descriptorProto);

    boolean outOfDate =
        helper.isPayloadSchemaOutOfDate(payload, new byte[0], () -> new byte[0], () -> descriptor);

    assertTrue(outOfDate);
  }

  @Test
  public void testIsPayloadSchemaOutOfDate_ignoreSchemaHashesTrue_noUnknownFields()
      throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(false, false, tableReference, true);
    StorageApiWritePayload payload = StorageApiWritePayload.of(new byte[] {1, 2, 3}, null, null);

    boolean outOfDate =
        helper.isPayloadSchemaOutOfDate(payload, new byte[0], () -> new byte[0], () -> null);

    assertFalse(outOfDate);
  }

  @Test
  public void testIsPayloadSchemaOutOfDate_ignoreSchemaHashesFalse_hashMatch() throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(false, false, tableReference, false);
    byte[] hash = "hash".getBytes(StandardCharsets.UTF_8);
    StorageApiWritePayload payload =
        StorageApiWritePayload.of(new byte[] {1, 2, 3}, null, null).withSchemaHash(hash);

    boolean outOfDate =
        helper.isPayloadSchemaOutOfDate(payload, new byte[0], () -> hash, () -> null);

    assertFalse(outOfDate);
  }

  @Test
  public void testIsPayloadSchemaOutOfDate_ignoreSchemaHashesFalse_hashMismatch_noUnknownFields()
      throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(false, false, tableReference, false);
    byte[] payloadHash = "hash1".getBytes(StandardCharsets.UTF_8);
    byte[] currentHash = "hash2".getBytes(StandardCharsets.UTF_8);
    StorageApiWritePayload payload =
        StorageApiWritePayload.of(new byte[0], null, null).withSchemaHash(payloadHash);

    DescriptorProtos.DescriptorProto descriptorProto =
        DescriptorProtos.DescriptorProto.newBuilder().setName("test").build();
    Descriptors.Descriptor descriptor =
        TableRowToStorageApiProto.wrapDescriptorProto(descriptorProto);

    // DynamicMessage will parse new byte[0] cleanly, and we have no unknown fields.
    boolean outOfDate =
        helper.isPayloadSchemaOutOfDate(payload, new byte[0], () -> currentHash, () -> descriptor);

    assertFalse(outOfDate);
  }

  @Test
  public void testIsPayloadSchemaOutOfDate_ignoreSchemaHashesFalse_hashMismatch_hasUnknownFields()
      throws Exception {
    SchemaChangeDetectorHelper helper =
        new SchemaChangeDetectorHelper(false, false, tableReference, false);
    byte[] payloadHash = "hash1".getBytes(StandardCharsets.UTF_8);
    byte[] currentHash = "hash2".getBytes(StandardCharsets.UTF_8);
    StorageApiWritePayload payload =
        StorageApiWritePayload.of(new byte[0], null, null).withSchemaHash(payloadHash);

    DescriptorProtos.DescriptorProto descriptorProto =
        DescriptorProtos.DescriptorProto.newBuilder().setName("test").build();
    Descriptors.Descriptor descriptor =
        TableRowToStorageApiProto.wrapDescriptorProto(descriptorProto);

    // To have unknown fields, we provide some valid protobuf bytes that contain a tag not in
    // descriptor.
    // Tag 1 (varint) = 1 << 3 | 0 = 8. Value 1.
    byte[] payloadWithUnknown = new byte[] {8, 1};

    boolean outOfDate =
        helper.isPayloadSchemaOutOfDate(
            payload, payloadWithUnknown, () -> currentHash, () -> descriptor);

    assertTrue(outOfDate);
  }
}
