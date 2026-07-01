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

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.ThrowingSupplier;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.PeekingIterator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@AutoValue
abstract class AppendRowsPacket {
  abstract ProtoRows getProtoRows();

  abstract List<Instant> getTimestamps();

  abstract List<@Nullable TableRow> getFailsafeTableRows();

  abstract BitSet getSchemaMismatchedRows();

  abstract Map<Integer, byte[]> getSchemaHashes();

  abstract Map<Integer, TableRow> getUnknownFields();

  abstract Map<Integer, byte[]> getOriginalPayloads();

  // Retry deadlines for handling schema updates
  abstract List<Instant> getDeadlines();

  private static final BitSet EMPTY_BIT_SET = new BitSet(0);

  static AppendRowsPacket fromStorageApiWritePayload(
      PeekingIterator<StoragePayloadWithDeadline> underlyingIterator,
      long maxByteSize,
      SchemaChangeDetectorHelper schemaChangeDetectorHelper,
      Instant elementTimestamp,
      Supplier<AppendClientInfo> appendClientInfoSupplier,
      Consumer<TimestampedValue<BigQueryStorageApiInsertError>> failedRowsConsumer,
      ThrowingSupplier<byte[]> getCurrentTableSchemaHash,
      ThrowingSupplier<Descriptors.Descriptor> getCurrentTableSchemaDescriptor) {
    List<Instant> timestamps = Lists.newArrayList();
    List<@Nullable TableRow> failsafeRows = Lists.newArrayList();
    Map<Integer, byte[]> schemaHashes = Maps.newHashMap();
    Map<Integer, TableRow> unknownFields = Maps.newHashMap();
    Map<Integer, byte[]> originalPayloads = Maps.newHashMap();
    List<Instant> deadlines = Lists.newArrayList();
    ProtoRows.Builder inserts = ProtoRows.newBuilder();
    long bytesSize = 0;
    BitSet mismatchedRows = new BitSet();
    try {
      while (underlyingIterator.hasNext()) {
        // Make sure that we don't exceed the maxByteSize over multiple elements. A single
        // element can exceed
        // the split threshold, but in that case it should be the only element returned.
        if ((bytesSize + underlyingIterator.peek().getStoragePayload().getPayload().length
                > maxByteSize)
            && inserts.getSerializedRowsCount() > 0) {
          break;
        }
        StoragePayloadWithDeadline payload = underlyingIterator.next();
        StorageApiWritePayload storagePayload = payload.getStoragePayload();

        @Nullable TableRow failsafeTableRow = null;
        try {
          failsafeTableRow = storagePayload.getFailsafeTableRow();
        } catch (IOException e) {
          // Do nothing, table row will be generated later from row bytes
        }

        // If autoUpdateSchema is set, we try to automatically merge in unknown fields.
        SchemaChangeDetectorHelper.MergePayloadResult mergeResult =
            schemaChangeDetectorHelper.getMergedPayload(
                storagePayload, elementTimestamp, failsafeTableRow, appendClientInfoSupplier.get());
        if (mergeResult.getKind() == SchemaChangeDetectorHelper.MergePayloadResult.Kind.FAILED) {
          failedRowsConsumer.accept(mergeResult.getFailed());
          continue;
        }
        ByteString byteString = mergeResult.getMerged();

        if (schemaChangeDetectorHelper.isPayloadSchemaOutOfDate(
            storagePayload,
            byteString.toByteArray(),
            getCurrentTableSchemaHash,
            getCurrentTableSchemaDescriptor)) {
          mismatchedRows.set(inserts.getSerializedRowsCount());
        }

        int currentIndex = inserts.getSerializedRowsCount();
        inserts.addSerializedRows(byteString);
        Instant timestamp = storagePayload.getTimestamp();
        if (timestamp == null) {
          timestamp = elementTimestamp;
        }
        timestamps.add(timestamp);
        failsafeRows.add(failsafeTableRow);
        if (storagePayload.getSchemaHash() != null) {
          schemaHashes.put(
              currentIndex, Preconditions.checkStateNotNull(storagePayload.getSchemaHash()));
        }
        if (storagePayload.getUnknownFields() != null) {
          unknownFields.put(
              currentIndex, Preconditions.checkStateNotNull(storagePayload.getUnknownFields()));
          originalPayloads.put(currentIndex, storagePayload.getPayload());
        }
        deadlines.add(payload.getDeadline());
        bytesSize += byteString.size();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return new AutoValue_AppendRowsPacket(
        inserts.build(),
        timestamps,
        failsafeRows,
        mismatchedRows,
        schemaHashes,
        unknownFields,
        originalPayloads,
        deadlines);
  }

  AppendRowsPacket getSchemaMismatchedRowsOnly() {
    ProtoRows.Builder inserts = ProtoRows.newBuilder();
    List<Instant> timestamps = Lists.newArrayList();
    List<@Nullable TableRow> failsafeTableRows = Lists.newArrayList();
    Map<Integer, byte[]> schemaHashes = Maps.newHashMap();
    Map<Integer, TableRow> unknownFields = Maps.newHashMap();
    Map<Integer, byte[]> originalPayloads = Maps.newHashMap();
    List<Instant> deadlines = Lists.newArrayList();
    if (!getSchemaMismatchedRows().isEmpty()) {
      int newIndex = 0;
      for (int i = 0; i < getProtoRows().getSerializedRowsCount(); i++) {
        if (getSchemaMismatchedRows().get(i)) {
          inserts.addSerializedRows(getProtoRows().getSerializedRows(i));
          timestamps.add(getTimestamps().get(i));
          failsafeTableRows.add(getFailsafeTableRows().get(i));
          if (getUnknownFields().containsKey(i)) {
            unknownFields.put(newIndex, Preconditions.checkStateNotNull(getUnknownFields().get(i)));
          }
          if (getOriginalPayloads().containsKey(i)) {
            originalPayloads.put(
                newIndex, Preconditions.checkStateNotNull(getOriginalPayloads().get(i)));
          }
          deadlines.add(getDeadlines().get(i));
          if (getSchemaHashes().containsKey(i)) {
            schemaHashes.put(newIndex, Preconditions.checkStateNotNull(getSchemaHashes().get(i)));
          }
          newIndex++;
        }
      }
    }
    BitSet allBits = new BitSet(inserts.getSerializedRowsCount());
    allBits.set(0, inserts.getSerializedRowsCount());
    return new AutoValue_AppendRowsPacket(
        inserts.build(),
        timestamps,
        failsafeTableRows,
        allBits,
        schemaHashes,
        unknownFields,
        originalPayloads,
        deadlines);
  }

  AppendRowsPacket getSchemaMatchedRowsOnly() {
    if (getSchemaMismatchedRows().isEmpty()) {
      return this;
    }

    ProtoRows.Builder inserts = ProtoRows.newBuilder();
    List<Instant> timestamps = Lists.newArrayList();
    Map<Integer, byte[]> schemaHashes = Maps.newHashMap();
    Map<Integer, TableRow> unknownFields = Maps.newHashMap();
    Map<Integer, byte[]> originalPayloads = Maps.newHashMap();
    List<Instant> deadlines = Lists.newArrayList();
    List<@Nullable TableRow> failsafeTableRows = Lists.newArrayList();
    int newIndex = 0;
    for (int i = 0; i < getProtoRows().getSerializedRowsCount(); i++) {
      if (!getSchemaMismatchedRows().get(i)) {
        inserts.addSerializedRows(getProtoRows().getSerializedRows(i));
        timestamps.add(getTimestamps().get(i));
        failsafeTableRows.add(getFailsafeTableRows().get(i));
        if (getSchemaHashes().containsKey(i)) {
          schemaHashes.put(newIndex, Preconditions.checkStateNotNull(getSchemaHashes().get(i)));
        }
        if (getUnknownFields().containsKey(i)) {
          unknownFields.put(newIndex, Preconditions.checkStateNotNull(getUnknownFields().get(i)));
        }
        if (getOriginalPayloads().containsKey(i)) {
          originalPayloads.put(
              newIndex, Preconditions.checkStateNotNull(getOriginalPayloads().get(i)));
        }
        deadlines.add(getDeadlines().get(i));
        newIndex++;
      }
    }
    return new AutoValue_AppendRowsPacket(
        inserts.build(),
        timestamps,
        failsafeTableRows,
        EMPTY_BIT_SET,
        schemaHashes,
        unknownFields,
        originalPayloads,
        deadlines);
  }

  Stream<StoragePayloadWithDeadline> toPayloadStream() {
    return IntStream.range(0, getProtoRows().getSerializedRowsCount())
        .mapToObj(
            i -> {
              try {
                byte @Nullable [] originalBytes = getOriginalPayloads().get(i);
                StorageApiWritePayload payload =
                    StorageApiWritePayload.of(
                            originalBytes != null
                                ? originalBytes
                                : getProtoRows().getSerializedRows(i).toByteArray(),
                            getUnknownFields().get(i),
                            getFailsafeTableRows().get(i))
                        .withTimestamp(getTimestamps().get(i));
                byte @Nullable [] hash = getSchemaHashes().get(i);
                if (hash != null) {
                  payload = payload.withSchemaHash(hash);
                }
                return StoragePayloadWithDeadline.of(payload, getDeadlines().get(i));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }
}
