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
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.PeekingIterator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@AutoValue
abstract class AppendRowsPacket {
  abstract ProtoRows getProtoRows();

  abstract List<Instant> getTimestamps();

  abstract List<@Nullable TableRow> getFailsafeTableRows();

  abstract List<StoragePayloadWithDeadline> getSchemaMismatchedRows();

  /**
   * Creates a packet.
   *
   * <p>Exists so that the generated AutoValue subclass is never named outside this file;
   * ErrorProne's AutoValueSubclassLeaked check forbids that, which would otherwise keep tests from
   * constructing a packet directly.
   */
  static AppendRowsPacket create(
      ProtoRows protoRows,
      List<Instant> timestamps,
      List<@Nullable TableRow> failsafeTableRows,
      List<StoragePayloadWithDeadline> schemaMismatchedRows) {
    return new AutoValue_AppendRowsPacket(
        protoRows, timestamps, failsafeTableRows, schemaMismatchedRows);
  }

  static AppendRowsPacket fromStorageApiWritePayload(
      PeekingIterator<StoragePayloadWithDeadline> underlyingIterator,
      long maxByteSize,
      SchemaChangeDetectorHelper schemaChangeDetectorHelper,
      Instant elementTimestamp,
      AppendClientInfo appendClientInfo,
      Function<TimestampedValue<BigQueryStorageApiInsertError>, Boolean> failedRowsHandler) {
    List<Instant> timestamps = Lists.newArrayList();
    List<@Nullable TableRow> failsafeRows = Lists.newArrayList();
    List<StoragePayloadWithDeadline> mismatchedRows = Lists.newArrayList();
    ProtoRows.Builder inserts = ProtoRows.newBuilder();
    long bytesSize = 0;
    try {
      while (underlyingIterator.hasNext()) {
        // Make sure that we don't exceed the maxByteSize over multiple elements. A single
        // element can exceed
        // the split threshold, but in that case it should be the only element returned.
        if ((bytesSize + underlyingIterator.peek().getStoragePayload().getPayload().length
                > maxByteSize)
            && (inserts.getSerializedRowsCount() > 0 || !mismatchedRows.isEmpty())) {
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
        ByteString byteString = ByteString.empty();
        SchemaChangeDetectorHelper.MergePayloadResult mergeResult =
            schemaChangeDetectorHelper.getMergedPayload(
                storagePayload, elementTimestamp, failsafeTableRow, appendClientInfo);
        boolean mismatched = false;
        if (mergeResult.getKind() == SchemaChangeDetectorHelper.MergePayloadResult.Kind.FAILED) {
          if (failedRowsHandler.apply(mergeResult.getFailed())) {
            continue;
          } else {
            // This implies that instead of skipping failed rows, we should mark it as a mismatched
            // row. Note that this path can only happen if there are unknown fields, as otherwise
            // the call to
            // getMergedPayload is a noop.
            mismatched = true;
          }
        } else {
          byteString = mergeResult.getMerged();
        }

        if (!mismatched
            && schemaChangeDetectorHelper.isPayloadSchemaOutOfDate(
                storagePayload,
                byteString,
                appendClientInfo::getTableSchemaHash,
                appendClientInfo::getWrappedDescriptor)) {
          mismatched = true;
        }

        Instant timestamp = storagePayload.getTimestamp();
        if (timestamp == null) {
          timestamp = elementTimestamp;
        }

        if (mismatched) {
          if (storagePayload.getTimestamp() == null) {
            payload =
                StoragePayloadWithDeadline.of(
                    storagePayload.withTimestamp(timestamp), payload.getDeadline());
          }
          mismatchedRows.add(payload);
          bytesSize += storagePayload.getPayload().length;
          continue;
        }

        inserts.addSerializedRows(byteString);
        timestamps.add(timestamp);
        failsafeRows.add(failsafeTableRow);
        bytesSize += byteString.size();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return create(inserts.build(), timestamps, failsafeRows, mismatchedRows);
  }

  AppendRowsPacket getSchemaMatchedRowsOnly() {
    if (getSchemaMismatchedRows().isEmpty()) {
      return this;
    }
    return create(getProtoRows(), getTimestamps(), getFailsafeTableRows(), Collections.emptyList());
  }
}
