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

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoOneOf;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.util.Optional;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.ThrowingSupplier;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

class SchemaChangeDetectorHelper {
  @AutoOneOf(MergePayloadResult.Kind.class)
  abstract static class MergePayloadResult {
    enum Kind {
      MERGED,
      FAILED
    };

    abstract Kind getKind();

    abstract ByteString getMerged();

    abstract TimestampedValue<BigQueryStorageApiInsertError> getFailed();

    static MergePayloadResult ofMerged(ByteString merged) {
      return AutoOneOf_SchemaChangeDetectorHelper_MergePayloadResult.merged(merged);
    }

    static MergePayloadResult ofFailed(TimestampedValue<BigQueryStorageApiInsertError> failed) {
      return AutoOneOf_SchemaChangeDetectorHelper_MergePayloadResult.failed(failed);
    }
  }

  private final boolean autoUpdateSchema;
  private final boolean ignoreUnknownValues;
  private final TableReference tableReference;
  private final boolean ignoreSchemaHashes;

  public SchemaChangeDetectorHelper(
      boolean autoUpdateSchema,
      boolean ignoreUnknownValues,
      TableReference tableReference,
      boolean ignoreSchemaHashes) {
    this.autoUpdateSchema = autoUpdateSchema;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.tableReference = tableReference;
    this.ignoreSchemaHashes = ignoreSchemaHashes;
  }

  Optional<TableSchema> checkUpdatedSchema(
      TableSchema currentSchema,
      String streamName,
      BigQueryServices.WriteStreamService writeStreamService) {
    if (autoUpdateSchema) {
      @Nullable TableSchema streamSchema = writeStreamService.getWriteStreamSchema(streamName);
      if (streamSchema != null) {
        return TableSchemaUpdateUtils.getUpdatedSchema(currentSchema, streamSchema);
      }
    }
    return Optional.empty();
  }

  Optional<TableSchema> checkResponseForUpdatedSchema(
      TableSchema currentSchema, BigQueryServices.StreamAppendClient streamAppendClient) {
    @Nullable TableSchema updatedSchemaReturned = streamAppendClient.getUpdatedSchema();

    // Update the table schema and clear the append client.
    if (updatedSchemaReturned != null) {
      return TableSchemaUpdateUtils.getUpdatedSchema(currentSchema, updatedSchemaReturned);
    }
    return Optional.empty();
  }

  MergePayloadResult getMergedPayload(
      StorageApiWritePayload payload,
      Instant elementTimestamp,
      @Nullable TableRow failsafeTableRow,
      AppendClientInfo appendClientInfo)
      throws IOException {
    ByteString byteString = ByteString.copyFrom(payload.getPayload());

    if (autoUpdateSchema) {
      @Nullable TableRow unknownFields = payload.getUnknownFields();

      if (unknownFields != null && !unknownFields.isEmpty()) {
        try {
          // Protocol buffer serialization format supports concatenation. We serialize any new
          // "known" fields into a proto and concatenate to the existing proto.
          byteString =
              Preconditions.checkStateNotNull(appendClientInfo)
                  .mergeNewFields(byteString, unknownFields, ignoreUnknownValues);
        } catch (TableRowToStorageApiProto.SchemaConversionException e) {
          // This generally implies that ignoreUnknownValues=false and there were still
          // unknown values here.
          // Reconstitute the TableRow and send it to the failed-rows consumer.
          TableRow tableRow =
              failsafeTableRow != null
                  ? failsafeTableRow
                  : appendClientInfo.toTableRow(byteString, Predicates.alwaysTrue());
          BigQueryStorageApiInsertError error =
              new BigQueryStorageApiInsertError(tableRow, e.toString(), tableReference);
          Instant timestamp = MoreObjects.firstNonNull(payload.getTimestamp(), elementTimestamp);
          return MergePayloadResult.ofFailed(TimestampedValue.of(error, timestamp));
        }
      }
    }
    return MergePayloadResult.ofMerged(byteString);
  }

  boolean isPayloadSchemaOutOfDate(
      StorageApiWritePayload payload,
      byte[] mergedPayload,
      ThrowingSupplier<byte[]> schemaHash,
      ThrowingSupplier<Descriptors.Descriptor> schemaDescriptor)
      throws Exception {
    if (ignoreSchemaHashes) {
      if (payload.getUnknownFields() != null
          && UpgradeTableSchema.missingUnknownField(
              Preconditions.checkStateNotNull(payload.getUnknownFields()), schemaDescriptor)) {
        return true;
      }
      // We currently rely on getting an append failure from Vortex if there are
      // missing required
      // fields. However
      // we should consider explicitly checking here in the future.
    } else {
      return UpgradeTableSchema.isPayloadSchemaOutOfDate(
          payload.getSchemaHash(), () -> mergedPayload, schemaHash, schemaDescriptor);
    }
    return false;
  }

  static void bufferMismatchedRows(
      Iterable<StoragePayloadWithDeadline> rows,
      BagState<StoragePayloadWithDeadline> bufferedBag,
      Timer retryRowsTimers,
      ValueState<Long> currentTimerValue,
      ValueState<Long> minPendingTimestamp,
      TableDestination tableDestination,
      DoFn.OutputReceiver<BigQueryStorageApiInsertError> failedRowsReceiver,
      @Nullable AppendClientInfo appendClientInfo,
      Counter rowsSentToFailedRowsCollection,
      Duration retryPeriod)
      throws IOException {
    org.joda.time.Instant minTimestamp =
        org.joda.time.Instant.ofEpochMilli(
            MoreObjects.firstNonNull(
                minPendingTimestamp.read(), BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));

    for (StoragePayloadWithDeadline row : rows) {
      if (appendClientInfo == null
          || row.getDeadline().isAfter(retryRowsTimers.getCurrentRelativeTime())) {
        bufferedBag.add(row);
        org.joda.time.@Nullable Instant timestamp = row.getStoragePayload().getTimestamp();
        if (timestamp != null && timestamp.isBefore(minTimestamp)) {
          minTimestamp = timestamp;
        }
      } else {
        @Nullable TableRow failedRow = row.getStoragePayload().getFailsafeTableRow();
        if (failedRow == null) {
          ByteString rowBytes = ByteString.copyFrom(row.getStoragePayload().getPayload());
          failedRow = appendClientInfo.toTableRow(rowBytes, Predicates.alwaysTrue());
        }
        BigQueryStorageApiInsertError error =
            new BigQueryStorageApiInsertError(
                failedRow, "Mismatched schema", tableDestination.getTableReference());
        failedRowsReceiver.outputWithTimestamp(
            error, Preconditions.checkStateNotNull(row.getStoragePayload().getTimestamp()));
        rowsSentToFailedRowsCollection.inc();
        BigQuerySinkMetrics.appendRowsRowStatusCounter(
                BigQuerySinkMetrics.RowStatus.FAILED,
                BigQuerySinkMetrics.SCHEMA_MISMATCHED,
                tableDestination.getShortTableUrn())
            .inc(1);
      }
    }
    minPendingTimestamp.write(minTimestamp.getMillis());

    long targetTs =
        MoreObjects.firstNonNull(
            currentTimerValue.read(),
            retryRowsTimers.getCurrentRelativeTime().getMillis() + retryPeriod.getMillis());
    retryRowsTimers
        .withOutputTimestamp(minTimestamp)
        .set(org.joda.time.Instant.ofEpochMilli(targetTs));
    currentTimerValue.write(targetTs);
  }
}
