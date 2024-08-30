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
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;

/**
 * This transform takes in key-value pairs of {@link TableRow} entries and the {@link
 * TableDestination} it should be written to. The BigQuery streaming-write service is used to stream
 * these writes to the appropriate table.
 *
 * <p>This transform assumes that all destination tables already exist by the time it sees a write
 * for that table.
 */
public class StreamingWriteTables<ElementT>
    extends PTransform<PCollection<KV<TableDestination, ElementT>>, WriteResult> {
  private BigQueryServices bigQueryServices;
  private InsertRetryPolicy retryPolicy;
  private boolean extendedErrorInfo;
  private static final String FAILED_INSERTS_TAG_ID = "failedInserts";
  private final boolean skipInvalidRows;
  private final boolean ignoreUnknownValues;
  private final boolean ignoreInsertIds;
  private final boolean autoSharding;
  private final boolean propagateSuccessful;
  private final @Nullable Coder<ElementT> elementCoder;
  private final @Nullable SerializableFunction<ElementT, TableRow> toTableRow;
  private final @Nullable SerializableFunction<ElementT, TableRow> toFailsafeTableRow;
  private final @Nullable SerializableFunction<ElementT, String> deterministicRecordIdFn;

  public StreamingWriteTables() {
    this(
        new BigQueryServicesImpl(),
        InsertRetryPolicy.alwaysRetry(),
        false, // extendedErrorInfo
        false, // skipInvalidRows
        false, // ignoreUnknownValues
        false, // ignoreInsertIds
        false, // autoSharding
        false, // propagateSuccessful
        null, // elementCoder
        null, // toTableRow
        null, // toFailsafeTableRow
        null); // deterministicRecordIdFn
  }

  private StreamingWriteTables(
      BigQueryServices bigQueryServices,
      InsertRetryPolicy retryPolicy,
      boolean extendedErrorInfo,
      boolean skipInvalidRows,
      boolean ignoreUnknownValues,
      boolean ignoreInsertIds,
      boolean autoSharding,
      boolean propagateSuccessful,
      @Nullable Coder<ElementT> elementCoder,
      @Nullable SerializableFunction<ElementT, TableRow> toTableRow,
      @Nullable SerializableFunction<ElementT, TableRow> toFailsafeTableRow,
      @Nullable SerializableFunction<ElementT, String> deterministicRecordIdFn) {
    this.bigQueryServices = bigQueryServices;
    this.retryPolicy = retryPolicy;
    this.extendedErrorInfo = extendedErrorInfo;
    this.skipInvalidRows = skipInvalidRows;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.ignoreInsertIds = ignoreInsertIds;
    this.autoSharding = autoSharding;
    this.propagateSuccessful = propagateSuccessful;
    this.elementCoder = elementCoder;
    this.toTableRow = toTableRow;
    this.toFailsafeTableRow = toFailsafeTableRow;
    this.deterministicRecordIdFn = deterministicRecordIdFn;
  }

  StreamingWriteTables<ElementT> withTestServices(BigQueryServices bigQueryServices) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        autoSharding,
        propagateSuccessful,
        elementCoder,
        toTableRow,
        toFailsafeTableRow,
        deterministicRecordIdFn);
  }

  StreamingWriteTables<ElementT> withInsertRetryPolicy(InsertRetryPolicy retryPolicy) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        autoSharding,
        propagateSuccessful,
        elementCoder,
        toTableRow,
        toFailsafeTableRow,
        deterministicRecordIdFn);
  }

  StreamingWriteTables<ElementT> withExtendedErrorInfo(boolean extendedErrorInfo) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        autoSharding,
        propagateSuccessful,
        elementCoder,
        toTableRow,
        toFailsafeTableRow,
        deterministicRecordIdFn);
  }

  StreamingWriteTables<ElementT> withSkipInvalidRows(boolean skipInvalidRows) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        autoSharding,
        propagateSuccessful,
        elementCoder,
        toTableRow,
        toFailsafeTableRow,
        deterministicRecordIdFn);
  }

  StreamingWriteTables<ElementT> withIgnoreUnknownValues(boolean ignoreUnknownValues) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        autoSharding,
        propagateSuccessful,
        elementCoder,
        toTableRow,
        toFailsafeTableRow,
        deterministicRecordIdFn);
  }

  StreamingWriteTables<ElementT> withIgnoreInsertIds(boolean ignoreInsertIds) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        autoSharding,
        propagateSuccessful,
        elementCoder,
        toTableRow,
        toFailsafeTableRow,
        deterministicRecordIdFn);
  }

  StreamingWriteTables<ElementT> withAutoSharding(boolean autoSharding) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        autoSharding,
        propagateSuccessful,
        elementCoder,
        toTableRow,
        toFailsafeTableRow,
        deterministicRecordIdFn);
  }

  StreamingWriteTables<ElementT> withPropagateSuccessful(boolean propagateSuccessful) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        autoSharding,
        propagateSuccessful,
        elementCoder,
        toTableRow,
        toFailsafeTableRow,
        deterministicRecordIdFn);
  }

  StreamingWriteTables<ElementT> withElementCoder(Coder<ElementT> elementCoder) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        autoSharding,
        propagateSuccessful,
        elementCoder,
        toTableRow,
        toFailsafeTableRow,
        deterministicRecordIdFn);
  }

  StreamingWriteTables<ElementT> withToTableRow(
      SerializableFunction<ElementT, TableRow> toTableRow) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        autoSharding,
        propagateSuccessful,
        elementCoder,
        toTableRow,
        toFailsafeTableRow,
        deterministicRecordIdFn);
  }

  StreamingWriteTables<ElementT> withToFailsafeTableRow(
      SerializableFunction<ElementT, TableRow> toFailsafeTableRow) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        autoSharding,
        propagateSuccessful,
        elementCoder,
        toTableRow,
        toFailsafeTableRow,
        deterministicRecordIdFn);
  }

  StreamingWriteTables<ElementT> withDeterministicRecordIdFn(
      @Nullable SerializableFunction<ElementT, String> deterministicRecordIdFn) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        autoSharding,
        propagateSuccessful,
        elementCoder,
        toTableRow,
        toFailsafeTableRow,
        deterministicRecordIdFn);
  }

  public <TupleTagT> TupleTag<TupleTagT> getFailedRowsTupleTag() {
    return new TupleTag<>(FAILED_INSERTS_TAG_ID);
  }

  @Override
  public WriteResult expand(PCollection<KV<TableDestination, ElementT>> input) {
    Preconditions.checkStateNotNull(elementCoder);
    if (extendedErrorInfo) {
      TupleTag<BigQueryInsertError> failedInsertsTag = getFailedRowsTupleTag();
      PCollectionTuple result =
          writeAndGetErrors(
              input,
              failedInsertsTag,
              BigQueryInsertErrorCoder.of(),
              ErrorContainer.BIG_QUERY_INSERT_ERROR_ERROR_CONTAINER);
      PCollection<BigQueryInsertError> failedInserts = result.get(failedInsertsTag);
      return WriteResult.withExtendedErrors(
          input.getPipeline(),
          failedInsertsTag,
          failedInserts,
          propagateSuccessful ? result.get(BatchedStreamingWrite.SUCCESSFUL_ROWS_TAG) : null);
    } else {
      TupleTag<TableRow> failedInsertsTag = getFailedRowsTupleTag();
      PCollectionTuple result =
          writeAndGetErrors(
              input,
              failedInsertsTag,
              TableRowJsonCoder.of(),
              ErrorContainer.TABLE_ROW_ERROR_CONTAINER);
      PCollection<TableRow> failedInserts = result.get(failedInsertsTag);
      return WriteResult.in(
          input.getPipeline(),
          failedInsertsTag,
          failedInserts,
          propagateSuccessful ? result.get(BatchedStreamingWrite.SUCCESSFUL_ROWS_TAG) : null,
          null,
          null,
          null,
          null,
          null,
          null);
    }
  }

  @RequiresNonNull({"elementCoder"})
  private <T> PCollectionTuple writeAndGetErrors(
      PCollection<KV<TableDestination, ElementT>> input,
      TupleTag<T> failedInsertsTag,
      AtomicCoder<T> coder,
      ErrorContainer<T> errorContainer) {
    BigQueryOptions options = input.getPipeline().getOptions().as(BigQueryOptions.class);

    // A naive implementation would be to simply stream data directly to BigQuery.
    // However, this could occasionally lead to duplicated data, e.g., when
    // a VM that runs this code is restarted and the code is re-run.

    // The above risk is mitigated in this implementation by relying on
    // BigQuery built-in best effort de-dup mechanism.
    // To use this mechanism, each input TableRow is tagged with a generated
    // unique id, which is then passed to BigQuery and used to ignore duplicates.

    // To prevent having the same TableRow processed more than once with regenerated
    // different unique ids, this implementation relies on "checkpointing", which is
    // achieved as a side effect of having BigQuery insertion immediately follow a GBK.

    if (autoSharding && deterministicRecordIdFn == null) {
      // If runner determined dynamic sharding is enabled, group TableRows on table destinations
      // that may be sharded during the runtime. Otherwise, we choose a fixed number of shards per
      // table destination following the logic below in the other branch.
      PCollection<KV<String, TableRowInfo<ElementT>>> unshardedTagged =
          input
              .apply(
                  "MapToTableSpec",
                  MapElements.via(
                      new SimpleFunction<KV<TableDestination, ElementT>, KV<String, ElementT>>() {
                        @Override
                        public KV<String, ElementT> apply(KV<TableDestination, ElementT> input) {
                          return KV.of(input.getKey().getTableSpec(), input.getValue());
                        }
                      }))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), elementCoder))
              .apply("TagWithUniqueIds", ParDo.of(new TagWithUniqueIds<>()))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), TableRowInfoCoder.of(elementCoder)));

      // Auto-sharding is achieved via GroupIntoBatches.WithShardedKey transform which groups and at
      // the same time batches the TableRows to be inserted to BigQuery.
      return unshardedTagged.apply(
          "StreamingWrite",
          new BatchedStreamingWrite<>(
                  bigQueryServices,
                  retryPolicy,
                  failedInsertsTag,
                  coder,
                  errorContainer,
                  skipInvalidRows,
                  ignoreUnknownValues,
                  ignoreInsertIds,
                  propagateSuccessful,
                  toTableRow,
                  toFailsafeTableRow)
              .viaStateful());
    } else {
      // We create 50 keys per BigQuery table to generate output on. This is few enough that we
      // get good batching into BigQuery's insert calls, and enough that we can max out the
      // streaming insert quota.
      int numShards = options.getNumStreamingKeys();
      PCollection<KV<ShardedKey<String>, TableRowInfo<ElementT>>> shardedTagged =
          input
              .apply("ShardTableWrites", ParDo.of(new GenerateShardedTable<>(numShards)))
              .setCoder(KvCoder.of(ShardedKeyCoder.of(StringUtf8Coder.of()), elementCoder))
              .apply("TagWithUniqueIds", ParDo.of(new TagWithUniqueIds<>(deterministicRecordIdFn)))
              .setCoder(
                  KvCoder.of(
                      ShardedKeyCoder.of(StringUtf8Coder.of()),
                      TableRowInfoCoder.of(elementCoder)));

      if (deterministicRecordIdFn == null) {
        // If not using a deterministic function for record ids, we must apply a reshuffle to ensure
        // determinism on the generated ids.
        shardedTagged = shardedTagged.apply(Reshuffle.of());
      }

      return shardedTagged
          // Put in the global window to ensure that DynamicDestinations side inputs are
          // accessed
          // correctly.
          .apply(
              "GlobalWindow",
              Window.<KV<ShardedKey<String>, TableRowInfo<ElementT>>>into(new GlobalWindows())
                  .triggering(DefaultTrigger.of())
                  .discardingFiredPanes())
          .apply(
              "StripShardId",
              MapElements.via(
                  new SimpleFunction<
                      KV<ShardedKey<String>, TableRowInfo<ElementT>>,
                      KV<String, TableRowInfo<ElementT>>>() {
                    @Override
                    public KV<String, TableRowInfo<ElementT>> apply(
                        KV<ShardedKey<String>, TableRowInfo<ElementT>> input) {
                      return KV.of(input.getKey().getKey(), input.getValue());
                    }
                  }))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), TableRowInfoCoder.of(elementCoder)))
          // Also batch the TableRows in a best effort manner via bundle finalization before
          // inserting to BigQuery.
          .apply(
              "StreamingWrite",
              new BatchedStreamingWrite<>(
                      bigQueryServices,
                      retryPolicy,
                      failedInsertsTag,
                      coder,
                      errorContainer,
                      skipInvalidRows,
                      ignoreUnknownValues,
                      ignoreInsertIds,
                      propagateSuccessful,
                      toTableRow,
                      toFailsafeTableRow)
                  .viaDoFnFinalization());
    }
  }
}
