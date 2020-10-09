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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

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
  private final Coder<ElementT> elementCoder;
  private final SerializableFunction<ElementT, TableRow> toTableRow;
  private final SerializableFunction<ElementT, TableRow> toFailsafeTableRow;

  public StreamingWriteTables() {
    this(
        new BigQueryServicesImpl(),
        InsertRetryPolicy.alwaysRetry(),
        false, // extendedErrorInfo
        false, // skipInvalidRows
        false, // ignoreUnknownValues
        false, // ignoreInsertIds
        null, // elementCoder
        null, // toTableRow
        null); // toFailsafeTableRow
  }

  private StreamingWriteTables(
      BigQueryServices bigQueryServices,
      InsertRetryPolicy retryPolicy,
      boolean extendedErrorInfo,
      boolean skipInvalidRows,
      boolean ignoreUnknownValues,
      boolean ignoreInsertIds,
      Coder<ElementT> elementCoder,
      SerializableFunction<ElementT, TableRow> toTableRow,
      SerializableFunction<ElementT, TableRow> toFailsafeTableRow) {
    this.bigQueryServices = bigQueryServices;
    this.retryPolicy = retryPolicy;
    this.extendedErrorInfo = extendedErrorInfo;
    this.skipInvalidRows = skipInvalidRows;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.ignoreInsertIds = ignoreInsertIds;
    this.elementCoder = elementCoder;
    this.toTableRow = toTableRow;
    this.toFailsafeTableRow = toFailsafeTableRow;
  }

  StreamingWriteTables<ElementT> withTestServices(BigQueryServices bigQueryServices) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        elementCoder,
        toTableRow,
        toFailsafeTableRow);
  }

  StreamingWriteTables<ElementT> withInsertRetryPolicy(InsertRetryPolicy retryPolicy) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        elementCoder,
        toTableRow,
        toFailsafeTableRow);
  }

  StreamingWriteTables<ElementT> withExtendedErrorInfo(boolean extendedErrorInfo) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        elementCoder,
        toTableRow,
        toFailsafeTableRow);
  }

  StreamingWriteTables<ElementT> withSkipInvalidRows(boolean skipInvalidRows) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        elementCoder,
        toTableRow,
        toFailsafeTableRow);
  }

  StreamingWriteTables<ElementT> withIgnoreUnknownValues(boolean ignoreUnknownValues) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        elementCoder,
        toTableRow,
        toFailsafeTableRow);
  }

  StreamingWriteTables<ElementT> withIgnoreInsertIds(boolean ignoreInsertIds) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        elementCoder,
        toTableRow,
        toFailsafeTableRow);
  }

  StreamingWriteTables<ElementT> withElementCoder(Coder<ElementT> elementCoder) {
    return new StreamingWriteTables<>(
        bigQueryServices,
        retryPolicy,
        extendedErrorInfo,
        skipInvalidRows,
        ignoreUnknownValues,
        ignoreInsertIds,
        elementCoder,
        toTableRow,
        toFailsafeTableRow);
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
        elementCoder,
        toTableRow,
        toFailsafeTableRow);
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
        elementCoder,
        toTableRow,
        toFailsafeTableRow);
  }

  @Override
  public WriteResult expand(PCollection<KV<TableDestination, ElementT>> input) {
    if (extendedErrorInfo) {
      TupleTag<BigQueryInsertError> failedInsertsTag = new TupleTag<>(FAILED_INSERTS_TAG_ID);
      PCollection<BigQueryInsertError> failedInserts =
          writeAndGetErrors(
              input,
              failedInsertsTag,
              BigQueryInsertErrorCoder.of(),
              ErrorContainer.BIG_QUERY_INSERT_ERROR_ERROR_CONTAINER);
      return WriteResult.withExtendedErrors(input.getPipeline(), failedInsertsTag, failedInserts);
    } else {
      TupleTag<TableRow> failedInsertsTag = new TupleTag<>(FAILED_INSERTS_TAG_ID);
      PCollection<TableRow> failedInserts =
          writeAndGetErrors(
              input,
              failedInsertsTag,
              TableRowJsonCoder.of(),
              ErrorContainer.TABLE_ROW_ERROR_CONTAINER);
      return WriteResult.in(input.getPipeline(), failedInsertsTag, failedInserts);
    }
  }

  private <T> PCollection<T> writeAndGetErrors(
      PCollection<KV<TableDestination, ElementT>> input,
      TupleTag<T> failedInsertsTag,
      AtomicCoder<T> coder,
      ErrorContainer<T> errorContainer) {
    BigQueryOptions options = input.getPipeline().getOptions().as(BigQueryOptions.class);
    int numShards = options.getNumStreamingKeys();

    // A naive implementation would be to simply stream data directly to BigQuery.
    // However, this could occasionally lead to duplicated data, e.g., when
    // a VM that runs this code is restarted and the code is re-run.

    // The above risk is mitigated in this implementation by relying on
    // BigQuery built-in best effort de-dup mechanism.

    // To use this mechanism, each input TableRow is tagged with a generated
    // unique id, which is then passed to BigQuery and used to ignore duplicates
    // We create 50 keys per BigQuery table to generate output on. This is few enough that we
    // get good batching into BigQuery's insert calls, and enough that we can max out the
    // streaming insert quota.
    PCollection<KV<ShardedKey<String>, TableRowInfo<ElementT>>> tagged =
        input
            .apply("ShardTableWrites", ParDo.of(new GenerateShardedTable<>(numShards)))
            .setCoder(KvCoder.of(ShardedKeyCoder.of(StringUtf8Coder.of()), elementCoder))
            .apply("TagWithUniqueIds", ParDo.of(new TagWithUniqueIds<>()))
            .setCoder(
                KvCoder.of(
                    ShardedKeyCoder.of(StringUtf8Coder.of()), TableRowInfoCoder.of(elementCoder)));

    TupleTag<Void> mainOutputTag = new TupleTag<>("mainOutput");

    // To prevent having the same TableRow processed more than once with regenerated
    // different unique ids, this implementation relies on "checkpointing", which is
    // achieved as a side effect of having StreamingWriteFn immediately follow a GBK,
    // performed by Reshuffle.
    PCollectionTuple tuple =
        tagged
            .apply(Reshuffle.of())
            // Put in the global window to ensure that DynamicDestinations side inputs are accessed
            // correctly.
            .apply(
                "GlobalWindow",
                Window.<KV<ShardedKey<String>, TableRowInfo<ElementT>>>into(new GlobalWindows())
                    .triggering(DefaultTrigger.of())
                    .discardingFiredPanes())
            .apply(
                "StreamingWrite",
                ParDo.of(
                        new StreamingWriteFn<>(
                            bigQueryServices,
                            retryPolicy,
                            failedInsertsTag,
                            errorContainer,
                            skipInvalidRows,
                            ignoreUnknownValues,
                            ignoreInsertIds,
                            toTableRow,
                            toFailsafeTableRow))
                    .withOutputTags(mainOutputTag, TupleTagList.of(failedInsertsTag)));
    PCollection<T> failedInserts = tuple.get(failedInsertsTag);
    failedInserts.setCoder(coder);
    return failedInserts;
  }
}
