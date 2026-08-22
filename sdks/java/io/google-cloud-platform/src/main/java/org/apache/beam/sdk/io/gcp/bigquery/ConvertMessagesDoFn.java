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
import com.google.cloud.bigquery.storage.v1.TableSchema;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * DoFn that interacts with the StorageApiDynamicDestinations instance to convert messages to
 * StorageApiWritePaylod. Messages that fail to convert are routed to the dead-letter PCollection.
 * If schemaUpdateOptions are set, then messages that fail to convert due to missing columns are
 * routed to a buffering transform that holds them until the table's schema has been updated.
 */
public class ConvertMessagesDoFn<DestinationT extends @NonNull Object, ElementT>
    extends DoFn<KV<DestinationT, ElementT>, KV<DestinationT, StorageApiWritePayload>> {
  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private TwoLevelMessageConverterCache<DestinationT, ElementT> messageConverters;
  private final BigQueryServices bqServices;
  private final TupleTag<BigQueryStorageApiInsertError> failedWritesTag;
  private final TupleTag<KV<DestinationT, StorageApiWritePayload>> successfulWritesTag;
  private final TupleTag<KV<@NonNull DestinationT, TableSchema>> patchTableSchemaTag;
  private final TupleTag<KV<@NonNull DestinationT, ElementT>> retryElementsWaitingForSchemaTag;
  private final @Nullable SerializableFunction<ElementT, RowMutationInformation> rowMutationFn;
  private final BadRecordRouter badRecordRouter;
  private final Coder<KV<DestinationT, ElementT>> elementCoder;
  private final Map<DestinationT, BufferedCollectorInformation> errorCollectors = Maps.newHashMap();
  private final boolean hasSchemaUpdateOptions;
  private transient BigQueryServices.@Nullable DatasetService datasetServiceInternal = null;
  private transient BigQueryServices.@Nullable WriteStreamService writeStreamServiceInternal = null;

  /** Keeps track of schema errors seen while converting messages. */
  static final class BufferedCollectorInformation {
    TableRowToStorageApiProto.@Nullable ErrorCollector collector = null;
    final TableSchema schema;
    Instant timestamp;

    public BufferedCollectorInformation(TableSchema schema) {
      this.schema = schema;
      this.timestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
    }

    void addCollector(TableRowToStorageApiProto.ErrorCollector collector, Instant ts)
        throws TableRowToStorageApiProto.SchemaConversionException {
      if (this.collector == null) {
        this.collector = collector;
      } else {
        this.collector.mergeInto(collector);
      }
      if (ts.isBefore(this.timestamp)) {
        this.timestamp = ts;
      }
    }
  }

  ConvertMessagesDoFn(
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      BigQueryServices bqServices,
      String operationName,
      TupleTag<BigQueryStorageApiInsertError> failedWritesTag,
      TupleTag<KV<DestinationT, StorageApiWritePayload>> successfulWritesTag,
      TupleTag<KV<@NonNull DestinationT, TableSchema>> patchTableSchemaTag,
      TupleTag<KV<@NonNull DestinationT, ElementT>> retryElementsWaitingForSchemaTag,
      @Nullable SerializableFunction<ElementT, RowMutationInformation> rowMutationFn,
      BadRecordRouter badRecordRouter,
      Coder<KV<DestinationT, ElementT>> elementCoder,
      boolean hasSchemaUpdateOptions) {
    this.dynamicDestinations = dynamicDestinations;
    this.messageConverters = new TwoLevelMessageConverterCache<>(operationName);
    this.bqServices = bqServices;
    this.failedWritesTag = failedWritesTag;
    this.successfulWritesTag = successfulWritesTag;
    this.patchTableSchemaTag = patchTableSchemaTag;
    this.retryElementsWaitingForSchemaTag = retryElementsWaitingForSchemaTag;
    this.rowMutationFn = rowMutationFn;
    this.badRecordRouter = badRecordRouter;
    this.elementCoder = elementCoder;
    this.hasSchemaUpdateOptions = hasSchemaUpdateOptions;
  }

  BigQueryServices.DatasetService getDatasetService(PipelineOptions pipelineOptions)
      throws IOException {
    if (datasetServiceInternal == null) {
      datasetServiceInternal =
          bqServices.getDatasetService(pipelineOptions.as(BigQueryOptions.class));
    }
    return datasetServiceInternal;
  }

  BigQueryServices.WriteStreamService getWriteStreamService(PipelineOptions pipelineOptions)
      throws IOException {
    if (writeStreamServiceInternal == null) {
      writeStreamServiceInternal =
          bqServices.getWriteStreamService(pipelineOptions.as(BigQueryOptions.class));
    }
    return writeStreamServiceInternal;
  }

  StorageApiDynamicDestinations<ElementT, DestinationT> getDynamicDestinations() {
    return dynamicDestinations;
  }

  TwoLevelMessageConverterCache<DestinationT, ElementT> getMessageConverters() {
    return messageConverters;
  }

  @Teardown
  public void onTeardown() {
    try {
      if (datasetServiceInternal != null) {
        datasetServiceInternal.close();
        datasetServiceInternal = null;
      }
      if (writeStreamServiceInternal != null) {
        writeStreamServiceInternal.close();
        writeStreamServiceInternal = null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @StartBundle
  public void startBundle() {
    this.errorCollectors.clear();
  }

  @ProcessElement
  public void processElement(
      ProcessContext c,
      PipelineOptions pipelineOptions,
      @Element KV<DestinationT, ElementT> element,
      @Timestamp Instant timestamp,
      MultiOutputReceiver o)
      throws Exception {
    DestinationT destination = element.getKey();

    dynamicDestinations.setSideInputAccessorFromProcessContext(c);
    // Should we do this across the entire bundle instead? Unfortunately that doesn't work because
    // we can't access
    // side inputs in finishBundle.
    StorageApiDynamicDestinations.MessageConverter<ElementT> messageConverter =
        messageConverters.get(
            destination,
            dynamicDestinations,
            pipelineOptions,
            getDatasetService(pipelineOptions),
            getWriteStreamService(pipelineOptions));
    TableRowToStorageApiProto.ErrorCollector errorCollector =
        hasSchemaUpdateOptions
            ? UpgradeTableSchema.newErrorCollector()
            : TableRowToStorageApiProto.ErrorCollector.DONT_COLLECT;
    Iterable<TimestampedValue<KV<DestinationT, ElementT>>> unProcessed =
        handleProcessElements(
            messageConverter,
            ImmutableList.of(TimestampedValue.of(element, timestamp)),
            o,
            errorCollector);
    if (!errorCollector.isEmpty()) {
      org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState(
          hasSchemaUpdateOptions);

      // Track all errors. Generate schema-update message in finishBundle.
      BufferedCollectorInformation bufferedCollectorInformation =
          errorCollectors.computeIfAbsent(
              destination,
              d -> new BufferedCollectorInformation(messageConverter.getTableSchema()));
      bufferedCollectorInformation.addCollector(errorCollector, timestamp);

      // Forward the message to the buffering stage to wait for the schema to be updated.
      unProcessed.forEach(
          tv ->
              o.get(retryElementsWaitingForSchemaTag)
                  .outputWithTimestamp(tv.getValue(), tv.getTimestamp()));
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) throws Exception {
    if (!errorCollectors.isEmpty()) {
      for (Map.Entry<DestinationT, BufferedCollectorInformation> e : errorCollectors.entrySet()) {
        if (e.getValue().collector != null) {
          c.output(
              patchTableSchemaTag,
              KV.of(
                  e.getKey(),
                  UpgradeTableSchema.getIncrementalSchema(
                      e.getValue().collector, e.getValue().schema)),
              e.getValue().timestamp,
              GlobalWindow.INSTANCE);
        }
      }
    }
    errorCollectors.clear();
  }

  public Iterable<TimestampedValue<KV<DestinationT, ElementT>>> handleProcessElements(
      StorageApiDynamicDestinations.MessageConverter<ElementT> messageConverter,
      Iterable<TimestampedValue<KV<DestinationT, ElementT>>> values,
      MultiOutputReceiver o,
      TableRowToStorageApiProto.ErrorCollector errorCollector)
      throws Exception {
    List<TimestampedValue<KV<DestinationT, ElementT>>> unprocessedElements = Lists.newArrayList();

    for (TimestampedValue<KV<DestinationT, ElementT>> value : values) {
      DestinationT destination = value.getValue().getKey();
      ElementT element = value.getValue().getValue();
      Instant timestamp = value.getTimestamp();

      RowMutationInformation rowMutationInformation = null;
      if (rowMutationFn != null) {
        rowMutationInformation = Preconditions.checkStateNotNull(rowMutationFn).apply(element);
      }
      try {
        StorageApiWritePayload payload =
            messageConverter
                .toMessage(element, rowMutationInformation, errorCollector)
                .withTimestamp(timestamp);
        if (errorCollector.isEmpty()) {
          o.get(successfulWritesTag).outputWithTimestamp(KV.of(destination, payload), timestamp);
        } else {
          // This should only happen if the user sets schemaUpdateOptions.
          unprocessedElements.add(value);
        }
      } catch (TableRowToStorageApiProto.SchemaConversionException conversionException) {
        // If the user did not set schemaUpdateOptions or if we encounter an error we cannot address
        // via schema update,
        // the error is caught here.
        TableRow failsafeTableRow;
        try {
          failsafeTableRow = messageConverter.toFailsafeTableRow(element);
        } catch (Exception e) {
          badRecordRouter.route(
              o,
              KV.of(destination, element),
              elementCoder,
              e,
              "Unable to convert value to TableRow");
          continue;
        }
        TableReference tableReference = null;
        TableDestination tableDestination = dynamicDestinations.getTable(destination);
        if (tableDestination != null) {
          tableReference = tableDestination.getTableReference();
        }
        o.get(failedWritesTag)
            .outputWithTimestamp(
                new BigQueryStorageApiInsertError(
                    failsafeTableRow, conversionException.toString(), tableReference),
                timestamp);
      } catch (Exception e) {
        badRecordRouter.route(
            o,
            KV.of(destination, element),
            elementCoder,
            e,
            "Unable to convert value to StorageWriteApiPayload");
      }
    }
    return unprocessedElements;
  }
}
