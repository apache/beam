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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.Exceptions.StreamFinalizedException;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.bigquery.storage.v1.WriteStream.Type;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StreamAppendClient;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.RetryType;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiFlushAndFinalizeDoFn.Operation;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A transform to write sharded records to BigQuery using the Storage API (Streaming). */
@SuppressWarnings({
  "FutureReturnValueIgnored",
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class StorageApiWritesShardedRecords<DestinationT extends @NonNull Object, ElementT>
    extends PTransform<
        PCollection<KV<ShardedKey<DestinationT>, Iterable<StorageApiWritePayload>>>,
        PCollectionTuple> {
  private static final Logger LOG = LoggerFactory.getLogger(StorageApiWritesShardedRecords.class);
  private static final Duration DEFAULT_STREAM_IDLE_TIME = Duration.standardHours(1);

  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private final CreateDisposition createDisposition;
  private final String kmsKey;
  private final BigQueryServices bqServices;
  private final Coder<DestinationT> destinationCoder;
  private final Coder<BigQueryStorageApiInsertError> failedRowsCoder;
  private final boolean autoUpdateSchema;
  private final boolean ignoreUnknownValues;
  private final AppendRowsRequest.MissingValueInterpretation defaultMissingValueInterpretation;

  private final Duration streamIdleTime = DEFAULT_STREAM_IDLE_TIME;
  private final TupleTag<BigQueryStorageApiInsertError> failedRowsTag;
  private final @Nullable TupleTag<TableRow> successfulRowsTag;
  private final Coder<TableRow> succussfulRowsCoder;

  private final TupleTag<KV<String, Operation>> flushTag = new TupleTag<>("flushTag");
  private static final ExecutorService closeWriterExecutor = Executors.newCachedThreadPool();

  // Context passed into RetryManager for each call.
  class AppendRowsContext extends RetryManager.Operation.Context<AppendRowsResponse> {
    final ShardedKey<DestinationT> key;
    String streamName = "";
    @Nullable StreamAppendClient client = null;
    long offset = -1;
    long numRows = 0;
    long tryIteration = 0;
    ProtoRows protoRows;

    List<org.joda.time.Instant> timestamps;

    AppendRowsContext(
        ShardedKey<DestinationT> key, ProtoRows protoRows, List<org.joda.time.Instant> timestamps) {
      this.key = key;
      this.protoRows = protoRows;
      this.timestamps = timestamps;
    }

    @Override
    public String toString() {
      return "Context: key="
          + key
          + " streamName="
          + streamName
          + " offset="
          + offset
          + " numRows="
          + numRows
          + " tryIteration: "
          + tryIteration;
    }
  };

  private static final Cache<ShardedKey<?>, AppendClientInfo> APPEND_CLIENTS =
      CacheBuilder.newBuilder()
          .expireAfterAccess(5, TimeUnit.MINUTES)
          .removalListener(
              (RemovalNotification<ShardedKey<?>, AppendClientInfo> removal) -> {
                final @Nullable AppendClientInfo appendClientInfo = removal.getValue();
                if (appendClientInfo != null) {
                  appendClientInfo.close();
                }
              })
          .build();

  static void clearCache() {
    APPEND_CLIENTS.invalidateAll();
  }

  // Run a closure asynchronously, ignoring failures.
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static void runAsyncIgnoreFailure(ExecutorService executor, ThrowingRunnable task) {
    executor.submit(
        () -> {
          try {
            task.run();
          } catch (Exception e) {
            String msg =
                e.toString()
                    + "\n"
                    + Arrays.stream(e.getStackTrace())
                        .map(StackTraceElement::toString)
                        .collect(Collectors.joining("\n"));
            System.err.println("Exception happened while executing async task. Ignoring: " + msg);
          }
        });
  }

  public StorageApiWritesShardedRecords(
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      CreateDisposition createDisposition,
      String kmsKey,
      BigQueryServices bqServices,
      Coder<DestinationT> destinationCoder,
      Coder<BigQueryStorageApiInsertError> failedRowsCoder,
      Coder<TableRow> successfulRowsCoder,
      TupleTag<BigQueryStorageApiInsertError> failedRowsTag,
      @Nullable TupleTag<TableRow> successfulRowsTag,
      boolean autoUpdateSchema,
      boolean ignoreUnknownValues,
      AppendRowsRequest.MissingValueInterpretation defaultMissingValueInterpretation) {
    this.dynamicDestinations = dynamicDestinations;
    this.createDisposition = createDisposition;
    this.kmsKey = kmsKey;
    this.bqServices = bqServices;
    this.destinationCoder = destinationCoder;
    this.failedRowsCoder = failedRowsCoder;
    this.failedRowsTag = failedRowsTag;
    this.successfulRowsTag = successfulRowsTag;
    this.succussfulRowsCoder = successfulRowsCoder;
    this.autoUpdateSchema = autoUpdateSchema;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.defaultMissingValueInterpretation = defaultMissingValueInterpretation;
  }

  @Override
  public PCollectionTuple expand(
      PCollection<KV<ShardedKey<DestinationT>, Iterable<StorageApiWritePayload>>> input) {
    BigQueryOptions bigQueryOptions = input.getPipeline().getOptions().as(BigQueryOptions.class);
    final long splitSize = bigQueryOptions.getStorageApiAppendThresholdBytes();
    final long maxRequestSize = bigQueryOptions.getStorageWriteApiMaxRequestSize();

    String operationName = input.getName() + "/" + getName();
    TupleTagList tupleTagList = TupleTagList.of(failedRowsTag);
    if (successfulRowsTag != null) {
      tupleTagList = tupleTagList.and(successfulRowsTag);
    }
    // Append records to the Storage API streams.
    PCollectionTuple writeRecordsResult =
        input.apply(
            "Write Records",
            ParDo.of(new WriteRecordsDoFn(operationName, streamIdleTime, splitSize, maxRequestSize))
                .withSideInputs(dynamicDestinations.getSideInputs())
                .withOutputTags(flushTag, tupleTagList));

    SchemaCoder<Operation> operationCoder;
    try {
      SchemaRegistry schemaRegistry = input.getPipeline().getSchemaRegistry();
      operationCoder =
          SchemaCoder.of(
              schemaRegistry.getSchema(Operation.class),
              TypeDescriptor.of(Operation.class),
              schemaRegistry.getToRowFunction(Operation.class),
              schemaRegistry.getFromRowFunction(Operation.class));
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }

    // Send all successful writes to be flushed.
    writeRecordsResult
        .get(flushTag)
        .setCoder(KvCoder.of(StringUtf8Coder.of(), operationCoder))
        .apply(
            Window.<KV<String, Operation>>configure()
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(1))))
                .discardingFiredPanes())
        .apply("maxFlushPosition", Combine.perKey(Max.naturalOrder(new Operation(-1, false))))
        .apply(
            "Flush and finalize writes", ParDo.of(new StorageApiFlushAndFinalizeDoFn(bqServices)));
    writeRecordsResult.get(failedRowsTag).setCoder(failedRowsCoder);
    if (successfulRowsTag != null) {
      writeRecordsResult.get(successfulRowsTag).setCoder(succussfulRowsCoder);
    }
    return writeRecordsResult;
  }

  class WriteRecordsDoFn
      extends DoFn<
          KV<ShardedKey<DestinationT>, Iterable<StorageApiWritePayload>>, KV<String, Operation>> {
    private final Counter recordsAppended =
        Metrics.counter(WriteRecordsDoFn.class, "recordsAppended");
    private final Counter streamsCreated =
        Metrics.counter(WriteRecordsDoFn.class, "streamsCreated");
    private final Counter streamsIdle =
        Metrics.counter(WriteRecordsDoFn.class, "idleStreamsFinalized");
    private final Counter appendFailures =
        Metrics.counter(WriteRecordsDoFn.class, "appendFailures");
    private final Counter appendOffsetFailures =
        Metrics.counter(WriteRecordsDoFn.class, "appendOffsetFailures");
    private final Counter flushesScheduled =
        Metrics.counter(WriteRecordsDoFn.class, "flushesScheduled");
    private final Distribution appendLatencyDistribution =
        Metrics.distribution(WriteRecordsDoFn.class, "appendLatencyDistributionMs");
    private final Distribution appendSizeDistribution =
        Metrics.distribution(WriteRecordsDoFn.class, "appendSizeDistribution");
    private final Distribution appendSplitDistribution =
        Metrics.distribution(WriteRecordsDoFn.class, "appendSplitDistribution");
    private final Counter rowsSentToFailedRowsCollection =
        Metrics.counter(WriteRecordsDoFn.class, "rowsSentToFailedRowsCollection");

    private TwoLevelMessageConverterCache<DestinationT, ElementT> messageConverters;

    private Map<DestinationT, TableDestination> destinations = Maps.newHashMap();

    private transient @Nullable DatasetService datasetServiceInternal = null;

    // Stores the current stream for this key.
    @StateId("streamName")
    private final StateSpec<ValueState<String>> streamNameSpec = StateSpecs.value();

    // Stores the current stream offset.
    @StateId("streamOffset")
    private final StateSpec<ValueState<Long>> streamOffsetSpec = StateSpecs.value();

    @StateId("updatedSchema")
    private final StateSpec<ValueState<TableSchema>> updatedSchema =
        StateSpecs.value(ProtoCoder.of(TableSchema.class));

    @TimerId("idleTimer")
    private final TimerSpec idleTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    private final Duration streamIdleTime;
    private final long splitSize;
    private final long maxRequestSize;

    public WriteRecordsDoFn(
        String operationName, Duration streamIdleTime, long splitSize, long maxRequestSize) {
      this.messageConverters = new TwoLevelMessageConverterCache<>(operationName);
      this.streamIdleTime = streamIdleTime;
      this.splitSize = splitSize;
      this.maxRequestSize = maxRequestSize;
    }

    @StartBundle
    public void startBundle() throws IOException {
      destinations = Maps.newHashMap();
    }

    // Get the current stream for this key. If there is no current stream, create one and store the
    // stream name in persistent state.
    String getOrCreateStream(
        String tableId,
        ValueState<String> streamName,
        ValueState<Long> streamOffset,
        Timer streamIdleTimer,
        DatasetService datasetService,
        Callable<Boolean> tryCreateTable) {
      try {
        final @Nullable String streamValue = streamName.read();
        AtomicReference<String> stream = new AtomicReference<>();
        if (streamValue == null || "".equals(streamValue)) {
          // In a buffered stream, data is only visible up to the offset to which it was flushed.
          CreateTableHelpers.createTableWrapper(
              () -> {
                stream.set(datasetService.createWriteStream(tableId, Type.BUFFERED).getName());
                return null;
              },
              tryCreateTable);

          streamName.write(stream.get());
          streamOffset.write(0L);
          streamsCreated.inc();
        } else {
          stream.set(streamValue);
        }
        // Reset the idle timer.
        streamIdleTimer.offset(streamIdleTime).withNoOutputTimestamp().setRelative();

        return stream.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private DatasetService getDatasetService(PipelineOptions pipelineOptions) throws IOException {
      if (datasetServiceInternal == null) {
        datasetServiceInternal =
            bqServices.getDatasetService(pipelineOptions.as(BigQueryOptions.class));
      }
      return datasetServiceInternal;
    }

    @Teardown
    public void onTeardown() {
      try {
        if (datasetServiceInternal != null) {
          datasetServiceInternal.close();
          datasetServiceInternal = null;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @ProcessElement
    public void process(
        ProcessContext c,
        final PipelineOptions pipelineOptions,
        @Element KV<ShardedKey<DestinationT>, Iterable<StorageApiWritePayload>> element,
        @Timestamp org.joda.time.Instant elementTs,
        final @AlwaysFetched @StateId("streamName") ValueState<String> streamName,
        final @AlwaysFetched @StateId("streamOffset") ValueState<Long> streamOffset,
        final @StateId("updatedSchema") ValueState<TableSchema> updatedSchema,
        @TimerId("idleTimer") Timer idleTimer,
        final MultiOutputReceiver o)
        throws Exception {
      BigQueryOptions bigQueryOptions = pipelineOptions.as(BigQueryOptions.class);

      if (autoUpdateSchema) {
        updatedSchema.readLater();
      }

      dynamicDestinations.setSideInputAccessorFromProcessContext(c);
      TableDestination tableDestination =
          destinations.computeIfAbsent(
              element.getKey().getKey(),
              dest -> {
                TableDestination tableDestination1 = dynamicDestinations.getTable(dest);
                checkArgument(
                    tableDestination1 != null,
                    "DynamicDestinations.getTable() may not return null, "
                        + "but %s returned null for destination %s",
                    dynamicDestinations,
                    dest);
                return tableDestination1;
              });
      final String tableId = tableDestination.getTableUrn(bigQueryOptions);
      final String shortTableId = tableDestination.getShortTableUrn();
      final DatasetService datasetService = getDatasetService(pipelineOptions);

      Coder<DestinationT> destinationCoder = dynamicDestinations.getDestinationCoder();
      Callable<Boolean> tryCreateTable =
          () -> {
            DestinationT dest = element.getKey().getKey();
            CreateTableHelpers.possiblyCreateTable(
                c.getPipelineOptions().as(BigQueryOptions.class),
                tableDestination,
                () -> dynamicDestinations.getSchema(dest),
                () -> dynamicDestinations.getTableConstraints(dest),
                createDisposition,
                destinationCoder,
                kmsKey,
                bqServices);
            return true;
          };

      Supplier<String> getOrCreateStream =
          () ->
              getOrCreateStream(
                  tableId, streamName, streamOffset, idleTimer, datasetService, tryCreateTable);
      Callable<AppendClientInfo> getAppendClientInfo =
          () -> {
            @Nullable TableSchema tableSchema;
            DescriptorProtos.DescriptorProto descriptor;
            TableSchema updatedSchemaValue = updatedSchema.read();
            if (autoUpdateSchema && updatedSchemaValue != null) {
              // We've seen an updated schema, so we use that instead of querying the
              // MessageConverter.
              tableSchema = updatedSchemaValue;
              descriptor =
                  TableRowToStorageApiProto.descriptorSchemaFromTableSchema(
                      tableSchema, true, false);
            } else {
              // Start off with the base schema. As we get notified of schema updates, we
              // will update the descriptor.
              StorageApiDynamicDestinations.MessageConverter<?> converter =
                  messageConverters.get(
                      element.getKey().getKey(), dynamicDestinations, datasetService);
              tableSchema = converter.getTableSchema();
              descriptor = converter.getDescriptor(false);
            }
            AppendClientInfo info =
                AppendClientInfo.of(
                        Preconditions.checkStateNotNull(tableSchema),
                        descriptor,
                        // Make sure that the client is always closed in a different thread
                        // to
                        // avoid blocking.
                        client ->
                            runAsyncIgnoreFailure(
                                closeWriterExecutor,
                                () -> {
                                  // Remove the pin that is "owned" by the cache.
                                  client.unpin();
                                  client.close();
                                }))
                    .withAppendClient(
                        datasetService,
                        getOrCreateStream,
                        false,
                        defaultMissingValueInterpretation);
            // This pin is "owned" by the cache.
            Preconditions.checkStateNotNull(info.getStreamAppendClient()).pin();
            return info;
          };

      AtomicReference<AppendClientInfo> appendClientInfo =
          new AtomicReference<>(APPEND_CLIENTS.get(element.getKey(), getAppendClientInfo));
      String currentStream = getOrCreateStream.get();
      if (!currentStream.equals(appendClientInfo.get().getStreamName())) {
        // Cached append client is inconsistent with persisted state. Throw away cached item and
        // force it to be
        // recreated.
        APPEND_CLIENTS.invalidate(element.getKey());
        appendClientInfo.set(APPEND_CLIENTS.get(element.getKey(), getAppendClientInfo));
      }

      TableSchema updatedSchemaValue = updatedSchema.read();
      if (autoUpdateSchema && updatedSchemaValue != null) {
        if (appendClientInfo.get().hasSchemaChanged(updatedSchemaValue)) {
          appendClientInfo.set(
              AppendClientInfo.of(
                  updatedSchemaValue, appendClientInfo.get().getCloseAppendClient(), false));
          APPEND_CLIENTS.invalidate(element.getKey());
          APPEND_CLIENTS.put(element.getKey(), appendClientInfo.get());
        }
      }

      // Each ProtoRows object contains at most 1MB of rows.
      // TODO: Push messageFromTableRow up to top level. That we we cans skip TableRow entirely if
      // already proto or already schema.
      Iterable<SplittingIterable.Value> messages =
          new SplittingIterable(
              element.getValue(),
              splitSize,
              (fields, ignore) -> appendClientInfo.get().encodeUnknownFields(fields, ignore),
              bytes -> appendClientInfo.get().toTableRow(bytes),
              (failedRow, errorMessage) -> {
                o.get(failedRowsTag)
                    .outputWithTimestamp(
                        new BigQueryStorageApiInsertError(failedRow.getValue(), errorMessage),
                        failedRow.getTimestamp());
                rowsSentToFailedRowsCollection.inc();
                BigQuerySinkMetrics.appendRowsRowStatusCounter(
                        BigQuerySinkMetrics.RowStatus.FAILED,
                        BigQuerySinkMetrics.PAYLOAD_TOO_LARGE,
                        shortTableId)
                    .inc(1);
              },
              autoUpdateSchema,
              ignoreUnknownValues,
              elementTs);

      // Initialize stream names and offsets for all contexts. This will be called initially, but
      // will also be called if we roll over to a new stream on a retry.
      BiConsumer<Iterable<AppendRowsContext>, Boolean> initializeContexts =
          (contexts, isFailure) -> {
            try {
              if (isFailure) {
                // Clear the stream name, forcing a new one to be created.
                streamName.write("");
              }
              appendClientInfo.set(
                  appendClientInfo
                      .get()
                      .withAppendClient(
                          datasetService,
                          getOrCreateStream,
                          false,
                          defaultMissingValueInterpretation));
              StreamAppendClient streamAppendClient =
                  Preconditions.checkArgumentNotNull(
                      appendClientInfo.get().getStreamAppendClient());
              String streamNameRead = Preconditions.checkArgumentNotNull(streamName.read());
              long currentOffset = Preconditions.checkArgumentNotNull(streamOffset.read());
              for (AppendRowsContext context : contexts) {
                context.streamName = streamNameRead;
                streamAppendClient.pin();
                context.client = appendClientInfo.get().getStreamAppendClient();
                context.offset = currentOffset;
                ++context.tryIteration;
                currentOffset = context.offset + context.protoRows.getSerializedRowsCount();
              }
              streamOffset.write(currentOffset);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          };

      Consumer<Iterable<AppendRowsContext>> clearClients =
          contexts -> {
            APPEND_CLIENTS.invalidate(element.getKey());
            appendClientInfo.set(appendClientInfo.get().withNoAppendClient());
            APPEND_CLIENTS.put(element.getKey(), appendClientInfo.get());
            for (AppendRowsContext context : contexts) {
              if (context.client != null) {
                // Unpin in a different thread, as it may execute a blocking close.
                runAsyncIgnoreFailure(closeWriterExecutor, context.client::unpin);
                context.client = null;
              }
            }
          };

      Function<AppendRowsContext, ApiFuture<AppendRowsResponse>> runOperation =
          context -> {
            if (context.protoRows.getSerializedRowsCount() == 0) {
              // This might happen if all rows in a batch failed and were sent to the failed-rows
              // PCollection.
              return ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build());
            }
            try {
              appendClientInfo.set(
                  appendClientInfo
                      .get()
                      .withAppendClient(
                          datasetService,
                          getOrCreateStream,
                          false,
                          defaultMissingValueInterpretation));
              return Preconditions.checkStateNotNull(appendClientInfo.get().getStreamAppendClient())
                  .appendRows(context.offset, context.protoRows);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          };

      Function<Iterable<AppendRowsContext>, RetryType> onError =
          failedContexts -> {
            // The first context is always the one that fails.
            AppendRowsContext failedContext =
                Preconditions.checkStateNotNull(Iterables.getFirst(failedContexts, null));
            BigQuerySinkMetrics.reportFailedRPCMetrics(
                failedContext, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS);
            String errorCode =
                BigQuerySinkMetrics.throwableToGRPCCodeString(failedContext.getError());

            // AppendSerializationError means that BigQuery detected errors on individual rows, e.g.
            // a row not conforming
            // to bigQuery invariants. These errors are persistent, so we redirect those rows to the
            // failedInserts
            // PCollection, and retry with the remaining rows.
            if (failedContext.getError() != null
                && failedContext.getError() instanceof Exceptions.AppendSerializtionError) {
              Exceptions.AppendSerializtionError error =
                  Preconditions.checkArgumentNotNull(
                      (Exceptions.AppendSerializtionError) failedContext.getError());

              Set<Integer> failedRowIndices = error.getRowIndexToErrorMessage().keySet();
              for (int failedIndex : failedRowIndices) {
                // Convert the message to a TableRow and send it to the failedRows collection.
                ByteString protoBytes = failedContext.protoRows.getSerializedRows(failedIndex);
                TableRow failedRow = appendClientInfo.get().toTableRow(protoBytes);
                org.joda.time.Instant timestamp = failedContext.timestamps.get(failedIndex);
                o.get(failedRowsTag)
                    .outputWithTimestamp(
                        new BigQueryStorageApiInsertError(
                            failedRow, error.getRowIndexToErrorMessage().get(failedIndex)),
                        timestamp);
              }
              int failedRows = failedRowIndices.size();
              rowsSentToFailedRowsCollection.inc(failedRows);
              BigQuerySinkMetrics.appendRowsRowStatusCounter(
                      BigQuerySinkMetrics.RowStatus.FAILED, errorCode, shortTableId)
                  .inc(failedRows);

              // Remove the failed row from the payload, so we retry the batch without the failed
              // rows.
              ProtoRows.Builder retryRows = ProtoRows.newBuilder();
              @Nullable List<org.joda.time.Instant> timestamps = Lists.newArrayList();
              for (int i = 0; i < failedContext.protoRows.getSerializedRowsCount(); ++i) {
                if (!failedRowIndices.contains(i)) {
                  ByteString rowBytes = failedContext.protoRows.getSerializedRows(i);
                  retryRows.addSerializedRows(rowBytes);
                  timestamps.add(failedContext.timestamps.get(i));
                }
              }
              failedContext.protoRows = retryRows.build();
              failedContext.timestamps = timestamps;
              int retriedRows = failedContext.protoRows.getSerializedRowsCount();
              BigQuerySinkMetrics.appendRowsRowStatusCounter(
                      BigQuerySinkMetrics.RowStatus.RETRIED, errorCode, shortTableId)
                  .inc(retriedRows);

              // Since we removed rows, we need to update the insert offsets for all remaining rows.
              long offset = failedContext.offset;
              for (AppendRowsContext context : failedContexts) {
                context.offset = offset;
                offset += context.protoRows.getSerializedRowsCount();
              }
              streamOffset.write(offset);
              return RetryType.RETRY_ALL_OPERATIONS;
            }

            Throwable error = Preconditions.checkStateNotNull(failedContext.getError());
            Status.Code statusCode = Status.fromThrowable(error).getCode();

            // This means that the offset we have stored does not match the current end of
            // the stream in the Storage API. Usually this happens because a crash or a bundle
            // failure
            // happened after an append but before the worker could checkpoint it's
            // state. The records that were appended in a failed bundle will be retried,
            // meaning that the unflushed tail of the stream must be discarded to prevent
            // duplicates.
            boolean offsetMismatch =
                statusCode.equals(Code.OUT_OF_RANGE) || statusCode.equals(Code.ALREADY_EXISTS);

            boolean quotaError = statusCode.equals(Code.RESOURCE_EXHAUSTED);
            if (!offsetMismatch) {
              // Don't log errors for expected offset mismatch. These will be logged as warnings
              // below.
              LOG.error(
                  "Got error " + failedContext.getError() + " closing " + failedContext.streamName);
            }

            try {
              // TODO: Only do this on explicit NOT_FOUND errors once BigQuery reliably produces
              // them.
              tryCreateTable.call();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }

            if (!quotaError) {
              // This forces us to close and reopen all gRPC connections to Storage API on error,
              // which empirically fixes random stuckness issues.
              clearClients.accept(failedContexts);
            }
            appendFailures.inc();
            int retriedRows = failedContext.protoRows.getSerializedRowsCount();
            BigQuerySinkMetrics.appendRowsRowStatusCounter(
                    BigQuerySinkMetrics.RowStatus.RETRIED, errorCode, shortTableId)
                .inc(retriedRows);

            boolean explicitStreamFinalized =
                failedContext.getError() instanceof StreamFinalizedException;
            // This implies that the stream doesn't exist or has already been finalized. In this
            // case we have no choice but to create a new stream.
            boolean streamDoesNotExist =
                explicitStreamFinalized
                    || statusCode.equals(Code.INVALID_ARGUMENT)
                    || statusCode.equals(Code.NOT_FOUND)
                    || statusCode.equals(Code.FAILED_PRECONDITION);
            if (offsetMismatch || streamDoesNotExist) {
              appendOffsetFailures.inc();
              LOG.warn(
                  "Append to "
                      + failedContext
                      + " failed with "
                      + failedContext.getError()
                      + " Will retry with a new stream");
              // Finalize the stream and clear streamName so a new stream will be created.
              o.get(flushTag)
                  .output(
                      KV.of(
                          failedContext.streamName, new Operation(failedContext.offset - 1, true)));
              // Reinitialize all contexts with the new stream and new offsets.
              initializeContexts.accept(failedContexts, true);

              // Offset failures imply that all subsequent parallel appends will also fail.
              // Retry them all.
              return RetryType.RETRY_ALL_OPERATIONS;
            }

            return RetryType.RETRY_ALL_OPERATIONS;
          };

      Consumer<AppendRowsContext> onSuccess =
          context -> {
            AppendRowsResponse response = Preconditions.checkStateNotNull(context.getResult());
            o.get(flushTag)
                .output(
                    KV.of(
                        context.streamName,
                        new Operation(
                            context.offset + context.protoRows.getSerializedRowsCount() - 1,
                            false)));
            int flushedRows = context.protoRows.getSerializedRowsCount();
            flushesScheduled.inc(flushedRows);
            BigQuerySinkMetrics.reportSuccessfulRpcMetrics(
                context, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, shortTableId);
            BigQuerySinkMetrics.appendRowsRowStatusCounter(
                BigQuerySinkMetrics.RowStatus.SUCCESSFUL, BigQuerySinkMetrics.OK, shortTableId);

            if (successfulRowsTag != null) {
              for (int i = 0; i < context.protoRows.getSerializedRowsCount(); ++i) {
                ByteString protoBytes = context.protoRows.getSerializedRows(i);
                org.joda.time.Instant timestamp = context.timestamps.get(i);
                o.get(successfulRowsTag)
                    .outputWithTimestamp(appendClientInfo.get().toTableRow(protoBytes), timestamp);
              }
            }
          };
      Instant now = Instant.now();
      List<AppendRowsContext> contexts = Lists.newArrayList();
      RetryManager<AppendRowsResponse, AppendRowsContext> retryManager =
          new RetryManager<>(
              Duration.standardSeconds(1),
              Duration.standardSeconds(10),
              1000,
              BigQuerySinkMetrics.throttledTimeCounter(BigQuerySinkMetrics.RpcMethod.APPEND_ROWS));
      int numAppends = 0;
      for (SplittingIterable.Value splitValue : messages) {
        // Handle the case of a row that is too large.
        if (splitValue.getProtoRows().getSerializedSize() >= maxRequestSize) {
          if (splitValue.getProtoRows().getSerializedRowsCount() > 1) {
            // TODO(reuvenlax): Is it worth trying to handle this case by splitting the protoRows?
            // Given that we split
            // the ProtoRows iterable at 2MB and the max request size is 10MB, this scenario seems
            // nearly impossible.
            LOG.error(
                "A request containing more than one row is over the request size limit of "
                    + maxRequestSize
                    + ". This is unexpected. All rows in the request will be sent to the failed-rows PCollection.");
          }
          for (int i = 0; i < splitValue.getProtoRows().getSerializedRowsCount(); ++i) {
            ByteString rowBytes = splitValue.getProtoRows().getSerializedRows(i);
            org.joda.time.Instant timestamp = splitValue.getTimestamps().get(i);
            TableRow failedRow = appendClientInfo.get().toTableRow(rowBytes);
            o.get(failedRowsTag)
                .outputWithTimestamp(
                    new BigQueryStorageApiInsertError(
                        failedRow, "Row payload too large. Maximum size " + maxRequestSize),
                    timestamp);
          }
          int numRowsFailed = splitValue.getProtoRows().getSerializedRowsCount();
          rowsSentToFailedRowsCollection.inc(numRowsFailed);
          BigQuerySinkMetrics.appendRowsRowStatusCounter(
                  BigQuerySinkMetrics.RowStatus.FAILED,
                  BigQuerySinkMetrics.PAYLOAD_TOO_LARGE,
                  shortTableId)
              .inc(numRowsFailed);
        } else {
          ++numAppends;
          // RetryManager
          AppendRowsContext context =
              new AppendRowsContext(
                  element.getKey(), splitValue.getProtoRows(), splitValue.getTimestamps());
          contexts.add(context);
          retryManager.addOperation(runOperation, onError, onSuccess, context);
          recordsAppended.inc(splitValue.getProtoRows().getSerializedRowsCount());
          appendSizeDistribution.update(context.protoRows.getSerializedRowsCount());
        }
      }

      if (numAppends > 0) {
        initializeContexts.accept(contexts, false);
        try {
          retryManager.run(true);
        } finally {
          // Make sure that all pins are removed.
          for (AppendRowsContext context : contexts) {
            if (context.client != null) {
              runAsyncIgnoreFailure(closeWriterExecutor, context.client::unpin);
            }
          }
        }
        appendSplitDistribution.update(numAppends);

        if (autoUpdateSchema) {
          @Nullable
          StreamAppendClient streamAppendClient = appendClientInfo.get().getStreamAppendClient();
          TableSchema originalSchema = appendClientInfo.get().getTableSchema();
          ;
          @Nullable
          TableSchema updatedSchemaReturned =
              (streamAppendClient != null) ? streamAppendClient.getUpdatedSchema() : null;
          // Update the table schema and clear the append client.
          if (updatedSchemaReturned != null) {
            Optional<TableSchema> newSchema =
                TableSchemaUpdateUtils.getUpdatedSchema(originalSchema, updatedSchemaReturned);
            if (newSchema.isPresent()) {
              appendClientInfo.set(
                  AppendClientInfo.of(
                      newSchema.get(), appendClientInfo.get().getCloseAppendClient(), false));
              APPEND_CLIENTS.invalidate(element.getKey());
              APPEND_CLIENTS.put(element.getKey(), appendClientInfo.get());
              LOG.debug(
                  "Fetched updated schema for table {}:\n\t{}", tableId, updatedSchemaReturned);
              updatedSchema.write(newSchema.get());
            }
          }
        }

        java.time.Duration timeElapsed = java.time.Duration.between(now, Instant.now());
        appendLatencyDistribution.update(timeElapsed.toMillis());
      }
      idleTimer.offset(streamIdleTime).withNoOutputTimestamp().setRelative();
    }

    // called by the idleTimer and window-expiration handlers.
    private void finalizeStream(
        @AlwaysFetched @StateId("streamName") ValueState<String> streamName,
        @AlwaysFetched @StateId("streamOffset") ValueState<Long> streamOffset,
        ShardedKey<DestinationT> key,
        MultiOutputReceiver o,
        org.joda.time.Instant finalizeElementTs) {
      String stream = MoreObjects.firstNonNull(streamName.read(), "");

      if (!Strings.isNullOrEmpty(stream)) {
        // Finalize the stream
        long nextOffset = MoreObjects.firstNonNull(streamOffset.read(), 0L);
        o.get(flushTag)
            .outputWithTimestamp(
                KV.of(stream, new Operation(nextOffset - 1, true)), finalizeElementTs);
        streamName.clear();
        streamOffset.clear();
        // Make sure that the stream object is closed.
        APPEND_CLIENTS.invalidate(key);
      }
    }

    @OnTimer("idleTimer")
    public void onTimer(
        @Key ShardedKey<DestinationT> key,
        @AlwaysFetched @StateId("streamName") ValueState<String> streamName,
        @AlwaysFetched @StateId("streamOffset") ValueState<Long> streamOffset,
        MultiOutputReceiver o,
        BoundedWindow window) {
      // Stream is idle - clear it.
      // Note: this is best effort. We are explicitly emiting a timestamp that is before
      // the default output timestamp, which means that in some cases (usually when draining
      // a pipeline) this finalize element will be dropped as late. This is usually ok as
      // BigQuery will eventually garbage collect the stream. We attempt to finalize idle streams
      // merely to remove the pressure of large numbers of orphaned streams from BigQuery.
      finalizeStream(streamName, streamOffset, key, o, window.maxTimestamp());
      streamsIdle.inc();
    }

    @OnWindowExpiration
    public void onWindowExpiration(
        @Key ShardedKey<DestinationT> key,
        @AlwaysFetched @StateId("streamName") ValueState<String> streamName,
        @AlwaysFetched @StateId("streamOffset") ValueState<Long> streamOffset,
        MultiOutputReceiver o,
        BoundedWindow window) {
      // Window is done - usually because the pipeline has been drained. Make sure to clean up
      // streams so that they are not leaked.
      finalizeStream(streamName, streamOffset, key, o, window.maxTimestamp());
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
    }
  }
}
