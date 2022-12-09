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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.bigquery.storage.v1.WriteStream.Type;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StreamAppendClient;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.RetryType;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiDynamicDestinations.MessageConverter;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write records to the Storage API using a standard batch approach. PENDING streams are used, which
 * do not become visible until they are finalized and committed. Each input bundle to the DoFn
 * creates a stream per output table, appends all records in the bundle to the stream, and schedules
 * a finalize/commit operation at the end.
 */
@SuppressWarnings({"FutureReturnValueIgnored"})
public class StorageApiWriteUnshardedRecords<DestinationT, ElementT>
    extends PTransform<PCollection<KV<DestinationT, StorageApiWritePayload>>, PCollectionTuple> {
  private static final Logger LOG = LoggerFactory.getLogger(StorageApiWriteUnshardedRecords.class);

  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private final BigQueryServices bqServices;
  private final TupleTag<BigQueryStorageApiInsertError> failedRowsTag;
  private final TupleTag<KV<String, String>> finalizeTag = new TupleTag<>("finalizeTag");
  private final Coder<BigQueryStorageApiInsertError> failedRowsCoder;
  private static final ExecutorService closeWriterExecutor = Executors.newCachedThreadPool();

  /**
   * The Guava cache object is thread-safe. However our protocol requires that client pin the
   * StreamAppendClient after looking up the cache, and we must ensure that the cache is not
   * accessed in between the lookup and the pin (any access of the cache could trigger element
   * expiration). Therefore most used of APPEND_CLIENTS should synchronize.
   */
  private static final Cache<String, AppendClientInfo> APPEND_CLIENTS =
      CacheBuilder.newBuilder()
          .expireAfterAccess(15, TimeUnit.MINUTES)
          .removalListener(
              (RemovalNotification<String, AppendClientInfo> removal) -> {
                LOG.info("Expiring append client for " + removal.getKey());
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
            //
          }
        });
  }

  public StorageApiWriteUnshardedRecords(
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      BigQueryServices bqServices,
      TupleTag<BigQueryStorageApiInsertError> failedRowsTag,
      Coder<BigQueryStorageApiInsertError> failedRowsCoder) {
    this.dynamicDestinations = dynamicDestinations;
    this.bqServices = bqServices;
    this.failedRowsTag = failedRowsTag;
    this.failedRowsCoder = failedRowsCoder;
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<DestinationT, StorageApiWritePayload>> input) {
    String operationName = input.getName() + "/" + getName();
    BigQueryOptions options = input.getPipeline().getOptions().as(BigQueryOptions.class);
    org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument(
        !options.getUseStorageApiConnectionPool(),
        "useStorageApiConnectionPool only supported " + "when using STORAGE_API_AT_LEAST_ONCE");
    PCollectionTuple writeResults =
        input.apply(
            "Write Records",
            ParDo.of(
                    new WriteRecordsDoFn<>(
                        operationName,
                        dynamicDestinations,
                        bqServices,
                        false,
                        options.getStorageApiAppendThresholdBytes(),
                        options.getStorageApiAppendThresholdRecordCount(),
                        options.getNumStorageWriteApiStreamAppendClients(),
                        finalizeTag,
                        failedRowsTag))
                .withOutputTags(finalizeTag, TupleTagList.of(failedRowsTag))
                .withSideInputs(dynamicDestinations.getSideInputs()));

    writeResults
        .get(finalizeTag)
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        // Calling Reshuffle makes the output stable - once this completes, the append operations
        // will not retry.
        // TODO(reuvenlax): This should use RequiresStableInput instead.
        .apply("Reshuffle", Reshuffle.of())
        .apply("Finalize writes", ParDo.of(new StorageApiFinalizeWritesDoFn(bqServices)));
    writeResults.get(failedRowsTag).setCoder(failedRowsCoder);
    return writeResults;
  }

  static class WriteRecordsDoFn<DestinationT extends @NonNull Object, ElementT>
      extends DoFn<KV<DestinationT, StorageApiWritePayload>, KV<String, String>> {
    private final Counter forcedFlushes = Metrics.counter(WriteRecordsDoFn.class, "forcedFlushes");
    private final TupleTag<KV<String, String>> finalizeTag;
    private final TupleTag<BigQueryStorageApiInsertError> failedRowsTag;

    static class AppendRowsContext extends RetryManager.Operation.Context<AppendRowsResponse> {
      long offset;
      ProtoRows protoRows;

      public AppendRowsContext(long offset, ProtoRows protoRows) {
        this.offset = offset;
        this.protoRows = protoRows;
      }
    }

    class DestinationState {
      private final String tableUrn;
      private String streamName = "";
      private @Nullable AppendClientInfo appendClientInfo = null;
      private long currentOffset = 0;
      private List<ByteString> pendingMessages;
      private transient @Nullable DatasetService maybeDatasetService;
      private final Counter recordsAppended =
          Metrics.counter(WriteRecordsDoFn.class, "recordsAppended");
      private final Counter appendFailures =
          Metrics.counter(WriteRecordsDoFn.class, "appendFailures");
      private final Distribution inflightWaitSecondsDistribution =
          Metrics.distribution(WriteRecordsDoFn.class, "streamWriterWaitSeconds");
      private final Counter rowsSentToFailedRowsCollection =
          Metrics.counter(
              StorageApiWritesShardedRecords.WriteRecordsDoFn.class,
              "rowsSentToFailedRowsCollection");

      private final boolean useDefaultStream;
      private TableSchema tableSchema;
      private Instant nextCacheTickle = Instant.MAX;
      private final int clientNumber;
      private final boolean usingMultiplexing;
      private final long maxRequestSize;

      public DestinationState(
          String tableUrn,
          MessageConverter<ElementT> messageConverter,
          DatasetService datasetService,
          boolean useDefaultStream,
          int streamAppendClientCount,
          boolean usingMultiplexing,
          long maxRequestSize)
          throws Exception {
        this.tableUrn = tableUrn;
        this.pendingMessages = Lists.newArrayList();
        this.maybeDatasetService = datasetService;
        this.useDefaultStream = useDefaultStream;
        this.tableSchema = messageConverter.getTableSchema();
        this.clientNumber = new Random().nextInt(streamAppendClientCount);
        this.usingMultiplexing = usingMultiplexing;
        this.maxRequestSize = maxRequestSize;
      }

      void teardown() {
        maybeTickleCache();
        if (appendClientInfo != null) {
          if (appendClientInfo.streamAppendClient != null) {
            runAsyncIgnoreFailure(closeWriterExecutor, appendClientInfo.streamAppendClient::unpin);
          }
          appendClientInfo = null;
        }
      }

      String getDefaultStreamName() {
        return BigQueryHelpers.stripPartitionDecorator(tableUrn) + "/streams/_default";
      }

      String getStreamAppendClientCacheEntryKey() {
        if (useDefaultStream) {
          return getDefaultStreamName() + "-client" + clientNumber;
        }
        return this.streamName;
      }

      String getOrCreateStreamName() {
        try {
          if (!useDefaultStream) {
            this.streamName =
                Preconditions.checkStateNotNull(maybeDatasetService)
                    .createWriteStream(tableUrn, Type.PENDING)
                    .getName();
          } else {
            this.streamName = getDefaultStreamName();
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return this.streamName;
      }

      AppendClientInfo generateClient() throws Exception {
        Preconditions.checkStateNotNull(maybeDatasetService);
        AppendClientInfo appendClientInfo =
            new AppendClientInfo(
                tableSchema,
                // Make sure that the client is always closed in a different thread to avoid
                // blocking.
                client -> runAsyncIgnoreFailure(closeWriterExecutor, client::close));
        appendClientInfo =
            appendClientInfo.createAppendClient(
                maybeDatasetService, () -> streamName, usingMultiplexing);
        Preconditions.checkStateNotNull(appendClientInfo.streamAppendClient).pin();
        return appendClientInfo;
      }

      AppendClientInfo getAppendClientInfo(boolean lookupCache) {
        try {
          if (this.appendClientInfo == null) {
            getOrCreateStreamName();
            final AppendClientInfo newAppendClientInfo;
            synchronized (APPEND_CLIENTS) {
              if (lookupCache) {
                newAppendClientInfo =
                    APPEND_CLIENTS.get(
                        getStreamAppendClientCacheEntryKey(), () -> generateClient());
              } else {
                newAppendClientInfo = generateClient();
                // override the clients in the cache.
                APPEND_CLIENTS.put(getStreamAppendClientCacheEntryKey(), newAppendClientInfo);
              }
            }
            this.currentOffset = 0;
            nextCacheTickle = Instant.now().plus(java.time.Duration.ofMinutes(1));
            this.appendClientInfo = newAppendClientInfo;
          }
          return appendClientInfo;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      void maybeTickleCache() {
        if (appendClientInfo != null && Instant.now().isAfter(nextCacheTickle)) {
          synchronized (APPEND_CLIENTS) {
            APPEND_CLIENTS.getIfPresent(getStreamAppendClientCacheEntryKey());
          }
          nextCacheTickle = Instant.now().plus(java.time.Duration.ofMinutes(1));
        }
      }

      void invalidateWriteStream() {
        if (appendClientInfo != null) {
          synchronized (APPEND_CLIENTS) {
            // Unpin in a different thread, as it may execute a blocking close.
            if (appendClientInfo.streamAppendClient != null) {
              runAsyncIgnoreFailure(
                  closeWriterExecutor, appendClientInfo.streamAppendClient::unpin);
            }
            // The default stream is cached across multiple different DoFns. If they all try and
            // invalidate, then we can get races between threads invalidating and recreating
            // streams. For this reason,
            // we check to see that the cache still contains the object we created before
            // invalidating (in case another
            // thread has already invalidated and recreated the stream).
            String cacheEntryKey = getStreamAppendClientCacheEntryKey();
            @Nullable
            AppendClientInfo cachedAppendClient = APPEND_CLIENTS.getIfPresent(cacheEntryKey);
            if (cachedAppendClient != null
                && System.identityHashCode(cachedAppendClient)
                    == System.identityHashCode(appendClientInfo)) {
              APPEND_CLIENTS.invalidate(cacheEntryKey);
            }
          }
          appendClientInfo = null;
        }
      }

      void addMessage(StorageApiWritePayload payload) throws Exception {
        maybeTickleCache();
        ByteString payloadBytes = ByteString.copyFrom(payload.getPayload());
        pendingMessages.add(payloadBytes);
      }

      long flush(
          RetryManager<AppendRowsResponse, AppendRowsContext> retryManager,
          OutputReceiver<BigQueryStorageApiInsertError> failedRowsReceiver)
          throws Exception {
        if (pendingMessages.isEmpty()) {
          return 0;
        }

        final ProtoRows.Builder insertsBuilder = ProtoRows.newBuilder();
        insertsBuilder.addAllSerializedRows(pendingMessages);
        final ProtoRows inserts = insertsBuilder.build();
        pendingMessages.clear();

        // Handle the case where the request is too large.
        if (inserts.getSerializedSize() >= maxRequestSize) {
          if (inserts.getSerializedRowsCount() > 1) {
            // TODO(reuvenlax): Is it worth trying to handle this case by splitting the protoRows?
            // Given that we split
            // the ProtoRows iterable at 2MB and the max request size is 10MB, this scenario seems
            // nearly impossible.
            LOG.error(
                "A request containing more than one row is over the request size limit of "
                    + maxRequestSize
                    + ". This is unexpected. All rows in the request will be sent to the failed-rows PCollection.");
          }
          for (ByteString rowBytes : inserts.getSerializedRowsList()) {
            TableRow failedRow =
                TableRowToStorageApiProto.tableRowFromMessage(
                    DynamicMessage.parseFrom(getAppendClientInfo(true).descriptor, rowBytes));
            failedRowsReceiver.output(
                new BigQueryStorageApiInsertError(
                    failedRow, "Row payload too large. Maximum size " + maxRequestSize));
          }
          return 0;
        }

        long offset = -1;
        if (!this.useDefaultStream) {
          offset = this.currentOffset;
          this.currentOffset += inserts.getSerializedRowsCount();
        }
        AppendRowsContext appendRowsContext = new AppendRowsContext(offset, inserts);

        retryManager.addOperation(
            c -> {
              if (c.protoRows.getSerializedRowsCount() == 0) {
                // This might happen if all rows in a batch failed and were sent to the failed-rows
                // PCollection.
                return ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build());
              }
              try {
                StreamAppendClient writeStream =
                    Preconditions.checkStateNotNull(getAppendClientInfo(true).streamAppendClient);
                ApiFuture<AppendRowsResponse> response =
                    writeStream.appendRows(c.offset, c.protoRows);
                inflightWaitSecondsDistribution.update(writeStream.getInflightWaitSeconds());
                if (!usingMultiplexing) {
                  if (writeStream.getInflightWaitSeconds() > 5) {
                    LOG.warn(
                        "Storage Api write delay more than {} seconds.",
                        writeStream.getInflightWaitSeconds());
                  }
                }
                return response;
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            },
            contexts -> {
              AppendRowsContext failedContext =
                  Preconditions.checkStateNotNull(Iterables.getFirst(contexts, null));
              if (failedContext.getError() != null
                  && failedContext.getError() instanceof Exceptions.AppendSerializtionError) {
                Exceptions.AppendSerializtionError error =
                    Preconditions.checkStateNotNull(
                        (Exceptions.AppendSerializtionError) failedContext.getError());
                Set<Integer> failedRowIndices = error.getRowIndexToErrorMessage().keySet();
                for (int failedIndex : failedRowIndices) {
                  // Convert the message to a TableRow and send it to the failedRows collection.
                  ByteString protoBytes = failedContext.protoRows.getSerializedRows(failedIndex);
                  try {
                    TableRow failedRow =
                        TableRowToStorageApiProto.tableRowFromMessage(
                            DynamicMessage.parseFrom(
                                Preconditions.checkStateNotNull(appendClientInfo).descriptor,
                                protoBytes));
                    failedRowsReceiver.output(
                        new BigQueryStorageApiInsertError(
                            failedRow, error.getRowIndexToErrorMessage().get(failedIndex)));
                  } catch (InvalidProtocolBufferException e) {
                    LOG.error("Failed to insert row and could not parse the result!");
                  }
                }
                rowsSentToFailedRowsCollection.inc(failedRowIndices.size());

                // Remove the failed row from the payload, so we retry the batch without the failed
                // rows.
                ProtoRows.Builder retryRows = ProtoRows.newBuilder();
                for (int i = 0; i < failedContext.protoRows.getSerializedRowsCount(); ++i) {
                  if (!failedRowIndices.contains(i)) {
                    ByteString rowBytes = failedContext.protoRows.getSerializedRows(i);
                    retryRows.addSerializedRows(rowBytes);
                  }
                }
                failedContext.protoRows = retryRows.build();

                // Since we removed rows, we need to update the insert offsets for all remaining
                // rows.
                long newOffset = failedContext.offset;
                for (AppendRowsContext context : contexts) {
                  context.offset = newOffset;
                  newOffset += context.protoRows.getSerializedRowsCount();
                }
                this.currentOffset = newOffset;
                return RetryType.RETRY_ALL_OPERATIONS;
              }

              LOG.warn(
                  "Append to stream {} by client #{} failed with error, operations will be retried. Details: {}",
                  streamName,
                  clientNumber,
                  retrieveErrorDetails(contexts));
              invalidateWriteStream();
              appendFailures.inc();
              return RetryType.RETRY_ALL_OPERATIONS;
            },
            c -> {
              recordsAppended.inc(c.protoRows.getSerializedRowsCount());
            },
            appendRowsContext);
        maybeTickleCache();
        return inserts.getSerializedRowsCount();
      }

      String retrieveErrorDetails(Iterable<AppendRowsContext> failedContext) {
        return StreamSupport.stream(failedContext.spliterator(), false)
            .<@Nullable Throwable>map(AppendRowsContext::getError)
            .filter(err -> err != null)
            .flatMap(thrw -> Arrays.stream(Preconditions.checkStateNotNull(thrw).getStackTrace()))
            .map(StackTraceElement::toString)
            .collect(Collectors.joining("\n"));
      }
    }

    private @Nullable Map<DestinationT, DestinationState> destinations = Maps.newHashMap();
    private final TwoLevelMessageConverterCache<DestinationT, ElementT> messageConverters;
    private transient @Nullable DatasetService maybeDatasetService;
    private int numPendingRecords = 0;
    private int numPendingRecordBytes = 0;
    private final int flushThresholdBytes;
    private final int flushThresholdCount;
    private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
    private final BigQueryServices bqServices;
    private final boolean useDefaultStream;
    private int streamAppendClientCount;

    WriteRecordsDoFn(
        String operationName,
        StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
        BigQueryServices bqServices,
        boolean useDefaultStream,
        int flushThresholdBytes,
        int flushThresholdCount,
        int streamAppendClientCount,
        TupleTag<KV<String, String>> finalizeTag,
        TupleTag<BigQueryStorageApiInsertError> failedRowsTag) {
      this.messageConverters = new TwoLevelMessageConverterCache<>(operationName);
      this.dynamicDestinations = dynamicDestinations;
      this.bqServices = bqServices;
      this.useDefaultStream = useDefaultStream;
      this.flushThresholdBytes = flushThresholdBytes;
      this.flushThresholdCount = flushThresholdCount;
      this.streamAppendClientCount = streamAppendClientCount;
      this.finalizeTag = finalizeTag;
      this.failedRowsTag = failedRowsTag;
    }

    boolean shouldFlush() {
      return numPendingRecords > flushThresholdCount || numPendingRecordBytes > flushThresholdBytes;
    }

    void flushIfNecessary(OutputReceiver<BigQueryStorageApiInsertError> failedRowsReceiver)
        throws Exception {
      if (shouldFlush()) {
        forcedFlushes.inc();
        // Too much memory being used. Flush the state and wait for it to drain out.
        // TODO(reuvenlax): Consider waiting for memory usage to drop instead of waiting for all the
        // appends to finish.
        flushAll(failedRowsReceiver);
      }
    }

    void flushAll(OutputReceiver<BigQueryStorageApiInsertError> failedRowsReceiver)
        throws Exception {
      List<RetryManager<AppendRowsResponse, AppendRowsContext>> retryManagers =
          Lists.newArrayListWithCapacity(Preconditions.checkStateNotNull(destinations).size());
      long numRowsWritten = 0;
      for (DestinationState destinationState :
          Preconditions.checkStateNotNull(destinations).values()) {
        RetryManager<AppendRowsResponse, AppendRowsContext> retryManager =
            new RetryManager<>(Duration.standardSeconds(1), Duration.standardSeconds(10), 1000);
        retryManagers.add(retryManager);
        numRowsWritten += destinationState.flush(retryManager, failedRowsReceiver);
        retryManager.run(false);
      }
      if (numRowsWritten > 0) {
        // TODO(reuvenlax): Can we await in parallel instead? Failure retries aren't triggered until
        // await is called, so
        // this approach means that if one call fais, it has to wait for all prior calls to complete
        // before a retry happens.
        for (RetryManager<AppendRowsResponse, AppendRowsContext> retryManager : retryManagers) {
          retryManager.await();
        }
      }
      numPendingRecords = 0;
      numPendingRecordBytes = 0;
    }

    private DatasetService initializeDatasetService(PipelineOptions pipelineOptions) {
      if (maybeDatasetService == null) {
        maybeDatasetService =
            bqServices.getDatasetService(pipelineOptions.as(BigQueryOptions.class));
      }
      return maybeDatasetService;
    }

    @StartBundle
    public void startBundle() throws IOException {
      destinations = Maps.newHashMap();
      numPendingRecords = 0;
      numPendingRecordBytes = 0;
    }

    DestinationState createDestinationState(
        ProcessContext c,
        DestinationT destination,
        DatasetService datasetService,
        BigQueryOptions bigQueryOptions) {
      TableDestination tableDestination1 = dynamicDestinations.getTable(destination);
      checkArgument(
          tableDestination1 != null,
          "DynamicDestinations.getTable() may not return null, "
              + "but %s returned null for destination %s",
          dynamicDestinations,
          destination);
      MessageConverter<ElementT> messageConverter;
      try {
        messageConverter = messageConverters.get(destination, dynamicDestinations, datasetService);
        return new DestinationState(
            tableDestination1.getTableUrn(),
            messageConverter,
            datasetService,
            useDefaultStream,
            streamAppendClientCount,
            bigQueryOptions.getUseStorageApiConnectionPool(),
            bigQueryOptions.getStorageWriteApiMaxRequestSize());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @ProcessElement
    public void process(
        ProcessContext c,
        PipelineOptions pipelineOptions,
        @Element KV<DestinationT, StorageApiWritePayload> element,
        MultiOutputReceiver o)
        throws Exception {
      DatasetService initializedDatasetService = initializeDatasetService(pipelineOptions);
      dynamicDestinations.setSideInputAccessorFromProcessContext(c);
      Preconditions.checkStateNotNull(destinations);
      DestinationState state =
          destinations.computeIfAbsent(
              element.getKey(),
              k ->
                  createDestinationState(
                      c, k, initializedDatasetService, pipelineOptions.as(BigQueryOptions.class)));
      flushIfNecessary(o.get(failedRowsTag));
      state.addMessage(element.getValue());
      ++numPendingRecords;
      numPendingRecordBytes += element.getValue().getPayload().length;
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) throws Exception {
      flushAll(
          new OutputReceiver<BigQueryStorageApiInsertError>() {
            @Override
            public void output(BigQueryStorageApiInsertError output) {
              outputWithTimestamp(output, GlobalWindow.INSTANCE.maxTimestamp());
            }

            @Override
            public void outputWithTimestamp(
                BigQueryStorageApiInsertError output, org.joda.time.Instant timestamp) {
              context.output(failedRowsTag, output, timestamp, GlobalWindow.INSTANCE);
            }
          });

      final Map<DestinationT, DestinationState> destinations =
          Preconditions.checkStateNotNull(this.destinations);
      for (DestinationState state : destinations.values()) {
        if (!useDefaultStream && !Strings.isNullOrEmpty(state.streamName)) {
          context.output(
              finalizeTag,
              KV.of(state.tableUrn, state.streamName),
              GlobalWindow.INSTANCE.maxTimestamp(),
              GlobalWindow.INSTANCE);
        }
        state.teardown();
      }
      destinations.clear();
      this.destinations = null;
    }

    @Teardown
    public void teardown() {
      destinations = null;
      try {
        if (maybeDatasetService != null) {
          maybeDatasetService.close();
          maybeDatasetService = null;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
