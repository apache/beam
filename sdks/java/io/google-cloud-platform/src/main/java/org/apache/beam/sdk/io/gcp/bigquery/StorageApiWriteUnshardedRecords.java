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
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.WriteStream.Type;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StreamAppendClient;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.Operation.Context;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.RetryType;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiDynamicDestinations.DescriptorWrapper;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiDynamicDestinations.MessageConverter;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.RemovalNotification;
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
    extends PTransform<PCollection<KV<DestinationT, StorageApiWritePayload>>, PCollection<Void>> {
  private static final Logger LOG = LoggerFactory.getLogger(StorageApiWriteUnshardedRecords.class);

  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private final BigQueryServices bqServices;
  private static final ExecutorService closeWriterExecutor = Executors.newCachedThreadPool();

  /**
   * The Guava cache object is thread-safe. However our protocol requires that client pin the
   * StreamAppendClient after looking up the cache, and we must ensure that the cache is not
   * accessed in between the lookup and the pin (any access of the cache could trigger element
   * expiration). Therefore most used of APPEND_CLIENTS should synchronize.
   */
  private static final Cache<String, StreamAppendClient> APPEND_CLIENTS =
      CacheBuilder.newBuilder()
          .expireAfterAccess(15, TimeUnit.MINUTES)
          .removalListener(
              (RemovalNotification<String, StreamAppendClient> removal) -> {
                LOG.info("Expiring append client for " + removal.getKey());
                final @Nullable StreamAppendClient streamAppendClient = removal.getValue();
                // Close the writer in a different thread so as not to block the main one.
                runAsyncIgnoreFailure(closeWriterExecutor, streamAppendClient::close);
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
      BigQueryServices bqServices) {
    this.dynamicDestinations = dynamicDestinations;
    this.bqServices = bqServices;
  }

  @Override
  public PCollection<Void> expand(PCollection<KV<DestinationT, StorageApiWritePayload>> input) {
    String operationName = input.getName() + "/" + getName();
    BigQueryOptions options = input.getPipeline().getOptions().as(BigQueryOptions.class);
    return input
        .apply(
            "Write Records",
            ParDo.of(
                    new WriteRecordsDoFn<>(
                        operationName,
                        dynamicDestinations,
                        bqServices,
                        false,
                        options.getStorageApiAppendThresholdBytes(),
                        options.getStorageApiAppendThresholdRecordCount(),
                        options.getNumStorageWriteApiStreamAppendClients()))
                .withSideInputs(dynamicDestinations.getSideInputs()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        // Calling Reshuffle makes the output stable - once this completes, the append operations
        // will not retry.
        // TODO(reuvenlax): This should use RequiresStableInput instead.
        .apply("Reshuffle", Reshuffle.of())
        .apply("Finalize writes", ParDo.of(new StorageApiFinalizeWritesDoFn(bqServices)));
  }

  static class WriteRecordsDoFn<DestinationT extends @NonNull Object, ElementT>
      extends DoFn<KV<DestinationT, StorageApiWritePayload>, KV<String, String>> {
    private final Counter forcedFlushes = Metrics.counter(WriteRecordsDoFn.class, "forcedFlushes");

    class DestinationState {
      private final String tableUrn;
      private final MessageConverter<ElementT> messageConverter;
      private String streamName = "";
      private @Nullable StreamAppendClient streamAppendClient = null;
      private long currentOffset = 0;
      private List<ByteString> pendingMessages;
      private transient @Nullable DatasetService maybeDatasetService;
      private final Counter recordsAppended =
          Metrics.counter(WriteRecordsDoFn.class, "recordsAppended");
      private final Counter appendFailures =
          Metrics.counter(WriteRecordsDoFn.class, "appendFailures");
      private final Counter schemaMismatches =
          Metrics.counter(WriteRecordsDoFn.class, "schemaMismatches");
      private final Distribution inflightWaitSecondsDistribution =
          Metrics.distribution(WriteRecordsDoFn.class, "streamWriterWaitSeconds");
      private final boolean useDefaultStream;
      private DescriptorWrapper descriptorWrapper;
      private Instant nextCacheTickle = Instant.MAX;
      private final int clientNumber;

      public DestinationState(
          String tableUrn,
          MessageConverter<ElementT> messageConverter,
          DatasetService datasetService,
          boolean useDefaultStream,
          int streamAppendClientCount) {
        this.tableUrn = tableUrn;
        this.messageConverter = messageConverter;
        this.pendingMessages = Lists.newArrayList();
        this.maybeDatasetService = datasetService;
        this.useDefaultStream = useDefaultStream;
        this.descriptorWrapper = messageConverter.getSchemaDescriptor();
        this.clientNumber = new Random().nextInt(streamAppendClientCount);
      }

      void teardown() {
        maybeTickleCache();
        if (streamAppendClient != null) {
          runAsyncIgnoreFailure(closeWriterExecutor, streamAppendClient::unpin);
          streamAppendClient = null;
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

      String createStreamIfNeeded() {
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

      StreamAppendClient generateClient() throws Exception {
        Preconditions.checkStateNotNull(maybeDatasetService);
        return maybeDatasetService.getStreamAppendClient(streamName, descriptorWrapper.descriptor);
      }

      StreamAppendClient getStreamAppendClient(boolean lookupCache) {
        try {
          if (this.streamAppendClient == null) {
            createStreamIfNeeded();
            final StreamAppendClient newStreamAppendClient;
            synchronized (APPEND_CLIENTS) {
              if (lookupCache) {
                newStreamAppendClient =
                    APPEND_CLIENTS.get(
                        getStreamAppendClientCacheEntryKey(), () -> generateClient());
              } else {
                newStreamAppendClient = generateClient();
                // override the clients in the cache
                APPEND_CLIENTS.put(getStreamAppendClientCacheEntryKey(), newStreamAppendClient);
              }
              newStreamAppendClient.pin();
            }
            this.currentOffset = 0;
            nextCacheTickle = Instant.now().plus(java.time.Duration.ofMinutes(1));
            this.streamAppendClient = newStreamAppendClient;
          }
          return streamAppendClient;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      void maybeTickleCache() {
        if (streamAppendClient != null && Instant.now().isAfter(nextCacheTickle)) {
          synchronized (APPEND_CLIENTS) {
            APPEND_CLIENTS.getIfPresent(getStreamAppendClientCacheEntryKey());
          }
          nextCacheTickle = Instant.now().plus(java.time.Duration.ofMinutes(1));
        }
      }

      void invalidateWriteStream() {
        if (streamAppendClient != null) {
          synchronized (APPEND_CLIENTS) {
            // Unpin in a different thread, as it may execute a blocking close.
            runAsyncIgnoreFailure(closeWriterExecutor, streamAppendClient::unpin);
            // The default stream is cached across multiple different DoFns. If they all try and
            // invalidate, then we can
            // get races between threads invalidating and recreating streams. For this reason, we
            // check to see that the
            // cache still contains the object we created before invalidating (in case another
            // thread has already invalidated
            // and recreated the stream).
            String cacheEntryKey = getStreamAppendClientCacheEntryKey();
            @Nullable
            StreamAppendClient cachedAppendClient = APPEND_CLIENTS.getIfPresent(cacheEntryKey);
            if (cachedAppendClient != null
                && System.identityHashCode(cachedAppendClient)
                    == System.identityHashCode(streamAppendClient)) {
              APPEND_CLIENTS.invalidate(cacheEntryKey);
            }
          }
          streamAppendClient = null;
        }
      }

      void addMessage(StorageApiWritePayload payload) throws Exception {
        maybeTickleCache();
        if (payload.getSchemaHash() != descriptorWrapper.hash) {
          schemaMismatches.inc();
          // The descriptor on the payload doesn't match the descriptor we know about. This
          // means that the table has been updated, but that this transform hasn't found out
          // about that yet. Refresh the schema and force a new StreamAppendClient to be
          // created.
          messageConverter.refreshSchema(payload.getSchemaHash());
          descriptorWrapper = messageConverter.getSchemaDescriptor();
          invalidateWriteStream();
          if (useDefaultStream) {
            // Since the default stream client is shared across many bundles and threads, we can't
            // simply look it upfrom the cache, as another thread may have recreated it with the old
            // schema.
            getStreamAppendClient(false);
          }
          // Validate that the record can now be parsed.
          DynamicMessage msg =
              DynamicMessage.parseFrom(descriptorWrapper.descriptor, payload.getPayload());
          if (msg.getUnknownFields() != null && !msg.getUnknownFields().asMap().isEmpty()) {
            throw new RuntimeException(
                "Record schema does not match table. Unknown fields: " + msg.getUnknownFields());
          }
        }
        pendingMessages.add(ByteString.copyFrom(payload.getPayload()));
      }

      void flush(RetryManager<AppendRowsResponse, Context<AppendRowsResponse>> retryManager)
          throws Exception {
        if (pendingMessages.isEmpty()) {
          return;
        }
        final ProtoRows.Builder inserts = ProtoRows.newBuilder();
        inserts.addAllSerializedRows(pendingMessages);

        ProtoRows protoRows = inserts.build();
        pendingMessages.clear();

        retryManager.addOperation(
            c -> {
              try {
                StreamAppendClient writeStream = getStreamAppendClient(true);
                long offset = -1;
                if (!this.useDefaultStream) {
                  offset = this.currentOffset;
                  this.currentOffset += inserts.getSerializedRowsCount();
                }
                ApiFuture<AppendRowsResponse> response = writeStream.appendRows(offset, protoRows);
                inflightWaitSecondsDistribution.update(writeStream.getInflightWaitSeconds());
                if (writeStream.getInflightWaitSeconds() > 5) {
                  LOG.warn(
                      "Storage Api write delay more than {} seconds.",
                      writeStream.getInflightWaitSeconds());
                }
                return response;
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            },
            contexts -> {
              LOG.warn(
                  "Append to stream {} by client #{} failed with error, operations will be retried. Details: {}",
                  streamName,
                  clientNumber,
                  retrieveErrorDetails(contexts));
              invalidateWriteStream();
              appendFailures.inc();
              return RetryType.RETRY_ALL_OPERATIONS;
            },
            response -> {
              recordsAppended.inc(protoRows.getSerializedRowsCount());
            },
            new Context<>());
        maybeTickleCache();
      }

      String retrieveErrorDetails(Iterable<Context<AppendRowsResponse>> contexts) {
        return StreamSupport.stream(contexts.spliterator(), false)
            .<@Nullable Throwable>map(ctx -> ctx.getError())
            .map(
                err ->
                    (err == null)
                        ? "no error"
                        : Lists.newArrayList(err.getStackTrace()).stream()
                            .map(se -> se.toString())
                            .collect(Collectors.joining("\n")))
            .collect(Collectors.joining(","));
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
        int streamAppendClientCount) {
      this.messageConverters = new TwoLevelMessageConverterCache<>(operationName);
      this.dynamicDestinations = dynamicDestinations;
      this.bqServices = bqServices;
      this.useDefaultStream = useDefaultStream;
      this.flushThresholdBytes = flushThresholdBytes;
      this.flushThresholdCount = flushThresholdCount;
      this.streamAppendClientCount = streamAppendClientCount;
    }

    boolean shouldFlush() {
      return numPendingRecords > flushThresholdCount || numPendingRecordBytes > flushThresholdBytes;
    }

    void flushIfNecessary() throws Exception {
      if (shouldFlush()) {
        forcedFlushes.inc();
        // Too much memory being used. Flush the state and wait for it to drain out.
        // TODO(reuvenlax): Consider waiting for memory usage to drop instead of waiting for all the
        // appends to finish.
        flushAll();
      }
    }

    void flushAll() throws Exception {
      RetryManager<AppendRowsResponse, RetryManager.Operation.Context<AppendRowsResponse>>
          retryManager =
              new RetryManager<>(Duration.standardSeconds(1), Duration.standardSeconds(10), 1000);
      Preconditions.checkStateNotNull(destinations);
      for (DestinationState destinationState : destinations.values()) {
        destinationState.flush(retryManager);
      }
      retryManager.run(true);
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
        ProcessContext c, DestinationT destination, DatasetService datasetService) {
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
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return new DestinationState(
          tableDestination1.getTableUrn(),
          messageConverter,
          datasetService,
          useDefaultStream,
          streamAppendClientCount);
    }

    @ProcessElement
    public void process(
        ProcessContext c,
        PipelineOptions pipelineOptions,
        @Element KV<DestinationT, StorageApiWritePayload> element)
        throws Exception {
      DatasetService initializedDatasetService = initializeDatasetService(pipelineOptions);
      dynamicDestinations.setSideInputAccessorFromProcessContext(c);
      Preconditions.checkStateNotNull(destinations);
      DestinationState state =
          destinations.computeIfAbsent(
              element.getKey(), k -> createDestinationState(c, k, initializedDatasetService));
      flushIfNecessary();
      state.addMessage(element.getValue());
      ++numPendingRecords;
      numPendingRecordBytes += element.getValue().getPayload().length;
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) throws Exception {
      flushAll();
      final Map<DestinationT, DestinationState> destinations =
          Preconditions.checkStateNotNull(this.destinations);
      for (DestinationState state : destinations.values()) {
        if (!useDefaultStream) {
          context.output(
              KV.of(state.tableUrn, state.streamName),
              BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.millis(1)),
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
