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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.bigquery.storage.v1.WriteStream.Type;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.DynamicMessage;
import io.grpc.Status;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StreamAppendClient;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.WriteStreamService;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.RetryType;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiDynamicDestinations.MessageConverter;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Lineage;
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
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
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
  private final @Nullable TupleTag<TableRow> successfulRowsTag;
  private final Predicate<String> successfulRowsPredicate;
  private final TupleTag<KV<String, String>> finalizeTag = new TupleTag<>("finalizeTag");
  private final Coder<BigQueryStorageApiInsertError> failedRowsCoder;
  private final Coder<TableRow> successfulRowsCoder;
  private final boolean autoUpdateSchema;
  private final boolean ignoreUnknownValues;
  private static final ExecutorService closeWriterExecutor = Executors.newCachedThreadPool();
  private final BigQueryIO.Write.CreateDisposition createDisposition;
  private final @Nullable String kmsKey;
  private final boolean usesCdc;
  private final AppendRowsRequest.MissingValueInterpretation defaultMissingValueInterpretation;
  private final @Nullable Map<String, String> bigLakeConfiguration;

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

  public StorageApiWriteUnshardedRecords(
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      BigQueryServices bqServices,
      TupleTag<BigQueryStorageApiInsertError> failedRowsTag,
      @Nullable TupleTag<TableRow> successfulRowsTag,
      Predicate<String> successfulRowsPredicate,
      Coder<BigQueryStorageApiInsertError> failedRowsCoder,
      Coder<TableRow> successfulRowsCoder,
      boolean autoUpdateSchema,
      boolean ignoreUnknownValues,
      BigQueryIO.Write.CreateDisposition createDisposition,
      @Nullable String kmsKey,
      boolean usesCdc,
      AppendRowsRequest.MissingValueInterpretation defaultMissingValueInterpretation,
      @Nullable Map<String, String> bigLakeConfiguration) {
    this.dynamicDestinations = dynamicDestinations;
    this.bqServices = bqServices;
    this.failedRowsTag = failedRowsTag;
    this.successfulRowsTag = successfulRowsTag;
    this.successfulRowsPredicate = successfulRowsPredicate;
    this.failedRowsCoder = failedRowsCoder;
    this.successfulRowsCoder = successfulRowsCoder;
    this.autoUpdateSchema = autoUpdateSchema;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.createDisposition = createDisposition;
    this.kmsKey = kmsKey;
    this.usesCdc = usesCdc;
    this.defaultMissingValueInterpretation = defaultMissingValueInterpretation;
    this.bigLakeConfiguration = bigLakeConfiguration;
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<DestinationT, StorageApiWritePayload>> input) {
    String operationName = input.getName() + "/" + getName();
    BigQueryOptions options = input.getPipeline().getOptions().as(BigQueryOptions.class);
    org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument(
        !options.getUseStorageApiConnectionPool(),
        "useStorageApiConnectionPool only supported " + "when using STORAGE_API_AT_LEAST_ONCE");
    TupleTagList tupleTagList = TupleTagList.of(failedRowsTag);
    if (successfulRowsTag != null) {
      tupleTagList = tupleTagList.and(successfulRowsTag);
    }
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
                        failedRowsTag,
                        successfulRowsTag,
                        successfulRowsPredicate,
                        autoUpdateSchema,
                        ignoreUnknownValues,
                        createDisposition,
                        kmsKey,
                        usesCdc,
                        defaultMissingValueInterpretation,
                        options.getStorageWriteApiMaxRetries(),
                        bigLakeConfiguration))
                .withOutputTags(finalizeTag, tupleTagList)
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
    if (successfulRowsTag != null) {
      writeResults.get(successfulRowsTag).setCoder(successfulRowsCoder);
    }
    return writeResults;
  }

  static class WriteRecordsDoFn<DestinationT extends @NonNull Object, ElementT>
      extends DoFn<KV<DestinationT, StorageApiWritePayload>, KV<String, String>> {
    private final Counter forcedFlushes = Metrics.counter(WriteRecordsDoFn.class, "forcedFlushes");
    private final TupleTag<KV<String, String>> finalizeTag;
    private final TupleTag<BigQueryStorageApiInsertError> failedRowsTag;
    private final @Nullable TupleTag<TableRow> successfulRowsTag;

    private final Predicate<String> successfulRowsPredicate;
    private final boolean autoUpdateSchema;
    private final boolean ignoreUnknownValues;
    private final BigQueryIO.Write.CreateDisposition createDisposition;
    private final @Nullable String kmsKey;
    private final boolean usesCdc;
    private final AppendRowsRequest.MissingValueInterpretation defaultMissingValueInterpretation;

    static class AppendRowsContext extends RetryManager.Operation.Context<AppendRowsResponse> {
      long offset;
      ProtoRows protoRows;
      List<org.joda.time.Instant> timestamps;
      List<@Nullable TableRow> failsafeTableRows;

      int failureCount;

      public AppendRowsContext(
          long offset,
          ProtoRows protoRows,
          List<org.joda.time.Instant> timestamps,
          List<@Nullable TableRow> failsafeTableRows) {
        this.offset = offset;
        this.protoRows = protoRows;
        this.timestamps = timestamps;
        this.failureCount = 0;
        this.failsafeTableRows = failsafeTableRows;
      }
    }

    class DestinationState {
      private final TableDestination tableDestination;
      private final String tableUrn;
      private final String shortTableUrn;
      private String streamName = "";
      private @Nullable AppendClientInfo appendClientInfo = null;
      private long currentOffset = 0;
      private List<ByteString> pendingMessages;
      private List<org.joda.time.Instant> pendingTimestamps;
      private List<@Nullable TableRow> pendingFailsafeTableRows;
      private transient @Nullable WriteStreamService maybeWriteStreamService;
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
      private final Callable<Boolean> tryCreateTable;

      private final boolean useDefaultStream;
      private TableSchema initialTableSchema;
      private DescriptorProtos.DescriptorProto initialDescriptor;
      private Instant nextCacheTickle = Instant.MAX;
      private final int clientNumber;
      private final boolean usingMultiplexing;
      private final long maxRequestSize;

      private final boolean includeCdcColumns;

      public DestinationState(
          TableDestination tableDestination,
          String tableUrn,
          String shortTableUrn,
          MessageConverter<ElementT> messageConverter,
          WriteStreamService writeStreamService,
          boolean useDefaultStream,
          int streamAppendClientCount,
          boolean usingMultiplexing,
          long maxRequestSize,
          Callable<Boolean> tryCreateTable,
          boolean includeCdcColumns)
          throws Exception {
        this.tableDestination = tableDestination;
        this.tableUrn = tableUrn;
        this.shortTableUrn = shortTableUrn;
        this.pendingMessages = Lists.newArrayList();
        this.pendingTimestamps = Lists.newArrayList();
        this.pendingFailsafeTableRows = Lists.newArrayList();
        this.maybeWriteStreamService = writeStreamService;
        this.useDefaultStream = useDefaultStream;
        this.initialTableSchema = messageConverter.getTableSchema();
        this.initialDescriptor = messageConverter.getDescriptor(includeCdcColumns);
        this.clientNumber = new Random().nextInt(streamAppendClientCount);
        this.usingMultiplexing = usingMultiplexing;
        this.maxRequestSize = maxRequestSize;
        this.tryCreateTable = tryCreateTable;
        this.includeCdcColumns = includeCdcColumns;
        if (includeCdcColumns) {
          checkState(useDefaultStream);
        }
      }

      public TableDestination getTableDestination() {
        return tableDestination;
      }

      void teardown() {
        maybeTickleCache();
        if (appendClientInfo != null) {
          StreamAppendClient client = appendClientInfo.getStreamAppendClient();
          if (client != null) {
            runAsyncIgnoreFailure(closeWriterExecutor, client::unpin);
          }
          // if this is a PENDING stream, we won't be using it again after cleaning up this
          // destination state, so clear it from the cache
          if (!useDefaultStream) {
            APPEND_CLIENTS.invalidate(streamName);
          }
          appendClientInfo = null;
        }
      }

      String getDefaultStreamName() {
        return BigQueryHelpers.stripPartitionDecorator(tableUrn) + "/streams/_default";
      }

      String getStreamAppendClientCacheEntryKey() {
        if (useDefaultStream) {
          String defaultStreamKey = getDefaultStreamName() + "-client" + clientNumber;
          // The storage write API doesn't currently allow both inserts and updates/deletes on the
          // same connection.
          // Once this limitation is removed, we can remove this.
          return includeCdcColumns ? defaultStreamKey + "-cdc" : defaultStreamKey;
        }
        return this.streamName;
      }

      String getOrCreateStreamName() throws Exception {
        if (Strings.isNullOrEmpty(this.streamName)) {
          CreateTableHelpers.createTableWrapper(
              () -> {
                if (!useDefaultStream) {
                  this.streamName =
                      Preconditions.checkStateNotNull(maybeWriteStreamService)
                          .createWriteStream(tableUrn, Type.PENDING)
                          .getName();
                  this.currentOffset = 0;
                } else {
                  this.streamName = getDefaultStreamName();
                }
                return null;
              },
              tryCreateTable);
        }
        return this.streamName;
      }

      AppendClientInfo generateClient(@Nullable TableSchema updatedSchema) throws Exception {
        SchemaAndDescriptor schemaAndDescriptor = getCurrentTableSchema(streamName, updatedSchema);

        AtomicReference<AppendClientInfo> appendClientInfo =
            new AtomicReference<>(
                AppendClientInfo.of(
                    schemaAndDescriptor.tableSchema,
                    schemaAndDescriptor.descriptor,
                    // Make sure that the client is always closed in a different thread to avoid
                    // blocking.
                    client ->
                        runAsyncIgnoreFailure(
                            closeWriterExecutor,
                            () -> {
                              synchronized (APPEND_CLIENTS) {
                                // Remove the pin owned by the cache.
                                client.unpin();
                                client.close();
                              }
                            })));

        CreateTableHelpers.createTableWrapper(
            () -> {
              appendClientInfo.set(
                  appendClientInfo
                      .get()
                      .withAppendClient(
                          Preconditions.checkStateNotNull(maybeWriteStreamService),
                          () -> streamName,
                          usingMultiplexing,
                          defaultMissingValueInterpretation));
              Preconditions.checkStateNotNull(appendClientInfo.get().getStreamAppendClient());
              return null;
            },
            tryCreateTable);

        // This pin is "owned" by the cache.
        Preconditions.checkStateNotNull(appendClientInfo.get().getStreamAppendClient()).pin();
        return appendClientInfo.get();
      }

      private class SchemaAndDescriptor {
        private final TableSchema tableSchema;
        private final DescriptorProtos.DescriptorProto descriptor;

        private SchemaAndDescriptor(
            TableSchema tableSchema, DescriptorProtos.DescriptorProto descriptor) {
          this.tableSchema = tableSchema;
          this.descriptor = descriptor;
        }
      }

      SchemaAndDescriptor getCurrentTableSchema(String stream, @Nullable TableSchema updatedSchema)
          throws Exception {
        if (updatedSchema != null) {
          return new SchemaAndDescriptor(
              updatedSchema,
              TableRowToStorageApiProto.descriptorSchemaFromTableSchema(
                  updatedSchema, true, includeCdcColumns));
        }

        AtomicReference<TableSchema> currentSchema = new AtomicReference<>(initialTableSchema);
        AtomicBoolean updated = new AtomicBoolean();
        CreateTableHelpers.createTableWrapper(
            () -> {
              if (autoUpdateSchema) {
                @Nullable
                WriteStream writeStream =
                    Preconditions.checkStateNotNull(maybeWriteStreamService)
                        .getWriteStream(streamName);
                if (writeStream != null && writeStream.hasTableSchema()) {
                  Optional<TableSchema> newSchema =
                      TableSchemaUpdateUtils.getUpdatedSchema(
                          initialTableSchema, writeStream.getTableSchema());
                  if (newSchema.isPresent()) {
                    currentSchema.set(newSchema.get());
                    updated.set(true);
                    LOG.debug(
                        "Fetched updated schema for table {}:\n\t{}", tableUrn, newSchema.get());
                  }
                }
              }
              return null;
            },
            tryCreateTable);
        // Note: While it may appear that these two branches are the same, it's important to return
        // the actual
        // initial descriptor if the schema has not changed. Simply converting the schema back into
        // a descriptor isn't
        // the same, and would break the direct-from-proto ingestion path.
        DescriptorProtos.DescriptorProto descriptor =
            updated.get()
                ? TableRowToStorageApiProto.descriptorSchemaFromTableSchema(
                    currentSchema.get(), true, includeCdcColumns)
                : initialDescriptor;
        return new SchemaAndDescriptor(currentSchema.get(), descriptor);
      }

      AppendClientInfo getAppendClientInfo(
          boolean lookupCache, final @Nullable TableSchema updatedSchema) {
        try {
          if (this.appendClientInfo == null) {
            getOrCreateStreamName();
            final AppendClientInfo newAppendClientInfo;
            synchronized (APPEND_CLIENTS) {
              if (lookupCache) {
                newAppendClientInfo =
                    APPEND_CLIENTS.get(
                        getStreamAppendClientCacheEntryKey(), () -> generateClient(updatedSchema));
              } else {
                newAppendClientInfo = generateClient(updatedSchema);
                // override the clients in the cache.
                APPEND_CLIENTS.put(getStreamAppendClientCacheEntryKey(), newAppendClientInfo);
              }
              // This pin is "owned" by the current DoFn.
              Preconditions.checkStateNotNull(newAppendClientInfo.getStreamAppendClient()).pin();
            }
            nextCacheTickle = Instant.now().plus(java.time.Duration.ofMinutes(1));
            this.appendClientInfo = newAppendClientInfo;
          }
          return Preconditions.checkStateNotNull(appendClientInfo);
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
            StreamAppendClient client = appendClientInfo.getStreamAppendClient();
            if (client != null) {
              runAsyncIgnoreFailure(closeWriterExecutor, client::unpin);
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

      void addMessage(
          StorageApiWritePayload payload,
          org.joda.time.Instant elementTs,
          OutputReceiver<BigQueryStorageApiInsertError> failedRowsReceiver)
          throws Exception {
        maybeTickleCache();
        ByteString payloadBytes = ByteString.copyFrom(payload.getPayload());
        @Nullable TableRow failsafeTableRow = payload.getFailsafeTableRow();
        if (autoUpdateSchema) {
          if (appendClientInfo == null) {
            appendClientInfo = getAppendClientInfo(true, null);
          }
          @Nullable TableRow unknownFields = payload.getUnknownFields();
          if (unknownFields != null) {
            try {
              payloadBytes =
                  payloadBytes.concat(
                      Preconditions.checkStateNotNull(appendClientInfo)
                          .encodeUnknownFields(unknownFields, ignoreUnknownValues));
            } catch (TableRowToStorageApiProto.SchemaConversionException e) {
              @Nullable TableRow tableRow = payload.getFailsafeTableRow();
              if (tableRow == null) {
                tableRow =
                    checkNotNull(appendClientInfo)
                        .toTableRow(payloadBytes, Predicates.alwaysTrue());
              }
              // TODO(24926, reuvenlax): We need to merge the unknown fields in! Currently we only
              // execute this
              // codepath when ignoreUnknownFields==true, so we should never hit this codepath.
              // However once
              // 24926 is fixed, we need to merge the unknownFields back into the main row before
              // outputting to the
              // failed-rows consumer.
              org.joda.time.Instant timestamp = payload.getTimestamp();
              rowsSentToFailedRowsCollection.inc();
              failedRowsReceiver.outputWithTimestamp(
                  new BigQueryStorageApiInsertError(tableRow, e.toString()),
                  timestamp != null ? timestamp : elementTs);
              return;
            }
          }
        }
        pendingMessages.add(payloadBytes);
        pendingFailsafeTableRows.add(failsafeTableRow);

        org.joda.time.Instant timestamp = payload.getTimestamp();
        pendingTimestamps.add(timestamp != null ? timestamp : elementTs);
      }

      long flush(
          RetryManager<AppendRowsResponse, AppendRowsContext> retryManager,
          OutputReceiver<BigQueryStorageApiInsertError> failedRowsReceiver,
          @Nullable OutputReceiver<TableRow> successfulRowsReceiver)
          throws Exception {
        if (pendingMessages.isEmpty()) {
          return 0;
        }

        final ProtoRows.Builder insertsBuilder = ProtoRows.newBuilder();
        insertsBuilder.addAllSerializedRows(pendingMessages);
        pendingMessages.clear();
        final ProtoRows inserts = insertsBuilder.build();
        List<org.joda.time.Instant> insertTimestamps = pendingTimestamps;
        List<@Nullable TableRow> failsafeTableRows = pendingFailsafeTableRows;
        pendingTimestamps = Lists.newArrayList();
        pendingFailsafeTableRows = Lists.newArrayList();

        // Handle the case where the request is too large.
        if (inserts.getSerializedSize() >= maxRequestSize) {
          if (inserts.getSerializedRowsCount() > 1) {
            // TODO(reuvenlax): Is it worth trying to handle this case by splitting the protoRows?
            // Given that we split
            // the ProtoRows iterable at 2MB and the max request size is 10MB, this scenario seems
            // nearly impossible.
            LOG.error(
                "A request containing more than one row is over the request size limit of {}. "
                    + "This is unexpected. All rows in the request will be sent to the failed-rows PCollection.",
                maxRequestSize);
          }
          for (int i = 0; i < inserts.getSerializedRowsCount(); ++i) {
            @Nullable TableRow failedRow = failsafeTableRows.get(i);
            if (failedRow == null) {
              ByteString rowBytes = inserts.getSerializedRows(i);
              failedRow =
                  TableRowToStorageApiProto.tableRowFromMessage(
                      DynamicMessage.parseFrom(
                          TableRowToStorageApiProto.wrapDescriptorProto(
                              getAppendClientInfo(true, null).getDescriptor()),
                          rowBytes),
                      true,
                      successfulRowsPredicate);
            }
            org.joda.time.Instant timestamp = insertTimestamps.get(i);
            failedRowsReceiver.outputWithTimestamp(
                new BigQueryStorageApiInsertError(
                    failedRow, "Row payload too large. Maximum size " + maxRequestSize),
                timestamp);
          }
          int numRowsFailed = inserts.getSerializedRowsCount();
          BigQuerySinkMetrics.appendRowsRowStatusCounter(
                  BigQuerySinkMetrics.RowStatus.FAILED,
                  BigQuerySinkMetrics.PAYLOAD_TOO_LARGE,
                  shortTableUrn)
              .inc(numRowsFailed);
          rowsSentToFailedRowsCollection.inc(numRowsFailed);
          return 0;
        }

        long offset = -1;
        if (!this.useDefaultStream) {
          getOrCreateStreamName(); // Force creation of the stream before we get offsets.
          offset = this.currentOffset;
          this.currentOffset += inserts.getSerializedRowsCount();
        }
        AppendRowsContext appendRowsContext =
            new AppendRowsContext(offset, inserts, insertTimestamps, failsafeTableRows);

        retryManager.addOperation(
            c -> {
              if (c.protoRows.getSerializedRowsCount() == 0) {
                // This might happen if all rows in a batch failed and were sent to the failed-rows
                // PCollection.
                return ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build());
              }
              try {
                StreamAppendClient writeStream =
                    Preconditions.checkStateNotNull(
                        getAppendClientInfo(true, null).getStreamAppendClient());
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
              BigQuerySinkMetrics.reportFailedRPCMetrics(
                  failedContext, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, shortTableUrn);
              String errorCode =
                  BigQuerySinkMetrics.throwableToGRPCCodeString(failedContext.getError());

              if (failedContext.getError() != null
                  && failedContext.getError() instanceof Exceptions.AppendSerializationError) {
                Exceptions.AppendSerializationError error =
                    Preconditions.checkStateNotNull(
                        (Exceptions.AppendSerializationError) failedContext.getError());

                Set<Integer> failedRowIndices = error.getRowIndexToErrorMessage().keySet();
                for (int failedIndex : failedRowIndices) {
                  // Convert the message to a TableRow and send it to the failedRows collection.
                  BigQueryStorageApiInsertError element = null;
                  org.joda.time.Instant timestamp = failedContext.timestamps.get(failedIndex);
                  try {
                    TableRow failedRow = failedContext.failsafeTableRows.get(failedIndex);
                    if (failedRow == null) {
                      ByteString protoBytes =
                          failedContext.protoRows.getSerializedRows(failedIndex);
                      failedRow =
                          TableRowToStorageApiProto.tableRowFromMessage(
                              DynamicMessage.parseFrom(
                                  TableRowToStorageApiProto.wrapDescriptorProto(
                                      Preconditions.checkStateNotNull(appendClientInfo)
                                          .getDescriptor()),
                                  protoBytes),
                              true,
                              Predicates.alwaysTrue());
                    }
                    element =
                        new BigQueryStorageApiInsertError(
                            failedRow, error.getRowIndexToErrorMessage().get(failedIndex));
                  } catch (Exception e) {
                    LOG.error("Failed to insert row and could not parse the result!", e);
                  }
                  // output outside try {} clause to avoid suppress downstream Exception
                  if (element != null) {
                    failedRowsReceiver.outputWithTimestamp(element, timestamp);
                  }
                }
                int numRowsFailed = failedRowIndices.size();
                rowsSentToFailedRowsCollection.inc(numRowsFailed);
                BigQuerySinkMetrics.appendRowsRowStatusCounter(
                        BigQuerySinkMetrics.RowStatus.FAILED, errorCode, shortTableUrn)
                    .inc(numRowsFailed);

                // Remove the failed row from the payload, so we retry the batch without the failed
                // rows.
                ProtoRows.Builder retryRows = ProtoRows.newBuilder();
                List<org.joda.time.Instant> retryTimestamps = Lists.newArrayList();
                for (int i = 0; i < failedContext.protoRows.getSerializedRowsCount(); ++i) {
                  if (!failedRowIndices.contains(i)) {
                    ByteString rowBytes = failedContext.protoRows.getSerializedRows(i);
                    retryRows.addSerializedRows(rowBytes);
                    retryTimestamps.add(failedContext.timestamps.get(i));
                  }
                }
                failedContext.protoRows = retryRows.build();
                failedContext.timestamps = retryTimestamps;
                int numRowsRetried = failedContext.protoRows.getSerializedRowsCount();
                BigQuerySinkMetrics.appendRowsRowStatusCounter(
                        BigQuerySinkMetrics.RowStatus.RETRIED, errorCode, shortTableUrn)
                    .inc(numRowsRetried);

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
                  "Append to stream {} by client #{} failed with error, operations will be retried.\n{}",
                  streamName,
                  clientNumber,
                  retrieveErrorDetails(contexts));
              failedContext.failureCount += 1;

              boolean quotaError = false;
              Throwable error = failedContext.getError();
              Status.Code statusCode = Status.Code.OK;
              if (error != null) {
                statusCode = Status.fromThrowable(error).getCode();
                quotaError = statusCode.equals(Status.Code.RESOURCE_EXHAUSTED);
              }

              int allowedRetry;

              if (!quotaError) {
                // This forces us to close and reopen all gRPC connections to Storage API on error,
                // which empirically fixes random stuckness issues.
                invalidateWriteStream();
                allowedRetry = 5;
              } else {
                allowedRetry = 35;
              }

              // Maximum number of times we retry before we fail the work item.
              if (failedContext.failureCount > allowedRetry) {
                throw new RuntimeException(
                    String.format(
                        "More than %d attempts to call AppendRows failed.", allowedRetry));
              }

              // The following errors are known to be persistent, so always fail the work item in
              // this case.
              if (statusCode.equals(Status.Code.OUT_OF_RANGE)
                  || statusCode.equals(Status.Code.ALREADY_EXISTS)) {
                throw new RuntimeException(
                    "Append to stream "
                        + this.streamName
                        + " failed with invalid "
                        + "offset of "
                        + failedContext.offset);
              }

              boolean hasPersistentErrors =
                  failedContext.getError() instanceof Exceptions.StreamFinalizedException
                      || statusCode.equals(Status.Code.INVALID_ARGUMENT)
                      || statusCode.equals(Status.Code.NOT_FOUND)
                      || statusCode.equals(Status.Code.FAILED_PRECONDITION);
              if (hasPersistentErrors) {
                throw new RuntimeException(
                    String.format(
                        "Append to stream %s failed with Status Code %s. The stream may not exist.",
                        this.streamName, statusCode),
                    error);
              }
              // TODO: Only do this on explicit NOT_FOUND errors once BigQuery reliably produces
              // them.
              try {
                tryCreateTable.call();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }

              int numRowsRetried = failedContext.protoRows.getSerializedRowsCount();
              BigQuerySinkMetrics.appendRowsRowStatusCounter(
                      BigQuerySinkMetrics.RowStatus.RETRIED, errorCode, shortTableUrn)
                  .inc(numRowsRetried);

              appendFailures.inc();
              return RetryType.RETRY_ALL_OPERATIONS;
            },
            c -> {
              int numRecordsAppended = c.protoRows.getSerializedRowsCount();
              recordsAppended.inc(numRecordsAppended);
              BigQuerySinkMetrics.appendRowsRowStatusCounter(
                      BigQuerySinkMetrics.RowStatus.SUCCESSFUL,
                      BigQuerySinkMetrics.OK,
                      shortTableUrn)
                  .inc(numRecordsAppended);

              BigQuerySinkMetrics.reportSuccessfulRpcMetrics(
                  c, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, shortTableUrn);

              if (successfulRowsReceiver != null) {
                Descriptor descriptor = null;
                try {
                  descriptor =
                      TableRowToStorageApiProto.wrapDescriptorProto(
                          Preconditions.checkStateNotNull(appendClientInfo).getDescriptor());
                } catch (DescriptorValidationException e) {
                  LOG.warn(
                      "Failure getting proto descriptor. Successful output will not be produced.",
                      e);
                }
                if (descriptor != null) {
                  for (int i = 0; i < c.protoRows.getSerializedRowsCount(); ++i) {
                    ByteString rowBytes = c.protoRows.getSerializedRowsList().get(i);
                    try {
                      TableRow row =
                          TableRowToStorageApiProto.tableRowFromMessage(
                              DynamicMessage.parseFrom(descriptor, rowBytes),
                              true,
                              successfulRowsPredicate);
                      org.joda.time.Instant timestamp = c.timestamps.get(i);
                      successfulRowsReceiver.outputWithTimestamp(row, timestamp);
                    } catch (Exception e) {
                      LOG.warn("Failure parsing TableRow", e);
                    }
                  }
                }
              }
            },
            appendRowsContext);
        maybeTickleCache();
        return inserts.getSerializedRowsCount();
      }

      String retrieveErrorDetails(Iterable<AppendRowsContext> failedContext) {
        return StreamSupport.stream(failedContext.spliterator(), false)
            .<@Nullable Throwable>map(AppendRowsContext::getError)
            .filter(Objects::nonNull)
            .map(
                thrw ->
                    Preconditions.checkStateNotNull(thrw).toString()
                        + "\n"
                        + Arrays.stream(Preconditions.checkStateNotNull(thrw).getStackTrace())
                            .map(StackTraceElement::toString)
                            .collect(Collectors.joining("\n")))
            .collect(Collectors.joining("\n"));
      }

      void postFlush() {
        // If we got a response indicating an updated schema, recreate the client.
        if (this.appendClientInfo != null && autoUpdateSchema) {
          @Nullable
          StreamAppendClient streamAppendClient = appendClientInfo.getStreamAppendClient();
          @Nullable
          TableSchema updatedTableSchemaReturned =
              (streamAppendClient != null) ? streamAppendClient.getUpdatedSchema() : null;
          if (updatedTableSchemaReturned != null) {
            Optional<TableSchema> updatedTableSchema =
                TableSchemaUpdateUtils.getUpdatedSchema(
                    this.initialTableSchema, updatedTableSchemaReturned);
            if (updatedTableSchema.isPresent()) {
              invalidateWriteStream();
              appendClientInfo =
                  Preconditions.checkStateNotNull(
                      getAppendClientInfo(false, updatedTableSchema.get()));
            }
          }
        }
      }
    }

    private @Nullable Map<DestinationT, DestinationState> destinations = Maps.newHashMap();
    private final TwoLevelMessageConverterCache<DestinationT, ElementT> messageConverters;
    private transient @Nullable DatasetService maybeDatasetService;
    private transient @Nullable WriteStreamService maybeWriteStreamService;
    private int numPendingRecords = 0;
    private int numPendingRecordBytes = 0;
    private final int flushThresholdBytes;
    private final int flushThresholdCount;
    private final int maxRetries;
    private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
    private final BigQueryServices bqServices;
    private final boolean useDefaultStream;
    private int streamAppendClientCount;
    private final @Nullable Map<String, String> bigLakeConfiguration;

    WriteRecordsDoFn(
        String operationName,
        StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
        BigQueryServices bqServices,
        boolean useDefaultStream,
        int flushThresholdBytes,
        int flushThresholdCount,
        int streamAppendClientCount,
        TupleTag<KV<String, String>> finalizeTag,
        TupleTag<BigQueryStorageApiInsertError> failedRowsTag,
        @Nullable TupleTag<TableRow> successfulRowsTag,
        Predicate<String> successfulRowsPredicate,
        boolean autoUpdateSchema,
        boolean ignoreUnknownValues,
        BigQueryIO.Write.CreateDisposition createDisposition,
        @Nullable String kmsKey,
        boolean usesCdc,
        AppendRowsRequest.MissingValueInterpretation defaultMissingValueInterpretation,
        int maxRetries,
        @Nullable Map<String, String> bigLakeConfiguration) {
      this.messageConverters = new TwoLevelMessageConverterCache<>(operationName);
      this.dynamicDestinations = dynamicDestinations;
      this.bqServices = bqServices;
      this.useDefaultStream = useDefaultStream;
      this.flushThresholdBytes = flushThresholdBytes;
      this.flushThresholdCount = flushThresholdCount;
      this.streamAppendClientCount = streamAppendClientCount;
      this.finalizeTag = finalizeTag;
      this.failedRowsTag = failedRowsTag;
      this.successfulRowsTag = successfulRowsTag;
      this.successfulRowsPredicate = successfulRowsPredicate;
      this.autoUpdateSchema = autoUpdateSchema;
      this.ignoreUnknownValues = ignoreUnknownValues;
      this.createDisposition = createDisposition;
      this.kmsKey = kmsKey;
      this.usesCdc = usesCdc;
      this.defaultMissingValueInterpretation = defaultMissingValueInterpretation;
      this.maxRetries = maxRetries;
      this.bigLakeConfiguration = bigLakeConfiguration;
    }

    boolean shouldFlush() {
      return numPendingRecords > flushThresholdCount || numPendingRecordBytes > flushThresholdBytes;
    }

    void flushIfNecessary(
        OutputReceiver<BigQueryStorageApiInsertError> failedRowsReceiver,
        @Nullable OutputReceiver<TableRow> successfulRowsReceiver)
        throws Exception {
      if (shouldFlush()) {
        forcedFlushes.inc();
        // Too much memory being used. Flush the state and wait for it to drain out.
        // TODO(reuvenlax): Consider waiting for memory usage to drop instead of waiting for all the
        // appends to finish.
        flushAll(failedRowsReceiver, successfulRowsReceiver);
      }
    }

    void flushAll(
        OutputReceiver<BigQueryStorageApiInsertError> failedRowsReceiver,
        @Nullable OutputReceiver<TableRow> successfulRowsReceiver)
        throws Exception {
      List<RetryManager<AppendRowsResponse, AppendRowsContext>> retryManagers =
          Lists.newArrayListWithCapacity(Preconditions.checkStateNotNull(destinations).size());
      long numRowsWritten = 0;
      for (DestinationState destinationState :
          Preconditions.checkStateNotNull(destinations).values()) {

        RetryManager<AppendRowsResponse, AppendRowsContext> retryManager =
            new RetryManager<>(
                Duration.standardSeconds(1),
                Duration.standardSeconds(20),
                maxRetries,
                BigQuerySinkMetrics.throttledTimeCounter(
                    BigQuerySinkMetrics.RpcMethod.APPEND_ROWS));
        retryManagers.add(retryManager);
        numRowsWritten +=
            destinationState.flush(retryManager, failedRowsReceiver, successfulRowsReceiver);
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
      for (DestinationState destinationState :
          Preconditions.checkStateNotNull(destinations).values()) {
        destinationState.postFlush();
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

    private WriteStreamService initializeWriteStreamService(PipelineOptions pipelineOptions) {
      if (maybeWriteStreamService == null) {
        maybeWriteStreamService =
            bqServices.getWriteStreamService(pipelineOptions.as(BigQueryOptions.class));
      }
      return maybeWriteStreamService;
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
        boolean useCdc,
        DatasetService datasetService,
        WriteStreamService writeStreamService,
        BigQueryOptions bigQueryOptions) {
      TableDestination tableDestination1 = dynamicDestinations.getTable(destination);
      checkArgument(
          tableDestination1 != null,
          "DynamicDestinations.getTable() may not return null, "
              + "but %s returned null for destination %s",
          dynamicDestinations,
          destination);
      @Nullable Coder<DestinationT> destinationCoder = dynamicDestinations.getDestinationCoder();
      Callable<Boolean> tryCreateTable =
          () -> {
            CreateTableHelpers.possiblyCreateTable(
                c.getPipelineOptions().as(BigQueryOptions.class),
                tableDestination1,
                () -> dynamicDestinations.getSchema(destination),
                () -> dynamicDestinations.getTableConstraints(destination),
                createDisposition,
                destinationCoder,
                kmsKey,
                bqServices,
                bigLakeConfiguration);
            return true;
          };

      MessageConverter<ElementT> messageConverter;
      try {
        messageConverter = messageConverters.get(destination, dynamicDestinations, datasetService);
        return new DestinationState(
            tableDestination1,
            tableDestination1.getTableUrn(bigQueryOptions),
            tableDestination1.getShortTableUrn(),
            messageConverter,
            writeStreamService,
            useDefaultStream,
            streamAppendClientCount,
            bigQueryOptions.getUseStorageApiConnectionPool(),
            bigQueryOptions.getStorageWriteApiMaxRequestSize(),
            tryCreateTable,
            useCdc);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @ProcessElement
    public void process(
        ProcessContext c,
        PipelineOptions pipelineOptions,
        @Element KV<DestinationT, StorageApiWritePayload> element,
        @Timestamp org.joda.time.Instant elementTs,
        MultiOutputReceiver o)
        throws Exception {
      DatasetService initializedDatasetService = initializeDatasetService(pipelineOptions);
      WriteStreamService initializedWriteStreamService =
          initializeWriteStreamService(pipelineOptions);
      dynamicDestinations.setSideInputAccessorFromProcessContext(c);
      DestinationState state =
          Preconditions.checkStateNotNull(destinations)
              .computeIfAbsent(
                  element.getKey(),
                  destination ->
                      createDestinationState(
                          c,
                          destination,
                          usesCdc,
                          initializedDatasetService,
                          initializedWriteStreamService,
                          pipelineOptions.as(BigQueryOptions.class)));
      Lineage.getSinks()
          .add(
              "bigquery",
              BigQueryHelpers.dataCatalogSegments(
                  state.getTableDestination().getTableReference(),
                  pipelineOptions.as(BigQueryOptions.class)));

      OutputReceiver<BigQueryStorageApiInsertError> failedRowsReceiver = o.get(failedRowsTag);
      @Nullable
      OutputReceiver<TableRow> successfulRowsReceiver =
          (successfulRowsTag != null) ? o.get(successfulRowsTag) : null;
      flushIfNecessary(failedRowsReceiver, successfulRowsReceiver);
      state.addMessage(element.getValue(), elementTs, failedRowsReceiver);
      ++numPendingRecords;
      numPendingRecordBytes += element.getValue().getPayload().length;
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) throws Exception {
      OutputReceiver<BigQueryStorageApiInsertError> failedRowsReceiver =
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
          };
      @Nullable OutputReceiver<TableRow> successfulRowsReceiver = null;
      if (successfulRowsTag != null) {
        successfulRowsReceiver =
            new OutputReceiver<TableRow>() {
              @Override
              public void output(TableRow output) {
                outputWithTimestamp(output, GlobalWindow.INSTANCE.maxTimestamp());
              }

              @Override
              public void outputWithTimestamp(TableRow output, org.joda.time.Instant timestamp) {
                context.output(successfulRowsTag, output, timestamp, GlobalWindow.INSTANCE);
              }
            };
      }

      flushAll(failedRowsReceiver, successfulRowsReceiver);

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
        if (maybeWriteStreamService != null) {
          maybeWriteStreamService.close();
          maybeWriteStreamService = null;
        }
        if (maybeDatasetService != null) {
          maybeDatasetService.close();
          maybeDatasetService = null;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
    }
  }
}
