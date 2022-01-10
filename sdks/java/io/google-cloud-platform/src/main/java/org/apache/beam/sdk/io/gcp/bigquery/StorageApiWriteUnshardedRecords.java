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

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.WriteStream.Type;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StreamAppendClient;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.Operation.Context;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.RetryType;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiDynamicDestinations.MessageConverter;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"nullness", "FutureReturnValueIgnored"})
/**
 * Write records to the Storage API using a standard batch approach. PENDING streams are used, which
 * do not become visible until they are finalized and committed. Each input bundle to the DoFn
 * creates a stream per output table, appends all records in the bundle to the stream, and schedules
 * a finalize/commit operation at the end.
 */
public class StorageApiWriteUnshardedRecords<DestinationT, ElementT>
    extends PTransform<PCollection<KV<DestinationT, byte[]>>, PCollection<Void>> {
  private static final Logger LOG = LoggerFactory.getLogger(StorageApiWriteUnshardedRecords.class);

  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private final CreateDisposition createDisposition;
  private final String kmsKey;
  private final BigQueryServices bqServices;
  private final Coder<DestinationT> destinationCoder;
  private static final ExecutorService closeWriterExecutor = Executors.newCachedThreadPool();

  private static final Cache<String, StreamAppendClient> APPEND_CLIENTS =
      CacheBuilder.newBuilder()
          .expireAfterAccess(5, TimeUnit.MINUTES)
          .removalListener(
              (RemovalNotification<String, StreamAppendClient> removal) -> {
                @Nullable final StreamAppendClient streamAppendClient = removal.getValue();
                // Close the writer in a different thread so as not to block the main one.
                runAsyncIgnoreFailure(closeWriterExecutor, streamAppendClient::close);
              })
          .build();

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
      CreateDisposition createDisposition,
      String kmsKey,
      BigQueryServices bqServices,
      Coder<DestinationT> destinationCoder) {
    this.dynamicDestinations = dynamicDestinations;
    this.createDisposition = createDisposition;
    this.kmsKey = kmsKey;
    this.bqServices = bqServices;
    this.destinationCoder = destinationCoder;
  }

  @Override
  public PCollection<Void> expand(PCollection<KV<DestinationT, byte[]>> input) {
    String operationName = input.getName() + "/" + getName();
    return input
        .apply(
            "Write Records",
            ParDo.of(
                    new WriteRecordsDoFn<>(
                        operationName,
                        dynamicDestinations,
                        bqServices,
                        destinationCoder,
                        createDisposition,
                        kmsKey,
                        false))
                .withSideInputs(dynamicDestinations.getSideInputs()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        // Calling Reshuffle makes the output stable - once this completes, the append operations
        // will not retry.
        // TODO(reuvenlax): This should use RequiresStableInput instead.
        .apply("Reshuffle", Reshuffle.of())
        .apply("Finalize writes", ParDo.of(new StorageApiFinalizeWritesDoFn(bqServices)));
  }

  static class WriteRecordsDoFn<DestinationT, ElementT>
      extends DoFn<KV<DestinationT, byte[]>, KV<String, String>> {
    class DestinationState {
      private final String tableUrn;
      private final MessageConverter<ElementT> messageConverter;
      private String streamName = "";
      private @Nullable StreamAppendClient streamAppendClient = null;
      private long currentOffset = 0;
      private List<ByteString> pendingMessages;
      private transient @Nullable DatasetService datasetService;
      private final Counter recordsAppended =
          Metrics.counter(WriteRecordsDoFn.class, "recordsAppended");
      private final Counter appendFailures =
          Metrics.counter(WriteRecordsDoFn.class, "appendFailures");
      private final boolean useDefaultStream;

      public DestinationState(
          String tableUrn,
          MessageConverter<ElementT> messageConverter,
          DatasetService datasetService,
          boolean useDefaultStream) {
        this.tableUrn = tableUrn;
        this.messageConverter = messageConverter;
        this.pendingMessages = Lists.newArrayList();
        this.datasetService = datasetService;
        this.useDefaultStream = useDefaultStream;
      }

      void close() {
        if (streamAppendClient != null) {
          try {
            streamAppendClient.close();
            streamAppendClient = null;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }

      String getDefaultStreamName() {
        return BigQueryHelpers.stripPartitionDecorator(tableUrn) + "/streams/_default";
      }

      StreamAppendClient getWriteStream() {
        try {
          if (streamAppendClient == null) {
            if (!useDefaultStream) {
              this.streamName =
                  Preconditions.checkNotNull(datasetService)
                      .createWriteStream(tableUrn, Type.PENDING)
                      .getName();
            } else {
              this.streamName = getDefaultStreamName();
            }
            this.streamAppendClient =
                APPEND_CLIENTS.get(
                    streamName,
                    () ->
                        datasetService.getStreamAppendClient(
                            streamName, messageConverter.getSchemaDescriptor()));
            this.currentOffset = 0;
          }
          return streamAppendClient;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      void invalidateWriteStream() {
        try {
          runAsyncIgnoreFailure(closeWriterExecutor, streamAppendClient::close);
          streamAppendClient = null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      void addMessage(byte[] message) throws Exception {
        pendingMessages.add(ByteString.copyFrom(message));
      }

      @SuppressWarnings({"nullness"})
      void flush(RetryManager<AppendRowsResponse, Context<AppendRowsResponse>> retryManager)
          throws Exception {
        if (pendingMessages.isEmpty()) {
          return;
        }
        final ProtoRows.Builder inserts = ProtoRows.newBuilder();
        for (ByteString m : pendingMessages) {
          inserts.addSerializedRows(m);
        }

        ProtoRows protoRows = inserts.build();
        pendingMessages.clear();

        retryManager.addOperation(
            c -> {
              try {
                StreamAppendClient writeStream = getWriteStream();
                long offset = -1;
                if (!this.useDefaultStream) {
                  offset = this.currentOffset;
                  this.currentOffset += inserts.getSerializedRowsCount();
                }
                return writeStream.appendRows(offset, protoRows);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            },
            contexts -> {
              LOG.info(
                  "Append to stream "
                      + streamName
                      + " failed with error "
                      + Iterables.getFirst(contexts, null).getError());
              invalidateWriteStream();
              appendFailures.inc();
              return RetryType.RETRY_ALL_OPERATIONS;
            },
            response -> {
              recordsAppended.inc(protoRows.getSerializedRowsCount());
            },
            new Context<>());
      }
    }

    private Map<DestinationT, DestinationState> destinations = Maps.newHashMap();
    private final TwoLevelMessageConverterCache<DestinationT, ElementT> messageConverters;
    private transient @Nullable DatasetService datasetService;
    private int numPendingRecords = 0;
    private int numPendingRecordBytes = 0;
    private static final int FLUSH_THRESHOLD_RECORDS = 100;
    private static final int FLUSH_THRESHOLD_RECORD_BYTES = 2 * 1024 * 1024;
    private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
    private final BigQueryServices bqServices;
    private final Coder<DestinationT> destinationCoder;
    private final CreateDisposition createDisposition;
    private final String kmsKey;
    private final boolean useDefaultStream;

    WriteRecordsDoFn(
        String operationName,
        StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
        BigQueryServices bqServices,
        Coder<DestinationT> destinationCoder,
        CreateDisposition createDisposition,
        String kmsKey,
        boolean useDefaultStream) {
      this.messageConverters = new TwoLevelMessageConverterCache<>(operationName);
      this.dynamicDestinations = dynamicDestinations;
      this.bqServices = bqServices;
      this.destinationCoder = destinationCoder;
      this.createDisposition = createDisposition;
      this.kmsKey = kmsKey;
      this.useDefaultStream = useDefaultStream;
    }

    boolean shouldFlush() {
      return numPendingRecords > FLUSH_THRESHOLD_RECORDS
          || numPendingRecordBytes > FLUSH_THRESHOLD_RECORD_BYTES;
    }

    void flushIfNecessary() throws Exception {
      if (shouldFlush()) {
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
      for (DestinationState destinationState : destinations.values()) {
        destinationState.flush(retryManager);
      }
      retryManager.run(true);
      numPendingRecords = 0;
      numPendingRecordBytes = 0;
    }

    private void initializeDatasetService(PipelineOptions pipelineOptions) {
      if (datasetService == null) {
        datasetService = bqServices.getDatasetService(pipelineOptions.as(BigQueryOptions.class));
      }
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
      Supplier<TableSchema> schemaSupplier = () -> dynamicDestinations.getSchema(destination);
      TableDestination createdTable =
          CreateTableHelpers.possiblyCreateTable(
              c,
              tableDestination1,
              schemaSupplier,
              createDisposition,
              destinationCoder,
              kmsKey,
              bqServices);

      MessageConverter<ElementT> messageConverter;
      try {
        messageConverter = messageConverters.get(destination, dynamicDestinations, datasetService);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return new DestinationState(
          createdTable.getTableUrn(), messageConverter, datasetService, useDefaultStream);
    }

    @ProcessElement
    public void process(
        ProcessContext c,
        PipelineOptions pipelineOptions,
        @Element KV<DestinationT, byte[]> element)
        throws Exception {
      initializeDatasetService(pipelineOptions);
      dynamicDestinations.setSideInputAccessorFromProcessContext(c);
      DestinationState state =
          destinations.computeIfAbsent(
              element.getKey(), k -> createDestinationState(c, k, datasetService));
      flushIfNecessary();
      state.addMessage(element.getValue());
      ++numPendingRecords;
      numPendingRecordBytes += element.getValue().length;
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) throws Exception {
      flushAll();
      if (!useDefaultStream) {
        for (DestinationState state : destinations.values()) {
          context.output(
              KV.of(state.tableUrn, state.streamName),
              BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.millis(1)),
              GlobalWindow.INSTANCE);
        }
      }
    }

    @Teardown
    public void teardown() {
      for (DestinationState state : destinations.values()) {
        state.close();
      }
      destinations.clear();
      try {
        if (datasetService != null) {
          datasetService.close();
          datasetService = null;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
