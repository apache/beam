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

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.Operation.Context;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.RetryType;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiFlushAndFinalizeDoFn.Operation;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This DoFn flushes and optionally (if requested) finalizes Storage API streams. */
public class StorageApiFlushAndFinalizeDoFn extends DoFn<KV<String, Operation>, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(StorageApiFlushAndFinalizeDoFn.class);

  private final BigQueryServices bqServices;
  private transient @Nullable DatasetService datasetService = null;
  private final Counter flushOperationsSent =
      Metrics.counter(StorageApiFlushAndFinalizeDoFn.class, "flushOperationsSent");
  private final Counter flushOperationsSucceeded =
      Metrics.counter(StorageApiFlushAndFinalizeDoFn.class, "flushOperationsSucceeded");
  private final Counter flushOperationsFailed =
      Metrics.counter(StorageApiFlushAndFinalizeDoFn.class, "flushOperationsFailed");
  private final Counter flushOperationsAlreadyExists =
      Metrics.counter(StorageApiFlushAndFinalizeDoFn.class, "flushOperationsAlreadyExists");
  private final Counter flushOperationsInvalidArgument =
      Metrics.counter(StorageApiFlushAndFinalizeDoFn.class, "flushOperationsInvalidArgument");
  private final Distribution flushLatencyDistribution =
      Metrics.distribution(StorageApiFlushAndFinalizeDoFn.class, "flushOperationLatencyMs");
  private final Counter finalizeOperationsSent =
      Metrics.counter(StorageApiFlushAndFinalizeDoFn.class, "finalizeOperationsSent");
  private final Counter finalizeOperationsSucceeded =
      Metrics.counter(StorageApiFlushAndFinalizeDoFn.class, "finalizeOperationsSucceeded");
  private final Counter finalizeOperationsFailed =
      Metrics.counter(StorageApiFlushAndFinalizeDoFn.class, "finalizeOperationsFailed");

  @DefaultSchema(JavaFieldSchema.class)
  static class Operation implements Comparable<Operation>, Serializable {
    final long flushOffset;
    final boolean finalizeStream;

    @SchemaCreate
    public Operation(long flushOffset, boolean finalizeStream) {
      this.flushOffset = flushOffset;
      this.finalizeStream = finalizeStream;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Operation operation = (Operation) o;
      return flushOffset == operation.flushOffset && finalizeStream == operation.finalizeStream;
    }

    @Override
    public int hashCode() {
      return Objects.hash(flushOffset, finalizeStream);
    }

    @Override
    public int compareTo(Operation other) {
      int compValue = Long.compare(this.flushOffset, other.flushOffset);
      if (compValue == 0) {
        compValue = Boolean.compare(this.finalizeStream, other.finalizeStream);
      }
      return compValue;
    }
  }

  public StorageApiFlushAndFinalizeDoFn(BigQueryServices bqServices) {
    this.bqServices = bqServices;
  }

  private DatasetService getDatasetService(PipelineOptions pipelineOptions) throws IOException {
    if (datasetService == null) {
      datasetService = bqServices.getDatasetService(pipelineOptions.as(BigQueryOptions.class));
    }
    return datasetService;
  }

  @Teardown
  public void onTeardown() {
    try {
      if (datasetService != null) {
        datasetService.close();
        datasetService = null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @ProcessElement
  public void process(PipelineOptions pipelineOptions, @Element KV<String, Operation> element)
      throws Exception {
    final String streamId = element.getKey();
    final Operation operation = element.getValue();
    final DatasetService datasetService = getDatasetService(pipelineOptions);
    // Flush the stream. If the flush offset < 0, that means we only need to finalize.
    long offset = operation.flushOffset;
    if (offset >= 0) {
      Instant now = Instant.now();
      RetryManager<FlushRowsResponse, Context<FlushRowsResponse>> retryManager =
          new RetryManager<>(
              Duration.standardSeconds(1),
              Duration.standardMinutes(1),
              3,
              BigQuerySinkMetrics.throttledTimeCounter(BigQuerySinkMetrics.RpcMethod.FLUSH_ROWS));
      retryManager.addOperation(
          // runOperation
          c -> {
            try {
              flushOperationsSent.inc();
              return datasetService.flush(streamId, offset);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          },
          // onError
          contexts -> {
            Context<FlushRowsResponse> failedContext =
                Preconditions.checkArgumentNotNull(Iterables.getFirst(contexts, null));
            Throwable error = failedContext.getError();
            LOG.warn(
                "Flush of stream " + streamId + " to offset " + offset + " failed with " + error);
            flushOperationsFailed.inc();
            BigQuerySinkMetrics.reportFailedRPCMetrics(
                failedContext, BigQuerySinkMetrics.RpcMethod.FLUSH_ROWS);

            if (error instanceof ApiException) {
              Code statusCode = ((ApiException) error).getStatusCode().getCode();
              if (statusCode.equals(Code.ALREADY_EXISTS)) {
                flushOperationsAlreadyExists.inc();
                // Implies that we have already flushed up to this point, so don't retry.
                return RetryType.DONT_RETRY;
              }
              if (statusCode.equals(Code.INVALID_ARGUMENT)) {
                flushOperationsInvalidArgument.inc();
                // Implies that the stream has already been finalized.
                // TODO: Storage API should provide a more-specific way of identifying this failure.
                return RetryType.DONT_RETRY;
              }
              if (statusCode.equals(Code.NOT_FOUND)) {
                return RetryType.DONT_RETRY;
              }
            }
            return RetryType.RETRY_ALL_OPERATIONS;
          },
          // onSuccess
          c -> {
            BigQuerySinkMetrics.reportSuccessfulRpcMetrics(
                c, BigQuerySinkMetrics.RpcMethod.FLUSH_ROWS);
            flushOperationsSucceeded.inc();
          },
          new Context<>());
      retryManager.run(true);
      java.time.Duration timeElapsed = java.time.Duration.between(now, Instant.now());
      flushLatencyDistribution.update(timeElapsed.toMillis());
    }

    // Finalize the stream. No need to commit the stream, since we are only dealing with BUFFERED
    // streams here that have
    // already been flushed. Note that in the case of errors upstream, we will leave an unflushed
    // tail in the stream.
    // This is by design - those records will be retried on a new stream, so we don't want to flush
    // them in this stream
    // or we would end up with duplicates.
    if (operation.finalizeStream) {
      RetryManager<FinalizeWriteStreamResponse, Context<FinalizeWriteStreamResponse>> retryManager =
          new RetryManager<>(
              Duration.standardSeconds(1),
              Duration.standardMinutes(1),
              3,
              BigQuerySinkMetrics.throttledTimeCounter(
                  BigQuerySinkMetrics.RpcMethod.FINALIZE_STREAM));
      retryManager.addOperation(
          c -> {
            finalizeOperationsSent.inc();

            return datasetService.finalizeWriteStream(streamId);
          },
          contexts -> {
            LOG.warn(
                "Finalize of stream "
                    + streamId
                    + " failed with "
                    + Preconditions.checkArgumentNotNull(Iterables.getFirst(contexts, null))
                        .getError());
            finalizeOperationsFailed.inc();
            @Nullable
            Context<FinalizeWriteStreamResponse> firstContext = Iterables.getFirst(contexts, null);
            BigQuerySinkMetrics.reportFailedRPCMetrics(
                firstContext, BigQuerySinkMetrics.RpcMethod.FINALIZE_STREAM);
            @Nullable Throwable error = firstContext == null ? null : firstContext.getError();

            if (error instanceof ApiException) {
              Code statusCode = ((ApiException) error).getStatusCode().getCode();
              if (statusCode.equals(Code.NOT_FOUND)) {
                return RetryType.DONT_RETRY;
              }
            }
            return RetryType.RETRY_ALL_OPERATIONS;
          },
          r -> {
            BigQuerySinkMetrics.reportSuccessfulRpcMetrics(
                r, BigQuerySinkMetrics.RpcMethod.FINALIZE_STREAM);
            finalizeOperationsSucceeded.inc();
          },
          new Context<>());
      retryManager.run(true);
    }
  }
}
