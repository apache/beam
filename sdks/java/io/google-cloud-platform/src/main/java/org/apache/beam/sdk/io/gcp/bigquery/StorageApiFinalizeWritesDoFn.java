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

import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.bigquery.storage.v1.StorageError.StorageErrorCode;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.Operation.Context;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.RetryType;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This DoFn finalizes and commits Storage API streams. */
class StorageApiFinalizeWritesDoFn extends DoFn<KV<String, String>, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(StorageApiFinalizeWritesDoFn.class);

  private final Counter finalizeOperationsSent =
      Metrics.counter(StorageApiFinalizeWritesDoFn.class, "finalizeOperationsSent");
  private final Counter finalizeOperationsSucceeded =
      Metrics.counter(StorageApiFinalizeWritesDoFn.class, "finalizeOperationsSucceeded");
  private final Counter finalizeOperationsFailed =
      Metrics.counter(StorageApiFinalizeWritesDoFn.class, "finalizeOperationsFailed");
  private final Counter batchCommitOperationsSent =
      Metrics.counter(StorageApiFinalizeWritesDoFn.class, "batchCommitOperationsSent");
  private final Counter batchCommitOperationsSucceeded =
      Metrics.counter(StorageApiFinalizeWritesDoFn.class, "batchCommitOperationsSucceeded");
  private final Counter batchCommitOperationsFailed =
      Metrics.counter(StorageApiFinalizeWritesDoFn.class, "batchCommitOperationsFailed");

  private Map<String, Collection<String>> commitStreams;
  private final BigQueryServices bqServices;
  private transient @Nullable DatasetService datasetService;

  public StorageApiFinalizeWritesDoFn(BigQueryServices bqServices) {
    this.bqServices = bqServices;
    this.commitStreams = Maps.newHashMap();
    this.datasetService = null;
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

  @StartBundle
  public void startBundle() throws IOException {
    commitStreams = Maps.newHashMap();
  }

  @ProcessElement
  @SuppressWarnings({"nullness"})
  public void process(PipelineOptions pipelineOptions, @Element KV<String, String> element)
      throws Exception {
    String tableId = element.getKey();
    String streamId = element.getValue();
    DatasetService datasetService = getDatasetService(pipelineOptions);

    RetryManager<FinalizeWriteStreamResponse, Context<FinalizeWriteStreamResponse>> retryManager =
        new RetryManager<>(Duration.standardSeconds(1), Duration.standardMinutes(1), 3);
    retryManager.addOperation(
        c -> {
          finalizeOperationsSent.inc();
          return datasetService.finalizeWriteStream(streamId);
        },
        contexts -> {
          LOG.error(
              "Finalize of stream "
                  + streamId
                  + " failed with "
                  + Iterables.getFirst(contexts, null).getError());
          finalizeOperationsFailed.inc();
          return RetryType.RETRY_ALL_OPERATIONS;
        },
        c -> {
          LOG.info("Finalize of stream " + streamId + " finished with " + c.getResult());
          finalizeOperationsSucceeded.inc();
          commitStreams.computeIfAbsent(tableId, d -> Lists.newArrayList()).add(streamId);
        },
        new Context<>());
    retryManager.run(true);
  }

  @FinishBundle
  @SuppressWarnings({"nullness"})
  public void finishBundle(PipelineOptions pipelineOptions) throws Exception {
    DatasetService datasetService = getDatasetService(pipelineOptions);
    for (Map.Entry<String, Collection<String>> entry : commitStreams.entrySet()) {
      final String tableId = entry.getKey();
      final Collection<String> streamNames = entry.getValue();
      final Set<String> alreadyCommittedStreams = Sets.newHashSet();
      RetryManager<BatchCommitWriteStreamsResponse, Context<BatchCommitWriteStreamsResponse>>
          retryManager =
              new RetryManager<>(Duration.standardSeconds(1), Duration.standardMinutes(1), 3);
      retryManager.addOperation(
          c -> {
            Iterable<String> streamsToCommit =
                Iterables.filter(streamNames, s -> !alreadyCommittedStreams.contains(s));
            batchCommitOperationsSent.inc();
            return datasetService.commitWriteStreams(tableId, streamsToCommit);
          },
          contexts -> {
            LOG.error(
                "BatchCommit failed. tableId "
                    + tableId
                    + " streamNames "
                    + streamNames
                    + " error: "
                    + Iterables.getFirst(contexts, null).getError());
            batchCommitOperationsFailed.inc();
            return RetryType.RETRY_ALL_OPERATIONS;
          },
          c -> {
            LOG.info("BatchCommit succeeded for tableId " + tableId + " response " + c.getResult());
            batchCommitOperationsSucceeded.inc();
          },
          response -> {
            if (!response.hasCommitTime()) {
              for (StorageError storageError : response.getStreamErrorsList()) {
                if (storageError.getCode() == StorageErrorCode.STREAM_ALREADY_COMMITTED) {
                  // Make sure that we don't retry any streams that are already committed.
                  alreadyCommittedStreams.add(storageError.getEntity());
                }
              }
              Iterable<String> streamsToCommit =
                  Iterables.filter(streamNames, s -> !alreadyCommittedStreams.contains(s));
              // If there are no more streams left to commit, then report this operation as having
              // succeeded. Otherwise,
              // retry.
              return Iterables.isEmpty(streamsToCommit);
            }
            return true;
          },
          new Context<>());
      retryManager.run(true);
    }
  }
}
