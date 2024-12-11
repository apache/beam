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
package org.apache.beam.sdk.io.solace.write;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.sdk.io.solace.SolaceIO.Write.FAILED_PUBLISH_TAG;
import static org.apache.beam.sdk.io.solace.SolaceIO.Write.SUCCESSFUL_PUBLISH_TAG;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.PublishResult;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This DoFn is the responsible for writing to Solace in batch mode (holding up any messages), and
 * emit the corresponding output (success or fail; only for persistent messages), so the
 * SolaceIO.Write connector can be composed with other subsequent transforms in the pipeline.
 *
 * <p>The DoFn will create several JCSMP sessions per VM, and the sessions and producers will be
 * reused across different threads (if the number of threads is higher than the number of sessions,
 * which is probably the most common case).
 *
 * <p>The producer uses the JCSMP send multiple mode to publish a batch of messages together with a
 * single API call. The acks from this publication are also processed in batch, and returned as the
 * output of the DoFn.
 *
 * <p>The batch size is 50, and this is currently the maximum value supported by Solace.
 *
 * <p>There are no acks if the delivery mode is set to DIRECT.
 *
 * <p>This writer DoFn offers higher throughput than {@link UnboundedStreamingSolaceWriter} but also
 * higher latency.
 */
@Internal
public final class UnboundedBatchedSolaceWriter extends UnboundedSolaceWriter {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedBatchedSolaceWriter.class);

  private final Counter sentToBroker =
      Metrics.counter(UnboundedBatchedSolaceWriter.class, "msgs_sent_to_broker");

  private final Counter batchesRejectedByBroker =
      Metrics.counter(UnboundedSolaceWriter.class, "batches_rejected");

  public UnboundedBatchedSolaceWriter(
      SerializableFunction<Record, Destination> destinationFn,
      SessionServiceFactory sessionServiceFactory,
      DeliveryMode deliveryMode,
      SubmissionMode submissionMode,
      int producersMapCardinality,
      boolean publishLatencyMetrics) {
    super(
        destinationFn,
        sessionServiceFactory,
        deliveryMode,
        submissionMode,
        producersMapCardinality,
        publishLatencyMetrics);
  }

  @ProcessElement
  public void processElement(
      @Element KV<Integer, Iterable<Solace.Record>> element,
      @Timestamp Instant timestamp,
      MultiOutputReceiver outputReceiver) {
    setCurrentBundleTimestamp(timestamp);
    Iterable<Solace.Record> records = element.getValue();
    // Materialize all the records
    List<Record> materializedRecords = new ArrayList<>();
    records.iterator().forEachRemaining(materializedRecords::add);

    materializedRecords.forEach(
        record ->
            solaceSessionServiceWithProducer()
                .getPublishedResultsQueue()
                .put(record.getMessageId(), SettableFuture.create()));

    publishBatch(materializedRecords);

    AckResults ackResults =
        waitForResponses(
            materializedRecords.stream().map(Record::getMessageId).collect(Collectors.toList()));

    ackResults.publishResults.forEach(
        result -> {
          // todo this check shouldn't be needed
          // if (result != null) {
          if (result.getPublished()) {
            outputReceiver.get(SUCCESSFUL_PUBLISH_TAG).output(result);
          } else {
            try {
              BadRecord badRecord =
                  BadRecord.fromExceptionInformation(
                      result,
                      null,
                      null,
                      Optional.ofNullable(result.getError())
                          .orElse("SolaceIO.Write: unknown error."));
              outputReceiver.get(FAILED_PUBLISH_TAG).output(badRecord);
            } catch (IOException e) {
              // ignore, the exception is thrown when the exception argument in the
              // `BadRecord.fromExceptionInformation` is not null.
            }
            // }
          }
        });
  }

  private AckResults waitForResponses(List<String> materializedRecordIds) {
    ScheduledExecutorService scheduledExecutorService = checkNotNull(this.scheduledExecutorService);
    List<ListenableFuture<PublishResult>> futuresWithTimeout =
        materializedRecordIds.stream()
            .map(
                key -> {
                  SettableFuture<PublishResult> futureFromResultsQueue =
                      solaceSessionServiceWithProducer().getPublishedResultsQueue().remove(key);
                  return futureFromResultsQueue == null
                      ? SettableFuture.<PublishResult>create()
                      : futureFromResultsQueue;
                })
            .map(
                future ->
                    Futures.withTimeout(
                        future, 2 * 1000, TimeUnit.MILLISECONDS, scheduledExecutorService))
            .collect(Collectors.toList());

    String message = "";
    UUID uuid = UUID.randomUUID();
    List<PublishResult> publishResults;
    try {
      long now = System.currentTimeMillis();
      publishResults =
          Futures.successfulAsList(futuresWithTimeout).get().stream()
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
      LOG.info(
          "bzablockilog {} first Futures.successfulAsList took {}ms",
          uuid,
          System.currentTimeMillis() - now);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }

    // map publishResults collection to list of ids
    Set<String> responseReceivedIds =
        publishResults.stream()
            // .filter(Objects::nonNull)
            .map(
                x -> {
                  // if (x == null) {
                  //   return "";
                  // }
                  return x.getMessageId();
                })
            .collect(Collectors.toSet());

    // compare sucessfulids and materializedRecordIds and return only  materializedRecordIds that
    // are not in successfullids
    Set<String> responseNotReceivedIds =
        materializedRecordIds.stream()
            .filter(id -> !responseReceivedIds.contains(id))
            .collect(Collectors.toSet());

    // publishResults(UnboundedSolaceWriter.BeamContextWrapper.of(context));
    LOG.info(
        "bzablockilog {} {} expected: [{}], received: [{}], missing: [{}]",
        uuid,
        message,
        String.join(",", materializedRecordIds),
        String.join(",", responseReceivedIds),
        String.join(",", responseNotReceivedIds));

    return new AckResults(publishResults, responseNotReceivedIds);
  }

  private static class AckResults {
    // todo this shouldn't be nullable, we filter it in the stream
    public final List<PublishResult> publishResults;
    public final Set<String> failedIds;

    private AckResults(List<PublishResult> publishResults, Set<String> failedIds) {
      this.publishResults = publishResults;
      this.failedIds = failedIds;
    }
  }

  private void publishBatch(List<Record> records) {
    try {
      int entriesPublished =
          solaceSessionServiceWithProducer()
              .getInitializedProducer(getSubmissionMode())
              .publishBatch(
                  records, shouldPublishLatencyMetrics(), getDestinationFn(), getDeliveryMode());
      sentToBroker.inc(entriesPublished);
    } catch (Exception e) {
      batchesRejectedByBroker.inc();
      // Solace.PublishResult errorPublish =
      //     Solace.PublishResult.builder()
      //         .setPublished(false)
      //         .setMessageId(String.format("BATCH_OF_%d_ENTRIES", records.size()))
      //         .setError(
      //             String.format(
      //                 "Batch could not be published after several retries. Error: %s",
      //                 e.getMessage()))
      //         .setLatencyNanos(System.nanoTime())
      //         .build();
      // todo handle error here
      // solaceSessionServiceWithProducer().getPublishedResultsQueue().add(errorPublish);
    }
  }
}
