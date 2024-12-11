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

import static org.apache.beam.sdk.io.solace.SolaceIO.Write.FAILED_PUBLISH_TAG;
import static org.apache.beam.sdk.io.solace.SolaceIO.Write.SUCCESSFUL_PUBLISH_TAG;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.PublishResult;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
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
//Log

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
      boolean publishLatencyMetrics,
      Duration maxWaitTimeForPublishResponses) {
    super(
        destinationFn,
        sessionServiceFactory,
        deliveryMode,
        submissionMode,
        producersMapCardinality,
        publishLatencyMetrics,
        maxWaitTimeForPublishResponses);
  }

  static class PublishResultCallback implements FutureCallback<PublishResult> {

    private final ConcurrentHashMap<String, PublishResult> receivedResponses;
    // private final Set<String> waitingForAckIdsReference;
    private final AtomicInteger messageCounter;
    private final SettableFuture<Boolean> allResponsesBack;

    public PublishResultCallback(
        ConcurrentHashMap<String, PublishResult> receivedResponses,
        AtomicInteger messageCounter,
        SettableFuture<Boolean> allResponsesBack) {
      this.receivedResponses = receivedResponses;
      // this.waitingForAckIdsReference = waitingForAckIdsReference;
      this.messageCounter = messageCounter;
      this.allResponsesBack = allResponsesBack;
    }

    @Override
    public void onSuccess(PublishResult result) {
      // synchronized (messageCounter) {
      // waitingForAckIdsReference.remove(result.getMessageId());
      receivedResponses.put(result.getMessageId(), result);
      int i = messageCounter.decrementAndGet();
      if (i<=0){
        allResponsesBack.set(true);
      }
      // }
    }

    @Override
    public void onFailure(Throwable t) {
      // todo this shouldn't happen but act here.
    }
  }

  static class FutureWithCallbackHelper {
    private final List<Record> materializedRecords = new ArrayList<>();
    private final ConcurrentHashMap<String, PublishResult> receivedResponses =
        new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduledExecutorService;
    private final SessionService sessionService;
    private final Duration maxWaitTimeForPublishResponses;
    private final AtomicInteger messageCounter = new AtomicInteger(0);
    private final SettableFuture<Boolean> allResponsesBack = SettableFuture.create();

    FutureWithCallbackHelper(
        ScheduledExecutorService scheduledExecutorService,
        SessionService sessionService,
        Duration maxWaitTimeForPublishResponses) {
      this.scheduledExecutorService = scheduledExecutorService;
      this.sessionService = sessionService;
      this.maxWaitTimeForPublishResponses = maxWaitTimeForPublishResponses;
    }

    public List<Record> getMaterializedRecords() {
      return materializedRecords;
    }

    private SettableFuture<PublishResult> createNewFutureWithCallback() {
      SettableFuture<PublishResult> future = SettableFuture.create();
      Futures.addCallback(
          future,
          new PublishResultCallback(receivedResponses, messageCounter, allResponsesBack),
          scheduledExecutorService);
      return future;
    }

    public void addObservedRecord(Record record) {
      materializedRecords.add(record);
      messageCounter.incrementAndGet();
    }

    // private boolean collectedAllResponses() {
    //   // return waitingForAckIds.isEmpty();
    //   return messageCounter.get() <= 0;
    // }

    public AckResults waitForResponses() {
      long now = System.currentTimeMillis();
      try {
        // waitingForResponses.get(maxWaitTimeForPublishResponses.getMillis(), TimeUnit.MILLISECONDS);
        allResponsesBack.get(maxWaitTimeForPublishResponses.getMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      } catch (TimeoutException e) {
        // Not all responses are back from Solace, but that's ok, we handle it below.
      }
      LOG.info("bzablockilog allResponsesBack.get() took {}ms", System.currentTimeMillis() - now);

      // Make a deep copy of received responses to have synchronized access to keys and values
      // separately.
      HashMap<String, PublishResult> copyOfReceivedResponses = new HashMap<>(receivedResponses);

      Set<String> responseReceivedIds = copyOfReceivedResponses.keySet();
      Collection<PublishResult> publishResults = copyOfReceivedResponses.values();

      // Infer a set of message ids that we have not received based on the list of records that we
      // published (materializedRecords) and the list of message ids that we received a response for
      // (responseReceivedIds).
      Set<String> responseNotReceivedIds =
          materializedRecords.stream()
              .map(Record::getMessageId)
              .filter(id -> !responseReceivedIds.contains(id))
              .collect(Collectors.toSet());

      // Remove entries from the map that stores the SettableFutures with Callbacks
      materializedRecords.stream()
          .map(Record::getMessageId)
          .forEach(sessionService.getPublishedResults()::remove);

      return new AckResults(publishResults, responseNotReceivedIds, maxWaitTimeForPublishResponses);
    }
  }

  @ProcessElement
  public void processElement(
      @Element KV<Integer, Iterable<Solace.Record>> element,
      @Timestamp Instant timestamp,
      MultiOutputReceiver outputReceiver) {
    setCurrentBundleTimestamp(timestamp);
    Iterable<Solace.Record> records = element.getValue();

    FutureWithCallbackHelper futureWithCallbackHelper =
        new FutureWithCallbackHelper(
            checkNotNull(scheduledExecutorService),
            solaceSessionServiceWithProducer(),
            maxWaitTimeForPublishResponses);

    // can be converted into a collection/list
    records
        .iterator()
        .forEachRemaining(
            record -> {
              futureWithCallbackHelper.addObservedRecord(record);

              solaceSessionServiceWithProducer()
                  .getPublishedResults()
                  .put(
                      record.getMessageId(),
                      futureWithCallbackHelper.createNewFutureWithCallback());
            });

    publishBatch(futureWithCallbackHelper.getMaterializedRecords());

    AckResults ackResults = futureWithCallbackHelper.waitForResponses();

    ackResults.outputToReceiver(outputReceiver, SUCCESSFUL_PUBLISH_TAG, FAILED_PUBLISH_TAG);
  }

  private static class AckResults {
    private final Collection<PublishResult> publishResults;
    private final Set<String> failedIds;
    private final Duration maxWaitTimeForPublishResponses;

    private AckResults(
        Collection<PublishResult> publishResults,
        Set<String> failedIds,
        Duration maxWaitTimeForPublishResponses) {
      this.publishResults = publishResults;
      this.failedIds = failedIds;
      this.maxWaitTimeForPublishResponses = maxWaitTimeForPublishResponses;
    }

    private List<PublishResult> getSuccessAndFailedPublishResults() {
      List<PublishResult> allPublishResults = new ArrayList<>(publishResults);

      failedIds.forEach(
          recordId -> {
            PublishResult.Builder resultBuilder = PublishResult.builder();
            resultBuilder.setMessageId(recordId);
            resultBuilder.setPublished(false);
            resultBuilder.setError(
                String.format(
                    "Did not receive delivery confirmation from Solace in the specified timeout of %s. Consider increasing the .withMaxWaitTimeForPublishResponses() option.",
                    maxWaitTimeForPublishResponses));
            allPublishResults.add(resultBuilder.build());
          });

      return allPublishResults;
    }

    public void outputToReceiver(
        MultiOutputReceiver outputReceiver,
        TupleTag<PublishResult> successfulPublishTag,
        TupleTag<BadRecord> failedPublishTag) {
      getSuccessAndFailedPublishResults()
          .forEach(
              result -> {
                if (result.getPublished()) {
                  outputReceiver.get(successfulPublishTag).output(result);
                } else {
                  BadRecord badRecord = createBadRecord(result);
                  if (badRecord != null) {
                    outputReceiver.get(failedPublishTag).output(badRecord);
                  }
                }
              });
    }

    private @Nullable BadRecord createBadRecord(PublishResult result) {
      try {
        return BadRecord.fromExceptionInformation(
            result,
            null,
            null,
            Optional.ofNullable(result.getError())
                .orElse(
                    "SolaceIO.Write: message delivery is not confirmed. The response from Solace was not returned before the timeout.")); // todo specify timeout
      } catch (IOException e) {
        // ignore, this method can throw the exception only when the `exception` argument is not
        // null.
        return null;
      }
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
