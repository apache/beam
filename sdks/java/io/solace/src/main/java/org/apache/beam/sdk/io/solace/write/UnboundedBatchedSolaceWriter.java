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

import com.google.common.collect.ImmutableList;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
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
  // Log

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

  @ProcessElement
  public void processElement(
      @Element KV<Integer, Iterable<Solace.Record>> element,
      @Timestamp Instant timestamp,
      BoundedWindow window,
      MultiOutputReceiver outputReceiver) {
    setCurrentBundleTimestamp(timestamp);
    setCurrentBundleWindow(window);
    Iterable<Solace.Record> records = element.getValue();

    ImmutableList<Record> materializedRecords = ImmutableList.copyOf(records);

    PublishPhaser phaser = checkNotNull(this.phaser);

    this.bundleRegistrationPhase = phaser.bulkRegister(materializedRecords.size());

    LOG.info(
        "bzablockilog {} registrationPhase {}, registered parties:{}",
        this.bundleUuid,
        bundleRegistrationPhase,
        phaser.getRegisteredParties());
    materializedRecords.forEach(
        record -> {
          solaceSessionServiceWithProducer()
              .getPublishedResults()
              .put(record.getMessageId(), phaser);
        });

    this.bundleRecords.addAll(materializedRecords);
    publishBatch(materializedRecords);
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    publishResults(BeamContextWrapper.of(context));
  }

  public void publishResults(BeamContextWrapper context) {

    PublishPhaser phaser = checkNotNull(this.phaser);
    LOG.info(
        "bzablockilog {} registered parties: {}",
        checkNotNull(this.bundleUuid),
        phaser.getRegisteredParties());
    long now = System.currentTimeMillis();
    try {
      phaser.awaitAdvanceInterruptibly(bundleRegistrationPhase, 20, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      // that's ok, we handle it below.
    }
    LOG.info(
        "bzablockilog {} awaitAdvanceInterruptibly took {}ms",
        checkNotNull(this.bundleUuid),
        System.currentTimeMillis() - now);

    if (phaser.getUnarrivedParties() > 0) {
      phaser.forceTermination();
    }

    KeySetView<String, PublishResult> responseReceivedIds = phaser.successfulRecords.keySet();
    // Infer a set of message ids that we have not received based on the list of records that we
    // published (materializedRecords) and the list of message ids that we received a response for
    // (responseReceivedIds).
    Set<String> responseNotReceivedIds =
        bundleRecords.stream()
            .map(Record::getMessageId)
            .filter(id -> !responseReceivedIds.contains(id))
            .collect(Collectors.toSet());

    LOG.info(
        "bzablockilog {} materializedRecords: {}, received:{}, responseNotReceivedIds: {}",
        checkNotNull(this.bundleUuid),
        this.bundleRecords.size(),
        responseReceivedIds.size(),
        responseNotReceivedIds.size());

    responseNotReceivedIds.forEach(
        recordId -> {
          BadRecord badRecord =
              createBadRecord(
                  PublishResult.builder()
                      .setPublished(false)
                      .setError("Did not receive response")
                      .setMessageId(recordId)
                      .build());
          if (badRecord != null) {
            context.output(
                FAILED_PUBLISH_TAG,
                badRecord,
                getCurrentBundleTimestamp(),
                getCurrentBundleWindow()); // todo nulls?
          }
        });

    phaser.successfulRecords.forEach(
        (key, value) -> {
          if (value.getPublished()) {
            context.output(
                SUCCESSFUL_PUBLISH_TAG,
                value,
                getCurrentBundleTimestamp(),
                getCurrentBundleWindow()); // todo nulls?
          } else {
            BadRecord badRecord = createBadRecord(value);
            if (badRecord != null) {
              context.output(
                  FAILED_PUBLISH_TAG,
                  badRecord,
                  getCurrentBundleTimestamp(),
                  getCurrentBundleWindow()); // todo nulls?
            }
          }
        });

    // Remove entries from the map that stores the SettableFutures with Callbacks
    this.bundleRecords.stream()
        .map(Record::getMessageId)
        .forEach(solaceSessionServiceWithProducer().getPublishedResults()::remove);
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
