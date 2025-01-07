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

import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * This DoFn is the responsible for writing to Solace in streaming mode (one message at a time, not
 * holding up any message), and emit the corresponding output (success or fail; only for persistent
 * messages), so the SolaceIO.Write connector can be composed with other subsequent transforms in
 * the pipeline.
 *
 * <p>The DoFn will create several JCSMP sessions per VM, and the sessions and producers will be
 * reused across different threads (if the number of threads is higher than the number of sessions,
 * which is probably the most common case).
 *
 * <p>The producer uses the JCSMP streaming mode to publish a single message at a time, processing
 * the acks from this publication, and returning them as output of the DoFn.
 *
 * <p>There are no acks if the delivery mode is set to DIRECT.
 *
 * <p>This writer DoFn offers lower latency and lower throughput than {@link
 * UnboundedBatchedSolaceWriter}.
 */
@Internal
public final class UnboundedStreamingSolaceWriter extends UnboundedSolaceWriter {

  private final Counter sentToBroker =
      Metrics.counter(UnboundedStreamingSolaceWriter.class, "msgs_sent_to_broker");

  private final Counter rejectedByBroker =
      Metrics.counter(UnboundedStreamingSolaceWriter.class, "msgs_rejected_by_broker");

  public UnboundedStreamingSolaceWriter(
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
      @Element KV<Integer, Iterable<Solace.Record>> element, @Timestamp Instant timestamp) {
    setCurrentBundleTimestamp(timestamp);
    Iterable<Solace.Record> records = element.getValue();
    records.iterator().forEachRemaining(this::publishRecord);
  }

  private void publishRecord(Record record) {
    // The publish method will retry, let's send a failure message if all the retries fail
    try {
      solaceSessionServiceWithProducer()
          .getInitializedProducer(getSubmissionMode())
          .publishSingleMessage(
              record,
              getDestinationFn().apply(record),
              shouldPublishLatencyMetrics(),
              getDeliveryMode());
      sentToBroker.inc();
    } catch (Exception e) {
      rejectedByBroker.inc();
      // Solace.PublishResult errorPublish =
      //     Solace.PublishResult.builder()
      //         .setPublished(false)
      //         .setMessageId(record.getMessageId())
      //         .setError(
      //             String.format(
      //                 "Message could not be published after several retries. Error: %s",
      //                 e.getMessage()))
      //         .setLatencyNanos(System.nanoTime())
      //         .build();
      // todo handle this
      // solaceSessionServiceWithProducer().getPublishedResultsQueue().add(errorPublish);
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    // publishResults(BeamContextWrapper.of(context));
  }
}
