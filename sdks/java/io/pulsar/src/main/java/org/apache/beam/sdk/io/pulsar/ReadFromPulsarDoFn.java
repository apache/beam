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
package org.apache.beam.sdk.io.pulsar;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transform for reading from Apache Pulsar. Support is currently incomplete, and there may be bugs;
 * see https://github.com/apache/beam/issues/31078 for more info, and comment in that issue if you
 * run into issues with this IO.
 */
@DoFn.UnboundedPerElement
@SuppressWarnings({"rawtypes", "nullness"})
public class ReadFromPulsarDoFn extends DoFn<PulsarSourceDescriptor, PulsarMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadFromPulsarDoFn.class);
  private SerializableFunction<String, PulsarClient> pulsarClientSerializableFunction;
  private PulsarClient client;
  private PulsarAdmin admin;
  private String clientUrl;
  private String adminUrl;

  private final SerializableFunction<Message<byte[]>, Instant> extractOutputTimestampFn;

  public ReadFromPulsarDoFn(PulsarIO.Read transform) {
    this.extractOutputTimestampFn = transform.getExtractOutputTimestampFn();
    this.clientUrl = transform.getClientUrl();
    this.adminUrl = transform.getAdminUrl();
    this.pulsarClientSerializableFunction = transform.getPulsarClient();
  }

  // Open connection to Pulsar clients
  @Setup
  public void initPulsarClients() throws Exception {
    if (this.clientUrl == null) {
      this.clientUrl = PulsarIOUtils.SERVICE_URL;
    }
    if (this.adminUrl == null) {
      this.adminUrl = PulsarIOUtils.SERVICE_HTTP_URL;
    }

    if (this.client == null) {
      this.client = pulsarClientSerializableFunction.apply(this.clientUrl);
      if (this.client == null) {
        this.client = PulsarClient.builder().serviceUrl(clientUrl).build();
      }
    }

    if (this.admin == null) {
      this.admin =
          PulsarAdmin.builder()
              .serviceHttpUrl(adminUrl)
              .tlsTrustCertsFilePath(null)
              .allowTlsInsecureConnection(false)
              .build();
    }
  }

  // Close connection to Pulsar clients
  @Teardown
  public void teardown() throws Exception {
    this.client.close();
    this.admin.close();
  }

  @GetInitialRestriction
  public OffsetRange getInitialRestriction(@Element PulsarSourceDescriptor pulsarSource) {
    long startTimestamp = 0L;
    long endTimestamp = Long.MAX_VALUE;

    if (pulsarSource.getStartOffset() != null) {
      startTimestamp = pulsarSource.getStartOffset();
    }

    if (pulsarSource.getEndOffset() != null) {
      endTimestamp = pulsarSource.getEndOffset();
    }

    return new OffsetRange(startTimestamp, endTimestamp);
  }

  /*
  It may define a DoFn.GetSize method or ensure that the RestrictionTracker implements
  RestrictionTracker.HasProgress. Poor auto-scaling of workers and/or splitting may result
  if size or progress is an inaccurate representation of work.
  See DoFn.GetSize and RestrictionTracker.HasProgress for further details.
  */
  @GetSize
  public double getSize(
      @Element PulsarSourceDescriptor pulsarSource, @Restriction OffsetRange range) {
    // TODO improve getsize estiamate, check pulsar stats to improve get size estimate
    // https://pulsar.apache.org/docs/en/admin-api-topics/#get-stats
    double estimateRecords =
        restrictionTracker(pulsarSource, range).getProgress().getWorkRemaining();
    return estimateRecords;
  }

  public Reader<byte[]> newReader(PulsarClient client, String topicPartition)
      throws PulsarClientException {
    ReaderBuilder<byte[]> builder =
        client.newReader().topic(topicPartition).startMessageId(MessageId.earliest);
    return builder.create();
  }

  @GetRestrictionCoder
  public Coder<OffsetRange> getRestrictionCoder() {
    return new OffsetRange.Coder();
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element PulsarSourceDescriptor pulsarSourceDescriptor,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator watermarkEstimator,
      OutputReceiver<PulsarMessage> output)
      throws IOException {
    long startTimestamp = tracker.currentRestriction().getFrom();
    String topicDescriptor = pulsarSourceDescriptor.getTopic();
    try (Reader<byte[]> reader = newReader(this.client, topicDescriptor)) {
      if (startTimestamp > 0) {
        reader.seek(startTimestamp);
      }
      while (true) {
        if (reader.hasReachedEndOfTopic()) {
          reader.close();
          return ProcessContinuation.stop();
        }
        Message<byte[]> message = reader.readNext();
        if (message == null) {
          return ProcessContinuation.resume();
        }
        Long currentTimestamp = message.getPublishTime();
        // if tracker.tryclaim() return true, sdf must execute work otherwise
        // doFn must exit processElement() without doing any work associated
        // or claiming more work
        if (!tracker.tryClaim(currentTimestamp)) {
          reader.close();
          return ProcessContinuation.stop();
        }
        if (pulsarSourceDescriptor.getEndMessageId() != null) {
          MessageId currentMsgId = message.getMessageId();
          boolean hasReachedEndMessageId =
              currentMsgId.compareTo(pulsarSourceDescriptor.getEndMessageId()) == 0;
          if (hasReachedEndMessageId) {
            return ProcessContinuation.stop();
          }
        }
        PulsarMessage pulsarMessage =
            new PulsarMessage(message.getTopicName(), message.getPublishTime(), message);
        Instant outputTimestamp = extractOutputTimestampFn.apply(message);
        output.outputWithTimestamp(pulsarMessage, outputTimestamp);
      }
    }
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  @NewWatermarkEstimator
  public WatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new WatermarkEstimators.MonotonicallyIncreasing(
        ensureTimestampWithinBounds(watermarkEstimatorState));
  }

  @NewTracker
  public OffsetRangeTracker restrictionTracker(
      @Element PulsarSourceDescriptor pulsarSource, @Restriction OffsetRange restriction) {
    if (restriction.getTo() < Long.MAX_VALUE) {
      return new OffsetRangeTracker(restriction);
    }

    PulsarLatestOffsetEstimator offsetEstimator =
        new PulsarLatestOffsetEstimator(this.admin, pulsarSource.getTopic());
    return new GrowableOffsetRangeTracker(restriction.getFrom(), offsetEstimator);
  }

  private static class PulsarLatestOffsetEstimator
      implements GrowableOffsetRangeTracker.RangeEndEstimator {

    private final Supplier<Message> memoizedBacklog;

    private PulsarLatestOffsetEstimator(PulsarAdmin admin, String topic) {
      this.memoizedBacklog =
          Suppliers.memoizeWithExpiration(
              () -> {
                try {
                  Message<byte[]> lastMsg = admin.topics().examineMessage(topic, "latest", 1);
                  return lastMsg;
                } catch (PulsarAdminException e) {
                  LOG.error(e.getMessage());
                  throw new RuntimeException(e);
                }
              },
              1,
              TimeUnit.SECONDS);
    }

    @Override
    public long estimate() {
      Message<byte[]> msg = memoizedBacklog.get();
      return msg.getPublishTime();
    }
  }

  private static Instant ensureTimestampWithinBounds(Instant timestamp) {
    if (timestamp.isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
      timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
    } else if (timestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      timestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
    }
    return timestamp;
  }
}
