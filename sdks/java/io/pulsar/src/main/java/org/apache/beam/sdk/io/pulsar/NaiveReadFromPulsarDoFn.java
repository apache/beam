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
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
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
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DoFn for reading from Apache Pulsar based on Pulsar {@link Reader} from the start message id. It
 * does not support split or acknowledge message get read.
 */
@DoFn.UnboundedPerElement
@SuppressWarnings("nullness")
public class NaiveReadFromPulsarDoFn<T> extends DoFn<PulsarSourceDescriptor, T> {

  private static final Logger LOG = LoggerFactory.getLogger(NaiveReadFromPulsarDoFn.class);
  private final SerializableFunction<String, PulsarClient> clientFn;
  private final SerializableFunction<String, PulsarAdmin> adminFn;
  private final SerializableFunction<Message<?>, T> outputFn;
  private final java.time.Duration pollingTimeout;
  private transient @MonotonicNonNull PulsarClient client;
  private transient @MonotonicNonNull PulsarAdmin admin;
  private @MonotonicNonNull String clientUrl;
  private @Nullable final String adminUrl;

  private final SerializableFunction<Message<byte[]>, Instant> extractOutputTimestampFn;

  public NaiveReadFromPulsarDoFn(PulsarIO.Read<T> transform) {
    this.extractOutputTimestampFn =
        transform.getTimestampType() == PulsarIO.ReadTimestampType.PUBLISH_TIME
            ? record -> new Instant(record.getPublishTime())
            : ignored -> Instant.now();
    this.pollingTimeout = Duration.ofSeconds(transform.getConsumerPollingTimeout());
    this.outputFn = transform.getOutputFn();
    this.clientUrl = transform.getClientUrl();
    this.adminUrl = transform.getAdminUrl();
    this.clientFn =
        MoreObjects.firstNonNull(
            transform.getPulsarClient(), PulsarIOUtils.PULSAR_CLIENT_SERIALIZABLE_FUNCTION);
    this.adminFn =
        MoreObjects.firstNonNull(
            transform.getPulsarAdmin(), PulsarIOUtils.PULSAR_ADMIN_SERIALIZABLE_FUNCTION);
    admin = null;
  }

  /** Open connection to Pulsar clients. */
  @Setup
  public void initPulsarClients() throws Exception {
    if (client == null) {
      if (clientUrl == null) {
        clientUrl = PulsarIOUtils.LOCAL_SERVICE_URL;
      }
      client = clientFn.apply(clientUrl);
    }

    // admin is optional
    if (this.admin == null && !Strings.isNullOrEmpty(adminUrl)) {
      admin = adminFn.apply(adminUrl);
    }
  }

  /** Close connection to Pulsar clients. */
  @Teardown
  public void teardown() throws Exception {
    this.client.close();
    if (this.admin != null) {
      this.admin.close();
    }
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
      WatermarkEstimator<Instant> watermarkEstimator,
      OutputReceiver<T> output)
      throws IOException {
    long startTimestamp = tracker.currentRestriction().getFrom();
    String topicDescriptor = pulsarSourceDescriptor.getTopic();
    try (Reader<byte[]> reader = newReader(this.client, topicDescriptor)) {
      if (startTimestamp > 0) {
        //  reader.seek moves the cursor at the first occurrence of the message published after the
        // assigned timestamp.
        // i.e. all messages should be captured within the rangeTracker is after cursor
        reader.seek(startTimestamp);
      }
      if (reader.hasReachedEndOfTopic()) {
        // topic has terminated
        tracker.tryClaim(Long.MAX_VALUE);
        reader.close();
        return ProcessContinuation.stop();
      }
      boolean claimed = false;
      ArrayList<Message<byte[]>> maybeLateMessages = new ArrayList<>();
      final Stopwatch pollTimer = Stopwatch.createUnstarted();
      Duration remainingTimeout = pollingTimeout;
      while (Duration.ZERO.compareTo(remainingTimeout) < 0) {
        pollTimer.reset().start();
        Message<byte[]> message =
            reader.readNext((int) remainingTimeout.toMillis(), TimeUnit.MILLISECONDS);
        final Duration elapsed = pollTimer.elapsed();
        try {
          remainingTimeout = remainingTimeout.minus(elapsed);
        } catch (ArithmeticException e) {
          remainingTimeout = Duration.ZERO;
        }
        // No progress when the polling timeout expired.
        // Self-checkpoint and move to process the next element.
        if (message == null) {
          return ProcessContinuation.resume();
        } // Trying to claim offset -1 before start of the range [0, 9223372036854775807)
        long currentTimestamp = message.getPublishTime();
        if (currentTimestamp < startTimestamp) {
          // This should not happen per pulsar spec (see comments around read.seek). If it
          // does happen, this prevents tryClaim crash (IllegalArgumentException: Trying to
          // claim offset before start of the range)
          LOG.warn(
              "Received late message of publish time {} before startTimestamp {}",
              currentTimestamp,
              startTimestamp);
        } else if (!tracker.tryClaim(currentTimestamp)) {
          // if tracker.tryclaim() return true, sdf must execute work otherwise
          // doFn must exit processElement() without doing any work associated
          // or claiming more work
          reader.close();
          return ProcessContinuation.stop();
        } else {
          claimed = true;
        }
        if (pulsarSourceDescriptor.getEndMessageId() != null) {
          MessageId currentMsgId = message.getMessageId();
          boolean hasReachedEndMessageId =
              currentMsgId.compareTo(pulsarSourceDescriptor.getEndMessageId()) == 0;
          if (hasReachedEndMessageId) {
            return ProcessContinuation.stop();
          }
        }
        if (claimed) {
          if (!maybeLateMessages.isEmpty()) {
            for (Message<byte[]> lateMessage : maybeLateMessages) {
              publishMessage(lateMessage, output);
            }
            maybeLateMessages.clear();
          }
          publishMessage(message, output);
        } else {
          maybeLateMessages.add(message);
        }
      }
    }
    return ProcessContinuation.resume();
  }

  private void publishMessage(Message<byte[]> message, OutputReceiver<T> output) {
    T messageT = outputFn.apply(message);
    Instant outputTimestamp = extractOutputTimestampFn.apply(message);
    output.outputWithTimestamp(messageT, outputTimestamp);
  }

  @SplitRestriction
  public void splitRestriction(
      @Restriction OffsetRange restriction,
      OutputReceiver<OffsetRange> receiver,
      PipelineOptions unused) {
    // read based on Reader does not support split
    receiver.output(restriction);
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

    private final @Nullable Supplier<Message<byte[]>> memoizedBacklog;

    private PulsarLatestOffsetEstimator(@Nullable PulsarAdmin admin, String topic) {
      if (admin != null) {
        this.memoizedBacklog =
            Suppliers.memoizeWithExpiration(
                () -> {
                  try {
                    return admin.topics().examineMessage(topic, "latest", 1);
                  } catch (PulsarAdminException e) {
                    throw new RuntimeException(e);
                  }
                },
                1,
                TimeUnit.SECONDS);
      } else {
        memoizedBacklog = null;
      }
    }

    @Override
    public long estimate() {
      if (memoizedBacklog != null) {
        Message<byte[]> msg = memoizedBacklog.get();
        return msg.getPublishTime();
      } else {
        return Long.MIN_VALUE;
      }
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
