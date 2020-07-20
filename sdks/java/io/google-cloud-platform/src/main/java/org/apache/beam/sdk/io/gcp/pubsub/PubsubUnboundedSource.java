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
package org.apache.beam.sdk.io.gcp.pubsub;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.client.util.Clock;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.ProjectPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.PubsubClientFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.SourceMetrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.BucketingFunction;
import org.apache.beam.sdk.util.MovingFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Users should use {@link PubsubIO#read} instead.
 *
 * <p>A PTransform which streams messages from Pubsub.
 *
 * <ul>
 *   <li>The underlying implementation in an {@link UnboundedSource} which receives messages in
 *       batches and hands them out one at a time.
 *   <li>The watermark (either in Pubsub processing time or custom timestamp time) is estimated by
 *       keeping track of the minimum of the last minutes worth of messages. This assumes Pubsub
 *       delivers the oldest (in Pubsub processing time) available message at least once a minute,
 *       and that custom timestamps are 'mostly' monotonic with Pubsub processing time.
 *       Unfortunately both of those assumptions are fragile. Thus the estimated watermark may get
 *       ahead of the 'true' watermark and cause some messages to be late.
 *   <li>Checkpoints are used both to ACK received messages back to Pubsub (so that they may be
 *       retired on the Pubsub end), and to NACK already consumed messages should a checkpoint need
 *       to be restored (so that Pubsub will resend those messages promptly).
 *   <li>The backlog is determined by each reader using the messages which have been pulled from
 *       Pubsub but not yet consumed downstream. The backlog does not take account of any messages
 *       queued by Pubsub for the subscription. Unfortunately there is currently no API to determine
 *       the size of the Pubsub queue's backlog.
 *   <li>The subscription must already exist.
 *   <li>The subscription timeout is read whenever a reader is started. However it is not checked
 *       thereafter despite the timeout being user-changeable on-the-fly.
 *   <li>We log vital stats every 30 seconds.
 *   <li>Though some background threads may be used by the underlying transport all Pubsub calls are
 *       blocking. We rely on the underlying runner to allow multiple {@link
 *       UnboundedSource.UnboundedReader} instances to execute concurrently and thus hide latency.
 * </ul>
 */
public class PubsubUnboundedSource extends PTransform<PBegin, PCollection<PubsubMessage>> {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubUnboundedSource.class);

  /** Default ACK timeout for created subscriptions. */
  private static final int DEAULT_ACK_TIMEOUT_SEC = 60;

  /** Coder for checkpoints. */
  private static final PubsubCheckpointCoder<?> CHECKPOINT_CODER = PubsubCheckpointCoder.of();

  /** Maximum number of messages per pull. */
  private static final int PULL_BATCH_SIZE = 1000;

  /** Maximum number of ACK ids per ACK or ACK extension call. */
  private static final int ACK_BATCH_SIZE = 2000;

  /** Maximum number of messages in flight. */
  private static final int MAX_IN_FLIGHT = 20000;

  /** Timeout for round trip from receiving a message to finally ACKing it back to Pubsub. */
  private static final Duration PROCESSING_TIMEOUT = Duration.standardMinutes(2);

  /** Percentage of ack timeout by which to extend acks when they are near timeout. */
  private static final int ACK_EXTENSION_PCT = 50;

  /**
   * Percentage of ack timeout we should use as a safety margin. We'll try to extend acks by this
   * margin before the ack actually expires.
   */
  private static final int ACK_SAFETY_PCT = 20;

  /**
   * For stats only: How close we can get to an ack deadline before we risk it being already
   * considered passed by Pubsub.
   */
  private static final Duration ACK_TOO_LATE = Duration.standardSeconds(2);

  /** Period of samples to determine watermark and other stats. */
  private static final Duration SAMPLE_PERIOD = Duration.standardMinutes(1);

  /** Period of updates to determine watermark and other stats. */
  private static final Duration SAMPLE_UPDATE = Duration.standardSeconds(5);

  /** Period for logging stats. */
  private static final Duration LOG_PERIOD = Duration.standardSeconds(30);

  /** Minimum number of unread messages required before considering updating watermark. */
  private static final int MIN_WATERMARK_MESSAGES = 10;

  /**
   * Minimum number of SAMPLE_UPDATE periods over which unread messages shoud be spread before
   * considering updating watermark.
   */
  private static final int MIN_WATERMARK_SPREAD = 2;

  /** Additional sharding so that we can hide read message latency. */
  private static final int SCALE_OUT = 4;

  // TODO: Would prefer to use MinLongFn but it is a BinaryCombineFn<Long> rather
  // than a BinaryCombineLongFn. [BEAM-285]
  private static final Combine.BinaryCombineLongFn MIN =
      new Combine.BinaryCombineLongFn() {
        @Override
        public long apply(long left, long right) {
          return Math.min(left, right);
        }

        @Override
        public long identity() {
          return Long.MAX_VALUE;
        }
      };

  private static final Combine.BinaryCombineLongFn MAX =
      new Combine.BinaryCombineLongFn() {
        @Override
        public long apply(long left, long right) {
          return Math.max(left, right);
        }

        @Override
        public long identity() {
          return Long.MIN_VALUE;
        }
      };

  private static final Combine.BinaryCombineLongFn SUM = Sum.ofLongs();

  // ================================================================================
  // Checkpoint
  // ================================================================================

  /**
   * Which messages have been durably committed and thus can now be ACKed. Which messages have been
   * read but not yet committed, in which case they should be NACKed if we need to restore.
   */
  @VisibleForTesting
  static class PubsubCheckpoint implements UnboundedSource.CheckpointMark {
    /**
     * The {@link SubscriptionPath} to the subscription the reader is reading from. May be {@code
     * null} if the {@link PubsubUnboundedSource} contains the subscription.
     */
    @VisibleForTesting @Nullable String subscriptionPath;

    /**
     * If the checkpoint is for persisting: the reader who's snapshotted state we are persisting. If
     * the checkpoint is for restoring: {@literal null}. Not persisted in durable checkpoint.
     * CAUTION: Between a checkpoint being taken and {@link #finalizeCheckpoint()} being called the
     * 'true' active reader may have changed.
     */
    private @Nullable PubsubReader reader;

    /**
     * If the checkpoint is for persisting: The ACK ids of messages which have been passed
     * downstream since the last checkpoint. If the checkpoint is for restoring: {@literal null}.
     * Not persisted in durable checkpoint.
     */
    private @Nullable List<String> safeToAckIds;

    /**
     * If the checkpoint is for persisting: The ACK ids of messages which have been received from
     * Pubsub but not yet passed downstream at the time of the snapshot. If the checkpoint is for
     * restoring: Same, but recovered from durable storage.
     */
    @VisibleForTesting final List<String> notYetReadIds;

    public PubsubCheckpoint(
        @Nullable String subscriptionPath,
        @Nullable PubsubReader reader,
        @Nullable List<String> safeToAckIds,
        List<String> notYetReadIds) {
      this.subscriptionPath = subscriptionPath;
      this.reader = reader;
      this.safeToAckIds = safeToAckIds;
      this.notYetReadIds = notYetReadIds;
    }

    private @Nullable SubscriptionPath getSubscription() {
      return subscriptionPath == null
          ? null
          : PubsubClient.subscriptionPathFromPath(subscriptionPath);
    }

    /**
     * BLOCKING All messages which have been passed downstream have now been durably committed. We
     * can ACK them upstream. CAUTION: This may never be called.
     */
    @Override
    public void finalizeCheckpoint() throws IOException {
      checkState(reader != null && safeToAckIds != null, "Cannot finalize a restored checkpoint");
      // Even if the 'true' active reader has changed since the checkpoint was taken we are
      // fine:
      // - The underlying Pubsub topic will not have changed, so the following ACKs will still
      // go to the right place.
      // - We'll delete the ACK ids from the readers in-flight state, but that only effects
      // flow control and stats, neither of which are relevant anymore.
      try {
        int n = safeToAckIds.size();
        List<String> batchSafeToAckIds = new ArrayList<>(Math.min(n, ACK_BATCH_SIZE));
        for (String ackId : safeToAckIds) {
          batchSafeToAckIds.add(ackId);
          if (batchSafeToAckIds.size() >= ACK_BATCH_SIZE) {
            reader.ackBatch(batchSafeToAckIds);
            n -= batchSafeToAckIds.size();
            // CAUTION: Don't reuse the same list since ackBatch holds on to it.
            batchSafeToAckIds = new ArrayList<>(Math.min(n, ACK_BATCH_SIZE));
          }
        }
        if (!batchSafeToAckIds.isEmpty()) {
          reader.ackBatch(batchSafeToAckIds);
        }
      } finally {
        int remainingInFlight = reader.numInFlightCheckpoints.decrementAndGet();
        checkState(remainingInFlight >= 0, "Miscounted in-flight checkpoints");
        reader.maybeCloseClient();
        reader = null;
        safeToAckIds = null;
      }
    }

    /** Return current time according to {@code reader}. */
    private static long now(PubsubReader reader) {
      if (reader.outer.outer.clock == null) {
        return System.currentTimeMillis();
      } else {
        return reader.outer.outer.clock.currentTimeMillis();
      }
    }

    /**
     * BLOCKING NACK all messages which have been read from Pubsub but not passed downstream. This
     * way Pubsub will send them again promptly.
     */
    public void nackAll(PubsubReader reader) throws IOException {
      checkState(this.reader == null, "Cannot nackAll on persisting checkpoint");
      List<String> batchYetToAckIds =
          new ArrayList<>(Math.min(notYetReadIds.size(), ACK_BATCH_SIZE));
      for (String ackId : notYetReadIds) {
        batchYetToAckIds.add(ackId);
        if (batchYetToAckIds.size() >= ACK_BATCH_SIZE) {
          long nowMsSinceEpoch = now(reader);
          reader.nackBatch(nowMsSinceEpoch, batchYetToAckIds);
          batchYetToAckIds.clear();
        }
      }
      if (!batchYetToAckIds.isEmpty()) {
        long nowMsSinceEpoch = now(reader);
        reader.nackBatch(nowMsSinceEpoch, batchYetToAckIds);
      }
    }
  }

  /** The coder for our checkpoints. */
  private static class PubsubCheckpointCoder<T> extends AtomicCoder<PubsubCheckpoint> {
    private static final Coder<String> SUBSCRIPTION_PATH_CODER =
        NullableCoder.of(StringUtf8Coder.of());
    private static final Coder<List<String>> LIST_CODER = ListCoder.of(StringUtf8Coder.of());

    public static <T> PubsubCheckpointCoder<T> of() {
      return new PubsubCheckpointCoder<>();
    }

    private PubsubCheckpointCoder() {}

    @Override
    public void encode(PubsubCheckpoint value, OutputStream outStream) throws IOException {
      SUBSCRIPTION_PATH_CODER.encode(value.subscriptionPath, outStream);
      LIST_CODER.encode(value.notYetReadIds, outStream);
    }

    @Override
    public PubsubCheckpoint decode(InputStream inStream) throws IOException {
      String path = SUBSCRIPTION_PATH_CODER.decode(inStream);
      List<String> notYetReadIds = LIST_CODER.decode(inStream);
      return new PubsubCheckpoint(path, null, null, notYetReadIds);
    }
  }

  // ================================================================================
  // Reader
  // ================================================================================

  /**
   * A reader which keeps track of which messages have been received from Pubsub but not yet
   * consumed downstream and/or ACKed back to Pubsub.
   */
  @VisibleForTesting
  static class PubsubReader extends UnboundedSource.UnboundedReader<PubsubMessage> {
    /** For access to topic and checkpointCoder. */
    private final PubsubSource outer;

    @VisibleForTesting final SubscriptionPath subscription;

    /** Client on which to talk to Pubsub. Contains a null value if the client has been closed. */
    private AtomicReference<PubsubClient> pubsubClient;

    /**
     * The closed state of this {@link PubsubReader}. If true, the reader has not yet been closed,
     * and it will have a non-null value within {@link #pubsubClient}.
     */
    private AtomicBoolean active = new AtomicBoolean(true);

    /**
     * Ack timeout, in ms, as set on subscription when we first start reading. Not updated
     * thereafter. -1 if not yet determined.
     */
    private int ackTimeoutMs;

    /** ACK ids of messages we have delivered downstream but not yet ACKed. */
    private Set<String> safeToAckIds;

    /**
     * Messages we have received from Pubsub and not yet delivered downstream. We preserve their
     * order.
     */
    private final Queue<PubsubClient.IncomingMessage> notYetRead;

    private static class InFlightState {
      /** When request which yielded message was issues. */
      long requestTimeMsSinceEpoch;

      /**
       * When Pubsub will consider this message's ACK to timeout and thus it needs to be extended.
       */
      long ackDeadlineMsSinceEpoch;

      public InFlightState(long requestTimeMsSinceEpoch, long ackDeadlineMsSinceEpoch) {
        this.requestTimeMsSinceEpoch = requestTimeMsSinceEpoch;
        this.ackDeadlineMsSinceEpoch = ackDeadlineMsSinceEpoch;
      }
    }

    /**
     * Map from ACK ids of messages we have received from Pubsub but not yet ACKed to their in
     * flight state. Ordered from earliest to latest ACK deadline.
     */
    private final LinkedHashMap<String, InFlightState> inFlight;

    /**
     * Batches of successfully ACKed ids which need to be pruned from the above. CAUTION: Accessed
     * by both reader and checkpointing threads.
     */
    private final Queue<List<String>> ackedIds;

    /** Byte size of undecoded elements in {@link #notYetRead}. */
    private long notYetReadBytes;

    /**
     * Bucketed map from received time (as system time, ms since epoch) to message timestamps
     * (mssince epoch) of all received but not-yet read messages. Used to estimate watermark.
     */
    private BucketingFunction minUnreadTimestampMsSinceEpoch;

    /**
     * Minimum of timestamps (ms since epoch) of all recently read messages. Used to estimate
     * watermark.
     */
    private MovingFunction minReadTimestampMsSinceEpoch;

    /**
     * System time (ms since epoch) we last received a message from Pubsub, or -1 if not yet
     * received any messages.
     */
    private long lastReceivedMsSinceEpoch;

    /** The last reported watermark (ms since epoch), or beginning of time if none yet reported. */
    private long lastWatermarkMsSinceEpoch;

    /** The current message, or {@literal null} if none. */
    private PubsubClient.@Nullable IncomingMessage current;

    /** Stats only: System time (ms since epoch) we last logs stats, or -1 if never. */
    private long lastLogTimestampMsSinceEpoch;

    /** Stats only: Total number of messages received. */
    private long numReceived;

    /** Stats only: Number of messages which have recently been received. */
    private MovingFunction numReceivedRecently;

    /** Stats only: Number of messages which have recently had their deadline extended. */
    private MovingFunction numExtendedDeadlines;

    /**
     * Stats only: Number of messages which have recenttly had their deadline extended even though
     * it may be too late to do so.
     */
    private MovingFunction numLateDeadlines;

    /** Stats only: Number of messages which have recently been ACKed. */
    private MovingFunction numAcked;

    /**
     * Stats only: Number of messages which have recently expired (ACKs were extended for too long).
     */
    private MovingFunction numExpired;

    /** Stats only: Number of messages which have recently been NACKed. */
    private MovingFunction numNacked;

    /** Stats only: Number of message bytes which have recently been read by downstream consumer. */
    private MovingFunction numReadBytes;

    /**
     * Stats only: Minimum of timestamp (ms since epoch) of all recently received messages. Used to
     * estimate timestamp skew. Does not contribute to watermark estimator.
     */
    private MovingFunction minReceivedTimestampMsSinceEpoch;

    /**
     * Stats only: Maximum of timestamp (ms since epoch) of all recently received messages. Used to
     * estimate timestamp skew.
     */
    private MovingFunction maxReceivedTimestampMsSinceEpoch;

    /** Stats only: Minimum of recent estimated watermarks (ms since epoch). */
    private MovingFunction minWatermarkMsSinceEpoch;

    /** Stats ony: Maximum of recent estimated watermarks (ms since epoch). */
    private MovingFunction maxWatermarkMsSinceEpoch;

    /**
     * Stats only: Number of messages with timestamps strictly behind the estimated watermark at the
     * time they are received. These may be considered 'late' by downstream computations.
     */
    private MovingFunction numLateMessages;

    /**
     * Stats only: Current number of checkpoints in flight. CAUTION: Accessed by both checkpointing
     * and reader threads.
     */
    private AtomicInteger numInFlightCheckpoints;

    /** Stats only: Maximum number of checkpoints in flight at any time. */
    private int maxInFlightCheckpoints;

    private static MovingFunction newFun(Combine.BinaryCombineLongFn function) {
      return new MovingFunction(
          SAMPLE_PERIOD.getMillis(),
          SAMPLE_UPDATE.getMillis(),
          MIN_WATERMARK_SPREAD,
          MIN_WATERMARK_MESSAGES,
          function);
    }

    /** Construct a reader. */
    public PubsubReader(PubsubOptions options, PubsubSource outer, SubscriptionPath subscription)
        throws IOException, GeneralSecurityException {
      this.outer = outer;
      this.subscription = subscription;
      pubsubClient =
          new AtomicReference<>(
              outer.outer.pubsubFactory.newClient(
                  outer.outer.timestampAttribute, outer.outer.idAttribute, options));
      ackTimeoutMs = -1;
      safeToAckIds = new HashSet<>();
      notYetRead = new ArrayDeque<>();
      inFlight = new LinkedHashMap<>();
      ackedIds = new ConcurrentLinkedQueue<>();
      notYetReadBytes = 0;
      minUnreadTimestampMsSinceEpoch =
          new BucketingFunction(
              SAMPLE_UPDATE.getMillis(), MIN_WATERMARK_SPREAD, MIN_WATERMARK_MESSAGES, MIN);
      minReadTimestampMsSinceEpoch = newFun(MIN);
      lastReceivedMsSinceEpoch = -1;
      lastWatermarkMsSinceEpoch = BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis();
      current = null;
      lastLogTimestampMsSinceEpoch = -1;
      numReceived = 0L;
      numReceivedRecently = newFun(SUM);
      numExtendedDeadlines = newFun(SUM);
      numLateDeadlines = newFun(SUM);
      numAcked = newFun(SUM);
      numExpired = newFun(SUM);
      numNacked = newFun(SUM);
      numReadBytes = newFun(SUM);
      minReceivedTimestampMsSinceEpoch = newFun(MIN);
      maxReceivedTimestampMsSinceEpoch = newFun(MAX);
      minWatermarkMsSinceEpoch = newFun(MIN);
      maxWatermarkMsSinceEpoch = newFun(MAX);
      numLateMessages = newFun(SUM);
      numInFlightCheckpoints = new AtomicInteger();
      maxInFlightCheckpoints = 0;
    }

    @VisibleForTesting
    PubsubClient getPubsubClient() {
      return pubsubClient.get();
    }

    /**
     * Acks the provided {@code ackIds} back to Pubsub, blocking until all of the messages are
     * ACKed.
     *
     * <p>CAUTION: May be invoked from a separate thread.
     *
     * <p>CAUTION: Retains {@code ackIds}.
     */
    void ackBatch(List<String> ackIds) throws IOException {
      pubsubClient.get().acknowledge(subscription, ackIds);
      ackedIds.add(ackIds);
    }

    /**
     * BLOCKING NACK (ie request deadline extension of 0) receipt of messages from Pubsub with the
     * given {@code ockIds}. Does not retain {@code ackIds}.
     */
    public void nackBatch(long nowMsSinceEpoch, List<String> ackIds) throws IOException {
      pubsubClient.get().modifyAckDeadline(subscription, ackIds, 0);
      numNacked.add(nowMsSinceEpoch, ackIds.size());
    }

    /**
     * BLOCKING Extend the processing deadline for messages from Pubsub with the given {@code
     * ackIds}. Does not retain {@code ackIds}.
     */
    private void extendBatch(long nowMsSinceEpoch, List<String> ackIds) throws IOException {
      int extensionSec = (ackTimeoutMs * ACK_EXTENSION_PCT) / (100 * 1000);
      pubsubClient.get().modifyAckDeadline(subscription, ackIds, extensionSec);
      numExtendedDeadlines.add(nowMsSinceEpoch, ackIds.size());
    }

    /** Return the current time, in ms since epoch. */
    private long now() {
      if (outer.outer.clock == null) {
        return System.currentTimeMillis();
      } else {
        return outer.outer.clock.currentTimeMillis();
      }
    }

    /**
     * Messages which have been ACKed (via the checkpoint finalize) are no longer in flight. This is
     * only used for flow control and stats.
     */
    private void retire() throws IOException {
      long nowMsSinceEpoch = now();
      while (true) {
        List<String> ackIds = ackedIds.poll();
        if (ackIds == null) {
          return;
        }
        numAcked.add(nowMsSinceEpoch, ackIds.size());
        for (String ackId : ackIds) {
          inFlight.remove(ackId);
          safeToAckIds.remove(ackId);
        }
      }
    }

    /**
     * BLOCKING Extend deadline for all messages which need it. CAUTION: If extensions can't keep up
     * with wallclock then we'll never return.
     */
    private void extend() throws IOException {
      while (true) {
        long nowMsSinceEpoch = now();
        List<String> assumeExpired = new ArrayList<>();
        List<String> toBeExtended = new ArrayList<>();
        List<String> toBeExpired = new ArrayList<>();
        // Messages will be in increasing deadline order.
        for (Map.Entry<String, InFlightState> entry : inFlight.entrySet()) {
          if (entry.getValue().ackDeadlineMsSinceEpoch - (ackTimeoutMs * ACK_SAFETY_PCT) / 100
              > nowMsSinceEpoch) {
            // All remaining messages don't need their ACKs to be extended.
            break;
          }

          if (entry.getValue().ackDeadlineMsSinceEpoch - ACK_TOO_LATE.getMillis()
              < nowMsSinceEpoch) {
            // Pubsub may have already considered this message to have expired.
            // If so it will (eventually) be made available on a future pull request.
            // If this message ends up being committed then it will be considered a duplicate
            // when re-pulled.
            assumeExpired.add(entry.getKey());
            continue;
          }

          if (entry.getValue().requestTimeMsSinceEpoch + PROCESSING_TIMEOUT.getMillis()
              < nowMsSinceEpoch) {
            // This message has been in-flight for too long.
            // Give up on it, otherwise we risk extending its ACK indefinitely.
            toBeExpired.add(entry.getKey());
            continue;
          }

          // Extend the ACK for this message.
          toBeExtended.add(entry.getKey());
          if (toBeExtended.size() >= ACK_BATCH_SIZE) {
            // Enough for one batch.
            break;
          }
        }

        if (assumeExpired.isEmpty() && toBeExtended.isEmpty() && toBeExpired.isEmpty()) {
          // Nothing to be done.
          return;
        }

        if (!assumeExpired.isEmpty()) {
          // If we didn't make the ACK deadline assume expired and no longer in flight.
          numLateDeadlines.add(nowMsSinceEpoch, assumeExpired.size());
          for (String ackId : assumeExpired) {
            inFlight.remove(ackId);
          }
        }

        if (!toBeExpired.isEmpty()) {
          // Expired messages are no longer considered in flight.
          numExpired.add(nowMsSinceEpoch, toBeExpired.size());
          for (String ackId : toBeExpired) {
            inFlight.remove(ackId);
          }
        }

        if (!toBeExtended.isEmpty()) {
          // Pubsub extends acks from it's notion of current time.
          // We'll try to track that on our side, but note the deadlines won't necessarily agree.
          long newDeadlineMsSinceEpoch = nowMsSinceEpoch + (ackTimeoutMs * ACK_EXTENSION_PCT) / 100;
          for (String ackId : toBeExtended) {
            // Maintain increasing ack deadline order.
            InFlightState state = inFlight.remove(ackId);
            inFlight.put(
                ackId, new InFlightState(state.requestTimeMsSinceEpoch, newDeadlineMsSinceEpoch));
          }
          // BLOCKs until extended.
          extendBatch(nowMsSinceEpoch, toBeExtended);
        }
      }
    }

    /** BLOCKING Fetch another batch of messages from Pubsub. */
    private void pull() throws IOException {
      if (inFlight.size() >= MAX_IN_FLIGHT) {
        // Wait for checkpoint to be finalized before pulling anymore.
        // There may be lag while checkpoints are persisted and the finalizeCheckpoint method
        // is invoked. By limiting the in-flight messages we can ensure we don't end up consuming
        // messages faster than we can checkpoint them.
        return;
      }

      long requestTimeMsSinceEpoch = now();
      long deadlineMsSinceEpoch = requestTimeMsSinceEpoch + ackTimeoutMs;

      // Pull the next batch.
      // BLOCKs until received.
      Collection<PubsubClient.IncomingMessage> receivedMessages =
          pubsubClient.get().pull(requestTimeMsSinceEpoch, subscription, PULL_BATCH_SIZE, true);
      if (receivedMessages.isEmpty()) {
        // Nothing available yet. Try again later.
        return;
      }

      lastReceivedMsSinceEpoch = requestTimeMsSinceEpoch;

      // Capture the received messages.
      for (PubsubClient.IncomingMessage incomingMessage : receivedMessages) {
        notYetRead.add(incomingMessage);
        notYetReadBytes += incomingMessage.message().getData().size();
        inFlight.put(
            incomingMessage.ackId(),
            new InFlightState(requestTimeMsSinceEpoch, deadlineMsSinceEpoch));
        numReceived++;
        numReceivedRecently.add(requestTimeMsSinceEpoch, 1L);
        minReceivedTimestampMsSinceEpoch.add(
            requestTimeMsSinceEpoch, incomingMessage.timestampMsSinceEpoch());
        maxReceivedTimestampMsSinceEpoch.add(
            requestTimeMsSinceEpoch, incomingMessage.timestampMsSinceEpoch());
        minUnreadTimestampMsSinceEpoch.add(
            requestTimeMsSinceEpoch, incomingMessage.timestampMsSinceEpoch());
      }
    }

    /** Log stats if time to do so. */
    private void stats() {
      long nowMsSinceEpoch = now();
      if (lastLogTimestampMsSinceEpoch < 0) {
        lastLogTimestampMsSinceEpoch = nowMsSinceEpoch;
        return;
      }
      long deltaMs = nowMsSinceEpoch - lastLogTimestampMsSinceEpoch;
      if (deltaMs < LOG_PERIOD.getMillis()) {
        return;
      }

      String messageSkew = "unknown";
      long minTimestamp = minReceivedTimestampMsSinceEpoch.get(nowMsSinceEpoch);
      long maxTimestamp = maxReceivedTimestampMsSinceEpoch.get(nowMsSinceEpoch);
      if (minTimestamp < Long.MAX_VALUE && maxTimestamp > Long.MIN_VALUE) {
        messageSkew = (maxTimestamp - minTimestamp) + "ms";
      }

      String watermarkSkew = "unknown";
      long minWatermark = minWatermarkMsSinceEpoch.get(nowMsSinceEpoch);
      long maxWatermark = maxWatermarkMsSinceEpoch.get(nowMsSinceEpoch);
      if (minWatermark < Long.MAX_VALUE && maxWatermark > Long.MIN_VALUE) {
        watermarkSkew = (maxWatermark - minWatermark) + "ms";
      }

      String oldestInFlight = "no";
      String oldestAckId = Iterables.getFirst(inFlight.keySet(), null);
      if (oldestAckId != null) {
        oldestInFlight =
            (nowMsSinceEpoch - inFlight.get(oldestAckId).requestTimeMsSinceEpoch) + "ms";
      }

      LOG.debug(
          "Pubsub {} has "
              + "{} received messages, "
              + "{} current unread messages, "
              + "{} current unread bytes, "
              + "{} current in-flight msgs, "
              + "{} oldest in-flight, "
              + "{} current in-flight checkpoints, "
              + "{} max in-flight checkpoints, "
              + "{}B/s recent read, "
              + "{} recent received, "
              + "{} recent extended, "
              + "{} recent late extended, "
              + "{} recent ACKed, "
              + "{} recent NACKed, "
              + "{} recent expired, "
              + "{} recent message timestamp skew, "
              + "{} recent watermark skew, "
              + "{} recent late messages, "
              + "{} last reported watermark",
          subscription,
          numReceived,
          notYetRead.size(),
          notYetReadBytes,
          inFlight.size(),
          oldestInFlight,
          numInFlightCheckpoints.get(),
          maxInFlightCheckpoints,
          numReadBytes.get(nowMsSinceEpoch) / (SAMPLE_PERIOD.getMillis() / 1000L),
          numReceivedRecently.get(nowMsSinceEpoch),
          numExtendedDeadlines.get(nowMsSinceEpoch),
          numLateDeadlines.get(nowMsSinceEpoch),
          numAcked.get(nowMsSinceEpoch),
          numNacked.get(nowMsSinceEpoch),
          numExpired.get(nowMsSinceEpoch),
          messageSkew,
          watermarkSkew,
          numLateMessages.get(nowMsSinceEpoch),
          new Instant(lastWatermarkMsSinceEpoch));

      lastLogTimestampMsSinceEpoch = nowMsSinceEpoch;
    }

    @Override
    public boolean start() throws IOException {
      // Determine the ack timeout.
      ackTimeoutMs = pubsubClient.get().ackDeadlineSeconds(subscription) * 1000;
      return advance();
    }

    /**
     * BLOCKING Return {@literal true} if a Pubsub message is available, {@literal false} if none is
     * available at this time or we are over-subscribed. May BLOCK while extending ACKs or fetching
     * available messages. Will not block waiting for messages.
     */
    @Override
    public boolean advance() throws IOException {
      // Emit stats.
      stats();

      if (current != null) {
        // Current is consumed. It can no longer contribute to holding back the watermark.
        minUnreadTimestampMsSinceEpoch.remove(current.requestTimeMsSinceEpoch());
        current = null;
      }

      // Retire state associated with ACKed messages.
      retire();

      // Extend all pressing deadlines.
      // Will BLOCK until done.
      // If the system is pulling messages only to let them sit in a downsteam queue then
      // this will have the effect of slowing down the pull rate.
      // However, if the system is genuinely taking longer to process each message then
      // the work to extend ACKs would be better done in the background.
      extend();

      if (notYetRead.isEmpty()) {
        // Pull another batch.
        // Will BLOCK until fetch returns, but will not block until a message is available.
        pull();
      }

      // Take one message from queue.
      current = notYetRead.poll();
      if (current == null) {
        // Try again later.
        return false;
      }
      notYetReadBytes -= current.message().getData().size();
      checkState(notYetReadBytes >= 0);
      long nowMsSinceEpoch = now();
      numReadBytes.add(nowMsSinceEpoch, current.message().getData().size());
      minReadTimestampMsSinceEpoch.add(nowMsSinceEpoch, current.timestampMsSinceEpoch());
      if (current.timestampMsSinceEpoch() < lastWatermarkMsSinceEpoch) {
        numLateMessages.add(nowMsSinceEpoch, 1L);
      }

      // Current message can be considered 'read' and will be persisted by the next
      // checkpoint. So it is now safe to ACK back to Pubsub.
      safeToAckIds.add(current.ackId());
      return true;
    }

    @Override
    public PubsubMessage getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return new PubsubMessage(
          current.message().getData().toByteArray(),
          current.message().getAttributesMap(),
          current.recordId());
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return new Instant(current.timestampMsSinceEpoch());
    }

    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current.recordId().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * {@inheritDoc}.
     *
     * <p>Marks this {@link PubsubReader} as no longer active. The {@link PubsubClient} continue to
     * exist and be active beyond the life of this call if there are any in-flight checkpoints. When
     * no in-flight checkpoints remain, the reader will be closed.
     */
    @Override
    public void close() throws IOException {
      active.set(false);
      maybeCloseClient();
    }

    /**
     * Close this reader's underlying {@link PubsubClient} if the reader has been closed and there
     * are no outstanding checkpoints.
     */
    private void maybeCloseClient() throws IOException {
      if (!active.get() && numInFlightCheckpoints.get() == 0) {
        // The reader has been closed and it has no more outstanding checkpoints. The client
        // must be closed so it doesn't leak
        PubsubClient client = pubsubClient.getAndSet(null);
        if (client != null) {
          client.close();
        }
      }
    }

    @Override
    public PubsubSource getCurrentSource() {
      return outer;
    }

    @Override
    public Instant getWatermark() {
      if (pubsubClient.get().isEOF() && notYetRead.isEmpty()) {
        // For testing only: Advance the watermark to the end of time to signal
        // the test is complete.
        return BoundedWindow.TIMESTAMP_MAX_VALUE;
      }

      // NOTE: We'll allow the watermark to go backwards. The underlying runner is responsible
      // for aggregating all reported watermarks and ensuring the aggregate is latched.
      // If we attempt to latch locally then it is possible a temporary starvation of one reader
      // could cause its estimated watermark to fast forward to current system time. Then when
      // the reader resumes its watermark would be unable to resume tracking.
      // By letting the underlying runner latch we avoid any problems due to localized starvation.
      long nowMsSinceEpoch = now();
      long readMin = minReadTimestampMsSinceEpoch.get(nowMsSinceEpoch);
      long unreadMin = minUnreadTimestampMsSinceEpoch.get();
      if (readMin == Long.MAX_VALUE
          && unreadMin == Long.MAX_VALUE
          && lastReceivedMsSinceEpoch >= 0
          && nowMsSinceEpoch > lastReceivedMsSinceEpoch + SAMPLE_PERIOD.getMillis()) {
        // We don't currently have any unread messages pending, we have not had any messages
        // read for a while, and we have not received any new messages from Pubsub for a while.
        // Advance watermark to current time.
        // TODO: Estimate a timestamp lag.
        lastWatermarkMsSinceEpoch = nowMsSinceEpoch;
      } else if (minReadTimestampMsSinceEpoch.isSignificant()
          || minUnreadTimestampMsSinceEpoch.isSignificant()) {
        // Take minimum of the timestamps in all unread messages and recently read messages.
        lastWatermarkMsSinceEpoch = Math.min(readMin, unreadMin);
      }
      // else: We're not confident enough to estimate a new watermark. Stick with the old one.
      minWatermarkMsSinceEpoch.add(nowMsSinceEpoch, lastWatermarkMsSinceEpoch);
      maxWatermarkMsSinceEpoch.add(nowMsSinceEpoch, lastWatermarkMsSinceEpoch);
      return new Instant(lastWatermarkMsSinceEpoch);
    }

    @Override
    public PubsubCheckpoint getCheckpointMark() {
      int cur = numInFlightCheckpoints.incrementAndGet();
      maxInFlightCheckpoints = Math.max(maxInFlightCheckpoints, cur);
      // It's possible for a checkpoint to be taken but never finalized.
      // So we simply copy whatever safeToAckIds we currently have.
      // It's possible a later checkpoint will be taken before an earlier one is finalized,
      // in which case we'll double ACK messages to Pubsub. However Pubsub is fine with that.
      List<String> snapshotSafeToAckIds = Lists.newArrayList(safeToAckIds);
      List<String> snapshotNotYetReadIds = new ArrayList<>(notYetRead.size());
      for (PubsubClient.IncomingMessage incomingMessage : notYetRead) {
        snapshotNotYetReadIds.add(incomingMessage.ackId());
      }
      if (outer.subscriptionPath == null) {
        // need to include the subscription in case we resume, as it's not stored in the source.
        return new PubsubCheckpoint(
            subscription.getPath(), this, snapshotSafeToAckIds, snapshotNotYetReadIds);
      }
      return new PubsubCheckpoint(null, this, snapshotSafeToAckIds, snapshotNotYetReadIds);
    }

    @Override
    public long getSplitBacklogBytes() {
      return notYetReadBytes;
    }
  }

  // ================================================================================
  // Source
  // ================================================================================

  @VisibleForTesting
  static class PubsubSource extends UnboundedSource<PubsubMessage, PubsubCheckpoint> {
    public final PubsubUnboundedSource outer;
    // The subscription to read from.
    @VisibleForTesting final ValueProvider<SubscriptionPath> subscriptionPath;

    public PubsubSource(PubsubUnboundedSource outer) {
      this(outer, outer.getSubscriptionProvider());
    }

    private PubsubSource(
        PubsubUnboundedSource outer, ValueProvider<SubscriptionPath> subscriptionPath) {
      this.outer = outer;
      this.subscriptionPath = subscriptionPath;
    }

    @Override
    public List<PubsubSource> split(int desiredNumSplits, PipelineOptions options)
        throws Exception {
      List<PubsubSource> result = new ArrayList<>(desiredNumSplits);
      PubsubSource splitSource = this;
      if (subscriptionPath == null) {
        splitSource =
            new PubsubSource(
                outer, StaticValueProvider.of(outer.createRandomSubscription(options)));
      }
      for (int i = 0; i < desiredNumSplits * SCALE_OUT; i++) {
        // Since the source is immutable and Pubsub automatically shards we simply
        // replicate ourselves the requested number of times
        result.add(splitSource);
      }
      return result;
    }

    @Override
    public PubsubReader createReader(
        PipelineOptions options, @Nullable PubsubCheckpoint checkpoint) {
      PubsubReader reader;
      SubscriptionPath subscription;
      if (subscriptionPath == null || subscriptionPath.get() == null) {
        if (checkpoint == null) {
          // This reader has never been started and there was no call to #split;
          // create a single random subscription, which will be kept in the checkpoint.
          subscription = outer.createRandomSubscription(options);
        } else {
          subscription = checkpoint.getSubscription();
        }
      } else {
        subscription = subscriptionPath.get();
      }
      try {
        reader = new PubsubReader(options.as(PubsubOptions.class), this, subscription);
      } catch (GeneralSecurityException | IOException e) {
        throw new RuntimeException("Unable to subscribe to " + subscriptionPath + ": ", e);
      }
      if (checkpoint != null) {
        // NACK all messages we may have lost.
        try {
          // Will BLOCK until NACKed.
          checkpoint.nackAll(reader);
        } catch (IOException e) {
          LOG.error(
              "Pubsub {} cannot have {} lost messages NACKed, ignoring: {}",
              subscriptionPath,
              checkpoint.notYetReadIds.size(),
              e);
        }
      }
      return reader;
    }

    @Nullable
    @Override
    public Coder<PubsubCheckpoint> getCheckpointMarkCoder() {
      return CHECKPOINT_CODER;
    }

    @Override
    public Coder<PubsubMessage> getOutputCoder() {
      if (outer.getNeedsMessageId()) {
        return outer.getNeedsAttributes()
            ? PubsubMessageWithAttributesAndMessageIdCoder.of()
            : PubsubMessageWithMessageIdCoder.of();
      } else {
        return outer.getNeedsAttributes()
            ? PubsubMessageWithAttributesCoder.of()
            : PubsubMessagePayloadOnlyCoder.of();
      }
    }

    @Override
    public void validate() {
      // Nothing to validate.
    }

    @Override
    public boolean requiresDeduping() {
      // We cannot prevent re-offering already read messages after a restore from checkpoint.
      return true;
    }
  }

  // ================================================================================
  // StatsFn
  // ================================================================================

  private static class StatsFn extends DoFn<PubsubMessage, PubsubMessage> {
    private final Counter elementCounter = SourceMetrics.elementsRead();

    private final PubsubClientFactory pubsubFactory;
    private final @Nullable ValueProvider<SubscriptionPath> subscription;
    private final @Nullable ValueProvider<TopicPath> topic;
    private final @Nullable String timestampAttribute;
    private final @Nullable String idAttribute;

    public StatsFn(
        PubsubClientFactory pubsubFactory,
        @Nullable ValueProvider<SubscriptionPath> subscription,
        @Nullable ValueProvider<TopicPath> topic,
        @Nullable String timestampAttribute,
        @Nullable String idAttribute) {
      checkArgument(pubsubFactory != null, "pubsubFactory should not be null");
      this.pubsubFactory = pubsubFactory;
      this.subscription = subscription;
      this.topic = topic;
      this.timestampAttribute = timestampAttribute;
      this.idAttribute = idAttribute;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      elementCounter.inc();
      c.output(c.element());
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("subscription", subscription))
          .addIfNotNull(DisplayData.item("topic", topic))
          .add(DisplayData.item("transport", pubsubFactory.getKind()))
          .addIfNotNull(DisplayData.item("timestampAttribute", timestampAttribute))
          .addIfNotNull(DisplayData.item("idAttribute", idAttribute));
    }
  }

  // ================================================================================
  // PubsubUnboundedSource
  // ================================================================================

  /** For testing only: Clock to use for all timekeeping. If {@literal null} use system clock. */
  private @Nullable Clock clock;

  /** Factory for creating underlying Pubsub transport. */
  private final PubsubClientFactory pubsubFactory;

  /** Project under which to create a subscription if only the {@link #topic} was given. */
  private final @Nullable ValueProvider<ProjectPath> project;

  /**
   * Topic to read from. If {@literal null}, then {@link #subscription} must be given. Otherwise
   * {@link #subscription} must be null.
   */
  private final @Nullable ValueProvider<TopicPath> topic;

  /**
   * Subscription to read from. If {@literal null} then {@link #topic} must be given. Otherwise
   * {@link #topic} must be null.
   *
   * <p>If no subscription is given a random one will be created when the transorm is applied. This
   * field will be update with that subscription's path. The created subscription is never deleted.
   */
  private @Nullable ValueProvider<SubscriptionPath> subscription;

  /**
   * Pubsub metadata field holding timestamp of each element, or {@literal null} if should use
   * Pubsub message publish timestamp instead.
   */
  private final @Nullable String timestampAttribute;

  /**
   * Pubsub metadata field holding id for each element, or {@literal null} if need to generate a
   * unique id ourselves.
   */
  private final @Nullable String idAttribute;

  /** Whether this source should load the attributes of the PubsubMessage, or only the payload. */
  private final boolean needsAttributes;

  /** Whether this source should include the messageId from PubSub. */
  private final boolean needsMessageId;

  @VisibleForTesting
  PubsubUnboundedSource(
      Clock clock,
      PubsubClientFactory pubsubFactory,
      @Nullable ValueProvider<ProjectPath> project,
      @Nullable ValueProvider<TopicPath> topic,
      @Nullable ValueProvider<SubscriptionPath> subscription,
      @Nullable String timestampAttribute,
      @Nullable String idAttribute,
      boolean needsAttributes,
      boolean needsMessageId) {
    checkArgument(
        (topic == null) != (subscription == null),
        "Exactly one of topic and subscription must be given");
    this.clock = clock;
    this.pubsubFactory = checkNotNull(pubsubFactory);
    this.project = project;
    this.topic = topic;
    this.subscription = subscription;
    this.timestampAttribute = timestampAttribute;
    this.idAttribute = idAttribute;
    this.needsAttributes = needsAttributes;
    this.needsMessageId = needsMessageId;
  }

  /** Construct an unbounded source to consume from the Pubsub {@code subscription}. */
  public PubsubUnboundedSource(
      PubsubClientFactory pubsubFactory,
      @Nullable ValueProvider<ProjectPath> project,
      @Nullable ValueProvider<TopicPath> topic,
      @Nullable ValueProvider<SubscriptionPath> subscription,
      @Nullable String timestampAttribute,
      @Nullable String idAttribute,
      boolean needsAttributes) {
    this(
        null,
        pubsubFactory,
        project,
        topic,
        subscription,
        timestampAttribute,
        idAttribute,
        needsAttributes,
        false);
  }

  /** Construct an unbounded source to consume from the Pubsub {@code subscription}. */
  public PubsubUnboundedSource(
      Clock clock,
      PubsubClientFactory pubsubFactory,
      @Nullable ValueProvider<ProjectPath> project,
      @Nullable ValueProvider<TopicPath> topic,
      @Nullable ValueProvider<SubscriptionPath> subscription,
      @Nullable String timestampAttribute,
      @Nullable String idAttribute,
      boolean needsAttributes) {
    this(
        clock,
        pubsubFactory,
        project,
        topic,
        subscription,
        timestampAttribute,
        idAttribute,
        needsAttributes,
        false);
  }

  /** Construct an unbounded source to consume from the Pubsub {@code subscription}. */
  public PubsubUnboundedSource(
      PubsubClientFactory pubsubFactory,
      @Nullable ValueProvider<ProjectPath> project,
      @Nullable ValueProvider<TopicPath> topic,
      @Nullable ValueProvider<SubscriptionPath> subscription,
      @Nullable String timestampAttribute,
      @Nullable String idAttribute,
      boolean needsAttributes,
      boolean needsMessageId) {
    this(
        null,
        pubsubFactory,
        project,
        topic,
        subscription,
        timestampAttribute,
        idAttribute,
        needsAttributes,
        needsMessageId);
  }

  /** Get the project path. */
  public @Nullable ProjectPath getProject() {
    return project == null ? null : project.get();
  }

  /** Get the topic being read from. */
  public @Nullable TopicPath getTopic() {
    return topic == null ? null : topic.get();
  }

  /** Get the {@link ValueProvider} for the topic being read from. */
  public @Nullable ValueProvider<TopicPath> getTopicProvider() {
    return topic;
  }

  /** Get the subscription being read from. */
  public @Nullable SubscriptionPath getSubscription() {
    return subscription == null ? null : subscription.get();
  }

  /** Get the {@link ValueProvider} for the subscription being read from. */
  public @Nullable ValueProvider<SubscriptionPath> getSubscriptionProvider() {
    return subscription;
  }

  /** Get the timestamp attribute. */
  public @Nullable String getTimestampAttribute() {
    return timestampAttribute;
  }

  /** Get the id attribute. */
  public @Nullable String getIdAttribute() {
    return idAttribute;
  }

  public boolean getNeedsAttributes() {
    return needsAttributes;
  }

  public boolean getNeedsMessageId() {
    return needsMessageId;
  }

  @Override
  public PCollection<PubsubMessage> expand(PBegin input) {
    return input
        .getPipeline()
        .begin()
        .apply(Read.from(new PubsubSource(this)))
        .apply(
            "PubsubUnboundedSource.Stats",
            ParDo.of(
                new StatsFn(pubsubFactory, subscription, topic, timestampAttribute, idAttribute)));
  }

  private SubscriptionPath createRandomSubscription(PipelineOptions options) {
    TopicPath topicPath = topic.get();

    ProjectPath projectPath;
    if (project != null) {
      projectPath = project.get();
    } else {
      String projectId = options.as(GcpOptions.class).getProject();
      checkState(
          projectId != null,
          "Cannot create subscription to topic %s because pipeline option 'project' not specified",
          topicPath);
      projectPath = PubsubClient.projectPathFromId(options.as(GcpOptions.class).getProject());
    }

    try {
      try (PubsubClient pubsubClient =
          pubsubFactory.newClient(
              timestampAttribute, idAttribute, options.as(PubsubOptions.class))) {
        SubscriptionPath subscriptionPath =
            pubsubClient.createRandomSubscription(projectPath, topicPath, DEAULT_ACK_TIMEOUT_SEC);
        LOG.warn(
            "Created subscription {} to topic {}."
                + " Note this subscription WILL NOT be deleted when the pipeline terminates",
            subscriptionPath,
            topic);
        return subscriptionPath;
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to create subscription to topic %s on project %s: %s",
              topicPath, projectPath, e.getMessage()),
          e);
    }
  }
}
