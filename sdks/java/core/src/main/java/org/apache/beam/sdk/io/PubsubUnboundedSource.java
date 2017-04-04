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

package org.apache.beam.sdk.io;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.util.Clock;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.PubsubIO.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PubsubOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.BucketingFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.MovingFunction;
import org.apache.beam.sdk.util.PubsubClient;
import org.apache.beam.sdk.util.PubsubClient.ProjectPath;
import org.apache.beam.sdk.util.PubsubClient.PubsubClientFactory;
import org.apache.beam.sdk.util.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.util.PubsubClient.TopicPath;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PTransform which streams messages from Pubsub.
 * <ul>
 * <li>The underlying implementation in an {@link UnboundedSource} which receives messages
 * in batches and hands them out one at a time.
 * <li>The watermark (either in Pubsub processing time or custom timestamp time) is estimated
 * by keeping track of the minimum of the last minutes worth of messages. This assumes Pubsub
 * delivers the oldest (in Pubsub processing time) available message at least once a minute,
 * and that custom timestamps are 'mostly' monotonic with Pubsub processing time. Unfortunately
 * both of those assumptions are fragile. Thus the estimated watermark may get ahead of
 * the 'true' watermark and cause some messages to be late.
 * <li>Checkpoints are used both to ACK received messages back to Pubsub (so that they may
 * be retired on the Pubsub end), and to NACK already consumed messages should a checkpoint
 * need to be restored (so that Pubsub will resend those messages promptly).
 * <li>The backlog is determined by each reader using the messages which have been pulled from
 * Pubsub but not yet consumed downstream. The backlog does not take account of any messages queued
 * by Pubsub for the subscription. Unfortunately there is currently no API to determine
 * the size of the Pubsub queue's backlog.
 * <li>The subscription must already exist.
 * <li>The subscription timeout is read whenever a reader is started. However it is not
 * checked thereafter despite the timeout being user-changeable on-the-fly.
 * <li>We log vital stats every 30 seconds.
 * <li>Though some background threads may be used by the underlying transport all Pubsub calls
 * are blocking. We rely on the underlying runner to allow multiple
 * {@link UnboundedSource.UnboundedReader} instances to execute concurrently and thus hide latency.
 * </ul>
 */
public class PubsubUnboundedSource<T> extends PTransform<PBegin, PCollection<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubUnboundedSource.class);

  /**
   * Default ACK timeout for created subscriptions.
   */
  private static final int DEAULT_ACK_TIMEOUT_SEC = 60;

  /**
   * Coder for checkpoints.
   */
  private static final PubsubCheckpointCoder<?> CHECKPOINT_CODER = new PubsubCheckpointCoder<>();

  /**
   * Maximum number of messages per pull.
   */
  private static final int PULL_BATCH_SIZE = 1000;

  /**
   * Maximum number of ACK ids per ACK or ACK extension call.
   */
  private static final int ACK_BATCH_SIZE = 2000;

  /**
   * Maximum number of messages in flight.
   */
  private static final int MAX_IN_FLIGHT = 20000;

  /**
   * Timeout for round trip from receiving a message to finally ACKing it back to Pubsub.
   */
  private static final Duration PROCESSING_TIMEOUT = Duration.standardSeconds(120);

  /**
   * Percentage of ack timeout by which to extend acks when they are near timeout.
   */
  private static final int ACK_EXTENSION_PCT = 50;

  /**
   * Percentage of ack timeout we should use as a safety margin. We'll try to extend acks
   * by this margin before the ack actually expires.
   */
  private static final int ACK_SAFETY_PCT = 20;

  /**
   * For stats only: How close we can get to an ack deadline before we risk it being already
   * considered passed by Pubsub.
   */
  private static final Duration ACK_TOO_LATE = Duration.standardSeconds(2);

  /**
   * Period of samples to determine watermark and other stats.
   */
  private static final Duration SAMPLE_PERIOD = Duration.standardMinutes(1);

  /**
   * Period of updates to determine watermark and other stats.
   */
  private static final Duration SAMPLE_UPDATE = Duration.standardSeconds(5);

  /**
   * Period for logging stats.
   */
  private static final Duration LOG_PERIOD = Duration.standardSeconds(30);

  /**
   * Minimum number of unread messages required before considering updating watermark.
   */
  private static final int MIN_WATERMARK_MESSAGES = 10;

  /**
   * Minimum number of SAMPLE_UPDATE periods over which unread messages shoud be spread
   * before considering updating watermark.
   */
  private static final int MIN_WATERMARK_SPREAD = 2;

  /**
   * Additional sharding so that we can hide read message latency.
   */
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
   * Which messages have been durably committed and thus can now be ACKed.
   * Which messages have been read but not yet committed, in which case they should be NACKed if
   * we need to restore.
   */
  @VisibleForTesting
  static class PubsubCheckpoint<T> implements UnboundedSource.CheckpointMark {
    /**
     * The {@link SubscriptionPath} to the subscription the reader is reading from. May be
     * {@code null} if the {@link PubsubUnboundedSource} contains the subscription.
     */
    @VisibleForTesting
    @Nullable String subscriptionPath;

    /**
     * If the checkpoint is for persisting: the reader who's snapshotted state we are persisting.
     * If the checkpoint is for restoring: {@literal null}.
     * Not persisted in durable checkpoint.
     * CAUTION: Between a checkpoint being taken and {@link #finalizeCheckpoint()} being called
     * the 'true' active reader may have changed.
     */
    @Nullable
    private PubsubReader<T> reader;

    /**
     * If the checkpoint is for persisting: The ACK ids of messages which have been passed
     * downstream since the last checkpoint.
     * If the checkpoint is for restoring: {@literal null}.
     * Not persisted in durable checkpoint.
     */
    @Nullable
    private List<String> safeToAckIds;

    /**
     * If the checkpoint is for persisting: The ACK ids of messages which have been received
     * from Pubsub but not yet passed downstream at the time of the snapshot.
     * If the checkpoint is for restoring: Same, but recovered from durable storage.
     */
    @VisibleForTesting
    final List<String> notYetReadIds;

    public PubsubCheckpoint(
        @Nullable String subscriptionPath,
        @Nullable PubsubReader<T> reader,
        @Nullable List<String> safeToAckIds,
        List<String> notYetReadIds) {
      this.subscriptionPath = subscriptionPath;
      this.reader = reader;
      this.safeToAckIds = safeToAckIds;
      this.notYetReadIds = notYetReadIds;
    }

    @Nullable
    private SubscriptionPath getSubscription() {
      return subscriptionPath == null
          ? null
          : PubsubClient.subscriptionPathFromPath(subscriptionPath);
    }

    /**
     * BLOCKING
     * All messages which have been passed downstream have now been durably committed.
     * We can ACK them upstream.
     * CAUTION: This may never be called.
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
        checkState(remainingInFlight >= 0,
                   "Miscounted in-flight checkpoints");
        reader.maybeCloseClient();
        reader = null;
        safeToAckIds = null;
      }
    }

    /**
     * Return current time according to {@code reader}.
     */
    private static long now(PubsubReader<?> reader) {
      if (reader.outer.outer.clock == null) {
        return System.currentTimeMillis();
      } else {
        return reader.outer.outer.clock.currentTimeMillis();
      }
    }

    /**
     * BLOCKING
     * NACK all messages which have been read from Pubsub but not passed downstream.
     * This way Pubsub will send them again promptly.
     */
    public void nackAll(PubsubReader<T> reader) throws IOException {
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
  private static class PubsubCheckpointCoder<T> extends AtomicCoder<PubsubCheckpoint<T>> {
    private static final Coder<String> SUBSCRIPTION_PATH_CODER =
        NullableCoder.of(StringUtf8Coder.of());
    private static final Coder<List<String>> LIST_CODER = ListCoder.of(StringUtf8Coder.of());

    @Override
    public void encode(PubsubCheckpoint<T> value, OutputStream outStream, Context context)
        throws IOException {
      SUBSCRIPTION_PATH_CODER.encode(
          value.subscriptionPath,
          outStream,
          context.nested());
      LIST_CODER.encode(value.notYetReadIds, outStream, context);
    }

    @Override
    public PubsubCheckpoint<T> decode(InputStream inStream, Context context) throws IOException {
      String path = SUBSCRIPTION_PATH_CODER.decode(inStream, context.nested());
      List<String> notYetReadIds = LIST_CODER.decode(inStream, context);
      return new PubsubCheckpoint<>(path, null, null, notYetReadIds);
    }
  }

  // ================================================================================
  // Reader
  // ================================================================================

  /**
   * A reader which keeps track of which messages have been received from Pubsub
   * but not yet consumed downstream and/or ACKed back to Pubsub.
   */
  @VisibleForTesting
  static class PubsubReader<T> extends UnboundedSource.UnboundedReader<T> {
    /**
     * For access to topic and checkpointCoder.
     */
    private final PubsubSource<T> outer;
    @VisibleForTesting
    final SubscriptionPath subscription;

    private final SimpleFunction<PubsubIO.PubsubMessage, T> parseFn;

    /**
     * Client on which to talk to Pubsub. Contains a null value if the client has been closed.
     */
    private AtomicReference<PubsubClient> pubsubClient;

    /**
     * The closed state of this {@link PubsubReader}. If true, the reader has not yet been closed,
     * and it will have a non-null value within {@link #pubsubClient}.
     */
    private AtomicBoolean active = new AtomicBoolean(true);

    /**
     * Ack timeout, in ms, as set on subscription when we first start reading. Not
     * updated thereafter. -1 if not yet determined.
     */
    private int ackTimeoutMs;

    /**
     * ACK ids of messages we have delivered downstream but not yet ACKed.
     */
    private Set<String> safeToAckIds;

    /**
     * Messages we have received from Pubsub and not yet delivered downstream.
     * We preserve their order.
     */
    private final Queue<PubsubClient.IncomingMessage> notYetRead;

    private static class InFlightState {
      /**
       * When request which yielded message was issues.
       */
      long requestTimeMsSinceEpoch;

      /**
       * When Pubsub will consider this message's ACK to timeout and thus it needs to be
       * extended.
       */
      long ackDeadlineMsSinceEpoch;

      public InFlightState(long requestTimeMsSinceEpoch, long ackDeadlineMsSinceEpoch) {
        this.requestTimeMsSinceEpoch = requestTimeMsSinceEpoch;
        this.ackDeadlineMsSinceEpoch = ackDeadlineMsSinceEpoch;
      }
    }

    /**
     * Map from ACK ids of messages we have received from Pubsub but not yet ACKed to their
     * in flight state. Ordered from earliest to latest ACK deadline.
     */
    private final LinkedHashMap<String, InFlightState> inFlight;

    /**
     * Batches of successfully ACKed ids which need to be pruned from the above.
     * CAUTION: Accessed by both reader and checkpointing threads.
     */
    private final Queue<List<String>> ackedIds;

    /**
     * Byte size of undecoded elements in {@link #notYetRead}.
     */
    private long notYetReadBytes;

    /**
     * Bucketed map from received time (as system time, ms since epoch) to message
     * timestamps (mssince epoch) of all received but not-yet read messages.
     * Used to estimate watermark.
     */
    private BucketingFunction minUnreadTimestampMsSinceEpoch;

    /**
     * Minimum of timestamps (ms since epoch) of all recently read messages.
     * Used to estimate watermark.
     */
    private MovingFunction minReadTimestampMsSinceEpoch;

    /**
     * System time (ms since epoch) we last received a message from Pubsub, or -1 if
     * not yet received any messages.
     */
    private long lastReceivedMsSinceEpoch;

    /**
     * The last reported watermark (ms since epoch), or beginning of time if none yet reported.
     */
    private long lastWatermarkMsSinceEpoch;

    /**
     * The current message, or {@literal null} if none.
     */
    @Nullable
    private PubsubClient.IncomingMessage current;

    /**
     * Stats only: System time (ms since epoch) we last logs stats, or -1 if never.
     */
    private long lastLogTimestampMsSinceEpoch;

    /**
     * Stats only: Total number of messages received.
     */
    private long numReceived;

    /**
     * Stats only: Number of messages which have recently been received.
     */
    private MovingFunction numReceivedRecently;

    /**
     * Stats only: Number of messages which have recently had their deadline extended.
     */
    private MovingFunction numExtendedDeadlines;

    /**
     * Stats only: Number of messages which have recenttly had their deadline extended even
     * though it may be too late to do so.
     */
    private MovingFunction numLateDeadlines;


    /**
     * Stats only: Number of messages which have recently been ACKed.
     */
    private MovingFunction numAcked;

    /**
     * Stats only: Number of messages which have recently expired (ACKs were extended for too
     * long).
     */
    private MovingFunction numExpired;

    /**
     * Stats only: Number of messages which have recently been NACKed.
     */
    private MovingFunction numNacked;

    /**
     * Stats only: Number of message bytes which have recently been read by downstream consumer.
     */
    private MovingFunction numReadBytes;

    /**
     * Stats only: Minimum of timestamp (ms since epoch) of all recently received messages.
     * Used to estimate timestamp skew. Does not contribute to watermark estimator.
     */
    private MovingFunction minReceivedTimestampMsSinceEpoch;

    /**
     * Stats only: Maximum of timestamp (ms since epoch) of all recently received messages.
     * Used to estimate timestamp skew.
     */
    private MovingFunction maxReceivedTimestampMsSinceEpoch;

    /**
     * Stats only: Minimum of recent estimated watermarks (ms since epoch).
     */
    private MovingFunction minWatermarkMsSinceEpoch;

    /**
     * Stats ony: Maximum of recent estimated watermarks (ms since epoch).
     */
    private MovingFunction maxWatermarkMsSinceEpoch;

    /**
     * Stats only: Number of messages with timestamps strictly behind the estimated watermark
     * at the time they are received. These may be considered 'late' by downstream computations.
     */
    private MovingFunction numLateMessages;

    /**
     * Stats only: Current number of checkpoints in flight.
     * CAUTION: Accessed by both checkpointing and reader threads.
     */
    private AtomicInteger numInFlightCheckpoints;

    /**
     * Stats only: Maximum number of checkpoints in flight at any time.
     */
    private int maxInFlightCheckpoints;

    private static MovingFunction newFun(Combine.BinaryCombineLongFn function) {
      return new MovingFunction(SAMPLE_PERIOD.getMillis(),
                                SAMPLE_UPDATE.getMillis(),
                                MIN_WATERMARK_SPREAD,
                                MIN_WATERMARK_MESSAGES,
                                function);
    }

    /**
     * Construct a reader.
     */
    public PubsubReader(PubsubOptions options, PubsubSource<T> outer, SubscriptionPath subscription,
                        SimpleFunction<PubsubIO.PubsubMessage, T> parseFn)
        throws IOException, GeneralSecurityException {
      this.outer = outer;
      this.subscription = subscription;
      this.parseFn = parseFn;
      pubsubClient =
          new AtomicReference<>(
              outer.outer.pubsubFactory.newClient(
                  outer.outer.timestampLabel, outer.outer.idLabel, options));
      ackTimeoutMs = -1;
      safeToAckIds = new HashSet<>();
      notYetRead = new ArrayDeque<>();
      inFlight = new LinkedHashMap<>();
      ackedIds = new ConcurrentLinkedQueue<>();
      notYetReadBytes = 0;
      minUnreadTimestampMsSinceEpoch = new BucketingFunction(SAMPLE_UPDATE.getMillis(),
                                                             MIN_WATERMARK_SPREAD,
                                                             MIN_WATERMARK_MESSAGES,
                                                             MIN);
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
     * BLOCKING
     * NACK (ie request deadline extension of 0) receipt of messages from Pubsub
     * with the given {@code ockIds}. Does not retain {@code ackIds}.
     */
    public void nackBatch(long nowMsSinceEpoch, List<String> ackIds) throws IOException {
      pubsubClient.get().modifyAckDeadline(subscription, ackIds, 0);
      numNacked.add(nowMsSinceEpoch, ackIds.size());
    }

    /**
     * BLOCKING
     * Extend the processing deadline for messages from Pubsub with the given {@code ackIds}.
     * Does not retain {@code ackIds}.
     */
    private void extendBatch(long nowMsSinceEpoch, List<String> ackIds) throws IOException {
      int extensionSec = (ackTimeoutMs * ACK_EXTENSION_PCT) / (100 * 1000);
      pubsubClient.get().modifyAckDeadline(subscription, ackIds, extensionSec);
      numExtendedDeadlines.add(nowMsSinceEpoch, ackIds.size());
    }

    /**
     * Return the current time, in ms since epoch.
     */
    private long now() {
      if (outer.outer.clock == null) {
        return System.currentTimeMillis();
      } else {
        return outer.outer.clock.currentTimeMillis();
      }
    }

    /**
     * Messages which have been ACKed (via the checkpoint finalize) are no longer in flight.
     * This is only used for flow control and stats.
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
     * BLOCKING
     * Extend deadline for all messages which need it.
     * CAUTION: If extensions can't keep up with wallclock then we'll never return.
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
            inFlight.put(ackId,
                         new InFlightState(state.requestTimeMsSinceEpoch, newDeadlineMsSinceEpoch));
          }
          // BLOCKs until extended.
          extendBatch(nowMsSinceEpoch, toBeExtended);
        }
      }
    }

    /**
     * BLOCKING
     * Fetch another batch of messages from Pubsub.
     */
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
        notYetReadBytes += incomingMessage.elementBytes.length;
        inFlight.put(incomingMessage.ackId,
                     new InFlightState(requestTimeMsSinceEpoch, deadlineMsSinceEpoch));
        numReceived++;
        numReceivedRecently.add(requestTimeMsSinceEpoch, 1L);
        minReceivedTimestampMsSinceEpoch.add(requestTimeMsSinceEpoch,
                                             incomingMessage.timestampMsSinceEpoch);
        maxReceivedTimestampMsSinceEpoch.add(requestTimeMsSinceEpoch,
                                             incomingMessage.timestampMsSinceEpoch);
        minUnreadTimestampMsSinceEpoch.add(requestTimeMsSinceEpoch,
                                           incomingMessage.timestampMsSinceEpoch);
      }
    }

    /**
     * Log stats if time to do so.
     */
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

      LOG.info("Pubsub {} has "
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
     * BLOCKING
     * Return {@literal true} if a Pubsub messaage is available, {@literal false} if
     * none is available at this time or we are over-subscribed. May BLOCK while extending
     * ACKs or fetching available messages. Will not block waiting for messages.
     */
    @Override
    public boolean advance() throws IOException {
      // Emit stats.
      stats();

      if (current != null) {
        // Current is consumed. It can no longer contribute to holding back the watermark.
        minUnreadTimestampMsSinceEpoch.remove(current.requestTimeMsSinceEpoch);
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
      notYetReadBytes -= current.elementBytes.length;
      checkState(notYetReadBytes >= 0);
      long nowMsSinceEpoch = now();
      numReadBytes.add(nowMsSinceEpoch, current.elementBytes.length);
      minReadTimestampMsSinceEpoch.add(nowMsSinceEpoch, current.timestampMsSinceEpoch);
      if (current.timestampMsSinceEpoch < lastWatermarkMsSinceEpoch) {
        numLateMessages.add(nowMsSinceEpoch, 1L);
      }

      // Current message can be considered 'read' and will be persisted by the next
      // checkpoint. So it is now safe to ACK back to Pubsub.
      safeToAckIds.add(current.ackId);
      return true;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      try {
        if (parseFn != null) {
          return parseFn.apply(new PubsubIO.PubsubMessage(
                  current.elementBytes, current.attributes));
        } else {
          return CoderUtils.decodeFromByteArray(outer.outer.elementCoder, current.elementBytes);
        }
      } catch (CoderException e) {
        throw new RuntimeException("Unable to decode element from Pubsub message: ", e);
      }
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return new Instant(current.timestampMsSinceEpoch);
    }

    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current.recordId.getBytes(Charsets.UTF_8);
    }

    /**
     * {@inheritDoc}.
     *
     * <p>Marks this {@link PubsubReader} as no longer active. The {@link PubsubClient}
     * continue to exist and be active beyond the life of this call if there are any in-flight
     * checkpoints. When no in-flight checkpoints remain, the reader will be closed.
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
    public PubsubSource<T> getCurrentSource() {
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
    public PubsubCheckpoint<T> getCheckpointMark() {
      int cur = numInFlightCheckpoints.incrementAndGet();
      maxInFlightCheckpoints = Math.max(maxInFlightCheckpoints, cur);
      // It's possible for a checkpoint to be taken but never finalized.
      // So we simply copy whatever safeToAckIds we currently have.
      // It's possible a later checkpoint will be taken before an earlier one is finalized,
      // in which case we'll double ACK messages to Pubsub. However Pubsub is fine with that.
      List<String> snapshotSafeToAckIds = Lists.newArrayList(safeToAckIds);
      List<String> snapshotNotYetReadIds = new ArrayList<>(notYetRead.size());
      for (PubsubClient.IncomingMessage incomingMessage : notYetRead) {
        snapshotNotYetReadIds.add(incomingMessage.ackId);
      }
      if (outer.subscriptionPath == null) {
        // need to include the subscription in case we resume, as it's not stored in the source.
        return new PubsubCheckpoint<>(
            subscription.getPath(), this, snapshotSafeToAckIds, snapshotNotYetReadIds);
      }
      return new PubsubCheckpoint<>(null, this, snapshotSafeToAckIds, snapshotNotYetReadIds);
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
  static class PubsubSource<T> extends UnboundedSource<T, PubsubCheckpoint<T>> {
    public final PubsubUnboundedSource<T> outer;
    // The subscription to read from.
    @VisibleForTesting
    final SubscriptionPath subscriptionPath;

    public PubsubSource(PubsubUnboundedSource<T> outer) {
      this(outer, outer.getSubscription());
    }

    private PubsubSource(PubsubUnboundedSource<T> outer, SubscriptionPath subscriptionPath) {
      this.outer = outer;
      this.subscriptionPath = subscriptionPath;
    }

    @Override
    public List<PubsubSource<T>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      List<PubsubSource<T>> result = new ArrayList<>(desiredNumSplits);
      PubsubSource<T> splitSource = this;
      if (subscriptionPath == null) {
        splitSource = new PubsubSource<>(outer, outer.createRandomSubscription(options));
      }
      for (int i = 0; i < desiredNumSplits * SCALE_OUT; i++) {
        // Since the source is immutable and Pubsub automatically shards we simply
        // replicate ourselves the requested number of times
        result.add(splitSource);
      }
      return result;
    }

    @Override
    public PubsubReader<T> createReader(
        PipelineOptions options,
        @Nullable PubsubCheckpoint<T> checkpoint) {
      PubsubReader<T> reader;
      SubscriptionPath subscription = subscriptionPath;
      if (subscription == null) {
        if (checkpoint == null) {
          // This reader has never been started and there was no call to #splitIntoBundles; create
          // a single random subscription, which will be kept in the checkpoint.
          subscription = outer.createRandomSubscription(options);
        } else {
          subscription = checkpoint.getSubscription();
        }
      }
      try {
        reader = new PubsubReader<>(options.as(PubsubOptions.class), this, subscription,
                outer.parseFn);
      } catch (GeneralSecurityException | IOException e) {
        throw new RuntimeException("Unable to subscribe to " + subscriptionPath + ": ", e);
      }
      if (checkpoint != null) {
        // NACK all messages we may have lost.
        try {
          // Will BLOCK until NACKed.
          checkpoint.nackAll(reader);
        } catch (IOException e) {
          LOG.error("Pubsub {} cannot have {} lost messages NACKed, ignoring: {}",
                    subscriptionPath, checkpoint.notYetReadIds.size(), e);
        }
      }
      return reader;
    }

    @Nullable
    @Override
    public Coder<PubsubCheckpoint<T>> getCheckpointMarkCoder() {
      @SuppressWarnings("unchecked") PubsubCheckpointCoder<T> typedCoder =
          (PubsubCheckpointCoder<T>) CHECKPOINT_CODER;
      return typedCoder;
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return outer.elementCoder;
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

  private static class StatsFn<T> extends DoFn<T, T> {
    private final Counter elementCounter = Metrics.counter(StatsFn.class, "elements");

    private final PubsubClientFactory pubsubFactory;
    @Nullable
    private final ValueProvider<SubscriptionPath> subscription;
    @Nullable
    private final ValueProvider<TopicPath> topic;
    @Nullable
    private final String timestampLabel;
    @Nullable
    private final String idLabel;

    public StatsFn(
        PubsubClientFactory pubsubFactory,
        @Nullable ValueProvider<SubscriptionPath> subscription,
        @Nullable ValueProvider<TopicPath> topic,
        @Nullable String timestampLabel,
        @Nullable String idLabel) {
      checkArgument(pubsubFactory != null, "pubsubFactory should not be null");
      this.pubsubFactory = pubsubFactory;
      this.subscription = subscription;
      this.topic = topic;
      this.timestampLabel = timestampLabel;
      this.idLabel = idLabel;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      elementCounter.inc();
      c.output(c.element());
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      if (subscription != null) {
        String subscriptionString = subscription.isAccessible()
            ? subscription.get().getPath()
            : subscription.toString();
        builder.add(DisplayData.item("subscription", subscriptionString));
      }
      if (topic != null) {
        String topicString = topic.isAccessible()
            ? topic.get().getPath()
            : topic.toString();
        builder.add(DisplayData.item("topic", topicString));
      }
      builder.add(DisplayData.item("transport", pubsubFactory.getKind()));
      builder.addIfNotNull(DisplayData.item("timestampLabel", timestampLabel));
      builder.addIfNotNull(DisplayData.item("idLabel", idLabel));
    }
  }

  // ================================================================================
  // PubsubUnboundedSource
  // ================================================================================

  /**
   * For testing only: Clock to use for all timekeeping. If {@literal null} use system clock.
   */
  @Nullable
  private Clock clock;

  /**
   * Factory for creating underlying Pubsub transport.
   */
  private final PubsubClientFactory pubsubFactory;

  /**
   * Project under which to create a subscription if only the {@link #topic} was given.
   */
  @Nullable
  private final ValueProvider<ProjectPath> project;

  /**
   * Topic to read from. If {@literal null}, then {@link #subscription} must be given.
   * Otherwise {@link #subscription} must be null.
   */
  @Nullable
  private final ValueProvider<TopicPath> topic;

  /**
   * Subscription to read from. If {@literal null} then {@link #topic} must be given.
   * Otherwise {@link #topic} must be null.
   *
   * <p>If no subscription is given a random one will be created when the transorm is
   * applied. This field will be update with that subscription's path. The created
   * subscription is never deleted.
   */
  @Nullable
  private ValueProvider<SubscriptionPath> subscription;

  /**
   * Coder for elements. Elements are effectively double-encoded: first to a byte array
   * using this checkpointCoder, then to a base-64 string to conform to Pubsub's payload
   * conventions.
   */
  private final Coder<T> elementCoder;

  /**
   * Pubsub metadata field holding timestamp of each element, or {@literal null} if should use
   * Pubsub message publish timestamp instead.
   */
  @Nullable
  private final String timestampLabel;

  /**
   * Pubsub metadata field holding id for each element, or {@literal null} if need to generate
   * a unique id ourselves.
   */
  @Nullable
  private final String idLabel;

  /**
   * If not {@literal null}, the user is asking for PubSub attributes. This parse function will be
   * used to parse {@link PubsubIO.PubsubMessage}s containing a payload and attributes.
   */
  @Nullable
  SimpleFunction<PubsubMessage, T> parseFn;

  @VisibleForTesting
  PubsubUnboundedSource(
      Clock clock,
      PubsubClientFactory pubsubFactory,
      @Nullable ValueProvider<ProjectPath> project,
      @Nullable ValueProvider<TopicPath> topic,
      @Nullable ValueProvider<SubscriptionPath> subscription,
      Coder<T> elementCoder,
      @Nullable String timestampLabel,
      @Nullable String idLabel,
      @Nullable SimpleFunction<PubsubIO.PubsubMessage, T> parseFn) {
    checkArgument((topic == null) != (subscription == null),
                  "Exactly one of topic and subscription must be given");
    checkArgument((topic == null) == (project == null),
                  "Project must be given if topic is given");
    this.clock = clock;
    this.pubsubFactory = checkNotNull(pubsubFactory);
    this.project = project;
    this.topic = topic;
    this.subscription = subscription;
    this.elementCoder = checkNotNull(elementCoder);
    this.timestampLabel = timestampLabel;
    this.idLabel = idLabel;
    this.parseFn = parseFn;
  }

  /**
   * Construct an unbounded source to consume from the Pubsub {@code subscription}.
   */
  public PubsubUnboundedSource(
      PubsubClientFactory pubsubFactory,
      @Nullable ValueProvider<ProjectPath> project,
      @Nullable ValueProvider<TopicPath> topic,
      @Nullable ValueProvider<SubscriptionPath> subscription,
      Coder<T> elementCoder,
      @Nullable String timestampLabel,
      @Nullable String idLabel,
      @Nullable SimpleFunction<PubsubIO.PubsubMessage, T> parseFn) {
    this(null, pubsubFactory, project, topic, subscription, elementCoder, timestampLabel, idLabel,
        parseFn);
  }

  /**
   * Get the coder used for elements.
   */
  public Coder<T> getElementCoder() {
    return elementCoder;
  }

  /**
   * Get the project path.
   */
  @Nullable
  public ProjectPath getProject() {
    return project == null ? null : project.get();
  }

  /**
   * Get the topic being read from.
   */
  @Nullable
  public TopicPath getTopic() {
    return topic == null ? null : topic.get();
  }

  /**
   * Get the {@link ValueProvider} for the topic being read from.
   */
  @Nullable
  public ValueProvider<TopicPath> getTopicProvider() {
    return topic;
  }

  /**
   * Get the subscription being read from.
   */
  @Nullable
  public SubscriptionPath getSubscription() {
    return subscription == null ? null : subscription.get();
  }

  /**
   * Get the {@link ValueProvider} for the subscription being read from.
   */
  @Nullable
  public ValueProvider<SubscriptionPath> getSubscriptionProvider() {
    return subscription;
  }

  /**
   * Get the timestamp label.
   */
  @Nullable
  public String getTimestampLabel() {
    return timestampLabel;
  }

  /**
   * Get the id label.
   */
  @Nullable
  public String getIdLabel() {
    return idLabel;
  }

  /**
   * Get the parsing function for PubSub attributes.
   */
  @Nullable
  public SimpleFunction<PubsubIO.PubsubMessage, T> getWithAttributesParseFn() {
    return parseFn;
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    return input.getPipeline().begin()
                .apply(Read.from(new PubsubSource<T>(this)))
                .apply("PubsubUnboundedSource.Stats",
                    ParDo.of(new StatsFn<T>(
                        pubsubFactory, subscription, topic, timestampLabel, idLabel)));
  }

  private SubscriptionPath createRandomSubscription(PipelineOptions options) {
    try {
      try (PubsubClient pubsubClient =
          pubsubFactory.newClient(timestampLabel, idLabel, options.as(PubsubOptions.class))) {
        checkState(project.isAccessible(), "createRandomSubscription must be called at runtime.");
        checkState(topic.isAccessible(), "createRandomSubscription must be called at runtime.");
        SubscriptionPath subscriptionPath =
            pubsubClient.createRandomSubscription(
                project.get(), topic.get(), DEAULT_ACK_TIMEOUT_SEC);
        LOG.warn(
            "Created subscription {} to topic {}."
                + " Note this subscription WILL NOT be deleted when the pipeline terminates",
            subscriptionPath,
            topic);
        return subscriptionPath;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create subscription: ", e);
    }
  }
}
