/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

/**
 * A PTransform which streams messages from pub/sub.
 * <ul>
 * <li>The underlying implementation in an {@link UnboundedSource} which receives messages
 * in batches and hands them out one at a time.
 * <li>We use gRPC for its speed and low memory overhead.
 * <li>The watermark (either in pub/sub processing time or custom timestamp time) is estimated
 * by keeping track of the minimum of the last minutes worth of messages. This assumes pub/sub
 * delivers the oldest (in pub/sub processing time) available message at least once a minute,
 * and that custom timestamps are 'mostly' monotonic with pub/sub processing time. Unfortunately
 * both of those assumptions are currently false. Thus the estimated watermark may get ahead of
 * the 'true' watermark and cause some messages to be late.
 * <li>Checkpoints are used both to ACK received messages back to pub/sub (so that they may
 * be retired on the pub/sub end), and to NACK already consumed messages should a checkpoint
 * need to be restored (so that pub/sub will resend those messages promptly).
 * <li>The subscription must already exist.
 * <li>The subscription should have an ACK timeout of 60 seconds.
 * <li>We log vital stats every 30 seconds.
 * <li>Though some background threads are used by the underlying netty system all actual
 * pub/sub calls are blocking. We rely on the underlying runner to allow multiple
 * {@link UnboundedSource.UnboundedReader} instance to execute concurrently and thus hide latency.
 * </ul>
 */
public class PubsubUnboundedSource<T> extends PTransform<PBegin, PCollection<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubUnboundedSource.class);

  /**
   * Coder for checkpoints.
   */
  private static final CheckpointCoder<?> CHECKPOINT_CODER = new CheckpointCoder<>();

  /**
   * Maximum number of messages per pull.
   */
  private static final int PULL_BATCH_SIZE = 1000;

  /**
   * Maximum number of ack ids per ack or ack extension.
   */
  private static final int ACK_BATCH_SIZE = 2000;

  /**
   * Maximum number of messages in flight.
   */
  private static final int MAX_IN_FLIGHT = 20000;

  /**
   * Ack timeout for initial get.
   */
  private static final Duration ACK_TIMEOUT = Duration.standardSeconds(60);

  /**
   * Duration by which to extend acks when they are near timeout.
   */
  private static final Duration ACK_EXTENSION = Duration.standardSeconds(30);

  /*
   * How close we can get to a deadline before we need to extend it.
   */
  private static final Duration ACK_SAFETY = Duration.standardSeconds(15);

  /**
   * How close we can get to a deadline before we need to consider it passed.
   */
  private static final Duration ACK_TOO_LATE = Duration.standardSeconds(5);

  /**
   * Period of samples to determine watermark and other stats.
   */
  private static final Duration SAMPLE_PERIOD = Duration.standardMinutes(1);

  /**
   * Period of updates to determine watermark and other stats.
   */
  private static final Duration SAMPLE_UPDATE = Duration.standardSeconds(5);

  /**
   * How frequently to log stats.
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

  // ================================================================================
  // Checkpoint
  // ================================================================================

  /**
   * Which messages have been durably committed and thus can now be acked.
   * Which messages have been read but not yet committed, in which case they should be nacked if
   * we need to restore.
   */
  public static class Checkpoint<T> implements UnboundedSource.CheckpointMark {
    /**
     * If the checkpoint is for persisting: the reader who's snapshotted state we are persisting.
     * If the checkpoint is for restoring: initially {@literal null}, then explicitly set.
     * Not persisted in durable checkpoint.
     * CAUTION: Between a checkpoint being taken and {@link #finalizeCheckpoint()} being called
     * the 'true' active reader may have changed.
     */
    @Nullable
    private Reader<T> reader;

    /**
     * If the checkpoint is for persisting: The ack ids of messages which have been passed
     * downstream since the last checkpoint.
     * If the checkpoint is for restoring: {@literal null}.
     * Not persisted in durable checkpoint.
     */
    @Nullable
    private final List<String> safeToAckIds;

    /**
     * If the checkpoint is for persisting: The ack ids of messages which have been received
     * from pub/sub but not yet passed downstream at the time of the snapshot.
     * If the checkpoint is for restoring: Same, but recovered from durable storage.
     */
    private final List<String> notYetReadIds;

    public Checkpoint(
        @Nullable Reader<T> reader, @Nullable List<String> safeToAckIds,
        List<String> notYetReadIds) {
      this.reader = reader;
      this.safeToAckIds = safeToAckIds;
      this.notYetReadIds = notYetReadIds;
    }

    public void setReader(Reader<T> reader) {
      Preconditions.checkState(this.reader == null, "Cannot setReader on persisting checkpoint");
      this.reader = reader;
    }

    /**
     * BLOCKING
     * All messages which have been passed downstream have now been durably committed.
     * We can ack them upstream.
     */
    @Override
    public void finalizeCheckpoint() throws IOException {
      Preconditions.checkState(reader != null && safeToAckIds != null,
                               "Cannot finalize a restored checkpoint");
      // Even if the 'true' active reader has changed since the checkpoint was taken we are
      // fine:
      // - The underlying pub/sub topic will not have changed, so the following acks will still
      // go to the right place.
      // - We'll delete the ack ids from the readers in-flight state, but that only effects
      // flow control and stats, neither of which are relevent anymore.
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
        Preconditions.checkState(reader.numInFlightCheckpoints.decrementAndGet() >= 0);
      }
    }

    /**
     * BLOCKING
     * Nack all messages which have been read from pub/sub but not passed downstream.
     * This way pub/sub will send them again promptly.
     */
    public void nackAll() throws IOException {
      Preconditions.checkState(reader != null, "Reader was not set");
      List<String> batchYetToAckIds =
          new ArrayList<>(Math.min(notYetReadIds.size(), ACK_BATCH_SIZE));
      for (String ackId : notYetReadIds) {
        batchYetToAckIds.add(ackId);
        if (batchYetToAckIds.size() >= ACK_BATCH_SIZE) {
          long nowMsSinceEpoch = System.currentTimeMillis();
          reader.nackBatch(nowMsSinceEpoch, batchYetToAckIds);
          batchYetToAckIds.clear();
        }
      }
      if (!batchYetToAckIds.isEmpty()) {
        long nowMsSinceEpoch = System.currentTimeMillis();
        reader.nackBatch(nowMsSinceEpoch, batchYetToAckIds);
      }
    }
  }

  /**
   * The coder for our checkpoints.
   */
  private static class CheckpointCoder<T> extends AtomicCoder<Checkpoint<T>> {
    private static final Coder<List<String>> LIST_CODER = ListCoder.of(StringUtf8Coder.of());

    @Override
    public void encode(Checkpoint<T> value, OutputStream outStream, Context context)
        throws IOException {
      LIST_CODER.encode(value.notYetReadIds, outStream, context);
    }

    @Override
    public Checkpoint<T> decode(InputStream inStream, Context context) throws IOException {
      List<String> notYetReadIds = LIST_CODER.decode(inStream, context);
      return new Checkpoint<>(null, null, notYetReadIds);
    }
  }

  // ================================================================================
  // Reader
  // ================================================================================

  /**
   * A reader which keeps track of which messages have been received from pub/sub
   * but not yet consumed downstream and/or acked back to pub/sub.
   */
  private static class Reader<T> extends UnboundedSource.UnboundedReader<T> {
    /**
     * For access to topic and checkpointCoder.
     */
    private final Source<T> outer;

    /**
     * Client on which to talk to pub/sub. Null if closed.
     */
    @Nullable
    private PubsubGrpcClient pubsubClient;

    /**
     * Ack ids of messages we have delivered downstream but not yet acked.
     */
    private List<String> safeToAckIds;

    /**
     * Messages we have received from pub/sub and not yet delivered downstream.
     * We preserve their order.
     */
    private final Queue<PubsubGrpcClient.IncomingMessage> notYetRead;

    /**
     * Map from ack ids of messages we have received from pub/sub but not yet acked to their
     * current deadline. Ordered from earliest to latest deadline.
     */
    private final LinkedHashMap<String, Instant> inFlight;

    /**
     * Batches of successfully acked ids which need to be pruned from the above.
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
     * System time (ms since epoch) we last received a message from pub/sub, or -1 if
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
    private PubsubGrpcClient.IncomingMessage current;

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
     * Stats only: Number of messages which have recently been acked.
     */
    private MovingFunction numAcked;

    /**
     * Stats only: Number of messages which have recently been nacked.
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

    private static MovingFunction newFun(SimpleFunction function) {
      return new MovingFunction(SAMPLE_PERIOD.getMillis(),
                                SAMPLE_UPDATE.getMillis(),
                                MIN_WATERMARK_SPREAD,
                                MIN_WATERMARK_MESSAGES,
                                function);
    }

    /**
     * Construct a reader.
     */
    public Reader(GcpOptions options, Source<T> outer) throws IOException,
        GeneralSecurityException {
      this.outer = outer;
      pubsubClient = PubsubGrpcClient.newClient(outer.outer.timestampLabel,
                                                outer.outer.idLabel,
                                                options);
      safeToAckIds = new ArrayList<>();
      notYetRead = new ArrayDeque<>();
      inFlight = new LinkedHashMap<>();
      ackedIds = new ConcurrentLinkedQueue<>();
      notYetReadBytes = 0;
      minUnreadTimestampMsSinceEpoch = new BucketingFunction(SAMPLE_UPDATE.getMillis(),
                                                             MIN_WATERMARK_SPREAD,
                                                             MIN_WATERMARK_MESSAGES,
                                                             SimpleFunction.MIN);
      minReadTimestampMsSinceEpoch = newFun(SimpleFunction.MIN);
      lastReceivedMsSinceEpoch = -1;
      lastWatermarkMsSinceEpoch = BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis();
      current = null;
      lastLogTimestampMsSinceEpoch = -1;
      numReceived = 0L;
      numReceivedRecently = newFun(SimpleFunction.SUM);
      numExtendedDeadlines = newFun(SimpleFunction.SUM);
      numLateDeadlines = newFun(SimpleFunction.SUM);
      numAcked = newFun(SimpleFunction.SUM);
      numNacked = newFun(SimpleFunction.SUM);
      numReadBytes = newFun(SimpleFunction.SUM);
      minReceivedTimestampMsSinceEpoch = newFun(SimpleFunction.MIN);
      maxReceivedTimestampMsSinceEpoch = newFun(SimpleFunction.MAX);
      minWatermarkMsSinceEpoch = newFun(SimpleFunction.MIN);
      maxWatermarkMsSinceEpoch = newFun(SimpleFunction.MAX);
      numLateMessages = newFun(SimpleFunction.SUM);
      numInFlightCheckpoints = new AtomicInteger();
      maxInFlightCheckpoints = 0;
    }

    /**
     * BLOCKING
     * Ack receipt of messages from pub/sub with the given {@code ackIds}.
     * CAUTION: May be invoked from a separate checkpointing thread.
     * CAUTION: Retains {@code ackIds}.
     */
    public void ackBatch(List<String> ackIds) throws IOException {
      pubsubClient.acknowledge(outer.outer.subscription, ackIds);
      ackedIds.add(ackIds);
    }

    /**
     * BLOCKING
     * 'Nack' (ie request deadline extension of 0) receipt of messages from pub/sub
     * with the given {@code ockIds}. Does not retain {@code ackIds}.
     */
    public void nackBatch(long nowMsSinceEpoch, List<String> ackIds) throws IOException {
      pubsubClient.modifyAckDeadline(outer.outer.subscription, ackIds, 0);
      numNacked.add(nowMsSinceEpoch, ackIds.size());
    }

    /**
     * BLOCKING
     * Extend the processing deadline for messages from pub/sub  with the given {@code ackIds}.
     * Does not retain {@code ackIds}.
     */
    private void extendBatch(long nowMsSinceEpoch, List<String> ackIds) throws IOException {
      pubsubClient.modifyAckDeadline(outer.outer.subscription, ackIds,
                                     (int) ACK_EXTENSION.getStandardSeconds());
      numExtendedDeadlines.add(nowMsSinceEpoch, ackIds.size());
    }

    /**
     * Messages which have been acked (via the checkpoint finalize) are no longer in flight.
     * This is only used for flow control and stats.
     */
    private void retire() {
      long nowMsSinceEpoch = System.currentTimeMillis();
      while (true) {
        List<String> ackIds = ackedIds.poll();
        if (ackIds == null) {
          return;
        }
        numAcked.add(nowMsSinceEpoch, ackIds.size());
        for (String ackId : ackIds) {
          inFlight.remove(ackId);
        }
      }
    }

    /**
     * BLOCKING
     * Extend deadline for all messages which need it.
     * <p>
     * CAUTION: If extensions can't keep up with wallclock then we'll never return.
     */
    private void extend() throws IOException {
      while (true) {
        long nowMsSinceEpoch = System.currentTimeMillis();
        List<Map.Entry<String, Instant>> toBeExtended = new ArrayList<>();
        for (Map.Entry<String, Instant> entry : inFlight.entrySet()) {
          long deadlineMsSinceEpoch = entry.getValue().getMillis();
          if (deadlineMsSinceEpoch < nowMsSinceEpoch + ACK_TOO_LATE.getMillis()) {
            // This message may have already expired, in which case it will (eventually) be
            // made available on a future pull request.
            // If the message ends up being commited then a future resend will be ignored
            // downsteam and acked as usual.
            numLateDeadlines.add(nowMsSinceEpoch, 1);
          }
          if (deadlineMsSinceEpoch > nowMsSinceEpoch + ACK_SAFETY.getMillis()) {
            // No later messages need extending.
            break;
          }
          toBeExtended.add(entry);
          if (toBeExtended.size() >= ACK_BATCH_SIZE) {
            // Enough for one batch.
            break;
          }
        }
        if (toBeExtended.isEmpty()) {
          // No messages need to be extended.
          return;
        }
        List<String> extensionAckIds = new ArrayList<>(toBeExtended.size());
        long newDeadlineMsSinceEpoch = nowMsSinceEpoch + ACK_EXTENSION.getMillis();
        for (Map.Entry<String, Instant> entry : toBeExtended) {
          inFlight.remove(entry.getKey());
          inFlight.put(entry.getKey(), new Instant(newDeadlineMsSinceEpoch));
          extensionAckIds.add(entry.getKey());
        }
        // BLOCKs until extended.
        extendBatch(nowMsSinceEpoch, extensionAckIds);
      }
    }

    /**
     * BLOCKING
     * Fetch another batch of messages from pub/sub.
     */
    private void pull() throws IOException {
      if (inFlight.size() >= MAX_IN_FLIGHT) {
        // Wait for checkpoint to be finalized before pulling anymore.
        // There may be lag while checkpoints are persisted and the finalizeCheckpoint method
        // is invoked. By limiting the in-flight messages we can ensure we don't end up consuming
        // messages faster than we can checkpoint them.
        return;
      }

      long requestTimeMsSinceEpoch = System.currentTimeMillis();
      long deadlineMsSinceEpoch = requestTimeMsSinceEpoch + ACK_TIMEOUT.getMillis();

      // Pull the next batch.
      // BLOCKs until received.
      Collection<PubsubGrpcClient.IncomingMessage> receivedMessages =
          pubsubClient.pull(requestTimeMsSinceEpoch,
                            outer.outer.subscription,
                            PULL_BATCH_SIZE);
      if (receivedMessages.isEmpty()) {
        // Nothing available yet. Try again later.
        return;
      }

      lastReceivedMsSinceEpoch = requestTimeMsSinceEpoch;

      // Capture the received messages.
      for (PubsubGrpcClient.IncomingMessage incomingMessage : receivedMessages) {
        notYetRead.add(incomingMessage);
        notYetReadBytes += incomingMessage.elementBytes.length;
        inFlight.put(incomingMessage.ackId, new Instant(deadlineMsSinceEpoch));
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
      long nowMsSinceEpoch = System.currentTimeMillis();
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

      LOG.warn("Pubsub {} has "
               + "{} received messages, "
               + "{} current unread messages, "
               + "{} current unread bytes, "
               + "{} current in-flight msgs, "
               + "{} current in-flight checkpoints, "
               + "{} max in-flight checkpoints, "
               + "{}B/s recent read, "
               + "{} recent received, "
               + "{} recent extended, "
               + "{} recent late extended, "
               + "{} recent acked, "
               + "{} recent nacked, "
               + "{} recent message timestamp skew, "
               + "{} recent watermark skew, "
               + "{} recent late messages, "
               + "{} last reported watermark",
               outer.outer.subscription,
               numReceived,
               notYetRead.size(),
               notYetReadBytes,
               inFlight.size(),
               numInFlightCheckpoints.get(),
               maxInFlightCheckpoints,
               numReadBytes.get(nowMsSinceEpoch) / (SAMPLE_PERIOD.getMillis() / 1000L),
               numReceivedRecently.get(nowMsSinceEpoch),
               numExtendedDeadlines.get(nowMsSinceEpoch),
               numLateDeadlines.get(nowMsSinceEpoch),
               numAcked.get(nowMsSinceEpoch),
               numNacked.get(nowMsSinceEpoch),
               messageSkew,
               watermarkSkew,
               numLateMessages.get(nowMsSinceEpoch),
               new Instant(lastWatermarkMsSinceEpoch));

      lastLogTimestampMsSinceEpoch = nowMsSinceEpoch;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    /**
     * BLOCKING
     * Return {@literal true} if a pub/sub messaage is available, {@literal false} if
     * none is available at this time or we are over-subscribed. May BLOCK while extending
     * acks or fetching available messages. Will not block waiting for messages.
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

      // Retire state associated with acked messages.
      retire();

      // Extend all pressing deadlines.
      // Will BLOCK until done.
      // If the system is pulling messages only to let them sit in a downsteam queue then
      // this will have the effect of slowing down the pull rate.
      // However, if the system is genuinely taking longer to process each message then
      // the work to extend acks would be better done in the background.
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
      Preconditions.checkState(notYetReadBytes >= 0);
      long nowMsSinceEpoch = System.currentTimeMillis();
      numReadBytes.add(nowMsSinceEpoch, current.elementBytes.length);
      minReadTimestampMsSinceEpoch.add(nowMsSinceEpoch, current.timestampMsSinceEpoch);
      if (current.timestampMsSinceEpoch < lastWatermarkMsSinceEpoch) {
        numLateMessages.add(nowMsSinceEpoch, 1L);
      }

      // Current message will be persisted by the next checkpoint so it is now safe to ack.
      safeToAckIds.add(current.ackId);
      return true;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      try {
        return CoderUtils.decodeFromByteArray(outer.outer.elementCoder, current.elementBytes);
      } catch (CoderException e) {
        throw new RuntimeException("Unable to decode element from pub/sub message: ", e);
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
      return current.recordId;
    }

    @Override
    public void close() throws IOException {
      pubsubClient.close();
      pubsubClient = null;
    }

    @Override
    public Source<T> getCurrentSource() {
      return outer;
    }

    @Override
    public Instant getWatermark() {
      // NOTE: We'll allow the watermark to go backwards. The underlying runner is responsible
      // for aggregating all reported watermarks and ensuring the aggregate is latched.
      // If we attempt to latch locally then it is possible a temporary starvation of one reader
      // could cause its estimated watermark to fast forward to current system time. Then when
      // the reader resumes its watermark would be unable to resume tracking.
      // By letting the underlying runner latch we avoid any problems due to localized starvation.
      long nowMsSinceEpoch = System.currentTimeMillis();
      long readMin = minReadTimestampMsSinceEpoch.get(nowMsSinceEpoch);
      long unreadMin = minUnreadTimestampMsSinceEpoch.get();
      if (readMin == Long.MAX_VALUE &&
          unreadMin == Long.MAX_VALUE &&
          lastReceivedMsSinceEpoch >= 0 &&
          nowMsSinceEpoch > lastReceivedMsSinceEpoch + SAMPLE_PERIOD.getMillis()) {
        // We don't currently have any unread messages pending, we have not had any messages
        // read for a while, and we have not received any new messages from pub/sub for a while.
        // Advance watermark to current time.
        // TODO: Estimate a timestamp lag.
        lastWatermarkMsSinceEpoch = nowMsSinceEpoch;
      } else if (minReadTimestampMsSinceEpoch.isSignificant() ||
                 minUnreadTimestampMsSinceEpoch.isSignificant()) {
        // Take minimum of the timestamps in all unread messages and recently read messages.
        lastWatermarkMsSinceEpoch = Math.min(readMin, unreadMin);
      }
      // else: We're not confident enough to estimate a new watermark. Stick with the old one.
      minWatermarkMsSinceEpoch.add(nowMsSinceEpoch, lastWatermarkMsSinceEpoch);
      maxWatermarkMsSinceEpoch.add(nowMsSinceEpoch, lastWatermarkMsSinceEpoch);
      return new Instant(lastWatermarkMsSinceEpoch);
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      int cur = numInFlightCheckpoints.incrementAndGet();
      maxInFlightCheckpoints = Math.max(maxInFlightCheckpoints, cur);
      // The checkpoint will either be finalized or we'll rollback to an earlier
      // checkpoint. Thus we can hand off these ack ids to the checkpoint.
      List<String> snapshotSafeToAckIds = safeToAckIds;
      safeToAckIds = new ArrayList<>();
      List<String> snapshotNotYetReadIds = new ArrayList<>(notYetRead.size());
      for (PubsubGrpcClient.IncomingMessage incomingMessage : notYetRead) {
        snapshotNotYetReadIds.add(incomingMessage.ackId);
      }
      return new Checkpoint<>(this, snapshotSafeToAckIds, snapshotNotYetReadIds);
    }

    @Override
    public long getSplitBacklogBytes() {
      return notYetReadBytes;
    }
  }

  // ================================================================================
  // Source
  // ================================================================================

  private static class Source<T> extends UnboundedSource<T, PubsubUnboundedSource.Checkpoint<T>> {
    PubsubUnboundedSource<T> outer;

    public Source(PubsubUnboundedSource<T> outer) {
      this.outer = outer;
    }

    @Override
    public List<Source<T>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      List<Source<T>> result = new ArrayList<>(desiredNumSplits);
      for (int i = 0; i < desiredNumSplits * SCALE_OUT; i++) {
        // Since the source is immutable and pub/sub automatically shards we simply
        // replicate ourselves the requested number of times
        result.add(this);
      }
      return result;
    }

    @Override
    public UnboundedReader<T> createReader(
        PipelineOptions options, @Nullable Checkpoint<T> checkpoint) {
      PubsubUnboundedSource.Reader<T> reader;
      try {
        reader =
            new PubsubUnboundedSource.Reader<>(options.as(GcpOptions.class), this);
      } catch (GeneralSecurityException | IOException e) {
        throw new RuntimeException("Unable to subscribe to " + outer.subscription + ": ", e);
      }
      if (checkpoint != null) {
        // Nack all messages we may have lost.
        try {
          checkpoint.setReader(reader);
          // Will BLOCK until nacked.
          checkpoint.nackAll();
        } catch (IOException e) {
          LOG.error("Pubsub {} cannot have {} lost messages nacked, ignoring: {}",
                    outer.subscription, checkpoint.notYetReadIds.size(), e);
        }
      }
      return reader;
    }

    @Nullable
    @Override
    public Coder<Checkpoint<T>> getCheckpointMarkCoder() {
      @SuppressWarnings("unchecked") CheckpointCoder<T> typedCoder =
          (CheckpointCoder<T>) CHECKPOINT_CODER;
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
    private final Aggregator<Long, Long> elementCounter =
        createAggregator("elements", new Sum.SumLongFn());


    @Override
    public void processElement(ProcessContext c) throws Exception {
      elementCounter.addValue(1L);
      c.output(c.element());
    }
  }

  // ================================================================================
  // PubsubUnboundedSource
  // ================================================================================

  /**
   * Subscription to read from.
   */
  private final String subscription;

  /**
   * Coder for elements. Elements are effectively double-encoded: first to a byte array
   * using this checkpointCoder, then to a base-64 string to conform to pub/sub's payload
   * conventions.
   */
  private final Coder<T> elementCoder;

  /**
   * Pub/sub metadata field holding timestamp of each element, or {@literal null} if should use
   * pub/sub message publish timestamp instead.
   */
  @Nullable
  private final String timestampLabel;

  /**
   * Pub/sub metadata field holding id for each element, or {@literal null} if need to generate
   * a unique id ourselves.
   */
  @Nullable
  private final String idLabel;

  /**
   * Construct an unbounded source to consume from the pub/sub {@code subscription}.
   */
  public PubsubUnboundedSource(
      String subscription, Coder<T> elementCoder, @Nullable String timestampLabel,
      @Nullable String idLabel) {
    this.subscription = Preconditions.checkNotNull(subscription);
    this.elementCoder = Preconditions.checkNotNull(elementCoder);
    this.timestampLabel = timestampLabel;
    this.idLabel = idLabel;
  }

  @Override
  public PCollection<T> apply(PBegin input) {
    String label = "PubsubSource(" + subscription.replaceFirst(".*/", "") + ")";
    return input.getPipeline().begin()
                .apply(Read.from(new Source<T>(this)))
                .apply(ParDo.named(label).of(new StatsFn<T>()));
  }
}
