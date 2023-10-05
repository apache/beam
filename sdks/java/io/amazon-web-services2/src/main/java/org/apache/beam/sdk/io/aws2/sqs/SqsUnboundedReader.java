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
package org.apache.beam.sdk.io.aws2.sqs;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Collections2.transform;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams.mapWithIndex;
import static software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName.SENT_TIMESTAMP;
import static software.amazon.awssdk.services.sqs.model.QueueAttributeName.VISIBILITY_TIMEOUT;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
import java.util.function.Function;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.BucketingFunction;
import org.apache.beam.sdk.util.MovingFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams.FunctionWithIndex;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class SqsUnboundedReader extends UnboundedSource.UnboundedReader<SqsMessage> {
  private static final String RECEIPT_HANDLE_IS_INVALID = "ReceiptHandleIsInvalid";

  private static final Logger LOG = LoggerFactory.getLogger(SqsUnboundedReader.class);

  /** Maximum number of messages to pull from SQS per request. */
  public static final int MAX_NUMBER_OF_MESSAGES = 10;

  /** Maximum times to retry batch SQS operations upon partial success. */
  private static final int BATCH_OPERATION_MAX_RETIRES = 5;

  /** Timeout for round trip from receiving a message to finally deleting it from SQS. */
  private static final Duration PROCESSING_TIMEOUT = Duration.standardMinutes(2);

  /**
   * Percentage of visibility timeout by which to extend visibility timeout when they are near
   * timeout.
   */
  private static final int VISIBILITY_EXTENSION_PCT = 50;

  /**
   * Percentage of ack timeout we should use as a safety margin. We'll try to extend visibility
   * timeout by this margin before the visibility timeout actually expires.
   */
  private static final int VISIBILITY_SAFETY_PCT = 20;

  /**
   * For stats only: How close we can get to an visibility deadline before we risk it being already
   * considered passed by SQS.
   */
  private static final Duration VISIBILITY_TOO_LATE = Duration.standardSeconds(2);

  /** Maximum number of message ids per delete or visibility extension call. */
  private static final int DELETE_BATCH_SIZE = 10;

  /** Maximum number of messages in flight. */
  private static final int MAX_IN_FLIGHT = 20000;

  /** Period of samples to determine watermark and other stats. */
  private static final Duration SAMPLE_PERIOD = Duration.standardMinutes(1);

  /** Period of updates to determine watermark and other stats. */
  private static final Duration SAMPLE_UPDATE = Duration.standardSeconds(5);

  /** Period for logging stats. */
  private static final Duration LOG_PERIOD = Duration.standardSeconds(30);

  /** Minimum number of unread messages required before considering updating watermark. */
  private static final int MIN_WATERMARK_MESSAGES = 10;

  /**
   * Minimum number of SAMPLE_UPDATE periods over which unread messages should be spread before
   * considering updating watermark.
   */
  private static final int MIN_WATERMARK_SPREAD = 2;

  private static final Combine.BinaryCombineLongFn MIN = Min.ofLongs();

  private static final Combine.BinaryCombineLongFn MAX = Max.ofLongs();

  private static final Combine.BinaryCombineLongFn SUM = Sum.ofLongs();

  /** For access to topic and SQS client. */
  private final SqsUnboundedSource source;

  /** Clock for internal time. */
  private final Clock clock;

  /** AWS options. */
  private final AwsOptions awsOptions;

  /**
   * The closed state of this {@link SqsUnboundedReader}. If true, the reader has not yet been
   * closed, and it will have a non-null value within {@link #SqsUnboundedReader}.
   */
  private AtomicBoolean active = new AtomicBoolean(true);

  /** SQS client of this reader instance. */
  private SqsClient sqsClient = null;

  /** The current message, or {@literal null} if none. */
  private SqsMessage current;

  /**
   * Messages we have received from SQS and not yet delivered downstream. We preserve their order.
   */
  private final Queue<SqsMessage> messagesNotYetRead;

  /** Message ids of messages we have delivered downstream but not yet deleted. */
  private Set<String> safeToDeleteIds;

  /**
   * Visibility timeout, in ms, as set on subscription when we first start reading. Not updated
   * thereafter. -1 if not yet determined.
   */
  private long visibilityTimeoutMs;

  /** Byte size of undecoded elements in {@link #messagesNotYetRead}. */
  private long notYetReadBytes;

  /**
   * Bucketed map from received time (as system time, ms since epoch) to message timestamps (mssince
   * epoch) of all received but not-yet read messages. Used to estimate watermark.
   */
  private BucketingFunction minUnreadTimestampMsSinceEpoch;

  /**
   * Minimum of timestamps (ms since epoch) of all recently read messages. Used to estimate
   * watermark.
   */
  private MovingFunction minReadTimestampMsSinceEpoch;

  private static class InFlightState {
    /** Receipt handle of message. */
    String receiptHandle;

    /** When request which yielded message was issued. */
    long requestTimeMsSinceEpoch;

    /**
     * When SQS will consider this message's visibility timeout to timeout and thus it needs to be
     * extended.
     */
    long visibilityDeadlineMsSinceEpoch;

    public InFlightState(
        String receiptHandle, long requestTimeMsSinceEpoch, long visibilityDeadlineMsSinceEpoch) {
      this.receiptHandle = receiptHandle;
      this.requestTimeMsSinceEpoch = requestTimeMsSinceEpoch;
      this.visibilityDeadlineMsSinceEpoch = visibilityDeadlineMsSinceEpoch;
    }
  }

  /**
   * Map from message ids of messages we have received from SQS but not yet deleted to their in
   * flight state. Ordered from earliest to latest visibility deadline.
   */
  private final LinkedHashMap<String, InFlightState> inFlight;

  /**
   * Batches of successfully deleted message ids which need to be pruned from the above. CAUTION:
   * Accessed by both reader and checkpointing threads.
   */
  private final Queue<List<String>> deletedIds;

  /**
   * System time (ms since epoch) we last received a message from SQS, or -1 if not yet received any
   * messages.
   */
  private long lastReceivedMsSinceEpoch;

  /** The last reported watermark (ms since epoch), or beginning of time if none yet reported. */
  private long lastWatermarkMsSinceEpoch;

  /** Stats only: System time (ms since epoch) we last logs stats, or -1 if never. */
  private long lastLogTimestampMsSinceEpoch;

  /** Stats only: Total number of messages received. */
  private long numReceived;

  /** Stats only: Number of messages which have recently been received. */
  private MovingFunction numReceivedRecently;

  /** Stats only: Number of messages which have recently had their deadline extended. */
  private MovingFunction numExtendedDeadlines;

  /**
   * Stats only: Number of messages which have recently had their deadline extended even though it
   * may be too late to do so.
   */
  private MovingFunction numLateDeadlines;

  /** Stats only: Number of messages which have recently been deleted. */
  private MovingFunction numDeleted;

  /**
   * Stats only: Number of messages which have recently expired (visibility timeout were extended
   * for too long).
   */
  private MovingFunction numExpired;

  /** Stats only: Number of messages which have recently been returned to visible on SQS. */
  private MovingFunction numReleased;

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
  AtomicInteger numInFlightCheckpoints;

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

  public SqsUnboundedReader(
      SqsUnboundedSource source, SqsCheckpointMark sqsCheckpointMark, AwsOptions awsOptions)
      throws IOException {
    this(source, sqsCheckpointMark, awsOptions, Clock.systemUTC());
  }

  @VisibleForTesting
  SqsUnboundedReader(
      SqsUnboundedSource source,
      SqsCheckpointMark sqsCheckpointMark,
      AwsOptions awsOptions,
      Clock clock)
      throws IOException {
    this.source = source;
    this.clock = clock;
    this.awsOptions = awsOptions;

    messagesNotYetRead = new ArrayDeque<>(MAX_NUMBER_OF_MESSAGES);
    safeToDeleteIds = new HashSet<>();
    inFlight = new LinkedHashMap<>();
    deletedIds = new ConcurrentLinkedQueue<>();
    visibilityTimeoutMs = -1;
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
    numDeleted = newFun(SUM);
    numExpired = newFun(SUM);
    numReleased = newFun(SUM);
    numReadBytes = newFun(SUM);
    minReceivedTimestampMsSinceEpoch = newFun(MIN);
    maxReceivedTimestampMsSinceEpoch = newFun(MAX);
    minWatermarkMsSinceEpoch = newFun(MIN);
    maxWatermarkMsSinceEpoch = newFun(MAX);
    numLateMessages = newFun(SUM);
    numInFlightCheckpoints = new AtomicInteger();
    maxInFlightCheckpoints = 0;

    if (sqsCheckpointMark != null) {
      initClient();
      expireBatchForRedelivery(sqsCheckpointMark.notYetReadReceipts);
    }
  }

  @Override
  public Instant getWatermark() {

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
      // read for a while, and we have not received any new messages from SQS for a while.
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
  public SqsMessage getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }

    return new Instant(current.getTimeStamp());
  }

  @Override
  public byte[] getCurrentRecordId() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current.getMessageId().getBytes(UTF_8);
  }

  @Override
  public CheckpointMark getCheckpointMark() {
    int cur = numInFlightCheckpoints.incrementAndGet();
    maxInFlightCheckpoints = Math.max(maxInFlightCheckpoints, cur);
    return new SqsCheckpointMark(
        this, safeToDeleteIds, transform(messagesNotYetRead, SqsMessage::getReceiptHandle));
  }

  @Override
  public SqsUnboundedSource getCurrentSource() {
    return source;
  }

  @Override
  public boolean start() throws IOException {
    initClient();
    String timeout =
        sqsClient
            .getQueueAttributes(b -> b.queueUrl(queueUrl()).attributeNames(VISIBILITY_TIMEOUT))
            .attributes()
            .get(VISIBILITY_TIMEOUT);
    visibilityTimeoutMs = Integer.parseInt(timeout) * 1000L;
    return advance();
  }

  private String queueUrl() {
    return source.getRead().queueUrl();
  }

  private void initClient() {
    if (sqsClient == null) {
      ClientConfiguration config = source.getRead().clientConfiguration();
      sqsClient = ClientBuilderFactory.buildClient(awsOptions, SqsClient.builder(), config);
    }
  }

  @Override
  public boolean advance() throws IOException {
    // Emit stats.
    stats();

    if (current != null) {
      // Current is consumed. It can no longer contribute to holding back the watermark.
      minUnreadTimestampMsSinceEpoch.remove(current.getRequestTimeStamp());
      current = null;
    }

    // Retire state associated with deleted messages.
    retire();

    // Extend all pressing deadlines.
    // Will BLOCK until done.
    // If the system is pulling messages only to let them sit in a downstream queue then
    // this will have the effect of slowing down the pull rate.
    // However, if the system is genuinely taking longer to process each message then
    // the work to extend visibility timeout would be better done in the background.
    extend();

    if (messagesNotYetRead.isEmpty()) {
      // Pull another batch.
      // Will BLOCK until fetch returns, but will not block until a message is available.
      pull();
    }

    // Take one message from queue.
    current = messagesNotYetRead.poll();
    if (current == null) {
      // Try again later.
      return false;
    }
    int currentBytes = current.getBody().getBytes(UTF_8).length;
    notYetReadBytes -= currentBytes;
    checkState(notYetReadBytes >= 0);
    long nowMsSinceEpoch = now();
    numReadBytes.add(nowMsSinceEpoch, currentBytes);
    minReadTimestampMsSinceEpoch.add(nowMsSinceEpoch, getCurrentTimestamp().getMillis());

    if (getCurrentTimestamp().getMillis() < lastWatermarkMsSinceEpoch) {
      numLateMessages.add(nowMsSinceEpoch, 1L);
    }

    // Current message can be considered 'read' and will be persisted by the next
    // checkpoint. So it is now safe to delete from SQS.
    safeToDeleteIds.add(current.getMessageId());

    return true;
  }

  /**
   * {@inheritDoc}.
   *
   * <p>Marks this {@link SqsUnboundedReader} as no longer active. The {@link SqsClient} continue to
   * exist and be active beyond the life of this call if there are any in-flight checkpoints. When
   * no in-flight checkpoints remain, the reader will be closed.
   */
  @Override
  public void close() throws IOException {
    active.set(false);
    maybeCloseClient();
  }

  /**
   * Close this reader's underlying {@link SqsClient} if the reader has been closed and there are no
   * outstanding checkpoints.
   */
  void maybeCloseClient() throws IOException {
    if (!active.get() && numInFlightCheckpoints.get() == 0) {
      // The reader has been closed and it has no more outstanding checkpoints. The client
      // must be closed so it doesn't leak
      if (sqsClient != null) {
        sqsClient.close();
      }
    }
  }

  /**
   * Delete the provided {@code messageIds} from SQS in multiple batches. Each batch except the last
   * one is of size {@code DELETE_BATCH_SIZE}. Message ids that already got removed from {@code
   * inFlight} messages are ignored.
   *
   * <p>CAUTION: May be invoked from a separate thread.
   */
  void delete(List<String> messageIds) throws IOException {
    ArrayList<String> receiptHandles = new ArrayList<>(DELETE_BATCH_SIZE);
    for (String msgId : messageIds) {
      InFlightState state = inFlight.get(msgId);
      if (state == null) {
        continue;
      }
      receiptHandles.add(state.receiptHandle);
      if (receiptHandles.size() == DELETE_BATCH_SIZE) {
        deleteBatch(receiptHandles);
        receiptHandles.clear();
      }
    }
    if (!receiptHandles.isEmpty()) {
      deleteBatch(receiptHandles);
    }
    deletedIds.add(messageIds);
  }

  /**
   * Delete the provided {@code receiptHandles} from SQS. Blocking until all messages are deleted.
   *
   * <p>CAUTION: May be invoked from a separate thread.
   */
  private void deleteBatch(List<String> receiptHandles) throws IOException {
    int retries = 0;

    FunctionWithIndex<String, DeleteMessageBatchRequestEntry> buildEntry =
        (handle, id) ->
            DeleteMessageBatchRequestEntry.builder()
                .id(Long.toString(id))
                .receiptHandle(handle)
                .build();

    Map<String, DeleteMessageBatchRequestEntry> pendingDeletes =
        mapWithIndex(receiptHandles.stream(), buildEntry).collect(toMap(e -> e.id(), identity()));

    while (!pendingDeletes.isEmpty()) {

      if (retries >= BATCH_OPERATION_MAX_RETIRES) {
        throw new IOException(
            "Failed to delete "
                + pendingDeletes.size()
                + " messages after "
                + retries
                + " retries");
      }

      DeleteMessageBatchResponse result =
          sqsClient.deleteMessageBatch(
              DeleteMessageBatchRequest.builder()
                  .queueUrl(queueUrl())
                  .entries(pendingDeletes.values())
                  .build());

      Map<Boolean, Set<String>> failures =
          result.failed().stream()
              .collect(partitioningBy(this::isHandleInvalid, mapping(e -> e.id(), toSet())));

      // Keep failed IDs only, but discard invalid receipt handles
      pendingDeletes.keySet().retainAll(failures.getOrDefault(FALSE, ImmutableSet.of()));

      int invalidHandles = failures.getOrDefault(TRUE, ImmutableSet.of()).size();
      if (invalidHandles > 0) {
        LOG.warn("Failed to delete {} messages due to expired receipt handles.", invalidHandles);
      }

      retries += 1;
    }
  }

  /** Check {@link BatchResultErrorEntry#code()} for invalid expired receipt handles. */
  private boolean isHandleInvalid(BatchResultErrorEntry error) {
    return RECEIPT_HANDLE_IS_INVALID.equals(error.code());
  }

  /**
   * Messages which have been deleted (via the checkpoint finalize) are no longer in flight. This is
   * only used for flow control and stats.
   */
  private void retire() {
    long nowMsSinceEpoch = now();
    while (true) {
      List<String> ackIds = deletedIds.poll();
      if (ackIds == null) {
        return;
      }
      numDeleted.add(nowMsSinceEpoch, ackIds.size());
      for (String ackId : ackIds) {
        inFlight.remove(ackId);
        safeToDeleteIds.remove(ackId);
      }
    }
  }

  /** BLOCKING. Fetch another batch of messages from SQS. */
  private void pull() {
    if (inFlight.size() >= MAX_IN_FLIGHT) {
      // Wait for checkpoint to be finalized before pulling anymore.
      // There may be lag while checkpoints are persisted and the finalizeCheckpoint method
      // is invoked. By limiting the in-flight messages we can ensure we don't end up consuming
      // messages faster than we can checkpoint them.
      return;
    }

    long requestTimeMsSinceEpoch = now();
    long deadlineMsSinceEpoch = requestTimeMsSinceEpoch + visibilityTimeoutMs;

    final ReceiveMessageRequest receiveMessageRequest =
        ReceiveMessageRequest.builder()
            .maxNumberOfMessages(MAX_NUMBER_OF_MESSAGES)
            .attributeNamesWithStrings(SENT_TIMESTAMP.toString())
            .queueUrl(queueUrl())
            .build();

    final ReceiveMessageResponse receiveMessageResponse =
        sqsClient.receiveMessage(receiveMessageRequest);

    final List<Message> messages = receiveMessageResponse.messages();

    if (messages == null || messages.isEmpty()) {
      return;
    }

    lastReceivedMsSinceEpoch = requestTimeMsSinceEpoch;

    // Capture the received messages.
    for (Message orgMsg : messages) {
      long msgTimeStamp = Long.parseLong(orgMsg.attributes().get(SENT_TIMESTAMP));
      SqsMessage message =
          SqsMessage.create(
              orgMsg.body(),
              orgMsg.messageId(),
              orgMsg.receiptHandle(),
              msgTimeStamp,
              requestTimeMsSinceEpoch);
      messagesNotYetRead.add(message);
      notYetReadBytes += message.getBody().getBytes(UTF_8).length;
      inFlight.put(
          message.getMessageId(),
          new InFlightState(
              message.getReceiptHandle(), requestTimeMsSinceEpoch, deadlineMsSinceEpoch));
      numReceived++;
      numReceivedRecently.add(requestTimeMsSinceEpoch, 1L);
      minReceivedTimestampMsSinceEpoch.add(requestTimeMsSinceEpoch, msgTimeStamp);
      maxReceivedTimestampMsSinceEpoch.add(requestTimeMsSinceEpoch, msgTimeStamp);
      minUnreadTimestampMsSinceEpoch.add(requestTimeMsSinceEpoch, msgTimeStamp);
    }
  }

  /** Return the current time, in ms since epoch. */
  long now() {
    return clock.millis();
  }

  /**
   * BLOCKING. Extend deadline for all messages which need it. CAUTION: If extensions can't keep up
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
        if (entry.getValue().visibilityDeadlineMsSinceEpoch
                - (visibilityTimeoutMs * VISIBILITY_SAFETY_PCT) / 100
            > nowMsSinceEpoch) {
          // All remaining messages don't need their visibility timeouts to be extended.
          break;
        }

        if (entry.getValue().visibilityDeadlineMsSinceEpoch - VISIBILITY_TOO_LATE.getMillis()
            < nowMsSinceEpoch) {
          // SQS may have already considered this message to have expired.
          // If so it will (eventually) be made available on a future pull request.
          // If this message ends up being committed then it will be considered a duplicate
          // when re-pulled.
          assumeExpired.add(entry.getKey());
          continue;
        }

        if (entry.getValue().requestTimeMsSinceEpoch + PROCESSING_TIMEOUT.getMillis()
            < nowMsSinceEpoch) {
          // This message has been in-flight for too long.
          // Give up on it, otherwise we risk extending its visibility timeout indefinitely.
          toBeExpired.add(entry.getKey());
          continue;
        }

        // Extend the visibility timeout for this message.
        toBeExtended.add(entry.getKey());
        if (toBeExtended.size() >= DELETE_BATCH_SIZE) {
          // Enough for one batch.
          break;
        }
      }

      if (assumeExpired.isEmpty() && toBeExtended.isEmpty() && toBeExpired.isEmpty()) {
        // Nothing to be done.
        return;
      }

      if (!assumeExpired.isEmpty()) {
        // If we didn't make the visibility deadline assume expired and no longer in flight.
        numLateDeadlines.add(nowMsSinceEpoch, assumeExpired.size());
        for (String messageId : assumeExpired) {
          inFlight.remove(messageId);
        }
      }

      if (!toBeExpired.isEmpty()) {
        // Expired messages are no longer considered in flight.
        numExpired.add(nowMsSinceEpoch, toBeExpired.size());
        for (String messageId : toBeExpired) {
          inFlight.remove(messageId);
        }
      }

      if (!toBeExtended.isEmpty()) {
        // SQS extends visibility timeout from it's notion of current time.
        // We'll try to track that on our side, but note the deadlines won't necessarily agree.
        long extensionMs = (int) ((visibilityTimeoutMs * VISIBILITY_EXTENSION_PCT) / 100L);
        long newDeadlineMsSinceEpoch = nowMsSinceEpoch + extensionMs;
        List<KV<String, String>> messages = new ArrayList<>(toBeExtended.size());

        for (String messageId : toBeExtended) {
          // Maintain increasing ack deadline order.
          InFlightState state = inFlight.remove(messageId);
          state.visibilityDeadlineMsSinceEpoch = newDeadlineMsSinceEpoch;
          inFlight.put(messageId, state);
          messages.add(KV.of(messageId, state.receiptHandle));
        }

        // BLOCKs until extended.
        extendBatch(nowMsSinceEpoch, messages, (int) (extensionMs / 1000));
      }
    }
  }

  /**
   * BLOCKING. Set the SQS visibility timeout for messages in {@code receiptHandles} to zero for
   * immediate redelivery.
   */
  void expireBatchForRedelivery(List<String> receiptHandles) throws IOException {
    List<KV<String, String>> messages =
        mapWithIndex(receiptHandles.stream(), (handle, idx) -> KV.of(Long.toString(idx), handle))
            .collect(toList());

    long nowMsSinceEpoch = now();
    extendBatch(nowMsSinceEpoch, messages, 0);
    numReleased.add(nowMsSinceEpoch, receiptHandles.size());
  }

  /**
   * BLOCKING. Extend the SQS visibility timeout for messages in {@code messages} as {@link KV} of
   * message id, receipt handle.
   */
  void extendBatch(long nowMsSinceEpoch, List<KV<String, String>> messages, int extensionSec)
      throws IOException {
    int retries = 0;

    Function<KV<String, String>, ChangeMessageVisibilityBatchRequestEntry> buildEntry =
        kv ->
            ChangeMessageVisibilityBatchRequestEntry.builder()
                .visibilityTimeout(extensionSec)
                .id(kv.getKey())
                .receiptHandle(kv.getValue())
                .build();

    Map<String, ChangeMessageVisibilityBatchRequestEntry> pendingExtends =
        messages.stream().collect(toMap(KV::getKey, buildEntry));

    while (!pendingExtends.isEmpty()) {

      if (retries >= BATCH_OPERATION_MAX_RETIRES) {
        throw new IOException(
            "Failed to extend visibility timeout for "
                + messages.size()
                + " messages after "
                + retries
                + " retries");
      }

      ChangeMessageVisibilityBatchResponse response =
          sqsClient.changeMessageVisibilityBatch(
              ChangeMessageVisibilityBatchRequest.builder()
                  .queueUrl(queueUrl())
                  .entries(pendingExtends.values())
                  .build());

      Map<Boolean, Set<String>> failures =
          response.failed().stream()
              .collect(partitioningBy(this::isHandleInvalid, mapping(e -> e.id(), toSet())));

      // Keep failed IDs only, but discard invalid (expired) receipt handles
      pendingExtends.keySet().retainAll(failures.getOrDefault(FALSE, ImmutableSet.of()));

      // Skip stats update and inFlight management if explicitly expiring messages for immediate
      // redelivery
      if (extensionSec > 0) {
        numExtendedDeadlines.add(nowMsSinceEpoch, response.successful().size());

        Set<String> invalidMsgIds = failures.getOrDefault(TRUE, ImmutableSet.of());
        if (invalidMsgIds.size() > 0) {
          // consider invalid (expired) messages no longer in flight
          numLateDeadlines.add(nowMsSinceEpoch, invalidMsgIds.size());
          for (String msgId : invalidMsgIds) {
            inFlight.remove(msgId);
          }
          LOG.warn(
              "Failed to extend visibility timeout for {} messages with expired receipt handles.",
              invalidMsgIds.size());
        }
      }

      retries += 1;
    }
  }

  @VisibleForTesting
  long getVisibilityTimeoutMs() {
    return visibilityTimeoutMs;
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
      oldestInFlight = (nowMsSinceEpoch - inFlight.get(oldestAckId).requestTimeMsSinceEpoch) + "ms";
    }

    LOG.info(
        "SQS {} has "
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
            + "{} recent deleted, "
            + "{} recent released, "
            + "{} recent expired, "
            + "{} recent message timestamp skew, "
            + "{} recent watermark skew, "
            + "{} recent late messages, "
            + "{} last reported watermark, "
            + "{} min recent read timestamp (significance = {}), "
            + "{} min recent unread timestamp (significance = {}), "
            + "{} last receive timestamp",
        queueUrl(),
        numReceived,
        messagesNotYetRead.size(),
        notYetReadBytes,
        inFlight.size(),
        oldestInFlight,
        numInFlightCheckpoints.get(),
        maxInFlightCheckpoints,
        numReadBytes.get(nowMsSinceEpoch) / (SAMPLE_PERIOD.getMillis() / 1000L),
        numReceivedRecently.get(nowMsSinceEpoch),
        numExtendedDeadlines.get(nowMsSinceEpoch),
        numLateDeadlines.get(nowMsSinceEpoch),
        numDeleted.get(nowMsSinceEpoch),
        numReleased.get(nowMsSinceEpoch),
        numExpired.get(nowMsSinceEpoch),
        messageSkew,
        watermarkSkew,
        numLateMessages.get(nowMsSinceEpoch),
        new Instant(lastWatermarkMsSinceEpoch),
        new Instant(minReadTimestampMsSinceEpoch.get(nowMsSinceEpoch)),
        minReadTimestampMsSinceEpoch.isSignificant(),
        new Instant(minUnreadTimestampMsSinceEpoch.get()),
        minUnreadTimestampMsSinceEpoch.isSignificant(),
        new Instant(lastReceivedMsSinceEpoch));

    lastLogTimestampMsSinceEpoch = nowMsSinceEpoch;
  }
}
