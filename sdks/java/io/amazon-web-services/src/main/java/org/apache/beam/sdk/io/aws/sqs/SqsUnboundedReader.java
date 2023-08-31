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
package org.apache.beam.sdk.io.aws.sqs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.MessageSystemAttributeName;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.BucketingFunction;
import org.apache.beam.sdk.util.MovingFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.EvictingQueue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class SqsUnboundedReader extends UnboundedSource.UnboundedReader<Message> {
  private static final Logger LOG = LoggerFactory.getLogger(SqsUnboundedReader.class);

  /** Request time attribute in {@link Message#getMessageAttributes()}. */
  static final String REQUEST_TIME = "requestTimeMsSinceEpoch";

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

  /** Maximum number of recent messages for calculating average message size. */
  private static final int MAX_AVG_BYTE_MESSAGES = 20;

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

  /**
   * The closed state of this {@link SqsUnboundedReader}. If true, the reader has not yet been
   * closed, and it will have a non-null value within {@link #SqsUnboundedReader}.
   */
  private AtomicBoolean active = new AtomicBoolean(true);

  /** SQS client of this reader instance. */
  private AmazonSQS sqsClient = null;

  /** The current message, or {@literal null} if none. */
  private Message current;

  /**
   * Messages we have received from SQS and not yet delivered downstream. We preserve their order.
   */
  final Queue<Message> messagesNotYetRead;

  /** Message ids of messages we have delivered downstream but not yet deleted. */
  private Set<String> safeToDeleteIds;

  /**
   * Visibility timeout, in ms, as set on subscription when we first start reading. Not updated
   * thereafter. -1 if not yet determined.
   */
  private long visibilityTimeoutMs;

  /** Byte size of undecoded elements in {@link #messagesNotYetRead}. */
  private long notYetReadBytes;

  /** Byte size of recent messages. */
  private EvictingQueue<Integer> recentMessageBytes;

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

  /** Number of recent empty receives. */
  private MovingFunction numEmptyReceives;

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

  public SqsUnboundedReader(SqsUnboundedSource source, SqsCheckpointMark sqsCheckpointMark)
      throws IOException {
    this.source = source;

    messagesNotYetRead = new ArrayDeque<>();
    safeToDeleteIds = new HashSet<>();
    inFlight = new LinkedHashMap<>();
    deletedIds = new ConcurrentLinkedQueue<>();
    visibilityTimeoutMs = -1;
    notYetReadBytes = 0;
    recentMessageBytes = EvictingQueue.create(MAX_AVG_BYTE_MESSAGES);
    minUnreadTimestampMsSinceEpoch =
        new BucketingFunction(
            SAMPLE_UPDATE.getMillis(), MIN_WATERMARK_SPREAD, MIN_WATERMARK_MESSAGES, MIN);
    minReadTimestampMsSinceEpoch = newFun(MIN);
    numEmptyReceives = newFun(SUM);
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
      long nowMsSinceEpoch = now();
      initClient();
      extendBatch(nowMsSinceEpoch, sqsCheckpointMark.notYetReadReceipts, 0);
      numReleased.add(nowMsSinceEpoch, sqsCheckpointMark.notYetReadReceipts.size());
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
        && numEmptyReceives.get(nowMsSinceEpoch) > 0
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
  public Message getCurrent() throws NoSuchElementException {
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

    return getTimestamp(current);
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
    List<String> snapshotSafeToDeleteIds = Lists.newArrayList(safeToDeleteIds);
    List<String> snapshotNotYetReadReceipts = new ArrayList<>(messagesNotYetRead.size());
    for (Message message : messagesNotYetRead) {
      snapshotNotYetReadReceipts.add(message.getReceiptHandle());
    }
    return new SqsCheckpointMark(this, snapshotSafeToDeleteIds, snapshotNotYetReadReceipts);
  }

  @Override
  public SqsUnboundedSource getCurrentSource() {
    return source;
  }

  @Override
  public long getTotalBacklogBytes() {
    long avgBytes = avgMessageBytes();
    List<String> requestAttributes =
        Collections.singletonList(QueueAttributeName.ApproximateNumberOfMessages.toString());
    Map<String, String> queueAttributes =
        sqsClient
            .getQueueAttributes(source.getRead().queueUrl(), requestAttributes)
            .getAttributes();
    long numMessages =
        Long.parseLong(
            queueAttributes.get(QueueAttributeName.ApproximateNumberOfMessages.toString()));

    // No messages consumed for estimating average message size
    if (avgBytes == -1 && numMessages > 0) {
      return BACKLOG_UNKNOWN;
    } else {
      return numMessages * avgBytes;
    }
  }

  @Override
  public boolean start() throws IOException {
    initClient();
    visibilityTimeoutMs =
        Integer.parseInt(
                sqsClient
                    .getQueueAttributes(
                        new GetQueueAttributesRequest(source.getRead().queueUrl())
                            .withAttributeNames("VisibilityTimeout"))
                    .getAttributes()
                    .get("VisibilityTimeout"))
            * 1000L;
    return advance();
  }

  private void initClient() {
    if (sqsClient == null) {
      sqsClient =
          AmazonSQSClientBuilder.standard()
              .withClientConfiguration(source.getSqsConfiguration().getClientConfiguration())
              .withCredentials(source.getSqsConfiguration().getAwsCredentialsProvider())
              .withRegion(source.getSqsConfiguration().getAwsRegion())
              .build();
    }
  }

  @Override
  public boolean advance() throws IOException {
    // Emit stats.
    stats();

    if (current != null) {
      // Current is consumed. It can no longer contribute to holding back the watermark.
      minUnreadTimestampMsSinceEpoch.remove(getRequestTimeMsSinceEpoch(current));
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
    notYetReadBytes -= current.getBody().getBytes(UTF_8).length;
    checkState(notYetReadBytes >= 0);
    long nowMsSinceEpoch = now();
    numReadBytes.add(nowMsSinceEpoch, current.getBody().getBytes(UTF_8).length);
    recentMessageBytes.add(current.getBody().getBytes(UTF_8).length);
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
   * <p>Marks this {@link SqsUnboundedReader} as no longer active. The {@link AmazonSQS} continue to
   * exist and be active beyond the life of this call if there are any in-flight checkpoints. When
   * no in-flight checkpoints remain, the reader will be closed.
   */
  @Override
  public void close() throws IOException {
    active.set(false);
    maybeCloseClient();
  }

  /**
   * Close this reader's underlying {@link AmazonSQS} if the reader has been closed and there are no
   * outstanding checkpoints.
   */
  void maybeCloseClient() throws IOException {
    if (!active.get() && numInFlightCheckpoints.get() == 0) {
      // The reader has been closed and it has no more outstanding checkpoints. The client
      // must be closed so it doesn't leak
      if (sqsClient != null) {
        sqsClient.shutdown();
      }
    }
  }

  /** delete the provided {@code messageIds} from SQS. */
  void delete(List<String> messageIds) throws IOException {
    AtomicInteger counter = new AtomicInteger();
    for (List<String> messageList :
        messageIds.stream()
            .collect(groupingBy(x -> counter.getAndIncrement() / DELETE_BATCH_SIZE))
            .values()) {
      deleteBatch(messageList);
    }
  }

  /**
   * delete the provided {@code messageIds} from SQS, blocking until all of the messages are
   * deleted.
   *
   * <p>CAUTION: May be invoked from a separate thread.
   *
   * <p>CAUTION: Retains {@code messageIds}.
   */
  private void deleteBatch(List<String> messageIds) throws IOException {
    int retries = 0;
    List<String> errorMessages = new ArrayList<>();
    Map<String, String> pendingReceipts =
        IntStream.range(0, messageIds.size())
            .boxed()
            .filter(i -> inFlight.containsKey(messageIds.get(i)))
            .collect(toMap(Object::toString, i -> inFlight.get(messageIds.get(i)).receiptHandle));

    while (!pendingReceipts.isEmpty()) {

      if (retries >= BATCH_OPERATION_MAX_RETIRES) {
        throw new IOException(
            "Failed to delete "
                + pendingReceipts.size()
                + " messages after "
                + retries
                + " retries: "
                + String.join(", ", errorMessages));
      }

      List<DeleteMessageBatchRequestEntry> entries =
          pendingReceipts.entrySet().stream()
              .map(r -> new DeleteMessageBatchRequestEntry(r.getKey(), r.getValue()))
              .collect(Collectors.toList());

      DeleteMessageBatchResult result =
          sqsClient.deleteMessageBatch(source.getRead().queueUrl(), entries);

      // Retry errors except invalid handles
      Set<BatchResultErrorEntry> retryErrors =
          result.getFailed().stream()
              .filter(e -> !e.getCode().equals("ReceiptHandleIsInvalid"))
              .collect(Collectors.toSet());

      pendingReceipts
          .keySet()
          .retainAll(
              retryErrors.stream().map(BatchResultErrorEntry::getId).collect(Collectors.toSet()));

      errorMessages =
          retryErrors.stream().map(BatchResultErrorEntry::getMessage).collect(Collectors.toList());

      retries += 1;
    }
    deletedIds.add(messageIds);
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

  /** BLOCKING Fetch another batch of messages from SQS. */
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
        new ReceiveMessageRequest(source.getRead().queueUrl());

    receiveMessageRequest.setMaxNumberOfMessages(MAX_NUMBER_OF_MESSAGES);
    receiveMessageRequest.setAttributeNames(
        Arrays.asList(MessageSystemAttributeName.SentTimestamp.toString()));
    final ReceiveMessageResult receiveMessageResult =
        sqsClient.receiveMessage(receiveMessageRequest);

    final List<Message> messages = receiveMessageResult.getMessages();

    if (messages == null || messages.isEmpty()) {
      numEmptyReceives.add(requestTimeMsSinceEpoch, 1L);
      return;
    }

    lastReceivedMsSinceEpoch = requestTimeMsSinceEpoch;

    // Capture the received messages.
    for (Message message : messages) {
      // Keep request time as message attribute for later usage
      MessageAttributeValue reqTime =
          new MessageAttributeValue().withStringValue(Long.toString(requestTimeMsSinceEpoch));
      message.setMessageAttributes(ImmutableMap.of(REQUEST_TIME, reqTime));
      messagesNotYetRead.add(message);
      notYetReadBytes += message.getBody().getBytes(UTF_8).length;
      inFlight.put(
          message.getMessageId(),
          new InFlightState(
              message.getReceiptHandle(), requestTimeMsSinceEpoch, deadlineMsSinceEpoch));
      numReceived++;
      numReceivedRecently.add(requestTimeMsSinceEpoch, 1L);

      long timestampMillis = getTimestamp(message).getMillis();
      minReceivedTimestampMsSinceEpoch.add(requestTimeMsSinceEpoch, timestampMillis);
      maxReceivedTimestampMsSinceEpoch.add(requestTimeMsSinceEpoch, timestampMillis);
      minUnreadTimestampMsSinceEpoch.add(requestTimeMsSinceEpoch, timestampMillis);
    }
  }

  /** Return the current time, in ms since epoch. */
  long now() {
    return System.currentTimeMillis();
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
        for (String messageId : toBeExtended) {
          // Maintain increasing ack deadline order.
          String receiptHandle = inFlight.get(messageId).receiptHandle;
          InFlightState state = inFlight.remove(messageId);

          inFlight.put(
              messageId,
              new InFlightState(
                  receiptHandle, state.requestTimeMsSinceEpoch, newDeadlineMsSinceEpoch));
        }
        List<String> receiptHandles =
            toBeExtended.stream()
                .map(inFlight::get)
                .filter(Objects::nonNull) // get rid of null values
                .map(m -> m.receiptHandle)
                .collect(Collectors.toList());
        // BLOCKs until extended.
        extendBatch(nowMsSinceEpoch, receiptHandles, (int) (extensionMs / 1000));
      }
    }
  }

  /**
   * BLOCKING Extend the visibility timeout for messages from SQS with the given {@code
   * receiptHandles}.
   */
  void extendBatch(long nowMsSinceEpoch, List<String> receiptHandles, int extensionSec)
      throws IOException {
    int retries = 0;
    int numMessages = receiptHandles.size();
    Map<String, String> pendingReceipts =
        IntStream.range(0, receiptHandles.size())
            .boxed()
            .collect(toMap(Object::toString, receiptHandles::get));
    List<String> errorMessages = new ArrayList<>();

    while (!pendingReceipts.isEmpty()) {

      if (retries >= BATCH_OPERATION_MAX_RETIRES) {
        throw new IOException(
            "Failed to extend visibility timeout for "
                + pendingReceipts.size()
                + " messages after "
                + retries
                + " retries: "
                + String.join(", ", errorMessages));
      }

      List<ChangeMessageVisibilityBatchRequestEntry> entries =
          pendingReceipts.entrySet().stream()
              .map(
                  r ->
                      new ChangeMessageVisibilityBatchRequestEntry(r.getKey(), r.getValue())
                          .withVisibilityTimeout(extensionSec))
              .collect(Collectors.toList());

      ChangeMessageVisibilityBatchResult result =
          sqsClient.changeMessageVisibilityBatch(source.getRead().queueUrl(), entries);

      // Retry errors except invalid handles
      Set<BatchResultErrorEntry> retryErrors =
          result.getFailed().stream()
              .filter(e -> !e.getCode().equals("ReceiptHandleIsInvalid"))
              .collect(Collectors.toSet());

      pendingReceipts
          .keySet()
          .retainAll(
              retryErrors.stream().map(BatchResultErrorEntry::getId).collect(Collectors.toSet()));

      errorMessages =
          retryErrors.stream().map(BatchResultErrorEntry::getMessage).collect(Collectors.toList());

      retries += 1;
    }
    numExtendedDeadlines.add(nowMsSinceEpoch, numMessages);
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

    LOG.debug(
        "SQS {} has "
            + "{} received messages, "
            + "{} current unread messages, "
            + "{} current unread bytes, "
            + "{} current in-flight msgs, "
            + "{} oldest in-flight, "
            + "{} current in-flight checkpoints, "
            + "{} max in-flight checkpoints, "
            + "{} bytes in backlog, "
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
            + "{} last reported watermark",
        source.getRead().queueUrl(),
        numReceived,
        messagesNotYetRead.size(),
        notYetReadBytes,
        inFlight.size(),
        oldestInFlight,
        numInFlightCheckpoints.get(),
        maxInFlightCheckpoints,
        getTotalBacklogBytes(),
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
        new Instant(lastWatermarkMsSinceEpoch));

    lastLogTimestampMsSinceEpoch = nowMsSinceEpoch;
  }

  /** Return the average byte size of all message read. -1 if no message read yet */
  private long avgMessageBytes() {
    if (!recentMessageBytes.isEmpty()) {
      return (long) recentMessageBytes.stream().mapToDouble(s -> s).average().getAsDouble();
    } else {
      return -1L;
    }
  }

  /** Extract the timestamp from the given {@code message}. */
  private Instant getTimestamp(final Message message) {
    return new Instant(
        Long.parseLong(
            message.getAttributes().get(MessageSystemAttributeName.SentTimestamp.toString())));
  }

  /** Extract the request timestamp from the given {@code message}. */
  private Long getRequestTimeMsSinceEpoch(final Message message) {
    return Long.parseLong(message.getMessageAttributes().get(REQUEST_TIME).getStringValue());
  }
}
