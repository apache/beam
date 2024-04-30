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
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.io.kafka.KafkaCheckpointMark.PartitionMark;
import org.apache.beam.sdk.io.kafka.KafkaIO.Read;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.SourceMetrics;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Closeables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded reader to read from Kafka. Each reader consumes messages from one or more Kafka
 * partitions. See {@link KafkaIO} for user visible documentation and example usage.
 */
class KafkaUnboundedReader<K, V> extends UnboundedReader<KafkaRecord<K, V>> {

  ///////////////////// Reader API ////////////////////////////////////////////////////////////
  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  public boolean start() throws IOException {
    Read<K, V> spec = source.getSpec();
    final Consumer<byte[], byte[]> consumer =
        spec.getConsumerFactoryFn().apply(spec.getConsumerConfig());
    this.consumer = consumer;
    List<TopicPartition> topicPartitions =
        Preconditions.checkStateNotNull(spec.getTopicPartitions());
    ConsumerSpEL.evaluateAssign(consumer, topicPartitions);

    keyDeserializerInstance =
        Preconditions.checkStateNotNull(spec.getKeyDeserializerProvider())
            .getDeserializer(spec.getConsumerConfig(), true);
    valueDeserializerInstance =
        Preconditions.checkStateNotNull(spec.getValueDeserializerProvider())
            .getDeserializer(spec.getConsumerConfig(), false);

    // Seek to start offset for each partition. This is the first interaction with the server.
    // Unfortunately it can block forever in case of network issues like incorrect ACLs.
    // Initialize partition in a separate thread and cancel it if takes longer than a minute.
    // This problem of blocking API calls to kafka is solved in higher versions of kafka
    // client by `KIP-266`
    for (final PartitionState<K, V> pState : partitionStates) {
      Future<?> future = consumerPollThread.submit(() -> setupInitialOffset(pState));
      try {
        Duration timeout = resolveDefaultApiTimeout(spec);
        future.get(timeout.getMillis(), TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        consumer.wakeup(); // This unblocks consumer stuck on network I/O.
        // Likely reason : Kafka servers are configured to advertise internal ips, but
        // those ips are not accessible from workers outside.
        String msg =
            String.format(
                "%s: Timeout while initializing partition '%s'. "
                    + "Kafka client may not be able to connect to servers.",
                this, pState.topicPartition);
        LOG.error("{}", msg);
        throw new IOException(msg);
      } catch (Exception e) {
        throw new IOException(e);
      }
      LOG.info(
          "{}: reading from {} starting at offset {}",
          name,
          pState.topicPartition,
          pState.nextOffset);
    }

    // Start consumer read loop.
    // Note that consumer is not thread safe, should not be accessed out side consumerPollLoop().
    consumerPollThread.submit(this::consumerPollLoop);

    // offsetConsumer setup :
    Map<String, Object> offsetConsumerConfig =
        KafkaIOUtils.getOffsetConsumerConfig(
            name, spec.getOffsetConsumerConfig(), spec.getConsumerConfig());

    offsetConsumer = spec.getConsumerFactoryFn().apply(offsetConsumerConfig);
    ConsumerSpEL.evaluateAssign(offsetConsumer, topicPartitions);

    // Fetch offsets once before running periodically.
    updateLatestOffsets();

    offsetFetcherThread.scheduleAtFixedRate(
        this::updateLatestOffsets, 0, OFFSET_UPDATE_INTERVAL_SECONDS, TimeUnit.SECONDS);

    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    /* Read first record (if any). we need to loop here because :
     *  - (a) some records initially need to be skipped if they are before consumedOffset
     *  - (b) if curBatch is empty, we want to fetch next batch and then advance.
     *  - (c) curBatch is an iterator of iterators. we interleave the records from each.
     *        curBatch.next() might return an empty iterator.
     */
    while (true) {
      if (curBatch.hasNext()) {
        PartitionState<K, V> pState = curBatch.next();

        if (!pState.recordIter.hasNext()) { // -- (c)
          pState.recordIter = Collections.emptyIterator(); // drop ref
          curBatch.remove();
          continue;
        }

        elementsRead.inc();
        elementsReadBySplit.inc();

        ConsumerRecord<byte[], byte[]> rawRecord = pState.recordIter.next();
        long expected = pState.nextOffset;
        long offset = rawRecord.offset();

        if (offset < expected) { // -- (a)
          // this can happen when compression is enabled in Kafka (seems to be fixed in 0.10)
          // should we check if the offset is way off from consumedOffset (say > 1M)?
          LOG.warn(
              "{}: ignoring already consumed offset {} for {}",
              this,
              offset,
              pState.topicPartition);
          continue;
        }

        long offsetGap = offset - expected; // could be > 0 when Kafka log compaction is enabled.

        if (curRecord == null) {
          LOG.info("{}: first record offset {}", name, offset);
          offsetGap = 0;
        }

        // Apply user deserializers. User deserializers might throw, which will be propagated up
        // and 'curRecord' remains unchanged. The runner should close this reader.
        // TODO: write records that can't be deserialized to a "dead-letter" additional output.
        Deserializer<K> keyDeserializerInstance =
            Preconditions.checkStateNotNull(this.keyDeserializerInstance);
        Deserializer<V> valueDeserializerInstance =
            Preconditions.checkStateNotNull(this.valueDeserializerInstance);
        KafkaRecord<K, V> record =
            new KafkaRecord<>(
                rawRecord.topic(),
                rawRecord.partition(),
                rawRecord.offset(),
                ConsumerSpEL.getRecordTimestamp(rawRecord),
                ConsumerSpEL.getRecordTimestampType(rawRecord),
                ConsumerSpEL.hasHeaders() ? rawRecord.headers() : null,
                ConsumerSpEL.deserializeKey(keyDeserializerInstance, rawRecord),
                ConsumerSpEL.deserializeValue(valueDeserializerInstance, rawRecord));

        curTimestamp =
            pState.timestampPolicy.getTimestampForRecord(pState.mkTimestampPolicyContext(), record);
        curRecord = record;

        int recordSize =
            (rawRecord.key() == null ? 0 : rawRecord.key().length)
                + (rawRecord.value() == null ? 0 : rawRecord.value().length);
        pState.recordConsumed(offset, recordSize, offsetGap);
        bytesRead.inc(recordSize);
        bytesReadBySplit.inc(recordSize);

        Distribution rawSizes =
            Metrics.distribution(
                METRIC_NAMESPACE, RAW_SIZE_METRIC_PREFIX + pState.topicPartition.toString());
        rawSizes.update(recordSize);

        for (Map.Entry<String, Long> backlogSplit : perPartitionBacklogMetrics.entrySet()) {
          backlogBytesOfSplit.set(backlogSplit.getValue());
        }
        return true;

      } else { // -- (b)
        nextBatch();

        if (!curBatch.hasNext()) {
          return false;
        }
      }
    }
  }

  @Override
  public Instant getWatermark() {

    SerializableFunction<KafkaRecord<K, V>, Instant> watermarkFn =
        source.getSpec().getWatermarkFn();
    if (watermarkFn != null) {
      // Support old API which requires a KafkaRecord to invoke watermarkFn.
      if (curRecord == null) {
        LOG.debug("{}: getWatermark() : no records have been read yet.", name);
        return initialWatermark;
      }
      return watermarkFn.apply(curRecord);
    }

    // Return minimum watermark among partitions.
    return partitionStates.stream()
        .map(PartitionState::updateAndGetWatermark)
        .min(Comparator.naturalOrder())
        .get();
  }

  @Override
  public CheckpointMark getCheckpointMark() {
    reportBacklog();
    return new KafkaCheckpointMark(
        partitionStates.stream()
            .map(
                p ->
                    new PartitionMark(
                        p.topicPartition.topic(),
                        p.topicPartition.partition(),
                        p.nextOffset,
                        p.lastWatermark.getMillis()))
            .collect(Collectors.toList()),
        source.getSpec().isCommitOffsetsInFinalizeEnabled() ? Optional.of(this) : Optional.empty());
  }

  @Override
  public UnboundedSource<KafkaRecord<K, V>, ?> getCurrentSource() {
    return source;
  }

  @Override
  public KafkaRecord<K, V> getCurrent() throws NoSuchElementException {
    // should we delay updating consumed offset till this point? Mostly not required.
    if (curRecord == null) {
      throw new NoSuchElementException();
    }
    return curRecord;
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    if (curTimestamp == null) {
      throw new NoSuchElementException();
    }
    return curTimestamp;
  }

  @Override
  public long getSplitBacklogBytes() {
    long backlogBytes = 0;

    for (PartitionState<K, V> p : partitionStates) {
      long pBacklog = p.approxBacklogInBytes();
      if (pBacklog == UnboundedReader.BACKLOG_UNKNOWN) {
        return UnboundedReader.BACKLOG_UNKNOWN;
      }
      backlogBytes += pBacklog;
    }

    return backlogBytes;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUnboundedReader.class);

  @VisibleForTesting static final String METRIC_NAMESPACE = "KafkaIOReader";

  @VisibleForTesting static final String RAW_SIZE_METRIC_PREFIX = "rawSize/";

  @VisibleForTesting
  static final String CHECKPOINT_MARK_COMMITS_ENQUEUED_METRIC = "checkpointMarkCommitsEnqueued";

  private static final String CHECKPOINT_MARK_COMMITS_SKIPPED_METRIC =
      "checkpointMarkCommitsSkipped";

  private final KafkaUnboundedSource<K, V> source;
  private final String name;
  private @Nullable Consumer<byte[], byte[]> consumer = null;
  private final List<PartitionState<K, V>> partitionStates;
  private @Nullable KafkaRecord<K, V> curRecord = null;
  private @Nullable Instant curTimestamp = null;
  private Iterator<PartitionState<K, V>> curBatch = Collections.emptyIterator();

  private @Nullable Deserializer<K> keyDeserializerInstance = null;
  private @Nullable Deserializer<V> valueDeserializerInstance = null;

  private final Counter elementsRead = SourceMetrics.elementsRead();
  private final Counter bytesRead = SourceMetrics.bytesRead();
  private final Counter elementsReadBySplit;
  private final Counter bytesReadBySplit;
  private final Gauge backlogBytesOfSplit;
  private final Gauge backlogElementsOfSplit;
  private HashMap<String, Long> perPartitionBacklogMetrics = new HashMap<String, Long>();;
  private final Counter checkpointMarkCommitsEnqueued =
      Metrics.counter(METRIC_NAMESPACE, CHECKPOINT_MARK_COMMITS_ENQUEUED_METRIC);
  // Checkpoint marks skipped in favor of newer mark (only the latest needs to be committed).
  private final Counter checkpointMarkCommitsSkipped =
      Metrics.counter(METRIC_NAMESPACE, CHECKPOINT_MARK_COMMITS_SKIPPED_METRIC);

  /**
   * The poll timeout while reading records from Kafka. If option to commit reader offsets in to
   * Kafka in {@link KafkaCheckpointMark#finalizeCheckpoint()} is enabled, it would be delayed until
   * this poll returns. It should be reasonably low as a result. At the same time it probably can't
   * be very low like 10 millis, I am not sure how it affects when the latency is high. Probably
   * good to experiment. Often multiple marks would be finalized in a batch, it reduce finalization
   * overhead to wait a short while and finalize only the last checkpoint mark.
   */
  private static final Duration KAFKA_POLL_TIMEOUT = Duration.millis(1000);

  private Duration recordsDequeuePollTimeout;
  private static final Duration RECORDS_DEQUEUE_POLL_TIMEOUT_MIN = Duration.millis(1);
  private static final Duration RECORDS_DEQUEUE_POLL_TIMEOUT_MAX = Duration.millis(20);
  private static final Duration RECORDS_ENQUEUE_POLL_TIMEOUT = Duration.millis(100);

  // Use a separate thread to read Kafka messages. Kafka Consumer does all its work including
  // network I/O inside poll(). Polling only inside #advance(), especially with a small timeout
  // like 100 milliseconds does not work well. This along with large receive buffer for
  // consumer achieved best throughput in tests (see `defaultConsumerProperties`).
  private final ExecutorService consumerPollThread =
      Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("KafkaConsumerPoll-thread")
              .build());
  private AtomicReference<Exception> consumerPollException = new AtomicReference<>();
  private final SynchronousQueue<ConsumerRecords<byte[], byte[]>> availableRecordsQueue =
      new SynchronousQueue<>();
  private AtomicReference<@Nullable KafkaCheckpointMark> finalizedCheckpointMark =
      new AtomicReference<>();
  private AtomicBoolean closed = new AtomicBoolean(false);

  // Backlog support :
  // Kafka consumer does not have an API to fetch latest offset for topic. We need to seekToEnd()
  // then look at position(). Use another consumer to do this so that the primary consumer does
  // not need to be interrupted. The latest offsets are fetched periodically on a thread. This is
  // still a bit of a hack, but so far there haven't been any issues reported by the users.
  private @Nullable Consumer<byte[], byte[]> offsetConsumer = null;
  private final ScheduledExecutorService offsetFetcherThread =
      Executors.newSingleThreadScheduledExecutor();
  private static final int OFFSET_UPDATE_INTERVAL_SECONDS = 1;

  private static final long UNINITIALIZED_OFFSET = -1;

  /** watermark before any records have been read. */
  private static Instant initialWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  @Override
  public String toString() {
    return name;
  }

  static class TimestampPolicyContext extends TimestampPolicy.PartitionContext {

    private final long messageBacklog;
    private final Instant backlogCheckTime;

    TimestampPolicyContext(long messageBacklog, Instant backlogCheckTime) {
      this.messageBacklog = messageBacklog;
      this.backlogCheckTime = backlogCheckTime;
    }

    @Override
    public long getMessageBacklog() {
      return messageBacklog;
    }

    @Override
    public Instant getBacklogCheckTime() {
      return backlogCheckTime;
    }
  }

  // maintains state of each assigned partition (buffered records, consumed offset, etc)
  private static class PartitionState<K, V> {
    private final TopicPartition topicPartition;
    private long nextOffset;
    private long latestOffset;
    private Instant latestOffsetFetchTime;
    private Instant lastWatermark; // As returned by timestampPolicy
    private final TimestampPolicy<K, V> timestampPolicy;

    private Iterator<ConsumerRecord<byte[], byte[]>> recordIter = Collections.emptyIterator();

    private KafkaIOUtils.MovingAvg avgRecordSize = new KafkaIOUtils.MovingAvg();
    private KafkaIOUtils.MovingAvg avgOffsetGap =
        new KafkaIOUtils.MovingAvg(); // > 0 only when log compaction is enabled.

    PartitionState(
        TopicPartition partition, long nextOffset, TimestampPolicy<K, V> timestampPolicy) {
      this.topicPartition = partition;
      this.nextOffset = nextOffset;
      this.latestOffset = UNINITIALIZED_OFFSET;
      this.latestOffsetFetchTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
      this.lastWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
      this.timestampPolicy = timestampPolicy;
    }

    // Update consumedOffset, avgRecordSize, and avgOffsetGap
    void recordConsumed(long offset, int size, long offsetGap) {
      nextOffset = offset + 1;

      // This is always updated from single thread. Probably not worth making atomic.
      avgRecordSize.update(size);
      avgOffsetGap.update(offsetGap);
    }

    synchronized void setLatestOffset(long latestOffset, Instant fetchTime) {
      this.latestOffset = latestOffset;
      this.latestOffsetFetchTime = fetchTime;
      LOG.debug(
          "{}: latest offset update for {} : {} (consumer offset {}, avg record size {})",
          this,
          topicPartition,
          latestOffset,
          nextOffset,
          avgRecordSize);
    }

    synchronized long approxBacklogInBytes() {
      // Note that is an estimate of uncompressed backlog.
      long backlogMessageCount = backlogMessageCount();
      if (backlogMessageCount == UnboundedReader.BACKLOG_UNKNOWN) {
        return UnboundedReader.BACKLOG_UNKNOWN;
      }
      return (long) (backlogMessageCount * avgRecordSize.get());
    }

    synchronized long backlogMessageCount() {
      if (latestOffset < 0 || nextOffset < 0) {
        return UnboundedReader.BACKLOG_UNKNOWN;
      }
      double remaining = (latestOffset - nextOffset) / (1 + avgOffsetGap.get());
      return Math.max(0, (long) Math.ceil(remaining));
    }

    synchronized TimestampPolicyContext mkTimestampPolicyContext() {
      return new TimestampPolicyContext(backlogMessageCount(), latestOffsetFetchTime);
    }

    Instant updateAndGetWatermark() {
      lastWatermark = timestampPolicy.getWatermark(mkTimestampPolicyContext());
      return lastWatermark;
    }

    String name() {
      return this.topicPartition.toString();
    }
  }

  KafkaUnboundedReader(
      KafkaUnboundedSource<K, V> source, @Nullable KafkaCheckpointMark checkpointMark) {
    this.source = source;
    this.name = "Reader-" + source.getId();

    List<TopicPartition> partitions =
        Preconditions.checkArgumentNotNull(source.getSpec().getTopicPartitions());
    List<PartitionState<K, V>> states = new ArrayList<>(partitions.size());

    if (checkpointMark != null) {
      checkState(
          checkpointMark.getPartitions().size() == partitions.size(),
          "checkPointMark and assignedPartitions should match");
    }

    for (int i = 0; i < partitions.size(); i++) {
      TopicPartition tp = partitions.get(i);
      long nextOffset = UNINITIALIZED_OFFSET;
      Optional<Instant> prevWatermark = Optional.empty();

      if (checkpointMark != null) {
        // Verify that assigned and check-pointed partitions match exactly and set next offset.

        PartitionMark ckptMark = checkpointMark.getPartitions().get(i);

        TopicPartition partition = new TopicPartition(ckptMark.getTopic(), ckptMark.getPartition());
        checkState(
            partition.equals(tp),
            "checkpointed partition %s and assigned partition %s don't match",
            partition,
            tp);
        nextOffset = ckptMark.getNextOffset();
        prevWatermark = Optional.of(new Instant(ckptMark.getWatermarkMillis()));
      }

      PartitionState<K, V> state =
          new PartitionState<K, V>(
              tp,
              nextOffset,
              source
                  .getSpec()
                  .getTimestampPolicyFactory()
                  .createTimestampPolicy(tp, prevWatermark));
      states.add(state);
      perPartitionBacklogMetrics.put(state.name(), 0L);
    }

    partitionStates = ImmutableList.copyOf(states);

    String splitId = String.valueOf(source.getId());
    elementsReadBySplit = SourceMetrics.elementsReadBySplit(splitId);
    bytesReadBySplit = SourceMetrics.bytesReadBySplit(splitId);
    backlogBytesOfSplit = SourceMetrics.backlogBytesOfSplit(splitId);
    backlogElementsOfSplit = SourceMetrics.backlogElementsOfSplit(splitId);
    recordsDequeuePollTimeout = Duration.millis(10);
  }

  private void consumerPollLoop() {
    // Read in a loop and enqueue the batch of records, if any, to availableRecordsQueue.
    Consumer<byte[], byte[]> consumer = Preconditions.checkStateNotNull(this.consumer);

    try {
      ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
      while (!closed.get()) {
        try {
          if (records.isEmpty()) {
            records = consumer.poll(KAFKA_POLL_TIMEOUT.getMillis());
          } else if (availableRecordsQueue.offer(
              records, RECORDS_ENQUEUE_POLL_TIMEOUT.getMillis(), TimeUnit.MILLISECONDS)) {
            records = ConsumerRecords.empty();
          }

          commitCheckpointMark();
        } catch (InterruptedException e) {
          LOG.warn("{}: consumer thread is interrupted", this, e); // not expected
          break;
        } catch (WakeupException e) {
          break;
        }
      }
      LOG.info("{}: Returning from consumer pool loop", this);
    } catch (Exception e) { // mostly an unrecoverable KafkaException.
      LOG.error("{}: Exception while reading from Kafka", this, e);
      consumerPollException.set(e);
      throw e;
    }
  }

  private void commitCheckpointMark() {
    KafkaCheckpointMark checkpointMark = finalizedCheckpointMark.getAndSet(null);

    if (checkpointMark != null) {
      LOG.debug("{}: Committing finalized checkpoint {}", this, checkpointMark);
      Consumer<byte[], byte[]> consumer = Preconditions.checkStateNotNull(this.consumer);

      consumer.commitSync(
          checkpointMark.getPartitions().stream()
              .filter(p -> p.getNextOffset() != UNINITIALIZED_OFFSET)
              .collect(
                  Collectors.toMap(
                      p -> new TopicPartition(p.getTopic(), p.getPartition()),
                      p -> new OffsetAndMetadata(p.getNextOffset()))));
    }
  }

  /**
   * Enqueue checkpoint mark to be committed to Kafka. This does not block until it is committed.
   * There could be a delay of up to KAFKA_POLL_TIMEOUT (1 second). Any checkpoint mark enqueued
   * earlier is dropped in favor of this checkpoint mark. Documentation for {@link
   * CheckpointMark#finalizeCheckpoint()} says these are finalized in order. Only the latest offsets
   * need to be committed.
   */
  void finalizeCheckpointMarkAsync(KafkaCheckpointMark checkpointMark) {
    if (finalizedCheckpointMark.getAndSet(checkpointMark) != null) {
      checkpointMarkCommitsSkipped.inc();
    }
    checkpointMarkCommitsEnqueued.inc();
  }

  private void nextBatch() throws IOException {
    curBatch = Collections.emptyIterator();

    ConsumerRecords<byte[], byte[]> records;
    try {
      // poll available records, wait (if necessary) up to the specified timeout.
      records =
          availableRecordsQueue.poll(recordsDequeuePollTimeout.getMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("{}: Unexpected", this, e);
      return;
    }

    if (records == null) {
      // Check if the poll thread failed with an exception.
      if (consumerPollException.get() != null) {
        throw new IOException("Exception while reading from Kafka", consumerPollException.get());
      }
      if (recordsDequeuePollTimeout.isLongerThan(RECORDS_DEQUEUE_POLL_TIMEOUT_MIN)) {
        recordsDequeuePollTimeout = recordsDequeuePollTimeout.minus(Duration.millis(1));
        LOG.debug("Reducing poll timeout for reader to " + recordsDequeuePollTimeout.getMillis());
      }
      return;
    }

    if (recordsDequeuePollTimeout.isShorterThan(RECORDS_DEQUEUE_POLL_TIMEOUT_MAX)) {
      recordsDequeuePollTimeout = recordsDequeuePollTimeout.plus(Duration.millis(1));
      LOG.debug("Increasing poll timeout for reader to " + recordsDequeuePollTimeout.getMillis());
      LOG.debug("Record count: " + records.count());
    }

    partitionStates.forEach(p -> p.recordIter = records.records(p.topicPartition).iterator());

    // cycle through the partitions in order to interleave records from each.
    curBatch = Iterators.cycle(new ArrayList<>(partitionStates));
  }

  private void setupInitialOffset(PartitionState<K, V> pState) {
    Read<K, V> spec = source.getSpec();
    Consumer<byte[], byte[]> consumer = Preconditions.checkStateNotNull(this.consumer);

    if (pState.nextOffset != UNINITIALIZED_OFFSET) {
      consumer.seek(pState.topicPartition, pState.nextOffset);
    } else {
      // nextOffset is uninitialized here, meaning start reading from latest record as of now
      // ('latest' is the default, and is configurable) or 'look up offset by startReadTime.
      // Remember the current position without waiting until the first record is read. This
      // ensures checkpoint is accurate even if the reader is closed before reading any records.
      Instant startReadTime = spec.getStartReadTime();
      if (startReadTime != null) {
        pState.nextOffset =
            ConsumerSpEL.offsetForTime(consumer, pState.topicPartition, startReadTime);
        consumer.seek(pState.topicPartition, pState.nextOffset);
      } else {
        pState.nextOffset = consumer.position(pState.topicPartition);
      }
    }
  }

  // Update latest offset for each partition.
  // Called from setupInitialOffset() at the start and then periodically from offsetFetcher thread.
  private void updateLatestOffsets() {
    Consumer<byte[], byte[]> offsetConsumer = Preconditions.checkStateNotNull(this.offsetConsumer);
    for (PartitionState<K, V> p : partitionStates) {
      try {
        Instant fetchTime = Instant.now();
        ConsumerSpEL.evaluateSeek2End(offsetConsumer, p.topicPartition);
        long offset = offsetConsumer.position(p.topicPartition);
        p.setLatestOffset(offset, fetchTime);
      } catch (Exception e) {
        if (closed.get()) { // Ignore the exception if the reader is closed.
          break;
        }
        LOG.warn(
            "{}: exception while fetching latest offset for partition {}. will be retried.",
            this,
            p.topicPartition,
            e);
        // Don't update the latest offset.
      }
    }

    LOG.debug("{}:  backlog {}", this, getSplitBacklogBytes());
  }

  private void reportBacklog() {
    long splitBacklogBytes = getSplitBacklogBytes();
    if (splitBacklogBytes < 0) {
      splitBacklogBytes = UnboundedReader.BACKLOG_UNKNOWN;
    }
    backlogBytesOfSplit.set(splitBacklogBytes);
    long splitBacklogMessages = getSplitBacklogMessageCount();
    if (splitBacklogMessages < 0) {
      splitBacklogMessages = UnboundedReader.BACKLOG_UNKNOWN;
    }
    backlogElementsOfSplit.set(splitBacklogMessages);
  }

  private long getSplitBacklogMessageCount() {
    long backlogCount = 0;

    for (PartitionState<K, V> p : partitionStates) {
      long pBacklog = p.backlogMessageCount();
      if (pBacklog == UnboundedReader.BACKLOG_UNKNOWN) {
        return UnboundedReader.BACKLOG_UNKNOWN;
      }
      perPartitionBacklogMetrics.put(p.name(), pBacklog);
      backlogCount += pBacklog;
    }

    return backlogCount;
  }

  @Override
  public void close() throws IOException {
    closed.set(true);
    consumerPollThread.shutdown();
    offsetFetcherThread.shutdown();

    boolean isShutdown = false;

    // Wait for threads to shutdown. Trying this as a loop to handle a tiny race where poll thread
    // might block to enqueue right after availableRecordsQueue.poll() below.
    while (!isShutdown) {

      if (consumer != null) {
        consumer.wakeup();
      }
      if (offsetConsumer != null) {
        offsetConsumer.wakeup();
      }
      availableRecordsQueue.poll(); // drain unread batch, this unblocks consumer thread.
      try {
        isShutdown =
            consumerPollThread.awaitTermination(10, TimeUnit.SECONDS)
                && offsetFetcherThread.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e); // not expected
      }

      if (!isShutdown) {
        LOG.warn("An internal thread is taking a long time to shutdown. will retry.");
      }
    }

    // Commit any pending finalized checkpoint before shutdown.
    commitCheckpointMark();

    Closeables.close(keyDeserializerInstance, true);
    Closeables.close(valueDeserializerInstance, true);

    Closeables.close(offsetConsumer, true);
    Closeables.close(consumer, true);
  }

  @VisibleForTesting
  static Duration resolveDefaultApiTimeout(Read<?, ?> spec) {

    // KIP-266 - let's allow to configure timeout in consumer settings. This is supported in
    // higher versions of kafka client. We allow users to set this timeout and it will be
    // respected
    // in all places where Beam's KafkaIO handles possibility of API call being blocked.
    // Later, we should replace the string with ConsumerConfig constant
    Duration timeout =
        tryParseDurationFromMillis(spec.getConsumerConfig().get("default.api.timeout.ms"));
    if (timeout == null) {
      Duration value =
          tryParseDurationFromMillis(
              spec.getConsumerConfig().get(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG));
      if (value != null) {
        // 2x request timeout to be compatible with previous version
        timeout = Duration.millis(2 * value.getMillis());
      }
    }

    return timeout == null ? Duration.standardSeconds(60) : timeout;
  }

  private static @Nullable Duration tryParseDurationFromMillis(@Nullable Object value) {
    if (value == null) {
      return null;
    }
    return value instanceof Integer
        ? Duration.millis((Integer) value)
        : Duration.millis(Integer.parseInt(value.toString()));
  }
}
