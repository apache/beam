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

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.PeekingIterator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Closeables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerPollThread {

  KafkaConsumerPollThread() {
    recordsDequeuePollTimeout = Duration.ofMillis(10);
    consumer = null;
    pollFuture = null;
  }

  private @Nullable Consumer<byte[], byte[]> consumer;

  /**
   * The poll timeout while reading records from Kafka. If option to commit reader offsets in to
   * Kafka in {@link KafkaCheckpointMark#finalizeCheckpoint()} is enabled, it would be delayed until
   * this poll returns. It should be reasonably low as a result. At the same time it probably can't
   * be very low like 10 millis, I am not sure how it affects when the latency is high. Probably
   * good to experiment. Often multiple marks would be finalized in a batch, it reduce finalization
   * overhead to wait a short while and finalize only the last checkpoint mark.
   */
  private static final Duration KAFKA_POLL_TIMEOUT = Duration.ofSeconds(1);

  private Duration recordsDequeuePollTimeout;
  private static final Duration RECORDS_DEQUEUE_POLL_TIMEOUT_MIN = Duration.ofMillis(1);
  private static final Duration RECORDS_DEQUEUE_POLL_TIMEOUT_MAX = Duration.ofMillis(20);
  private static final Duration RECORDS_ENQUEUE_POLL_TIMEOUT = Duration.ofMillis(100);

  private static final long UNINITIALIZED_OFFSET = -1;

  private final AtomicReference<Exception> consumerPollException = new AtomicReference<>();
  private final SynchronousQueue<ConsumerRecords<byte[], byte[]>> availableRecordsQueue =
      new SynchronousQueue<>();
  private final AtomicReference<@Nullable KafkaCheckpointMark> finalizedCheckpointMark =
      new AtomicReference<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Map<String, List<PartitionInfo>> topicsList = new ConcurrentHashMap<>();
  private final AtomicBoolean topicListUpdated = new AtomicBoolean(false);
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerPollThread.class);

  private @Nullable Future<?> pollFuture;
  private @Nullable PeekingIterator<ConsumerRecord<byte[], byte[]>> activeBatchIterator;

  void startOnExecutor(ExecutorService executorService, Consumer<byte[], byte[]> consumer) {
    this.consumer = consumer;
    // Use a separate thread to read Kafka messages. Kafka Consumer does all its work including
    // network I/O inside poll(). Polling only inside #advance(), especially with a small timeout
    // like 100 milliseconds does not work well. This along with large receive buffer for
    // consumer achieved the best throughput in tests (see `defaultConsumerProperties`).
    pollFuture = executorService.submit(this::consumerPollLoop);
  }

  void close() throws IOException {
    if (consumer == null) {
      LOG.debug("Closing consumer poll thread that was never started.");
      return;
    }
    Preconditions.checkStateNotNull(pollFuture);
    closed.set(true);
    try {
      // Wait for threads to shut down. Trying this as a loop to handle a tiny race where poll
      // thread
      // might block to enqueue right after availableRecordsQueue.poll() below.
      while (true) {
        if (consumer != null) {
          consumer.wakeup();
        }
        // todo will this drop unprocessed records?
        availableRecordsQueue.poll(); // drain unread batch, this unblocks consumer thread.
        try {
          Preconditions.checkStateNotNull(pollFuture);
          pollFuture.get(10, TimeUnit.SECONDS);
          break;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e); // not expected
        } catch (ExecutionException e) {
          throw new IOException(e.getCause());
        } catch (TimeoutException ignored) {
        }
        LOG.warn("An internal thread is taking a long time to shutdown. will retry.");
      }
    } finally {
      Closeables.close(consumer, true);
    }
  }

  @Nullable
  ConsumerRecord<byte[], byte[]> peek() throws IOException {
    PeekingIterator<ConsumerRecord<byte[], byte[]>> currentIterator = getOrInitialize();
    if (currentIterator.hasNext()) {
      return currentIterator.peek();
    }
    return null;
  }

  void advance() throws IOException {
    PeekingIterator<ConsumerRecord<byte[], byte[]>> currentIterator = getOrInitialize();
    if (currentIterator.hasNext()) {
      currentIterator.next();
    }
  }

  private PeekingIterator<ConsumerRecord<byte[], byte[]>> getOrInitialize() throws IOException {
    if (activeBatchIterator == null || !activeBatchIterator.hasNext()) {
      activeBatchIterator = Iterators.peekingIterator(readRecords().iterator());
    }
    return activeBatchIterator;
  }

  private void consumerPollLoop() {
    // Read in a loop and enqueue the batch of records, if any, to availableRecordsQueue.
    Consumer<byte[], byte[]> consumer = Preconditions.checkStateNotNull(this.consumer);

    try {
      ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
      while (!closed.get()) {
        try {
          if (records.isEmpty()) {
            records = consumer.poll(KAFKA_POLL_TIMEOUT);
            updateTopicList(consumer);
          } else if (availableRecordsQueue.offer(
              records, RECORDS_ENQUEUE_POLL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
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
    } catch (Exception e) { // mostly an unrecoverable KafkaException.
      LOG.error("{}: Exception while reading from Kafka", this, e);
      consumerPollException.set(e);
      throw e;
    }
    LOG.info("{}: Returning from consumer pool loop", this);
    // Commit any pending finalized checkpoint before shutdown.
    commitCheckpointMark();
  }

  private void updateTopicList(Consumer<byte[], byte[]> consumer) {
    synchronized (topicsList) {
      Map<String, List<PartitionInfo>> currentTopicsList = consumer.listTopics();
      topicsList.clear();
      topicsList.putAll(currentTopicsList);
      topicListUpdated.set(true);
    }
  }

  public boolean isTopicListUpdated() {
    return topicListUpdated.get();
  }

  public Map<String, List<PartitionInfo>> getTopicsList() {
    synchronized (topicsList) {
      return topicsList;
    }
  }

  ConsumerRecords<byte[], byte[]> readRecords() throws IOException {
    @Nullable ConsumerRecords<byte[], byte[]> records = null;
    try {
      // poll available records, wait (if necessary) up to the specified timeout.
      records =
          availableRecordsQueue.poll(recordsDequeuePollTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("{}: Unexpected", this, e);
    }

    if (records == null) {
      // Check if the poll thread failed with an exception.
      if (consumerPollException.get() != null) {
        throw new IOException("Exception while reading from Kafka", consumerPollException.get());
      }
      if (recordsDequeuePollTimeout.compareTo(RECORDS_DEQUEUE_POLL_TIMEOUT_MIN) > 0) {
        recordsDequeuePollTimeout = recordsDequeuePollTimeout.minus(Duration.ofMillis(1));
        LOG.debug("Reducing poll timeout for reader to " + recordsDequeuePollTimeout.toMillis());
      }
      return ConsumerRecords.empty();
    }
    if (recordsDequeuePollTimeout.compareTo(RECORDS_DEQUEUE_POLL_TIMEOUT_MAX) < 0) {
      recordsDequeuePollTimeout = recordsDequeuePollTimeout.plus(Duration.ofMillis(1));
      LOG.debug("Increasing poll timeout for reader to " + recordsDequeuePollTimeout.toMillis());
      LOG.debug("Record count: " + records.count());
    }
    return records;
  }

  /**
   * Enqueue checkpoint mark to be committed to Kafka. This does not block until it is committed.
   * There could be a delay of up to KAFKA_POLL_TIMEOUT (1 second). Any checkpoint mark enqueued
   * earlier is dropped in favor of this checkpoint mark. Documentation for {@link
   * UnboundedSource.CheckpointMark#finalizeCheckpoint()} says these are finalized in order. Only
   * the latest offsets need to be committed.
   *
   * <p>Returns if a existing checkpoint mark was skipped.
   */
  boolean finalizeCheckpointMarkAsync(KafkaCheckpointMark checkpointMark) {
    return finalizedCheckpointMark.getAndSet(checkpointMark) != null;
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
}
