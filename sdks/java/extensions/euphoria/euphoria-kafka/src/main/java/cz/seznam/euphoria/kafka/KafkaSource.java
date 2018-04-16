/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.kafka;

import cz.seznam.euphoria.core.client.io.UnboundedDataSource;
import cz.seznam.euphoria.core.client.io.UnboundedPartition;
import cz.seznam.euphoria.core.client.io.UnboundedReader;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.shadow.com.google.common.annotations.VisibleForTesting;
import cz.seznam.euphoria.shadow.com.google.common.collect.AbstractIterator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class KafkaSource
    implements UnboundedDataSource<Pair<byte[], byte[]>, Long> {

  public static final String CFG_RESET_OFFSET_TIMESTAMP_MILLIS = "reset.offset.timestamp.millis";
  public static final String CFG_STOP_AT_TIMESTAMP_MILLIS = "stop.at.timestamp.millis";

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  static final class ConsumerReader
      extends AbstractIterator<Pair<byte[], byte[]>>
      implements UnboundedReader<Pair<byte[], byte[]>, Long> {

    private final Consumer<byte[], byte[]> c;
    private final TopicPartition tp;
    private final long stopReadingAtStamp;
    private long offset;

    private Iterator<ConsumerRecord<byte[], byte[]>> next;

    ConsumerReader(
        Consumer<byte[], byte[]> c,
        TopicPartition tp,
        long stopReadingAtStamp) {

      this.c = c;
      this.tp = tp;
      this.stopReadingAtStamp = stopReadingAtStamp;
    }

    @Override
    protected Pair<byte[], byte[]> computeNext() {
      while (next == null || !next.hasNext()) {
        if (Thread.currentThread().isInterrupted()) {
          LOG.info("Terminating polling on topic due to thread interruption");
          endOfData();
          return null;
        }

        ConsumerRecords<byte[], byte[]> polled = c.poll(500);
        next = polled.iterator();
      }
      ConsumerRecord<byte[], byte[]> r = this.next.next();
      if (stopReadingAtStamp > 0) {
        long messageStamp = r.timestamp();
        if (messageStamp > stopReadingAtStamp) {
          LOG.info(
              "Terminating polling of topic, passed initial timestamp {} with value {}",
              stopReadingAtStamp, messageStamp);
          endOfData();
          return null;
        }
      }
      offset = r.offset();

      return Pair.of(r.key(), r.value());
    }

    @Override
    public void close() throws IOException {
      c.close();
    }

    @Override
    public Long getCurrentOffset() {
      return offset;
    }

    @Override
    public void reset(Long offset) {
      c.seek(tp, offset);
    }

    @Override
    public void commitOffset(Long offset) {
      c.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(offset)));
    }

  }

  static final class KafkaPartition
      implements UnboundedPartition<Pair<byte[], byte[]>, Long> {

    private final String brokerList;
    private final TopicPartition topicPartition;
    @Nullable
    private final Settings config;
    private final long startOffset;

    // should we stop reading when reaching the current offset?
    private final long stopReadingAtStamp;

    KafkaPartition(
        String brokerList,
        TopicPartition topicPartition,
        @Nullable Settings config,
        long startOffset,
        long stopReadingAtStamp) {

      this.brokerList = brokerList;
      this.topicPartition = topicPartition;
      this.config = config;
      this.startOffset = startOffset;
      this.stopReadingAtStamp = stopReadingAtStamp;
    }


    @Override
    public UnboundedReader<Pair<byte[], byte[]>, Long> openReader() throws IOException {
      final Consumer<byte[], byte[]> c =
          KafkaUtils.newConsumer(brokerList, null, config);
      final List<TopicPartition> partitionList = Collections.singletonList(topicPartition);
      c.assign(partitionList);
      if (startOffset > 0) {
        c.seek(topicPartition, startOffset);
      } else if (startOffset == 0) {
        c.seekToBeginning(partitionList);
      }
      return new ConsumerReader(c, topicPartition, stopReadingAtStamp);
    }

  }

  // ~ -----------------------------------------------------------------------------

  private final String brokerList;
  private final String topicId;
  @Nullable
  private final Settings config;

  public KafkaSource(
      String brokerList,
      String topicId,
      @Nullable Settings config) {

    this.brokerList = requireNonNull(brokerList);
    this.topicId = requireNonNull(topicId);
    this.config = config;
  }

  @Override
  public List<UnboundedPartition<Pair<byte[], byte[]>, Long>> getPartitions() {

    long offsetTimestamp = -1L;
    long stopReadingAtStamp = Long.MAX_VALUE;

    if (config != null) {
      offsetTimestamp = config.getLong(CFG_RESET_OFFSET_TIMESTAMP_MILLIS, -1L);
      if (offsetTimestamp > 0) {
        LOG.info("Resetting offset of kafka topic {} to {}",
            topicId, offsetTimestamp);
      } else if (offsetTimestamp == 0) {
        LOG.info("Going to read the whole contents of kafka topic {}",
            topicId);
      }
      stopReadingAtStamp = config.getLong(
          CFG_STOP_AT_TIMESTAMP_MILLIS, stopReadingAtStamp);

      if (stopReadingAtStamp < Long.MAX_VALUE) {
        LOG.info("Will stop polling kafka topic at current timestamp {}",
            stopReadingAtStamp);
      }
    }

    return getPartitions(offsetTimestamp, stopReadingAtStamp);
  }

    private List<UnboundedPartition<Pair<byte[], byte[]>, Long>> getPartitions(
        long startTimestamp, long endTimestamp) {
    try (Consumer<byte[], byte[]> consumer = newConsumer(
        brokerList, "euphoria.partition-probe-" + UUID.randomUUID().toString(),
        config)) {

      final List<PartitionInfo> partitions = consumer.partitionsFor(topicId);

      // sanity check

      if (partitions.isEmpty()) {
        throw new IllegalStateException("No kafka partitions found for topic " + topicId);
      }

      partitions.forEach(p -> {
        if (p.leader().id() == -1) {
          throw new IllegalStateException(
              "Leader for partition [" + p.partition() + "] is not for available");
        }
      });

      // convert to topic partitions

      final List<TopicPartition> topicPartitions = partitions.stream()
          .map(p -> new TopicPartition(p.topic(), p.partition()))
          .collect(Collectors.toList());

      // resolve offsets to read from

      final Map<TopicPartition, OffsetAndTimestamp> offsets;
      if (startTimestamp > 0) {
        final Map<TopicPartition, Long> offsetsToFetch = new HashMap<>();
        for (PartitionInfo partition : partitions) {
          offsetsToFetch.put(
              new TopicPartition(partition.topic(), partition.partition()),
              startTimestamp);
        }
        offsets =
            consumer.offsetsForTimes(offsetsToFetch);
      } else {
        offsets = Collections.emptyMap();
      }

      return topicPartitions
          .stream()
          .map(tp -> {
            final long offset = offsets.containsKey(tp)
                ? offsets.get(tp).offset()
                : startTimestamp;

            return new KafkaPartition(
                brokerList,
                tp,
                config,
                offset,
                endTimestamp);
          })
          .collect(Collectors.toList());
    }
  }

  @VisibleForTesting
  Consumer<byte[], byte[]> newConsumer(
      String brokerList, @Nullable String groupId, @Nullable Settings config) {
    return KafkaUtils.newConsumer(
        brokerList, groupId, config);
  }

  @Override
  public boolean isBounded() {
    return false;
  }
}
