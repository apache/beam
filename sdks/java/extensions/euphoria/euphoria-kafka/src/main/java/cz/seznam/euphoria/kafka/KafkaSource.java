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

import cz.seznam.euphoria.shadow.com.google.common.collect.AbstractIterator;
import cz.seznam.euphoria.shadow.com.google.common.collect.Lists;
import cz.seznam.euphoria.core.client.io.UnboundedDataSource;
import cz.seznam.euphoria.core.client.io.UnboundedPartition;
import cz.seznam.euphoria.core.client.io.UnboundedReader;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class KafkaSource
    implements UnboundedDataSource<Pair<byte[], byte[]>, Long> {

  // config options
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
    private final String topicId;
    private final int partition;
    @Nullable
    private final Settings config;
    private final long startOffset;

    // should we stop reading when reaching the current offset?
    private final long stopReadingAtStamp;

    KafkaPartition(
        String brokerList, String topicId,
        int partition,
        @Nullable Settings config,
        long startOffset,
        long stopReadingAtStamp) {

      this.brokerList = brokerList;
      this.topicId = topicId;
      this.partition = partition;
      this.config = config;
      this.startOffset = startOffset;
      this.stopReadingAtStamp = stopReadingAtStamp;
    }


    @Override
    public UnboundedReader<Pair<byte[], byte[]>, Long> openReader() throws IOException {
      Consumer<byte[], byte[]> c =
          KafkaUtils.newConsumer(brokerList, null, config);
      TopicPartition tp = new TopicPartition(topicId, partition);
      ArrayList<TopicPartition> partitionList = Lists.newArrayList(tp);
      c.assign(partitionList);
      if (startOffset > 0) {
        c.seek(tp, startOffset);
      } else if (startOffset == 0) {
        c.seekToBeginning(partitionList);
      }
      return new ConsumerReader(c, tp, stopReadingAtStamp);
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
    try (Consumer<?, ?> c = KafkaUtils.newConsumer(
        brokerList, "euphoria.partition-probe-" + UUID.randomUUID().toString(),
        config)) {

      final Map<Integer, Long> offs;
      try {
        offs = offsetTimestamp > 0
            ? KafkaUtils.getOffsetsBeforeTimestamp(brokerList, topicId, offsetTimestamp)
            : Collections.emptyMap();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      List<PartitionInfo> ps = c.partitionsFor(topicId);

      if (ps.isEmpty()) {
        throw new IllegalStateException("No kafka partitions found for topic " + topicId);
      }

      final long stopAtStamp = stopReadingAtStamp;
      final long defaultOffsetTimestamp = offsetTimestamp;
      return ps.stream()
              .map((PartitionInfo p) -> {
                if (p.leader().id() == -1) {
                  throw new IllegalStateException("Leader not available");
                }

                return new KafkaPartition(
                        brokerList, topicId, p.partition(),
                        config,
                        offs.getOrDefault(p.partition(), defaultOffsetTimestamp),
                        stopAtStamp);
              })
              .collect(Collectors.toList());
    }
  }

  @Override
  public boolean isBounded() {
    return false;
  }
}
