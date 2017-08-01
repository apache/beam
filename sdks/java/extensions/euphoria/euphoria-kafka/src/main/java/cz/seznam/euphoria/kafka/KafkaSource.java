/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.AbstractIterator;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Lists;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class KafkaSource implements DataSource<Pair<byte[], byte[]>> {

  // config options
  public static final String CFG_RESET_OFFSET_TIMESTAMP_MILLIS = "reset.offset.timestamp.millis";
  public static final String CFG_STOP_AT_TIMESTAMP_MILLIS = "stop.at.timestamp.millis";
  public static final String CFG_SINGLE_READER_ONLY = "single.reader.only";

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  static final class ConsumerReader
      extends AbstractIterator<Pair<byte[], byte[]>>
      implements Reader<Pair<byte[], byte[]>> {
    
    private final Consumer<byte[], byte[]> c;
    private final long stopReadingAtStamp;
    
    private Iterator<ConsumerRecord<byte[], byte[]>> next;
    private int uncommittedCount = 0;
        
    ConsumerReader(Consumer<byte[], byte[]> c, long stopReadingAtStamp) {
      this.c = c;
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

        commitIfNeeded();
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
          commitIfNeeded();
          endOfData();
          return null;
        }
      }      
      ++uncommittedCount;
      return Pair.of(r.key(), r.value());
    }

    @Override
    public void close() throws IOException {
      commitIfNeeded();
      c.close();
    }

    private void commitIfNeeded() {
      if (uncommittedCount > 0) {
        c.commitAsync();
        LOG.debug("Committed {} records.", uncommittedCount);
        uncommittedCount = 0;
      }
    }
  }

  static final class KafkaPartition implements Partition<Pair<byte[], byte[]>> {
    private final String brokerList;
    private final String topicId;
    private final String groupId;
    private final int partition;
    private final String host;
    @Nullable
    private final Settings config;
    private final long startOffset;

    // should we stop reading when reaching the current offset?
    private final long stopReadingAtStamp;

    KafkaPartition(String brokerList, String topicId,
                   String groupId,
                   int partition, String host,
                   @Nullable Settings config,
                   long startOffset,
                   long stopReadingAtStamp)
    {
      this.brokerList = brokerList;
      this.topicId = topicId;
      this.groupId = groupId;
      this.partition = partition;
      this.host = host;
      this.config = config;
      this.startOffset = startOffset;
      this.stopReadingAtStamp = stopReadingAtStamp;
    }

    @Override
    public Set<String> getLocations() {
      return Sets.newHashSet(host);
    }

    @Override
    public Reader<Pair<byte[], byte[]>> openReader() throws IOException {
      Consumer<byte[], byte[]> c =
          KafkaUtils.newConsumer(brokerList, groupId, config);
      TopicPartition tp = new TopicPartition(topicId, partition);
      ArrayList<TopicPartition> partitionList = Lists.newArrayList(tp);
      c.assign(partitionList);
      if (startOffset > 0) {
        c.seek(tp, startOffset);
      } else if (startOffset == 0) {
        c.seekToBeginning(partitionList);
      }
      return new ConsumerReader(c, stopReadingAtStamp);
    }

  }

  static final class AllPartitionsConsumer implements Partition<Pair<byte[], byte[]>> {
    private final String brokerList;
    private final String topicId;
    private final String groupId;
    @Nullable
    private final Settings config;
    private final long offsetTimestamp; // ~ effective iff > 0
    private final long stopReadingAtStamp;

    AllPartitionsConsumer(
        String brokerList,
        String topicId,
        String groupId,
        @Nullable Settings config,
        long offsetTimestamp,
        long stopReadingStamp) {

      this.brokerList = brokerList;
      this.topicId = topicId;
      this.groupId = groupId;
      this.config = config;
      this.offsetTimestamp = offsetTimestamp;
      this.stopReadingAtStamp = stopReadingStamp;
    }

    @Override
    public Set<String> getLocations() {
      return Collections.emptySet();
    }

    @Override
    public Reader<Pair<byte[], byte[]>> openReader() throws IOException {
      Consumer<byte[], byte[]> c = KafkaUtils.newConsumer(
          brokerList, groupId, config);

      c.assign(
          c.partitionsFor(topicId)
              .stream()
              .map(p -> new TopicPartition(p.topic(), p.partition()))
              .collect(Collectors.toList()));
      if (offsetTimestamp > 0) {
        Map<Integer, Long> offs =
            KafkaUtils.getOffsetsBeforeTimestamp(brokerList, topicId, offsetTimestamp);
        for (Map.Entry<Integer, Long> off : offs.entrySet()) {
          c.seek(new TopicPartition(topicId, off.getKey()), off.getValue());
        }
      }
      return new ConsumerReader(c, stopReadingAtStamp);
    }
  }

  // ~ -----------------------------------------------------------------------------

  private final String brokerList;
  private final String topicId;
  private final String groupId;
  @Nullable
  private final Settings config;

  public KafkaSource(
      String brokerList,
      String topicId,
      String groupId,
      @Nullable Settings config) {
    this.brokerList = requireNonNull(brokerList);
    this.topicId = requireNonNull(topicId);
    this.groupId = requireNonNull(groupId);
    this.config = config;
  }

  @Override
  public List<Partition<Pair<byte[], byte[]>>> getPartitions() {
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
      if (config.getBoolean(CFG_SINGLE_READER_ONLY, false)) {
        return Collections.singletonList(
          new AllPartitionsConsumer(brokerList, topicId, groupId,
              config, offsetTimestamp, stopReadingAtStamp));
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
      final long stopAtStamp = stopReadingAtStamp;
      final long defaultOffsetTimestamp = offsetTimestamp;
      return ps.stream().map(p ->
          // ~ FIXME a leader might not be available (check p.leader().id() == -1)
          // ... fail in this situation
          new KafkaPartition(
              brokerList, topicId, groupId, p.partition(),
              p.leader().host(), config,
              offs.getOrDefault(p.partition(), defaultOffsetTimestamp),
              stopAtStamp))
          .collect(Collectors.toList());
    }
  }

  @Override
  public boolean isBounded() {
    return false;
  }
}
