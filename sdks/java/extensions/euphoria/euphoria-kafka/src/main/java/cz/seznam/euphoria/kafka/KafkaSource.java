package cz.seznam.euphoria.kafka;

import cz.seznam.euphoria.guava.shaded.com.google.common.collect.AbstractIterator;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Lists;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.DataSourceFactory;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.core.util.URIParams;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class KafkaSource implements DataSource<Pair<byte[], byte[]>> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  static final class ConsumerReader
      extends AbstractIterator<Pair<byte[], byte[]>>
      implements Reader<Pair<byte[], byte[]>>
  {
    private final Consumer<byte[], byte[]> c;
    private Iterator<ConsumerRecord<byte[], byte[]>> next;
    private int uncommittedCount = 0;

    ConsumerReader(Consumer<byte[], byte[]> c) {
      this.c = c;
    }

    @Override
    protected Pair<byte[], byte[]> computeNext() {
      while (next == null || !next.hasNext()) {
        commitIfNeeded();
        LOG.debug("Polling for next consumer records: {}", c.assignment());
        next = c.poll(500).iterator();
      }
      ConsumerRecord<byte[], byte[]> r = this.next.next();
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
    private final int partition;
    private final String host;
    private final Settings config;
    private final long startOffset;

    KafkaPartition(String brokerList, String topicId,
                   int partition, String host,
                   Settings config /* optional */,
                   long startOffset)
    {
      this.brokerList = brokerList;
      this.topicId = topicId;
      this.partition = partition;
      this.host = host;
      this.config = config;
      this.startOffset = startOffset;
    }

    @Override
    public Set<String> getLocations() {
      return Sets.newHashSet(host);
    }

    @Override
    public Reader<Pair<byte[], byte[]>> openReader() throws IOException {
      Consumer<byte[], byte[]> c =
          KafkaUtils.newConsumer(brokerList, config);
      TopicPartition tp = new TopicPartition(topicId, partition);
      c.assign(Lists.newArrayList(tp));
      if (startOffset > 0) {
        c.seek(tp, startOffset);
      }
      return new ConsumerReader(c);
    }
  }

  static final class AllPartitionsConsumer implements Partition<Pair<byte[], byte[]>> {
    private final String brokerList;
    private final String topicId;
    private final Settings config; // ~ optional
    private final long offsetTimestamp; // ~ effective iff > 0

    AllPartitionsConsumer(
        String brokerList, String topicId, Settings config, long offsetTimestamp)
    {
      this.brokerList = brokerList;
      this.topicId = topicId;
      this.config = config;
      this.offsetTimestamp = offsetTimestamp;
    }

    @Override
    public Set<String> getLocations() {
      return Collections.emptySet();
    }

    @Override
    public Reader<Pair<byte[], byte[]>> openReader() throws IOException {
      Consumer<byte[], byte[]> c = KafkaUtils.newConsumer(brokerList, config);
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
      return new ConsumerReader(c);
    }
  }

  public static final class Factory implements DataSourceFactory {
    @Override
    @SuppressWarnings("unchecked")
    public <T> DataSource<T> get(URI uri, Settings settings) {
      String brokers = uri.getAuthority();
      String topic = uri.getPath().substring(1);

      String cname = URIParams.of(uri).getStringParam("cfg", null);
      Settings cconfig =  cname == null ? null : settings.nested(cname);
      return (DataSource<T>) new KafkaSource(brokers, topic, cconfig);
    }
  }

  // ~ -----------------------------------------------------------------------------

  private final String brokerList;
  private final String topicId;
  private final Settings config; // ~ optional

  KafkaSource(String brokerList, String topicId, Settings config) {
    this.brokerList = requireNonNull(brokerList);
    this.topicId = requireNonNull(topicId);
    this.config = config;
  }

  @Override
  public List<Partition<Pair<byte[], byte[]>>> getPartitions() {
    long offsetTimestamp = -1L;
    if (config != null) {
      offsetTimestamp = config.getLong("reset.offset.timestamp", -1L);
      LOG.info("Resetting offset of kafka topic {} to {}",
          topicId, offsetTimestamp);
    }
    if (config != null && config.getBoolean("single.reader.only", false)) {
      return Collections.singletonList(
          new AllPartitionsConsumer(brokerList, topicId, config, offsetTimestamp));
    }
    try (Consumer<?, ?> c = KafkaUtils.newConsumer(brokerList, config)) {
      final Map<Integer, Long> offs;
      try {
        offs = offsetTimestamp > 0
            ? KafkaUtils.getOffsetsBeforeTimestamp(brokerList, topicId, offsetTimestamp)
            : Collections.emptyMap();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      List<PartitionInfo> ps = c.partitionsFor(topicId);
      return ps.stream().map(p ->
          // ~ XXX a leader might not be available (check p.leader().id() == -1)
          // ... fail in this situation
          new KafkaPartition(
              brokerList, topicId, p.partition(),
              p.leader().host(), config,
              offs.getOrDefault(p.partition(), -1L)))
          .collect(Collectors.toList());
    }
  }

  @Override
  public boolean isBounded() {
    return false;
  }
}
