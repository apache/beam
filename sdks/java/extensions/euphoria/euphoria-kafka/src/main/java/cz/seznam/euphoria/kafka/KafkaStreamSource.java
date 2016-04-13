package cz.seznam.euphoria.kafka;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.DataSourceFactory;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.operator.Pair;
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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class KafkaStreamSource implements DataSource<Pair<byte[], byte[]>> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamSource.class);

  static final class ConsumerReader
      extends AbstractIterator<Pair<byte[], byte[]>>
      implements Reader<Pair<byte[], byte[]>>
  {
    private final Consumer<byte[], byte[]> c;
    private Iterator<ConsumerRecord<byte[], byte[]>> next;

    ConsumerReader(Consumer<byte[], byte[]> c) {
      this.c = c;
    }

    @Override
    protected Pair<byte[], byte[]> computeNext() {
      while (next == null || !next.hasNext()) {
        LOG.debug("Polling for next consumer records: {}", c.assignment());
        next = c.poll(500).iterator();
      }
      ConsumerRecord<byte[], byte[]> r = this.next.next();
      return Pair.of(r.key(), r.value());
    }

    @Override
    public void close() throws IOException {
      c.close();
    }
  }

  enum OffsetReset { NONE, LATEST, EARLIEST }

  static final class KafkaPartition implements Partition<Pair<byte[], byte[]>> {
    private final String brokerList;
    private final String groupId;
    private final String topicId;
    private final int partition;
    private final String host;
    private final OffsetReset offsetReset;
    private final String configProperties;

    KafkaPartition(String brokerList, String groupId, String topicId,
                   int partition, String host, OffsetReset offsetReset,
                   String configProperties /* optional */)
    {
      this.brokerList = brokerList;
      this.groupId = groupId;
      this.topicId = topicId;
      this.partition = partition;
      this.host = host;
      this.offsetReset = offsetReset;
      this.configProperties = configProperties;
    }

    @Override
    public Set<String> getLocations() {
      return Sets.newHashSet(host);
    }

    @Override
    public Reader<Pair<byte[], byte[]>> openReader() throws IOException {
      Consumer<byte[], byte[]> c =
          KafkaUtils.newConsumer(brokerList, groupId, configProperties);
      TopicPartition tp = new TopicPartition(topicId, partition);
      c.assign(Lists.newArrayList(tp));
      switch (offsetReset) {
        case EARLIEST:
          c.seekToBeginning(tp);
          break;
        case LATEST:
          c.seekToEnd(tp);
          break;
        default:
          // ~ nothing to do
          break;
      }
      return new ConsumerReader(c);
    }
  }

  public static final class Factory implements DataSourceFactory {
    @Override
    public <T> DataSource<T> get(URI uri, Settings settings) {
      String brokers = uri.getAuthority();
      String topic = uri.getPath().substring(1);

      URIParams params = URIParams.of(uri);
      String groupId = params.getStringParam("group");

      OffsetReset offsetReset = OffsetReset.NONE;
      switch (params.getStringParam("offset", "").toLowerCase(Locale.ENGLISH)) {
        case "latest":
          offsetReset = OffsetReset.LATEST;
          break;
        case "earliest":
          offsetReset = OffsetReset.EARLIEST;
          break;
        case "":
          // ~ nothing to do
          break;
        default:
          LOG.warn("Unknown value for the 'offset' parameter in uri: {}", uri);
          break;
      }

      String configResource = params.getStringParam("properties", null);
      return (DataSource<T>)
          new KafkaStreamSource(brokers, groupId, topic, offsetReset, configResource);
    }
  }

  // ~ -----------------------------------------------------------------------------

  private final String brokerList;
  private final String groupId;
  private final String topicId;
  private final OffsetReset offsetReset;
  private final String configProperties; // ~ optional

  KafkaStreamSource(
      String brokerList, String groupId, String topicId,
      OffsetReset offsetReset, String configProperties)
  {
    this.brokerList = requireNonNull(brokerList);
    this.groupId = requireNonNull(groupId);
    this.topicId = requireNonNull(topicId);
    this.offsetReset = requireNonNull(offsetReset);
    this.configProperties = configProperties;
  }

  @Override
  public List<Partition<Pair<byte[], byte[]>>> getPartitions() {
    try (Consumer<?, ?> c = KafkaUtils.newConsumer(brokerList, groupId)) {
      List<PartitionInfo> ps = c.partitionsFor(topicId);
      return ps.stream().map(p ->
          // ~ XXX a leader might not be available (check p.leader().id() == -1)
          // ... fail in this situation
          new KafkaPartition(
              brokerList, groupId, topicId, p.partition(),
              p.leader().host(), offsetReset, configProperties))
          .collect(Collectors.toList());
    }
  }

  @Override
  public boolean isBounded() {
    return false;
  }
}