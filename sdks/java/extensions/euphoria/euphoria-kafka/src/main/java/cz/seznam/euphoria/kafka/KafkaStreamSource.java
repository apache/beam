package cz.seznam.euphoria.kafka;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import java.util.Iterator;
import java.util.List;
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

  static final class KafkaPartition implements Partition<Pair<byte[], byte[]>> {
    private final String brokerList;
    private final String topicId;
    private final int partition;
    private final String host;
    private final Settings config;

    KafkaPartition(String brokerList, String topicId,
                   int partition, String host,
                   Settings config /* optional */)
    {
      this.brokerList = brokerList;
      this.topicId = topicId;
      this.partition = partition;
      this.host = host;
      this.config = config;
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
      return new ConsumerReader(c);
    }
  }

  public static final class Factory implements DataSourceFactory {
    @Override
    public <T> DataSource<T> get(URI uri, Settings settings) {
      String brokers = uri.getAuthority();
      String topic = uri.getPath().substring(1);

      String cname = URIParams.of(uri).getStringParam("cfg", null);
      Settings cconfig =  cname == null ? null : settings.nested(cname);
      return (DataSource<T>) new KafkaStreamSource(brokers, topic, cconfig);
    }
  }

  // ~ -----------------------------------------------------------------------------

  private final String brokerList;
  private final String topicId;
  private final Settings config; // ~ optional

  KafkaStreamSource(String brokerList, String topicId, Settings config) {
    this.brokerList = requireNonNull(brokerList);
    this.topicId = requireNonNull(topicId);
    this.config = config;
  }

  @Override
  public List<Partition<Pair<byte[], byte[]>>> getPartitions() {
    try (Consumer<?, ?> c = KafkaUtils.newConsumer(brokerList, config)) {
      List<PartitionInfo> ps = c.partitionsFor(topicId);
      return ps.stream().map(p ->
          // ~ XXX a leader might not be available (check p.leader().id() == -1)
          // ... fail in this situation
          new KafkaPartition(
              brokerList, topicId, p.partition(),
              p.leader().host(), config))
          .collect(Collectors.toList());
    }
  }

  @Override
  public boolean isBounded() {
    return false;
  }
}