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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaStreamSource implements DataSource<Pair<byte[], byte[]>> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamSource.class);

  // ~ utility method to create a new kafka consumer
  static Consumer<byte[], byte[]>
  newConsumer(String brokerList, String groupId)
  {
    final Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }

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
    private final String groupId;
    private final String topicId;
    private final int partition;
    private final String host;
    private final boolean startFromBeginning;

    KafkaPartition(String brokerList, String groupId, String topicId,
                   int partition, String host, boolean startFromBeginning) {
      this.brokerList = brokerList;
      this.groupId = groupId;
      this.topicId = topicId;
      this.partition = partition;
      this.host = host;
      this.startFromBeginning = startFromBeginning;
    }

    @Override
    public Set<String> getLocations() {
      return Sets.newHashSet(host);
    }

    @Override
    public Reader<Pair<byte[], byte[]>> openReader() throws IOException {
      Consumer<byte[], byte[]> c = newConsumer(brokerList, groupId);
      TopicPartition tp = new TopicPartition(topicId, partition);
      c.assign(Lists.newArrayList(tp));
      if (startFromBeginning) {
        c.seekToBeginning(tp);
      } else {
        c.seekToEnd(tp);
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
      String groupId = params.getStringParam("groupId");
      boolean startFromBeginning =
          "earliest".equalsIgnoreCase(params.getStringParam("offset", "latest"));
      return (DataSource<T>)
          new KafkaStreamSource(brokers, groupId, topic, startFromBeginning);
    }
  }

  // ~ -----------------------------------------------------------------------------

  private final String brokerList;
  private final String groupId;
  private final String topicId;
  private final boolean startFromBeginning;

  KafkaStreamSource(String brokerList, String groupId, String topicId,
                    boolean startFromBeginning)
  {
    this.brokerList = brokerList;
    this.groupId = groupId;
    this.topicId = topicId;
    this.startFromBeginning = startFromBeginning;
  }

  @Override
  public List<Partition<Pair<byte[], byte[]>>> getPartitions() {
    try (Consumer<?, ?> c = newConsumer(brokerList, groupId)) {
      List<PartitionInfo> ps = c.partitionsFor(topicId);
      return ps.stream().map(p ->
          new KafkaPartition(
              brokerList, groupId, topicId, p.partition(),
              p.leader().host(), startFromBeginning))
          .collect(Collectors.toList());
    }
  }

  @Override
  public boolean isBounded() {
    return false;
  }
}