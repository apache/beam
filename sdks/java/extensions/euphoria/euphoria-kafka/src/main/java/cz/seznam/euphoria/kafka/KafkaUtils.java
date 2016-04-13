package cz.seznam.euphoria.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

class KafkaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

  static Properties loadPropertiesResource(String clientType, String resource) {
    if (resource == null) {
      return new Properties();
    }
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = KafkaUtils.class.getClassLoader();
    }
    InputStream is = cl.getResourceAsStream(resource);
    if (is == null) {
      LOG.warn("Failed to locate {} properties resource: {}", clientType, resource);
      return new Properties();
    }
    try {
      Properties props = new Properties();
      props.load(is);
      return props;
    } catch (IOException e) {
      LOG.warn("Failed to load " + clientType + " properties resource: " + resource, e);
      return new Properties();
    }
  }

  public static Producer<byte [], byte []>
  newProducer(String brokerList, String propertiesResource)
  {
    final Properties props = loadPropertiesResource("producer", propertiesResource);
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    if (props.getProperty(ProducerConfig.ACKS_CONFIG) == null) {
      props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    }
    return new KafkaProducer<>(props);
  }

  public static Producer<byte [], byte []> newProducer(String brokerList) {
    return newProducer(brokerList, null);
  }

  public static Consumer<byte[], byte[]>
  newConsumer(String brokerList, String groupId, String propertiesResource)
  {
    final Properties props = loadPropertiesResource("consumer", propertiesResource);
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return new KafkaConsumer<>(props);
  }

  public static Consumer<byte[], byte[]> newConsumer(String brokerList, String groupId) {
    return newConsumer(brokerList, groupId, null);
  }
}
