package cz.seznam.euphoria.kafka;

import cz.seznam.euphoria.core.util.Settings;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.Properties;

class KafkaUtils {

  private static Properties toProperties(Settings properties) {
    final Properties ps = new Properties();
    if (properties != null) {
      for (Map.Entry<String, String> e : properties.getAll().entrySet()) {
        ps.setProperty(e.getKey(), e.getValue());
      }
    }
    return ps;
  }

  public static Producer<byte [], byte []>
  newProducer(String brokerList, Settings config)
  {
    final Properties ps = toProperties(config);
    ps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    ps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    ps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    if (ps.getProperty(ProducerConfig.ACKS_CONFIG) == null) {
      ps.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    }
    return new KafkaProducer<>(ps);
  }

  public static Consumer<byte[], byte[]>
  newConsumer(String brokerList, Settings config)
  {
    Properties ps = toProperties(config);
    ps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    ps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    ps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return new KafkaConsumer<>(ps);
  }
}
