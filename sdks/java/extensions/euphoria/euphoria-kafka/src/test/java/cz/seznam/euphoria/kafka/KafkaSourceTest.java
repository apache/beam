package cz.seznam.euphoria.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class KafkaSourceTest {

  /**
   * Test if where there are no partitions
   * an IllegalStateException is expected
   */
  @Test(expected = IllegalStateException.class)
  @SuppressWarnings("unchecked")
  public void testPartitions() {
    KafkaSource source = mock(KafkaSource.class);
    Consumer<byte[], byte[]> consumer = mock(Consumer.class);
    when(consumer.partitionsFor(any(String.class))).thenReturn(Collections.emptyList());
    when(source.newConsumer(any(), any(), any())).thenReturn(consumer);
    when(source.getPartitions()).thenCallRealMethod();
    source.getPartitions();
  }
}