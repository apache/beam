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