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

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSinkFactory;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.core.util.URIParams;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaSink implements DataSink<Pair<byte[], byte[]>> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);

  public static class Factory implements DataSinkFactory {
    @SuppressWarnings("unchecked")
    @Override
    public <T> DataSink<T> get(URI uri, Settings settings) {
      String brokers = uri.getAuthority();
      String topic = uri.getPath().substring(1);
      String cname = URIParams.of(uri).getStringParam("cfg", null);
      Settings cconfig = cname == null ? null : settings.nested(cname);
      return (DataSink<T>) new KafkaSink(brokers, topic, cconfig);
    }
  }

  private static class ProducerWriter implements Writer<Pair<byte[], byte[]>> {
    private final String topicId;
    private final Integer partition;

    private transient Producer producer;
    private transient ArrayDeque<Future> fs = new ArrayDeque<>();

    public ProducerWriter(Producer producer, String topicId, Integer partition) {
      this.producer = producer;
      this.topicId = topicId;
      this.partition = partition;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(Pair<byte[], byte[]> elem) throws IOException {
      final ProducerRecord r =
          new ProducerRecord(topicId, partition, elem.getFirst(), elem.getSecond());
      fs.addLast(producer.send(r));

      // ~ try to consume already finished futures ... preventing the pool of futures
      // from growing too large
      Future f;
      while ((f = fs.peekFirst()) != null && f.isDone()) {
        try {
          fs.removeFirst().get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
          throw new RuntimeException(e.getCause());
        }
      }
    }

    @Override
    public void commit() throws IOException {
      // ~ no-op .. since this is a non-transactional sink (so far)
    }

    @Override
    public void close() throws IOException {
      // ~ wait for all pending futures to finish
      if (LOG.isDebugEnabled()) {
        final int nItems = fs.size();
        long start = System.nanoTime();
        waitPendingConfirms();
        long end = System.nanoTime();
        LOG.debug("Finished waiting for confirmation of {} items in {}ms",
            nItems, TimeUnit.NANOSECONDS.toMillis(end - start));
      } else {
        waitPendingConfirms();
      }
    }

    private void waitPendingConfirms() {
      for (Future f : fs) {
        try {
          f.get(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (ExecutionException e) {
          throw new RuntimeException(e.getCause());
        } catch (TimeoutException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Static map of opened Kafka producers (one per each Kafka cluster)
   */
  private final static ConcurrentMap<String, Producer<byte[], byte[]>> PRODUCERS =
      new ConcurrentHashMap<>();

  private String brokers;
  private String topic;
  private Settings config;

  KafkaSink(String brokers, String topic, Settings config) {
    this.brokers = brokers;
    this.topic = topic;
    this.config = config;
  }

  @Override
  public Writer<Pair<byte[], byte[]>> openWriter(int partitionId) {
    String cacheKey = brokers;
    Producer<byte[], byte[]> producer = PRODUCERS.get(cacheKey);
    if (producer == null) {
      // ~ ok, let's create a new producer (this may take some time)
      final Producer<byte[], byte[]> p = KafkaUtils.newProducer(brokers, config);
      // ~ now, let's try to store it in our global cache
      final Producer<byte[], byte[]> p1 = PRODUCERS.putIfAbsent(cacheKey, p);
      if (p1 == null) {
        producer = p;
      } else {
        // ~ looks like somebody managed to create concurrently a new
        // producer in between and store it quicker into the global cache
        producer = p1;
        // ~ must close the created one to avoid leaking resources!
        p.close();
      }
    }
    final List<PartitionInfo> partitions = producer.partitionsFor(topic);
    return new ProducerWriter(producer, topic, partitionId % partitions.size());
  }

  @Override
  public void commit() throws IOException {
    // ~ no-op
  }

  @Override
  public void rollback() {
    // ~ no-op
  }
}
