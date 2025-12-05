/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.kafka;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Stores and exports metrics for a batch of Kafka Client RPCs. */
public interface KafkaMetrics {

  void updateSuccessfulRpcMetrics(String topic, Duration elapsedTime);

  void updateBacklogBytes(String topic, int partitionId, long backlog);

  /*Flushes the buffered metrics to the current metric container for this thread.*/
  void flushBufferedMetrics();

  /** No-op implementation of {@code KafkaResults}. */
  class NoOpKafkaMetrics implements KafkaMetrics {
    private NoOpKafkaMetrics() {}

    @Override
    public void updateSuccessfulRpcMetrics(String topic, Duration elapsedTime) {}

    @Override
    public void updateBacklogBytes(String topic, int partitionId, long backlog) {}

    @Override
    public void flushBufferedMetrics() {}

    private static NoOpKafkaMetrics singleton = new NoOpKafkaMetrics();

    static NoOpKafkaMetrics getInstance() {
      return singleton;
    }
  }

  /**
   * Metrics of a batch of RPCs. Member variables are thread safe; however, this class does not have
   * atomicity across member variables.
   *
   * <p>Expected usage: A number of threads record metrics in an instance of this class with the
   * member methods. Afterwards, a single thread should call {@code updateStreamingInsertsMetrics}
   * which will export all counters metrics and RPC latency distribution metrics to the underlying
   * {@code perWorkerMetrics} container. Afterwards, metrics should not be written/read from this
   * object.
   */
  @AutoValue
  abstract class KafkaMetricsImpl implements KafkaMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsImpl.class);

    private static final Map<String, Histogram> LATENCY_HISTOGRAMS =
        new ConcurrentHashMap<String, Histogram>();

    abstract ConcurrentHashMap<String, ConcurrentLinkedQueue<Duration>> perTopicRpcLatencies();;

    abstract ConcurrentHashMap<MetricName, Long> perTopicPartitionBacklogs();

    abstract AtomicBoolean isWritable();

    public static KafkaMetricsImpl create() {
      return new AutoValue_KafkaMetrics_KafkaMetricsImpl(
          new ConcurrentHashMap<String, ConcurrentLinkedQueue<Duration>>(),
          new ConcurrentHashMap<MetricName, Long>(),
          new AtomicBoolean(true));
    }

    /**
     * Record the rpc status and latency of a successful Kafka poll RPC call.
     *
     * <p>TODO(naireenhussain): It's possible that `isWritable().get()` is called before it's set to
     * false in another thread, allowing an extraneous measurement to slip in, so
     * perTopicRpcLatencies() isn't necessarily thread safe. One way to address this would be to add
     * synchronized blocks to ensure that there is only one thread ever reading/modifying the
     * perTopicRpcLatencies() map.
     */
    @Override
    public void updateSuccessfulRpcMetrics(String topic, Duration elapsedTime) {
      if (isWritable().get()) {
        ConcurrentLinkedQueue<Duration> latencies = perTopicRpcLatencies().get(topic);
        if (latencies == null) {
          latencies = new ConcurrentLinkedQueue<Duration>();
          latencies.add(elapsedTime);
          perTopicRpcLatencies().putIfAbsent(topic, latencies);
        } else {
          latencies.add(elapsedTime);
        }
      }
    }

    /**
     * This is for tracking backlog bytes to be added to the Metric Container at a later time.
     *
     * @param topicName topicName
     * @param partitionId partitionId
     * @param backlog backlog for the specific partitionID of topicName
     */
    @Override
    public void updateBacklogBytes(String topicName, int partitionId, long backlog) {
      if (isWritable().get()) {
        MetricName metricName = KafkaSinkMetrics.getMetricGaugeName(topicName, partitionId);
        perTopicPartitionBacklogs().put(metricName, backlog);
      }
    }

    /** Record rpc latency histogram metrics for all recorded topics. */
    private void recordRpcLatencyMetrics() {
      for (Map.Entry<String, ConcurrentLinkedQueue<Duration>> topicLatencies :
          perTopicRpcLatencies().entrySet()) {
        Histogram topicHistogram;
        if (LATENCY_HISTOGRAMS.containsKey(topicLatencies.getKey())) {
          topicHistogram = LATENCY_HISTOGRAMS.get(topicLatencies.getKey());
        } else {
          topicHistogram =
              KafkaSinkMetrics.createRPCLatencyHistogram(
                  KafkaSinkMetrics.RpcMethod.POLL, topicLatencies.getKey());
          LATENCY_HISTOGRAMS.put(topicLatencies.getKey(), topicHistogram);
        }
        // Update all the latencies
        for (Duration d : topicLatencies.getValue()) {
          Preconditions.checkArgumentNotNull(topicHistogram);
          topicHistogram.update(d.toMillis());
        }
      }
    }

    /** This is for creating gauges from backlog bytes recorded previously. */
    private void recordBacklogBytesInternal() {
      for (Map.Entry<MetricName, Long> backlog : perTopicPartitionBacklogs().entrySet()) {
        Gauge gauge = KafkaSinkMetrics.createBacklogGauge(backlog.getKey());
        gauge.set(backlog.getValue());
      }
    }

    /**
     * Export all metrics recorded in this instance to the underlying {@code perWorkerMetrics}
     * containers. This function will only report metrics once per instance. Subsequent calls to
     * this function will no-op.
     */
    @Override
    public void flushBufferedMetrics() {
      if (!isWritable().compareAndSet(true, false)) {
        LOG.warn("Updating stale Kafka metrics container");
        return;
      }
      recordBacklogBytesInternal();
      recordRpcLatencyMetrics();
    }
  }
}
