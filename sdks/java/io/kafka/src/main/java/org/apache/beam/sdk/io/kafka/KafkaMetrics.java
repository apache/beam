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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.util.Preconditions;

/** Stores and exports metrics for a batch of Kafka Client RPCs. */
public interface KafkaMetrics {

  void updateSuccessfulRpcMetrics(String topic, Duration elapsedTime);

  void updateKafkaMetrics();

  /** No-op implementation of {@code KafkaResults}. */
  class NoOpKafkaMetrics implements KafkaMetrics {
    private NoOpKafkaMetrics() {}

    @Override
    public void updateSuccessfulRpcMetrics(String topic, Duration elapsedTime) {}

    @Override
    public void updateKafkaMetrics() {}

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

    static HashMap<String, Histogram> latencyHistograms = new HashMap<String, Histogram>();

    abstract HashMap<String, ConcurrentLinkedQueue<Duration>> perTopicRpcLatencies();

    abstract AtomicBoolean isWritable();

    public static KafkaMetricsImpl create() {
      return new AutoValue_KafkaMetrics_KafkaMetricsImpl(
          new HashMap<String, ConcurrentLinkedQueue<Duration>>(), new AtomicBoolean(true));
    }

    /** Record the rpc status and latency of a successful Kafka poll RPC call. */
    @Override
    public void updateSuccessfulRpcMetrics(String topic, Duration elapsedTime) {
      if (isWritable().get()) {
        ConcurrentLinkedQueue<Duration> latencies = perTopicRpcLatencies().get(topic);
        if (latencies == null) {
          latencies = new ConcurrentLinkedQueue<Duration>();
          latencies.add(elapsedTime);
          perTopicRpcLatencies().put(topic, latencies);
        } else {
          latencies.add(elapsedTime);
        }
      }
    }

    /** Record rpc latency histogram metrics for all recorded topics. */
    private void recordRpcLatencyMetrics() {
      for (Map.Entry<String, ConcurrentLinkedQueue<Duration>> topicLatencies :
          perTopicRpcLatencies().entrySet()) {
        Histogram topicHistogram;
        if (latencyHistograms.containsKey(topicLatencies.getKey())) {
          topicHistogram = latencyHistograms.get(topicLatencies.getKey());
        } else {
          topicHistogram =
              KafkaSinkMetrics.createRPCLatencyHistogram(
                  KafkaSinkMetrics.RpcMethod.POLL, topicLatencies.getKey());
          latencyHistograms.put(topicLatencies.getKey(), topicHistogram);
        }
        // update all the latencies
        for (Duration d : topicLatencies.getValue()) {
          Preconditions.checkArgumentNotNull(topicHistogram);
          topicHistogram.update(d.toMillis());
        }
      }
    }

    /**
     * Export all metrics recorded in this instance to the underlying {@code perWorkerMetrics}
     * containers. This function will only report metrics once per instance. Subsequent calls to
     * this function will no-op.
     */
    @Override
    public void updateKafkaMetrics() {
      if (!isWritable().compareAndSet(true, false)) {
        return;
      }
      recordRpcLatencyMetrics();
    }
  }
}
