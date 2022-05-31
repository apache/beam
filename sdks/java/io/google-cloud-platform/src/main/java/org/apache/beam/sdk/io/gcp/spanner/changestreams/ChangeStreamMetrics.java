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
package org.apache.beam.sdk.io.gcp.spanner.changestreams;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.Metrics;
import org.joda.time.Duration;

/** Class to aggregate metrics related functionality. */
public class ChangeStreamMetrics implements Serializable {

  private static final long serialVersionUID = 8187140831756972470L;

  // ------------------------
  // Partition record metrics

  /**
   * Counter for the total number of partitions identified during the execution of the Connector.
   */
  public static final Counter PARTITION_RECORD_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "partition_record_count");

  /**
   * Counter for the total number of partition splits / moves identified during the execution of the
   * Connector.
   */
  public static final Counter PARTITION_RECORD_SPLIT_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "partition_record_split_count");

  /**
   * Counter for the total number of partition merges identified during the execution of the
   * Connector.
   */
  public static final Counter PARTITION_RECORD_MERGE_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "partition_record_merge_count");

  /**
   * Time in milliseconds that a partition took to transition from {@link State#CREATED} to {@link
   * State#SCHEDULED}.
   */
  public static final Distribution PARTITION_CREATED_TO_SCHEDULED_MS =
      Metrics.distribution(ChangeStreamMetrics.class, "partition_created_to_scheduled_ms");

  /**
   * Time in milliseconds that a partition took to transition from {@link State#SCHEDULED} to {@link
   * State#RUNNING}.
   */
  public static final Distribution PARTITION_SCHEDULED_TO_RUNNING_MS =
      Metrics.distribution(ChangeStreamMetrics.class, "partition_scheduled_to_running_ms");

  /** Counter for the active partition reads during the execution of the Connector. */
  public static final Counter ACTIVE_PARTITION_READ_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "active_partition_read_count");

  // -------------------
  // Data record metrics

  /**
   * Counter for the total number of data records identified during the execution of the Connector.
   */
  public static final Counter DATA_RECORD_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "data_record_count");

  /** Counter for the total number of queries issued during the execution of the Connector. */
  public static final Counter QUERY_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "query_count");

  /** Counter for record latencies [0, 1000) ms during the execution of the Connector. */
  public static final Counter DATA_RECORD_COMMITTED_TO_EMITTED_0MS_TO_1000MS_COUNT =
      Metrics.counter(
          ChangeStreamMetrics.class, "data_record_committed_to_emitted_0ms_to_1000ms_count");

  /** Counter for record latencies [1000, 3000) ms during the execution of the Connector. */
  public static final Counter DATA_RECORD_COMMITTED_TO_EMITTED_1000MS_TO_3000MS_COUNT =
      Metrics.counter(
          ChangeStreamMetrics.class, "data_record_committed_to_emitted_1000ms_to_3000ms_count");

  /** Counter for record latencies equal or above 3000ms during the execution of the Connector. */
  public static final Counter DATA_RECORD_COMMITTED_TO_EMITTED_3000MS_TO_INF_COUNT =
      Metrics.counter(
          ChangeStreamMetrics.class, "data_record_committed_to_emitted_3000ms_to_inf_count");

  // -------------------
  // Hearbeat record metrics

  /**
   * Counter for the total number of heartbeat records identified during the execution of the
   * Connector.
   */
  public static final Counter HEARTBEAT_RECORD_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "heartbeat_record_count");

  /**
   * If a metric is not within this set it will not be measured. Metrics enabled by default are
   * described in {@link ChangeStreamMetrics} default constructor.
   */
  private final Set<MetricName> enabledMetrics;

  /**
   * Constructs a ChangeStreamMetrics instance with the following metrics enabled by default.
   *
   * <ul>
   *   <li>{@link ChangeStreamMetrics#DATA_RECORD_COUNT}
   *   <li>{@link ChangeStreamMetrics#ACTIVE_PARTITION_READ_COUNT}
   *   <li>{@link ChangeStreamMetrics#QUERY_COUNT}
   *   <li>{@link ChangeStreamMetrics#DATA_RECORD_COMMITTED_TO_EMITTED_0MS_TO_1000MS_COUNT}
   *   <li>{@link ChangeStreamMetrics#DATA_RECORD_COMMITTED_TO_EMITTED_1000MS_TO_3000MS_COUNT}
   *   <li>{@link ChangeStreamMetrics#DATA_RECORD_COMMITTED_TO_EMITTED_3000MS_TO_INF_COUNT}
   * </ul>
   */
  public ChangeStreamMetrics() {
    enabledMetrics = new HashSet<>();
    enabledMetrics.add(DATA_RECORD_COUNT.getName());
    enabledMetrics.add(ACTIVE_PARTITION_READ_COUNT.getName());
    enabledMetrics.add(QUERY_COUNT.getName());
    enabledMetrics.add(DATA_RECORD_COMMITTED_TO_EMITTED_0MS_TO_1000MS_COUNT.getName());
    enabledMetrics.add(DATA_RECORD_COMMITTED_TO_EMITTED_1000MS_TO_3000MS_COUNT.getName());
    enabledMetrics.add(DATA_RECORD_COMMITTED_TO_EMITTED_3000MS_TO_INF_COUNT.getName());
  }

  /**
   * Constructs a ChangeStreamMetrics instance with the given metrics enabled.
   *
   * @param enabledMetrics metrics to be enabled during the Connector execution
   */
  public ChangeStreamMetrics(Set<MetricName> enabledMetrics) {
    this.enabledMetrics = enabledMetrics;
  }

  /**
   * Increments the {@link ChangeStreamMetrics#PARTITION_RECORD_COUNT} by 1 if the metric is
   * enabled.
   */
  public void incPartitionRecordCount() {
    inc(PARTITION_RECORD_COUNT);
  }

  /**
   * Increments the {@link ChangeStreamMetrics#PARTITION_RECORD_SPLIT_COUNT} by 1 if the metric is
   * enabled.
   */
  public void incPartitionRecordSplitCount() {
    inc(PARTITION_RECORD_SPLIT_COUNT);
  }

  /**
   * Increments the {@link ChangeStreamMetrics#PARTITION_RECORD_MERGE_COUNT} by 1 if the metric is
   * enabled.
   */
  public void incPartitionRecordMergeCount() {
    inc(PARTITION_RECORD_MERGE_COUNT);
  }

  /**
   * Adds measurement of an instance for the {@link
   * ChangeStreamMetrics#PARTITION_CREATED_TO_SCHEDULED_MS} if the metric is enabled.
   */
  public void updatePartitionCreatedToScheduled(Duration duration) {
    update(PARTITION_CREATED_TO_SCHEDULED_MS, duration.getMillis());
  }

  /**
   * Adds measurement of an instance for the {@link
   * ChangeStreamMetrics#PARTITION_SCHEDULED_TO_RUNNING_MS} if the metric is enabled.
   */
  public void updatePartitionScheduledToRunning(Duration duration) {
    update(PARTITION_SCHEDULED_TO_RUNNING_MS, duration.getMillis());
  }

  /**
   * Increments the {@link ChangeStreamMetrics#ACTIVE_PARTITION_READ_COUNT} by 1 if the metric is
   * enabled.
   */
  public void incActivePartitionReadCounter() {
    inc(ACTIVE_PARTITION_READ_COUNT);
  }

  /**
   * Decrements the {@link ChangeStreamMetrics#ACTIVE_PARTITION_READ_COUNT} by 1 if the metric is
   * enabled.
   */
  public void decActivePartitionReadCounter() {
    dec(ACTIVE_PARTITION_READ_COUNT);
  }

  /** Increments the {@link ChangeStreamMetrics#DATA_RECORD_COUNT} by 1 if the metric is enabled. */
  public void incDataRecordCounter() {
    inc(DATA_RECORD_COUNT);
  }

  /** Increments the {@link ChangeStreamMetrics#QUERY_COUNT} by 1 if the metric is enabled. */
  public void incQueryCounter() {
    inc(QUERY_COUNT);
  }

  /**
   * Increments the {@link ChangeStreamMetrics#HEARTBEAT_RECORD_COUNT} by 1 if the metric is
   * enabled.
   */
  public void incHeartbeatRecordCount() {
    inc(HEARTBEAT_RECORD_COUNT);
  }

  private void inc(Counter counter) {
    if (enabledMetrics.contains(counter.getName())) {
      counter.inc();
    }
  }

  private void dec(Counter counter) {
    if (enabledMetrics.contains(counter.getName())) {
      counter.dec();
    }
  }

  private void update(Distribution distribution, long value) {
    if (enabledMetrics.contains(distribution.getName())) {
      distribution.update(value);
    }
  }

  public void updateDataRecordCommittedToEmitted(Duration duration) {
    final long millis = duration.getMillis();
    if (millis < 1000) {
      inc(DATA_RECORD_COMMITTED_TO_EMITTED_0MS_TO_1000MS_COUNT);
    } else if (millis < 3000) {
      inc(DATA_RECORD_COMMITTED_TO_EMITTED_1000MS_TO_3000MS_COUNT);
    } else {
      inc(DATA_RECORD_COMMITTED_TO_EMITTED_3000MS_TO_INF_COUNT);
    }
  }
}
