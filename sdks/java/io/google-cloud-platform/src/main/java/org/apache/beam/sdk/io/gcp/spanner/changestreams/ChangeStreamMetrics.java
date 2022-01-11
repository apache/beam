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

  // ----
  // Tracing Labels

  /** Cloud Tracing label for Partition Tokens. */
  public static final String PARTITION_ID_ATTRIBUTE_LABEL = "PartitionID";

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

  // -------------------
  // Data record metrics

  /**
   * Counter for the total number of data records identified during the execution of the Connector.
   */
  public static final Counter DATA_RECORD_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "data_record_count");

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
   * Constructs a ChangeStreamMetrics instance with the following metrics enabled by default: {@link
   * ChangeStreamMetrics#PARTITION_RECORD_COUNT} and {@link ChangeStreamMetrics#DATA_RECORD_COUNT}.
   */
  public ChangeStreamMetrics() {
    enabledMetrics = new HashSet<>();
    enabledMetrics.add(PARTITION_RECORD_COUNT.getName());
    enabledMetrics.add(DATA_RECORD_COUNT.getName());
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

  /** Increments the {@link ChangeStreamMetrics#DATA_RECORD_COUNT} by 1 if the metric is enabled. */
  public void incDataRecordCounter() {
    inc(DATA_RECORD_COUNT);
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

  private void update(Distribution distribution, long value) {
    if (enabledMetrics.contains(distribution.getName())) {
      distribution.update(value);
    }
  }
}
