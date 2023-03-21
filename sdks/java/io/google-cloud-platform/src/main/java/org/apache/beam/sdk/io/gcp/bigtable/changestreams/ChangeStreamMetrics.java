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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;

/** Class to aggregate metrics related functionality. */
@Internal
public class ChangeStreamMetrics implements Serializable {
  private static final long serialVersionUID = 7298901109362981596L;
  // ------------------------
  // Partition record metrics

  /**
   * Counter for the total number of partitions identified during the execution of the Connector.
   */
  public static final Counter LIST_PARTITIONS_COUNT =
      Metrics.counter(
          org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics.class,
          "list_partitions_count");

  // -------------------
  // Read change stream metrics

  /**
   * Counter for the total number of heartbeats identified during the execution of the Connector.
   */
  public static final Counter HEARTBEAT_COUNT =
      Metrics.counter(
          org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics.class,
          "heartbeat_count");

  /**
   * Counter for the total number of heartbeats identified during the execution of the Connector.
   */
  public static final Counter CLOSESTREAM_COUNT =
      Metrics.counter(
          org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics.class,
          "closestream_count");

  /**
   * Counter for the total number of ChangeStreamMutations that are initiated by users (not garbage
   * collection) identified during the execution of the Connector.
   */
  public static final Counter CHANGE_STREAM_MUTATION_USER_COUNT =
      Metrics.counter(
          org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics.class,
          "change_stream_mutation_user_count");

  /**
   * Counter for the total number of ChangeStreamMutations that are initiated by garbage collection
   * (not user initiated) identified during the execution of the Connector.
   */
  public static final Counter CHANGE_STREAM_MUTATION_GC_COUNT =
      Metrics.counter(
          org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics.class,
          "change_stream_mutation_gc_count");

  /** Distribution for measuring processing delay from commit timestamp. */
  public static final Distribution PROCESSING_DELAY_FROM_COMMIT_TIMESTAMP =
      Metrics.distribution(
          org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics.class,
          "processing_delay_from_commit_timestamp");

  /** Counter for the total number of active partitions being streamed. */
  public static final Counter PARTITION_STREAM_COUNT =
      Metrics.counter(
          org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics.class,
          "partition_stream_count");

  /**
   * Increments the {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics#LIST_PARTITIONS_COUNT} by
   * 1 if the metric is enabled.
   */
  public void incListPartitionsCount() {
    inc(LIST_PARTITIONS_COUNT);
  }

  /**
   * Increments the {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics#HEARTBEAT_COUNT} by 1 if
   * the metric is enabled.
   */
  public void incHeartbeatCount() {
    inc(HEARTBEAT_COUNT);
  }

  /**
   * Increments the {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics#CLOSESTREAM_COUNT} by 1
   * if the metric is enabled.
   */
  public void incClosestreamCount() {
    inc(CLOSESTREAM_COUNT);
  }

  /**
   * Increments the {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics#CHANGE_STREAM_MUTATION_USER_COUNT}
   * by 1 if the metric is enabled.
   */
  public void incChangeStreamMutationUserCounter() {
    inc(CHANGE_STREAM_MUTATION_USER_COUNT);
  }

  /**
   * Increments the {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics#CHANGE_STREAM_MUTATION_GC_COUNT}
   * by 1 if the metric is enabled.
   */
  public void incChangeStreamMutationGcCounter() {
    inc(CHANGE_STREAM_MUTATION_GC_COUNT);
  }

  /**
   * Increments the {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics#PARTITION_STREAM_COUNT}
   * by 1.
   */
  public void incPartitionStreamCount() {
    inc(PARTITION_STREAM_COUNT);
  }

  /**
   * Decrements the {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics#PARTITION_STREAM_COUNT}
   * by 1.
   */
  public void decPartitionStreamCount() {
    dec(PARTITION_STREAM_COUNT);
  }

  /**
   * Adds measurement of an instance for the {@link
   * org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics#PROCESSING_DELAY_FROM_COMMIT_TIMESTAMP}.
   */
  public void updateProcessingDelayFromCommitTimestamp(long durationInMilli) {
    update(PROCESSING_DELAY_FROM_COMMIT_TIMESTAMP, durationInMilli);
  }

  private void inc(Counter counter) {
    counter.inc();
  }

  private void dec(Counter counter) {
    counter.dec();
  }

  private void update(Distribution distribution, long value) {
    distribution.update(value);
  }
}
