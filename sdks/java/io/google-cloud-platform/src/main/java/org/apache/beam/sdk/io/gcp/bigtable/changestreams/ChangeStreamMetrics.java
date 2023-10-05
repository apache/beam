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
      Metrics.counter(ChangeStreamMetrics.class, "list_partitions_count");

  /**
   * Counter for the total number of partition splits / moves identified during the execution of the
   * Connector.
   */
  public static final Counter PARTITION_SPLIT_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "partition_record_split_count");

  /**
   * Counter for the total number of partition merges identified during the execution of the
   * Connector.
   */
  public static final Counter PARTITION_MERGE_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "partition_record_merge_count");

  /** Counter for the total number of partitions reconciled with continuation tokens. */
  public static final Counter PARTITION_RECONCILED_WITH_TOKEN_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "partition_reconciled_with_token_count");

  /** Counter for the total number of partitions reconciled without continuation tokens. */
  public static final Counter PARTITION_RECONCILED_WITHOUT_TOKEN_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "partition_reconciled_without_token_count");

  /** Counter for the total number of orphaned new partitions cleaned up. */
  public static final Counter ORPHANED_NEW_PARTITION_CLEANED_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "orphaned_new_partition_cleaned_count");

  // -------------------
  // Read change stream metrics

  /**
   * Counter for the total number of heartbeats identified during the execution of the Connector.
   */
  public static final Counter HEARTBEAT_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "heartbeat_count");

  /**
   * Counter for the total number of heartbeats identified during the execution of the Connector.
   */
  public static final Counter CLOSESTREAM_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "closestream_count");

  /**
   * Counter for the total number of ChangeStreamMutations that are initiated by users (not garbage
   * collection) identified during the execution of the Connector.
   */
  public static final Counter CHANGE_STREAM_MUTATION_USER_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "change_stream_mutation_user_count");

  /**
   * Counter for the total number of ChangeStreamMutations that are initiated by garbage collection
   * (not user initiated) identified during the execution of the Connector.
   */
  public static final Counter CHANGE_STREAM_MUTATION_GC_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "change_stream_mutation_gc_count");

  /** Distribution for measuring processing delay from commit timestamp. */
  public static final Distribution PROCESSING_DELAY_FROM_COMMIT_TIMESTAMP =
      Metrics.distribution(ChangeStreamMetrics.class, "processing_delay_from_commit_timestamp");

  /** Counter for the total number of active partitions being streamed. */
  public static final Counter PARTITION_STREAM_COUNT =
      Metrics.counter(ChangeStreamMetrics.class, "partition_stream_count");

  /**
   * Increments the {@link ChangeStreamMetrics#LIST_PARTITIONS_COUNT} by 1 if the metric is enabled.
   */
  public void incListPartitionsCount() {
    inc(LIST_PARTITIONS_COUNT);
  }

  /**
   * Increments the {@link ChangeStreamMetrics#PARTITION_SPLIT_COUNT} by 1 if the metric is enabled.
   */
  public void incPartitionSplitCount() {
    inc(PARTITION_SPLIT_COUNT);
  }

  /**
   * Increments the {@link ChangeStreamMetrics#PARTITION_MERGE_COUNT} by 1 if the metric is enabled.
   */
  public void incPartitionMergeCount() {
    inc(PARTITION_MERGE_COUNT);
  }

  /** Increments the {@link ChangeStreamMetrics#PARTITION_RECONCILED_WITH_TOKEN_COUNT} by 1. */
  public void incPartitionReconciledWithTokenCount() {
    inc(PARTITION_RECONCILED_WITH_TOKEN_COUNT);
  }

  /** Increments the {@link ChangeStreamMetrics#PARTITION_RECONCILED_WITHOUT_TOKEN_COUNT} by 1. */
  public void incPartitionReconciledWithoutTokenCount() {
    inc(PARTITION_RECONCILED_WITHOUT_TOKEN_COUNT);
  }

  /** Increments the {@link ChangeStreamMetrics#ORPHANED_NEW_PARTITION_CLEANED_COUNT} by 1. */
  public void incOrphanedNewPartitionCleanedCount() {
    inc(ORPHANED_NEW_PARTITION_CLEANED_COUNT);
  }

  /** Increments the {@link ChangeStreamMetrics#HEARTBEAT_COUNT} by 1 if the metric is enabled. */
  public void incHeartbeatCount() {
    inc(HEARTBEAT_COUNT);
  }

  /** Increments the {@link ChangeStreamMetrics#CLOSESTREAM_COUNT} by 1 if the metric is enabled. */
  public void incClosestreamCount() {
    inc(CLOSESTREAM_COUNT);
  }

  /**
   * Increments the {@link ChangeStreamMetrics#CHANGE_STREAM_MUTATION_USER_COUNT} by 1 if the metric
   * is enabled.
   */
  public void incChangeStreamMutationUserCounter() {
    inc(CHANGE_STREAM_MUTATION_USER_COUNT);
  }

  /**
   * Increments the {@link ChangeStreamMetrics#CHANGE_STREAM_MUTATION_GC_COUNT} by 1 if the metric
   * is enabled.
   */
  public void incChangeStreamMutationGcCounter() {
    inc(CHANGE_STREAM_MUTATION_GC_COUNT);
  }

  /** Increments the {@link ChangeStreamMetrics#PARTITION_STREAM_COUNT} by 1. */
  public void incPartitionStreamCount() {
    inc(PARTITION_STREAM_COUNT);
  }

  /** Decrements the {@link ChangeStreamMetrics#PARTITION_STREAM_COUNT} by 1. */
  public void decPartitionStreamCount() {
    dec(PARTITION_STREAM_COUNT);
  }

  /**
   * Adds measurement of an instance for the {@link
   * ChangeStreamMetrics#PROCESSING_DELAY_FROM_COMMIT_TIMESTAMP}.
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
