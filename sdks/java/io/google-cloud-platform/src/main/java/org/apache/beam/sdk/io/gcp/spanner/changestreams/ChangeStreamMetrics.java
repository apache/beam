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
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.ReadChangeStream;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.Metrics;
import org.joda.time.Duration;

public class ChangeStreamMetrics implements Serializable {

  private static final long serialVersionUID = 8187140831756972470L;

  // ----
  // Tracing Labels

  public static final String PARTITION_ID_ATTRIBUTE_LABEL = "PartitionID";

  // ------------------------
  // Partition record metrics

  public static final Counter PARTITION_RECORD_COUNT =
      Metrics.counter(ReadChangeStream.class, "partition_record_count");
  public static final Counter PARTITION_RECORD_SPLIT_COUNT =
      Metrics.counter(ReadChangeStream.class, "partition_record_split_count");
  public static final Counter PARTITION_RECORD_MERGE_COUNT =
      Metrics.counter(ReadChangeStream.class, "partition_record_merge_count");
  public static final Distribution PARTITION_CREATED_TO_SCHEDULED_MS =
      Metrics.distribution(ReadChangeStream.class, "partition_created_to_scheduled_ms");
  public static final Distribution PARTITION_SCHEDULED_TO_RUNNING_MS =
      Metrics.distribution(ReadChangeStream.class, "partition_scheduled_to_running_ms");
  public static final Distribution PARTITION_RUNNING_TO_FINISHED_MS =
      Metrics.distribution(ReadChangeStream.class, "partition_running_to_finished_ms");

  // -------------------
  // Data record metrics

  public static final Counter DATA_RECORD_COUNT =
      Metrics.counter(ReadChangeStream.class, "data_record_count");
  public static final Counter DATA_RECORD_COMMITTED_TO_EMITTED_0MS_TO_500MS_COUNT =
      Metrics.counter(
          ReadChangeStream.class, "data_record_committed_to_emitted_0ms_to_500ms_count");
  public static final Counter DATA_RECORD_COMMITTED_TO_EMITTED_500MS_TO_1000MS_COUNT =
      Metrics.counter(
          ReadChangeStream.class, "data_record_committed_to_emitted_500ms_to_1000ms_count");
  public static final Counter DATA_RECORD_COMMITTED_TO_EMITTED_1000MS_TO_3000MS_COUNT =
      Metrics.counter(
          ReadChangeStream.class, "data_record_committed_to_emitted_1000ms_to_3000ms_count");
  public static final Counter DATA_RECORD_COMMITTED_TO_EMITTED_3000MS_TO_INF_COUNT =
      Metrics.counter(
          ReadChangeStream.class, "data_record_committed_to_emitted_3000ms_to_inf_count");
  public static final Distribution DATA_RECORD_COMMITTED_TO_EMITTED_MS =
      Metrics.distribution(ReadChangeStream.class, "data_record_committed_to_emitted_ms");
  public static final Counter DATA_RECORD_STREAM_0MS_TO_100MS_COUNTER =
      Metrics.counter(ReadChangeStream.class, "data_record_stream_0ms_to_100ms_count");
  public static final Counter DATA_RECORD_STREAM_100MS_TO_500MS_COUNTER =
      Metrics.counter(ReadChangeStream.class, "data_record_stream_100ms_to_500ms_count");
  public static final Counter DATA_RECORD_STREAM_500MS_TO_1000MS_COUNTER =
      Metrics.counter(ReadChangeStream.class, "data_record_stream_500ms_to_1000ms_count");
  public static final Counter DATA_RECORD_STREAM_1000MS_TO_5000MS_COUNTER =
      Metrics.counter(ReadChangeStream.class, "data_record_stream_1000ms_to_5000ms_count");
  public static final Counter DATA_RECORD_STREAM_5000MS_TO_INF_COUNTER =
      Metrics.counter(ReadChangeStream.class, "data_record_stream_5000ms_to_inf_count");
  public static final Distribution DATA_RECORD_STREAM_MS =
      Metrics.distribution(ReadChangeStream.class, "data_record_stream_ms");

  // -------------------
  // Hearbeat record metrics

  public static final Counter HEARTBEAT_RECORD_COUNT =
      Metrics.counter(ReadChangeStream.class, "heartbeat_record_count");

  // ----
  // DAO

  public static final Distribution DAO_COUNT_PARTITIONS_MS =
      Metrics.distribution(ReadChangeStream.class, "dao_count_partitions_ms");
  public static final Distribution DAO_GET_MIN_WATERMARK_MS =
      Metrics.distribution(ReadChangeStream.class, "dao_get_min_current_watermark_ms");

  private final Set<MetricName> enabledMetrics;

  public ChangeStreamMetrics() {
    enabledMetrics = new HashSet<>();
    enabledMetrics.add(PARTITION_RECORD_COUNT.getName());
    enabledMetrics.add(DATA_RECORD_COUNT.getName());
  }

  public ChangeStreamMetrics(Set<MetricName> enabledMetrics) {
    this.enabledMetrics = enabledMetrics;
  }

  public void incPartitionRecordCount() {
    inc(PARTITION_RECORD_COUNT);
  }

  public void incPartitionRecordSplitCount() {
    inc(PARTITION_RECORD_SPLIT_COUNT);
  }

  public void incPartitionRecordMergeCount() {
    inc(PARTITION_RECORD_MERGE_COUNT);
  }

  public void updatePartitionCreatedToScheduled(Duration duration) {
    update(PARTITION_CREATED_TO_SCHEDULED_MS, duration.getMillis());
  }

  public void updatePartitionScheduledToRunning(Duration duration) {
    update(PARTITION_SCHEDULED_TO_RUNNING_MS, duration.getMillis());
  }

  public void updatePartitionRunningToFinished(Duration duration) {
    update(PARTITION_RUNNING_TO_FINISHED_MS, duration.getMillis());
  }

  public void incDataRecordCounter() {
    inc(DATA_RECORD_COUNT);
  }

  public void updateDataRecordCommittedToEmitted(Duration duration) {
    final long millis = duration.getMillis();
    if (millis < 500) {
      inc(DATA_RECORD_COMMITTED_TO_EMITTED_0MS_TO_500MS_COUNT);
    } else if (millis < 1000) {
      inc(DATA_RECORD_COMMITTED_TO_EMITTED_500MS_TO_1000MS_COUNT);
    } else if (millis < 3000) {
      inc(DATA_RECORD_COMMITTED_TO_EMITTED_1000MS_TO_3000MS_COUNT);
    } else {
      inc(DATA_RECORD_COMMITTED_TO_EMITTED_3000MS_TO_INF_COUNT);
    }
    update(DATA_RECORD_COMMITTED_TO_EMITTED_MS, millis);
  }

  public void updateDataRecordStream(Duration duration) {
    final long millis = duration.getMillis();
    if (millis < 100) {
      inc(DATA_RECORD_STREAM_0MS_TO_100MS_COUNTER);
    } else if (millis < 500) {
      inc(DATA_RECORD_STREAM_100MS_TO_500MS_COUNTER);
    } else if (millis < 1000) {
      inc(DATA_RECORD_STREAM_500MS_TO_1000MS_COUNTER);
    } else if (millis < 5000) {
      inc(DATA_RECORD_STREAM_1000MS_TO_5000MS_COUNTER);
    } else {
      inc(DATA_RECORD_STREAM_5000MS_TO_INF_COUNTER);
    }
    update(DATA_RECORD_STREAM_MS, millis);
  }

  public void incHearbeatRecordCount() {
    inc(HEARTBEAT_RECORD_COUNT);
  }

  public void updateDaoCountPartitions(Duration duration) {
    update(DAO_COUNT_PARTITIONS_MS, duration.getMillis());
  }

  public void updateDaoGetMinWatermark(Duration duration) {
    update(DAO_GET_MIN_WATERMARK_MS, duration.getMillis());
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
