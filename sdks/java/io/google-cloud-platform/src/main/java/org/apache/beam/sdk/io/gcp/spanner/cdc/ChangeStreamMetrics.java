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
package org.apache.beam.sdk.io.gcp.spanner.cdc;

import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.ReadChangeStream;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;

public class ChangeStreamMetrics {

  // ------------------------
  // Partition record metrics

  public static final Counter PARTITION_RECORD_COUNT =
      Metrics.counter(ReadChangeStream.class, "partition_record_count");

  public static final Counter PARTITION_RECORD_SPLIT_COUNT =
      Metrics.counter(ReadChangeStream.class, "partition_record_split_count");

  public static final Counter PARTITION_RECORD_MERGE_COUNT =
      Metrics.counter(ReadChangeStream.class, "partition_record_merge_count");

  public static final Counter PARTITIONS_RUNNING_COUNTER =
      Metrics.counter(ReadChangeStream.class, "partitions_running_count");

  public static final Distribution INITIAL_PARTITION_CREATED_TO_SCHEDULED_MS =
      Metrics.distribution(ReadChangeStream.class, "initial_partition_created_to_scheduled_ms");

  public static final Distribution PARTITION_CREATED_TO_SCHEDULED_MS =
      Metrics.distribution(ReadChangeStream.class, "partition_created_to_scheduled_ms");

  public static final Distribution PARTITION_SCHEDULED_TO_RUNNING_MS =
      Metrics.distribution(ReadChangeStream.class, "partition_scheduled_to_running_ms");

  public static final Distribution PARTITION_RUNNING_TO_FINISHED_MS =
      Metrics.distribution(ReadChangeStream.class, "partition_running_to_finished_ms");

  // -------------------
  // Data record metrics

  public static final Counter DATA_RECORD_COUNTER =
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

  // ----
  // DAO

  public static final Distribution DAO_COUNT_PARTITIONS_MS =
      Metrics.distribution(ReadChangeStream.class, "dao_count_partitions_ms");

  // ----
  // Tracing Labels

  public static final String PARTITION_ID_ATTRIBUTE_LABEL = "PartitionID";
}
