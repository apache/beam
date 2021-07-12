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

public class CdcMetrics {

  public static final Counter PARTITIONS_DETECTED_COUNTER =
      Metrics.counter(ReadChangeStream.class, "partitions_detected_count");

  public static final Counter PARTITION_SPLIT_COUNTER =
      Metrics.counter(ReadChangeStream.class, "partition_split_count");

  public static final Counter PARTITION_MERGE_COUNTER =
      Metrics.counter(ReadChangeStream.class, "partition_merge_count");

  public static final Distribution INITIAL_PARTITION_CREATED_TO_SCHEDULED_MS =
      Metrics.distribution(ReadChangeStream.class, "initial_partition_created_to_scheduled_ms");

  public static final Distribution PARTITION_CREATED_TO_SCHEDULED_MS =
      Metrics.distribution(ReadChangeStream.class, "partition_created_to_scheduled_ms");

  // TODO(zoc): add this correctly
  public static final Distribution WATERMARK_TO_LATEST_RECORD_COMMIT_TIMESTAMP_MS =
      Metrics.distribution(
          ReadChangeStream.class, "watermark_to_latest_record_commit_timestamp_ms");

  public static final Distribution RECORD_COMMIT_TIMESTAMP_TO_READ_MS =
      Metrics.distribution(ReadChangeStream.class, "record_commit_timestamp_to_read_ms");

  public static final Distribution RECORD_READ_TO_EMITTED_MS =
      Metrics.distribution(ReadChangeStream.class, "record_read_to_emitted_ms");

  public static final Distribution RECORD_COMMIT_TIMESTAMP_TO_EMITTED_MS =
      Metrics.distribution(ReadChangeStream.class, "record_commit_timestamp_to_emitted_ms");

  public static final Counter DATA_RECORD_COUNTER =
      Metrics.counter(ReadChangeStream.class, "data_record_count");
}
