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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn;

import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecordMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DoFn class to gather metrics about the emitted {@link DataChangeRecord}s. It will simply
 * delegate the metrics gathering to the {@link ChangeStreamMetrics}. The metrics measured in this
 * component are:
 *
 * <ol>
 *   <li>The number of data records emitted.
 *   <li>The latency between a record's Cloud Spanner commit timestamp and the time it reached this
 *       component (referred as emit timestamp).
 *   <li>The streaming latency of a record from the change stream query.
 * </ol>
 */
public class PostProcessingMetricsDoFn extends DoFn<DataChangeRecord, DataChangeRecord>
    implements Serializable {

  private static final long serialVersionUID = -1515578871387565606L;
  private static final Logger LOG = LoggerFactory.getLogger(PostProcessingMetricsDoFn.class);
  private static final long COMMITTED_TO_EMITTED_THRESHOLD_MS = 100_000;
  private static final long STREAM_THRESHOLD_MS = 5_000;

  private final ChangeStreamMetrics metrics;

  public PostProcessingMetricsDoFn(ChangeStreamMetrics metrics) {
    this.metrics = metrics;
  }

  /**
   * Stage to measure a data records latencies and metrics. The metrics gathered are:
   *
   * <ol>
   *   <li>The number of data records emitted.
   *   <li>The latency between a record's Cloud Spanner commit timestamp and the time it reached
   *       this component (referred as emit timestamp).
   *   <li>The streaming latency of a record from the change stream query.
   * </ol>
   *
   * After measurement the record is re-emitted to the next stage.
   *
   * @param dataChangeRecord the record to gather metrics for
   * @param receiver the output receiver of the {@link PostProcessingMetricsDoFn} SDF
   */
  @ProcessElement
  public void processElement(
      @Element DataChangeRecord dataChangeRecord, OutputReceiver<DataChangeRecord> receiver) {
    final Instant commitInstant =
        new Instant(dataChangeRecord.getCommitTimestamp().toSqlTimestamp().getTime());

    metrics.incDataRecordCounter();
    measureCommitTimestampToEmittedMillis(dataChangeRecord);
    measureStreamMillis(dataChangeRecord);

    receiver.outputWithTimestamp(dataChangeRecord, commitInstant);
  }

  private void measureCommitTimestampToEmittedMillis(DataChangeRecord dataChangeRecord) {
    final com.google.cloud.Timestamp emittedTimestamp = com.google.cloud.Timestamp.now();
    final com.google.cloud.Timestamp commitTimestamp = dataChangeRecord.getCommitTimestamp();
    final Duration committedToEmitted =
        new Duration(
            commitTimestamp.toSqlTimestamp().getTime(),
            emittedTimestamp.toSqlTimestamp().getTime());
    final long commitedToEmittedMillis = committedToEmitted.getMillis();

    metrics.updateDataRecordCommittedToEmitted(committedToEmitted);

    if (commitedToEmittedMillis > COMMITTED_TO_EMITTED_THRESHOLD_MS) {
      LOG.debug(
          "Data record took {}ms to be emitted: {}",
          commitedToEmittedMillis,
          dataChangeRecord.getMetadata());
    }
  }

  private void measureStreamMillis(DataChangeRecord dataChangeRecord) {
    final ChangeStreamRecordMetadata metadata = dataChangeRecord.getMetadata();
    final com.google.cloud.Timestamp streamStartedAt = metadata.getRecordStreamStartedAt();
    final com.google.cloud.Timestamp streamEndedAt = metadata.getRecordStreamEndedAt();
    final Duration streamDuration =
        new Duration(
            streamStartedAt.toSqlTimestamp().getTime(), streamEndedAt.toSqlTimestamp().getTime());
    final long streamMillis = streamDuration.getMillis();

    if (streamMillis > STREAM_THRESHOLD_MS) {
      LOG.debug("Data record took {}ms to be streamed: {}", streamMillis, metadata);
    }
  }
}
