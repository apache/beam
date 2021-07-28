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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.DATA_RECORD_COMMITTED_TO_EMITTED_0MS_TO_500MS_COUNT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.DATA_RECORD_COMMITTED_TO_EMITTED_1000MS_TO_3000MS_COUNT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.DATA_RECORD_COMMITTED_TO_EMITTED_3000MS_TO_INF_COUNT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.DATA_RECORD_COMMITTED_TO_EMITTED_500MS_TO_1000MS_COUNT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.DATA_RECORD_COMMITTED_TO_EMITTED_MS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.DATA_RECORD_COUNTER;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.DATA_RECORD_STREAM_0MS_TO_100MS_COUNTER;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.DATA_RECORD_STREAM_1000MS_TO_5000MS_COUNTER;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.DATA_RECORD_STREAM_100MS_TO_500MS_COUNTER;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.DATA_RECORD_STREAM_5000MS_TO_INF_COUNTER;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.DATA_RECORD_STREAM_500MS_TO_1000MS_COUNTER;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.DATA_RECORD_STREAM_MS;

import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecordMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostProcessingMetricsDoFn extends DoFn<DataChangeRecord, DataChangeRecord>
    implements Serializable {

  private static final long serialVersionUID = -1515578871387565606L;
  private static final Logger LOG = LoggerFactory.getLogger(PostProcessingMetricsDoFn.class);
  private static final long COMMITTED_TO_EMITTED_THRESHOLD_MS = 100_000;
  private static final long STREAM_THRESHOLD_MS = 5_000;

  @ProcessElement
  public void processElement(
      @Element DataChangeRecord dataChangeRecord, OutputReceiver<DataChangeRecord> receiver) {
    final Instant commitInstant =
        new Instant(dataChangeRecord.getCommitTimestamp().toSqlTimestamp().getTime());

    DATA_RECORD_COUNTER.inc();
    measureCommitTimestampToEmittedMillis(dataChangeRecord);
    measureStreamMillis(dataChangeRecord);

    receiver.outputWithTimestamp(dataChangeRecord, commitInstant);
  }

  private void measureCommitTimestampToEmittedMillis(DataChangeRecord dataChangeRecord) {
    final com.google.cloud.Timestamp emittedTimestamp = com.google.cloud.Timestamp.now();
    final com.google.cloud.Timestamp commitTimestamp = dataChangeRecord.getCommitTimestamp();
    final long commitedToEmittedMillis =
        new Duration(
                commitTimestamp.toSqlTimestamp().getTime(),
                emittedTimestamp.toSqlTimestamp().getTime())
            .getMillis();

    DATA_RECORD_COMMITTED_TO_EMITTED_MS.update(commitedToEmittedMillis);
    if (commitedToEmittedMillis < 500) {
      DATA_RECORD_COMMITTED_TO_EMITTED_0MS_TO_500MS_COUNT.inc();
    } else if (commitedToEmittedMillis < 1000) {
      DATA_RECORD_COMMITTED_TO_EMITTED_500MS_TO_1000MS_COUNT.inc();
    } else if (commitedToEmittedMillis < 3000) {
      DATA_RECORD_COMMITTED_TO_EMITTED_1000MS_TO_3000MS_COUNT.inc();
    } else {
      DATA_RECORD_COMMITTED_TO_EMITTED_3000MS_TO_INF_COUNT.inc();
    }

    if (commitedToEmittedMillis > COMMITTED_TO_EMITTED_THRESHOLD_MS) {
      LOG.info(
          "Data record took "
              + commitedToEmittedMillis
              + "ms to be emitted: "
              + dataChangeRecord.getMetadata());
    }
  }

  private void measureStreamMillis(DataChangeRecord dataChangeRecord) {
    final ChangeStreamRecordMetadata metadata = dataChangeRecord.getMetadata();
    final com.google.cloud.Timestamp streamStartedAt = metadata.getRecordStreamStartedAt();
    final com.google.cloud.Timestamp streamEndedAt = metadata.getRecordStreamEndedAt();
    final long streamMillis =
        new Duration(
                streamStartedAt.toSqlTimestamp().getTime(),
                streamEndedAt.toSqlTimestamp().getTime())
            .getMillis();

    DATA_RECORD_STREAM_MS.update(streamMillis);
    if (streamMillis < 100) {
      DATA_RECORD_STREAM_0MS_TO_100MS_COUNTER.inc();
    } else if (streamMillis < 500) {
      DATA_RECORD_STREAM_100MS_TO_500MS_COUNTER.inc();
    } else if (streamMillis < 1000) {
      DATA_RECORD_STREAM_500MS_TO_1000MS_COUNTER.inc();
    } else if (streamMillis < 5000) {
      DATA_RECORD_STREAM_1000MS_TO_5000MS_COUNTER.inc();
    } else {
      DATA_RECORD_STREAM_5000MS_TO_INF_COUNTER.inc();
    }

    if (streamMillis > STREAM_THRESHOLD_MS) {
      LOG.info("Data record took " + streamMillis + "ms to be streamed: " + metadata);
    }
  }
}
