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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.CdcMetrics.DATA_RECORD_COUNTER;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.CdcMetrics.RECORD_COMMIT_TIMESTAMP_TO_EMITTED_MS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.CdcMetrics.RECORD_COMMIT_TIMESTAMP_TO_READ_MS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.CdcMetrics.RECORD_READ_TO_EMITTED_MS;

import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord.Metadata;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class PostProcessingMetricsDoFn extends DoFn<DataChangeRecord, DataChangeRecord>
    implements Serializable {

  @ProcessElement
  public void processElement(
      @Element DataChangeRecord dataChangeRecord, OutputReceiver<DataChangeRecord> receiver) {
    final Instant now = new Instant();
    final Metadata metadata = dataChangeRecord.getMetadata();
    final Instant readInstant = new Instant(metadata.getReadAt().toSqlTimestamp().getTime());
    final Instant commitInstant =
        new Instant(dataChangeRecord.getCommitTimestamp().toSqlTimestamp().getTime());

    RECORD_COMMIT_TIMESTAMP_TO_READ_MS.update(new Duration(commitInstant, readInstant).getMillis());
    RECORD_READ_TO_EMITTED_MS.update(new Duration(readInstant, now).getMillis());
    RECORD_COMMIT_TIMESTAMP_TO_EMITTED_MS.update(new Duration(commitInstant, now).getMillis());
    DATA_RECORD_COUNTER.inc();

    receiver.outputWithTimestamp(dataChangeRecord, commitInstant);
  }
}
