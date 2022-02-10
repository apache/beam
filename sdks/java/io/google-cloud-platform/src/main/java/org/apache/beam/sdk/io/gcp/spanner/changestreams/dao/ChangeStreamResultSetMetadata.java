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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dao;

import com.google.cloud.Timestamp;
import org.joda.time.Duration;

/**
 * Represents telemetry metadata gathered during the consumption of a change stream query. Within
 * this class the caller will be able to retrieve the following information:
 *
 * <ul>
 *   <li>The timestamp at which the query first started.
 *   <li>The timestamp at which a record streaming started.
 *   <li>The timestamp at which a record streaming ended.
 *   <li>The timestamp at which a record was read by the caller.
 *   <li>The total time for streaming all records within the query.
 *   <li>The total number of records streamed within the query.
 * </ul>
 */
public class ChangeStreamResultSetMetadata {
  private final Timestamp queryStartedAt;
  private final Timestamp recordStreamStartedAt;
  private final Timestamp recordStreamEndedAt;
  private final Timestamp recordReadAt;
  private final Duration totalStreamDuration;
  private final long numberOfRecordsRead;

  /** Constructs a change stream result set metadata with the given telemetry information. */
  ChangeStreamResultSetMetadata(
      Timestamp queryStartedAt,
      Timestamp recordStreamStartedAt,
      Timestamp recordStreamEndedAt,
      Timestamp recordReadAt,
      Duration totalStreamDuration,
      long numberOfRecordsRead) {
    this.queryStartedAt = queryStartedAt;
    this.recordStreamStartedAt = recordStreamStartedAt;
    this.recordStreamEndedAt = recordStreamEndedAt;
    this.recordReadAt = recordReadAt;
    this.totalStreamDuration = totalStreamDuration;
    this.numberOfRecordsRead = numberOfRecordsRead;
  }

  /**
   * Returns the timestamp at which the change stream query for a {@link ChangeStreamResultSet}
   * first started.
   */
  public Timestamp getQueryStartedAt() {
    return queryStartedAt;
  }

  /** Returns the timestamp at which a record first started to be streamed. */
  public Timestamp getRecordStreamStartedAt() {
    return recordStreamStartedAt;
  }

  /** Returns the timestamp at which a record finished to be streamed. */
  public Timestamp getRecordStreamEndedAt() {
    return recordStreamEndedAt;
  }

  /** Returns the timestamp at which a record was read from the {@link ChangeStreamResultSet}. */
  public Timestamp getRecordReadAt() {
    return recordReadAt;
  }

  /** Returns the total stream duration of change stream records so far. */
  public Duration getTotalStreamDuration() {
    return totalStreamDuration;
  }

  /** Returns the total number of records read from the change stream so far. */
  public long getNumberOfRecordsRead() {
    return numberOfRecordsRead;
  }
}
