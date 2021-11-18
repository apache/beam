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

public class ChangeStreamResultSetMetadata {
  private final Timestamp queryStartedAt;
  private final Timestamp recordStreamStartedAt;
  private final Timestamp recordStreamEndedAt;
  private final Timestamp recordReadAt;
  private final Duration totalStreamDuration;
  private final long numberOfRecordsRead;

  public ChangeStreamResultSetMetadata(
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

  public Timestamp getQueryStartedAt() {
    return queryStartedAt;
  }

  public Timestamp getRecordStreamStartedAt() {
    return recordStreamStartedAt;
  }

  public Timestamp getRecordStreamEndedAt() {
    return recordStreamEndedAt;
  }

  public Timestamp getRecordReadAt() {
    return recordReadAt;
  }

  public Duration getTotalStreamDuration() {
    return totalStreamDuration;
  }

  public long getNumberOfRecordsRead() {
    return numberOfRecordsRead;
  }
}
