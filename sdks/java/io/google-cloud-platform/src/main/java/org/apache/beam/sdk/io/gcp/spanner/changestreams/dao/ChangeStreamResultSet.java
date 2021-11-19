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
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import org.joda.time.Duration;

public class ChangeStreamResultSet implements AutoCloseable {

  private final ResultSet resultSet;
  private Timestamp queryStartedAt;
  private Timestamp recordStreamStartedAt;
  private Timestamp recordStreamEndedAt;
  private Timestamp recordReadAt;
  private Duration totalStreamDuration;
  private long numberOfRecordsRead;

  ChangeStreamResultSet(ResultSet resultSet) {
    this.resultSet = resultSet;
    this.totalStreamDuration = Duration.ZERO;
  }

  public boolean next() {
    if (queryStartedAt == null) {
      queryStartedAt = Timestamp.now();
    }
    recordStreamStartedAt = Timestamp.now();
    final boolean hasNext = resultSet.next();
    numberOfRecordsRead++;
    recordStreamEndedAt = Timestamp.now();
    totalStreamDuration =
        totalStreamDuration.withDurationAdded(
            new Duration(
                recordStreamStartedAt.toSqlTimestamp().getTime(),
                recordStreamEndedAt.toSqlTimestamp().getTime()),
            1);
    return hasNext;
  }

  public Struct getCurrentRowAsStruct() {
    recordReadAt = Timestamp.now();
    return resultSet.getCurrentRowAsStruct();
  }

  public ChangeStreamResultSetMetadata getMetadata() {
    return new ChangeStreamResultSetMetadata(
        queryStartedAt,
        recordStreamStartedAt,
        recordStreamEndedAt,
        recordReadAt,
        totalStreamDuration,
        numberOfRecordsRead);
  }

  @Override
  public void close() {
    resultSet.close();
  }
}
