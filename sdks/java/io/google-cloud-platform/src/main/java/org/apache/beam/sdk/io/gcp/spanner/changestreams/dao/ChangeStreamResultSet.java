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

/**
 * Decorator class over a {@link ResultSet} that provides telemetry for the streamed records. It
 * will be returned for a change stream query. By using this class one can obtain the following
 * metadata:
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
public class ChangeStreamResultSet implements AutoCloseable {

  private final ResultSet resultSet;
  private Timestamp queryStartedAt;
  private Timestamp recordStreamStartedAt;
  private Timestamp recordStreamEndedAt;
  private Timestamp recordReadAt;
  private Duration totalStreamDuration;
  private long numberOfRecordsRead;

  /**
   * Constructs a change stream result set on top of the received {@link ResultSet}.
   *
   * @param resultSet the {@link ResultSet} to be decorated
   */
  ChangeStreamResultSet(ResultSet resultSet) {
    this.resultSet = resultSet;
    this.queryStartedAt = Timestamp.MIN_VALUE;
    this.recordStreamStartedAt = Timestamp.MIN_VALUE;
    this.recordStreamEndedAt = Timestamp.MIN_VALUE;
    this.recordReadAt = Timestamp.MIN_VALUE;
    this.totalStreamDuration = Duration.ZERO;
    this.numberOfRecordsRead = 0L;
  }

  /**
   * Moves the pointer to the next record in the {@link ResultSet} if there is one. It also gathers
   * metrics for the next record, such as:
   *
   * <ul>
   *   <li>If this is the first record consumed, updates the time at which the query started.
   *   <li>The timestamp at which a record streaming started.
   *   <li>The timestamp at which a record streaming ended.
   *   <li>Increments the total time for streaming the all the records so far.
   *   <li>Increments the total number of all the records streamed so far.
   * </ul>
   *
   * @return true if there is another record within the result set. Returns false otherwise.
   */
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

  /**
   * Returns the record at the current pointer as a {@link Struct}. It also updates the timestamp at
   * which the record was read.
   *
   * <p>If {@link ChangeStreamResultSet#next()} was not called or if it was called but there are no
   * more records in the stream, null will be returned.
   *
   * <p>Should only be used for GoogleSQL databases.
   *
   * @return a change stream record as a {@link Struct} or null
   */
  public Struct getCurrentRowAsStruct() {
    recordReadAt = Timestamp.now();
    return resultSet.getCurrentRowAsStruct();
  }

  /**
   * Returns the record at the current pointer as {@link JsonB}. It also updates the timestamp at
   * which the record was read.
   *
   * <p>If {@link ChangeStreamResultSet#next()} was not called or if it was called but there are no
   * more records in the stream, null will be returned.
   *
   * <p>Should only be used for PostgreSQL databases.
   *
   * @return a change stream record as a {@link Struct} or null
   */
  public String getPgJsonb(int index) {
    recordReadAt = Timestamp.now();
    return resultSet.getPgJsonb(index);
  }

  /**
   * Returns the gathered metadata for the change stream query so far.
   *
   * @return a {@link ChangeStreamResultSetMetadata} contained telemetry information for the query
   *     so far
   */
  public ChangeStreamResultSetMetadata getMetadata() {
    return new ChangeStreamResultSetMetadata(
        queryStartedAt,
        recordStreamStartedAt,
        recordStreamEndedAt,
        recordReadAt,
        totalStreamDuration,
        numberOfRecordsRead);
  }

  /**
   * Closes the current change stream {@link ResultSet}. The stream will be terminated when this
   * method is called.
   *
   * <p>This method must always be called after the consumption of the result set or when an error
   * occurs. This makes sure there are no Session leaks and all the underlying resources are
   * released.
   */
  @Override
  public void close() {
    resultSet.close();
  }
}
