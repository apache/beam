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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.model;

import com.google.cloud.Timestamp;
import java.util.Objects;
import org.apache.avro.reflect.AvroEncode;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.encoder.TimestampEncoding;

/**
 * A heartbeat record serves as a notification that the change stream query has returned all changes
 * for the partition less or equal to the record timestamp.
 */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(AvroCoder.class)
public class HeartbeatRecord implements ChangeStreamRecord {

  private static final long serialVersionUID = 5331450064150969956L;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp timestamp;

  @Nullable private ChangeStreamRecordMetadata metadata;

  /** Default constructor for serialization only. */
  private HeartbeatRecord() {}

  /**
   * Constructs the heartbeat record with the given timestamp and metadata.
   *
   * @param timestamp the timestamp for which all changes in the partition have occurred
   * @param metadata connector execution metadata for the given record
   */
  public HeartbeatRecord(Timestamp timestamp, ChangeStreamRecordMetadata metadata) {
    this.timestamp = timestamp;
    this.metadata = metadata;
  }

  /**
   * Indicates the timestamp for which the change stream query has returned all changes. All records
   * emitted after the heartbeat record will have a timestamp greater than this one.
   *
   * @return the timestamp for which the change stream query has returned all changes
   */
  @Override
  public Timestamp getRecordTimestamp() {
    return getTimestamp();
  }

  /**
   * Indicates the timestamp for which the change stream query has returned all changes. All records
   * emitted after the heartbeat record will have a timestamp greater than this one.
   *
   * @return the timestamp for which the change stream query has returned all changes
   */
  public Timestamp getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HeartbeatRecord)) {
      return false;
    }
    HeartbeatRecord that = (HeartbeatRecord) o;
    return Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp);
  }

  @Override
  public String toString() {
    return "HeartbeatRecord{" + "timestamp=" + timestamp + ", metadata=" + metadata + '}';
  }
}
