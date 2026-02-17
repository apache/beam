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
 * A partition end record serves as a notification that the client should stop reading the
 * partition. No further records are expected to be retrieved on it.
 */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(AvroCoder.class)
public class PartitionEndRecord implements ChangeStreamRecord {

  private static final long serialVersionUID = 5406538761724655621L;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp endTimestamp;

  private String recordSequence;

  @Nullable private ChangeStreamRecordMetadata metadata;

  /** Default constructor for serialization only. */
  private PartitionEndRecord() {}

  /**
   * Constructs the partition end record with the given timestamp, record sequence and metadata.
   *
   * @param endTimestamp end timestamp at which the change stream partition is terminated
   * @param recordSequence the order within a partition and a transaction in which the record was
   *     put to the stream
   * @param metadata connector execution metadata for the given record
   */
  public PartitionEndRecord(
      Timestamp endTimestamp, String recordSequence, ChangeStreamRecordMetadata metadata) {
    this.endTimestamp = endTimestamp;
    this.recordSequence = recordSequence;
    this.metadata = metadata;
  }

  /**
   * Indicates the timestamp for which the change stream partition is terminated.
   *
   * @return the timestamp for which the change stream partition is terminated
   */
  @Override
  public Timestamp getRecordTimestamp() {
    return getEndTimestamp();
  }

  /**
   * The end timestamp at which the change stream partition is terminated.
   *
   * @return the timestamp for which the change stream partition is terminated
   */
  public Timestamp getEndTimestamp() {
    return endTimestamp;
  }

  /**
   * Indicates the order in which a record was put to the stream. Is unique and increasing within a
   * partition. It is relative to the scope of partition, commit timestamp, and
   * server_transaction_id. It is useful for readers downstream to dedup any duplicate records that
   * were read/recorded.
   *
   * @return record sequence of the record
   */
  public String getRecordSequence() {
    return recordSequence;
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionEndRecord)) {
      return false;
    }
    PartitionEndRecord that = (PartitionEndRecord) o;
    return Objects.equals(endTimestamp, that.endTimestamp)
        && Objects.equals(recordSequence, that.recordSequence);
  }

  @Override
  public int hashCode() {
    return Objects.hash(endTimestamp, recordSequence);
  }

  @Override
  public String toString() {
    return "PartitionEndRecord{"
        + "endTimestamp="
        + endTimestamp
        + ", recordSequence='"
        + recordSequence
        + '\''
        + ", metadata="
        + metadata
        + '}';
  }
}
