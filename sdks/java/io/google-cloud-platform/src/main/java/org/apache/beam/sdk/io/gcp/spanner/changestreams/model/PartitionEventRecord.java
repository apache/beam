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
 * A partition event record describes key range changes for a change stream partition. This record
 * is only used for updating watermark in the dataflow connector. MoveInEvent and MoveOutEvent are
 * not tracked as they will not be used in the dataflow connector.
 */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(AvroCoder.class)
public class PartitionEventRecord implements ChangeStreamRecord {

  private static final long serialVersionUID = 6431436477387396791L;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp commitTimestamp;

  private String recordSequence;

  @Nullable private ChangeStreamRecordMetadata metadata;

  /** Default constructor for serialization only. */
  private PartitionEventRecord() {}

  /**
   * Constructs the partition event record with the given partitions.
   *
   * @param commitTimestamp the timestamp at which the key range change occurred
   * @param recordSequence the order within a partition and a transaction in which the record was
   *     put to the stream
   * @param metadata connector execution metadata for the given record
   */
  public PartitionEventRecord(
      Timestamp commitTimestamp, String recordSequence, ChangeStreamRecordMetadata metadata) {
    this.commitTimestamp = commitTimestamp;
    this.recordSequence = recordSequence;
    this.metadata = metadata;
  }

  /**
   * Returns the timestamp at which the key range change occurred.
   *
   * @return the start timestamp of the partition
   */
  @Override
  public Timestamp getRecordTimestamp() {
    return getCommitTimestamp();
  }

  /**
   * Returns the timestamp at which the key range change occurred.
   *
   * @return the commit timestamp of the key range change
   */
  public Timestamp getCommitTimestamp() {
    return commitTimestamp;
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
    if (!(o instanceof PartitionEventRecord)) {
      return false;
    }
    PartitionEventRecord that = (PartitionEventRecord) o;
    return Objects.equals(commitTimestamp, that.commitTimestamp)
        && Objects.equals(recordSequence, that.recordSequence);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commitTimestamp, recordSequence);
  }

  @Override
  public String toString() {
    return "PartitionEventRecord{"
        + "commitTimestamp="
        + commitTimestamp
        + ", recordSequence='"
        + recordSequence
        + '\''
        + ", metadata="
        + metadata
        + '}';
  }
}
