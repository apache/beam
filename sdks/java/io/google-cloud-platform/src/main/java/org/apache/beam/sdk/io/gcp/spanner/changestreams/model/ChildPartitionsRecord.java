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
import java.util.List;
import java.util.Objects;
import org.apache.avro.reflect.AvroEncode;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.encoder.TimestampEncoding;

/**
 * Represents a ChildPartitionsRecord. This record will be emitted in one of the following cases: a
 * partition has been moved into a new partition, a partition has been split into multiple new child
 * partitions or partitions have been merged into a new partition
 *
 * <p>When receiving this record, the caller should perform new queries using the child partition
 * tokens received.
 */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(AvroCoder.class)
public class ChildPartitionsRecord implements ChangeStreamRecord {

  private static final long serialVersionUID = 5442772555232576887L;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp startTimestamp;

  private String recordSequence;
  private List<ChildPartition> childPartitions;
  @Nullable private ChangeStreamRecordMetadata metadata;

  /** Default constructor for serialization only. */
  private ChildPartitionsRecord() {}

  /**
   * Constructs a child partitions record containing one or more child partitions.
   *
   * @param startTimestamp the timestamp which this partition started being valid in Cloud Spanner
   * @param recordSequence the order within a partition and a transaction in which the record was
   *     put to the stream
   * @param childPartitions child partition tokens emitted within this record
   * @param metadata connector execution metadata for the given record
   */
  public ChildPartitionsRecord(
      Timestamp startTimestamp,
      String recordSequence,
      List<ChildPartition> childPartitions,
      ChangeStreamRecordMetadata metadata) {
    this.startTimestamp = startTimestamp;
    this.recordSequence = recordSequence;
    this.childPartitions = childPartitions;
    this.metadata = metadata;
  }

  /**
   * Returns the timestamp that which this partition started being valid in Cloud Spanner. The
   * caller must use this time as the change stream query start timestamp for the new partitions.
   *
   * @return the start timestamp of the partition
   */
  @Override
  public Timestamp getRecordTimestamp() {
    return getStartTimestamp();
  }

  /**
   * It is the partition_start_time of the child partition token. This partition_start_time is
   * guaranteed to be the same across all the child partitions yielded from a parent. When users
   * start new queries with the child partition tokens, the returned records must have a timestamp
   * >= partition_start_time.
   *
   * @return the start timestamp of the partition
   */
  public Timestamp getStartTimestamp() {
    return startTimestamp;
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

  /**
   * List of child partitions yielded within this record.
   *
   * @return child partitions
   */
  public List<ChildPartition> getChildPartitions() {
    return childPartitions;
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ChildPartitionsRecord)) {
      return false;
    }
    ChildPartitionsRecord that = (ChildPartitionsRecord) o;
    return Objects.equals(startTimestamp, that.startTimestamp)
        && Objects.equals(recordSequence, that.recordSequence)
        && Objects.equals(childPartitions, that.childPartitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startTimestamp, recordSequence, childPartitions);
  }

  @Override
  public String toString() {
    return "ChildPartitionsRecord{"
        + "startTimestamp="
        + startTimestamp
        + ", recordSequence='"
        + recordSequence
        + '\''
        + ", childPartitions="
        + childPartitions
        + ", metadata="
        + metadata
        + '}';
  }
}
