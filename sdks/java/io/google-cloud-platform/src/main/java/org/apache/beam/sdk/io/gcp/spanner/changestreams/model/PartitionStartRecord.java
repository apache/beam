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
 * A partition start record serves as a notification that the client should schedule the partitions
 * to be queried. PartitionStartRecord returns information about one or more partitions.
 */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(AvroCoder.class)
public class PartitionStartRecord implements ChangeStreamRecord {

  private static final long serialVersionUID = 1446342293580399634L;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp startTimestamp;

  private String recordSequence;
  private List<String> partitionTokens;
  @Nullable private ChangeStreamRecordMetadata metadata;

  /** Default constructor for serialization only. */
  private PartitionStartRecord() {}

  /**
   * Constructs the partition start record with the given partitions.
   *
   * @param startTimestamp the timestamp which these partitions started being valid in Cloud Spanner
   * @param recordSequence the order within a partition and a transaction in which the record was
   *     put to the stream
   * @param partitionTokens Unique partition identifiers to be used in queries
   * @param metadata connector execution metadata for the given record
   */
  public PartitionStartRecord(
      Timestamp startTimestamp,
      String recordSequence,
      List<String> partitionTokens,
      ChangeStreamRecordMetadata metadata) {
    this.startTimestamp = startTimestamp;
    this.recordSequence = recordSequence;
    this.partitionTokens = partitionTokens;
    this.metadata = metadata;
  }

  /**
   * Returns the timestamp that which these partitions started being valid in Cloud Spanner. The
   * caller must use this time as the change stream query start timestamp for the new partitions.
   *
   * @return the start timestamp of the partitions
   */
  @Override
  public Timestamp getRecordTimestamp() {
    return getStartTimestamp();
  }

  /**
   * It is the partition start time of the partition tokens.
   *
   * @return the start timestamp of the partitions
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
   * List of partitions yielded within this record.
   *
   * @return partition tokens
   */
  public List<String> getPartitionTokens() {
    return partitionTokens;
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionStartRecord)) {
      return false;
    }
    PartitionStartRecord that = (PartitionStartRecord) o;
    return Objects.equals(startTimestamp, that.startTimestamp)
        && Objects.equals(recordSequence, that.recordSequence)
        && Objects.equals(partitionTokens, that.partitionTokens);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startTimestamp, recordSequence, partitionTokens);
  }

  @Override
  public String toString() {
    return "PartitionStartRecord{"
        + "startTimestamp="
        + startTimestamp
        + ", recordSequence='"
        + recordSequence
        + '\''
        + ", partitionTokens="
        + partitionTokens
        + ", metadata="
        + metadata
        + '}';
  }
}
