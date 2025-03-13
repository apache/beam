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
 * A data change record encodes modifications to Cloud Spanner rows. A record will contain one or
 * more modifications made in one table with the same {@link ModType}. There can be multiple data
 * change records for a transaction and commit timestamp.
 */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(AvroCoder.class)
public class DataChangeRecord implements ChangeStreamRecord {

  private static final long serialVersionUID = 1138762498767540898L;

  private String partitionToken;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp commitTimestamp;

  private String serverTransactionId;
  private boolean isLastRecordInTransactionInPartition;
  private String recordSequence;
  private String tableName;
  private List<ColumnType> rowType;
  private List<Mod> mods;
  private ModType modType;
  private ValueCaptureType valueCaptureType;
  private long numberOfRecordsInTransaction;
  private long numberOfPartitionsInTransaction;
  private String transactionTag;
  private boolean isSystemTransaction;
  @Nullable private ChangeStreamRecordMetadata metadata;

  /** Default constructor for serialization only. */
  private DataChangeRecord() {}

  /**
   * Constructs a data change record for a given partition, at a given timestamp, for a given
   * transaction. The data change record needs to be given information about the table modified, the
   * type of primary keys and modified columns, the modifications themselves and other metadata.
   *
   * @param partitionToken the unique identifier of the partition that generated this record
   * @param commitTimestamp the timestamp at which the modifications within were committed in Cloud
   *     Spanner
   * @param serverTransactionId the unique transaction id in which the modifications occurred
   * @param isLastRecordInTransactionInPartition indicates whether this record is the last emitted
   *     for the given transaction in the given partition
   * @param recordSequence indicates the order in which this record was put into the change stream
   *     in the scope of a partition, commit timestamp and transaction tuple
   * @param tableName the name of the table in which the modifications occurred
   * @param rowType the type of the primary keys and modified columns
   * @param mods the modifications occurred
   * @param modType the operation that caused the modification to occur
   * @param valueCaptureType the capture type of the change stream
   * @param numberOfRecordsInTransaction the total number of records for the given transaction
   * @param numberOfPartitionsInTransaction the total number of partitions within the given
   *     transaction
   * @param transactionTag the transaction tag associated with the given transaction
   * @param isSystemTransaction whether the given transaction is Spanner system transaction (or user
   *     transaction)
   * @param metadata connector execution metadata for the given record
   */
  public DataChangeRecord(
      String partitionToken,
      Timestamp commitTimestamp,
      String serverTransactionId,
      boolean isLastRecordInTransactionInPartition,
      String recordSequence,
      String tableName,
      List<ColumnType> rowType,
      List<Mod> mods,
      ModType modType,
      ValueCaptureType valueCaptureType,
      long numberOfRecordsInTransaction,
      long numberOfPartitionsInTransaction,
      String transactionTag,
      boolean isSystemTransaction,
      ChangeStreamRecordMetadata metadata) {
    this.commitTimestamp = commitTimestamp;
    this.partitionToken = partitionToken;
    this.serverTransactionId = serverTransactionId;
    this.isLastRecordInTransactionInPartition = isLastRecordInTransactionInPartition;
    this.recordSequence = recordSequence;
    this.tableName = tableName;
    this.rowType = rowType;
    this.mods = mods;
    this.modType = modType;
    this.valueCaptureType = valueCaptureType;
    this.numberOfRecordsInTransaction = numberOfRecordsInTransaction;
    this.numberOfPartitionsInTransaction = numberOfPartitionsInTransaction;
    this.transactionTag = transactionTag;
    this.isSystemTransaction = isSystemTransaction;
    this.metadata = metadata;
  }

  /** The timestamp at which the modifications within were committed in Cloud Spanner. */
  @Override
  public Timestamp getRecordTimestamp() {
    return commitTimestamp;
  }

  /** The unique identifier of the partition that generated this record. */
  public String getPartitionToken() {
    return partitionToken;
  }

  /** The timestamp at which the modifications within were committed in Cloud Spanner. */
  public Timestamp getCommitTimestamp() {
    return commitTimestamp;
  }

  /** The unique transaction id in which the modifications occurred. */
  public String getServerTransactionId() {
    return serverTransactionId;
  }

  /**
   * Indicates whether this record is the last emitted for the given transaction in the given
   * partition.
   */
  public boolean isLastRecordInTransactionInPartition() {
    return isLastRecordInTransactionInPartition;
  }

  /**
   * Indicates the order in which this record was put into the change stream in the scope of a
   * partition, commit timestamp and transaction tuple.
   */
  public String getRecordSequence() {
    return recordSequence;
  }

  /** The name of the table in which the modifications within this record occurred. */
  public String getTableName() {
    return tableName;
  }

  /** The type of the primary keys and modified columns within this record. */
  public List<ColumnType> getRowType() {
    return rowType;
  }

  /** The modifications within this record. */
  public List<Mod> getMods() {
    return mods;
  }

  /** The type of operation that caused the modifications within this record. */
  public ModType getModType() {
    return modType;
  }

  /** The capture type of the change stream that generated this record. */
  public ValueCaptureType getValueCaptureType() {
    return valueCaptureType;
  }

  /** The total number of data change records for the given transaction. */
  public long getNumberOfRecordsInTransaction() {
    return numberOfRecordsInTransaction;
  }

  /** The total number of partitions for the given transaction. */
  public long getNumberOfPartitionsInTransaction() {
    return numberOfPartitionsInTransaction;
  }

  /** The transaction tag associated with the given transaction. */
  public String getTransactionTag() {
    return transactionTag;
  }

  /**
   * Whether the given transaction is Spanner system transaction. Otherwise it is user/application
   * transaction.
   */
  public boolean isSystemTransaction() {
    return isSystemTransaction;
  }

  /** The connector execution metadata for this record. */
  public ChangeStreamRecordMetadata getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DataChangeRecord)) {
      return false;
    }
    DataChangeRecord that = (DataChangeRecord) o;
    return isLastRecordInTransactionInPartition == that.isLastRecordInTransactionInPartition
        && numberOfRecordsInTransaction == that.numberOfRecordsInTransaction
        && numberOfPartitionsInTransaction == that.numberOfPartitionsInTransaction
        && Objects.equals(transactionTag, that.transactionTag)
        && isSystemTransaction == that.isSystemTransaction
        && Objects.equals(partitionToken, that.partitionToken)
        && Objects.equals(commitTimestamp, that.commitTimestamp)
        && Objects.equals(serverTransactionId, that.serverTransactionId)
        && Objects.equals(recordSequence, that.recordSequence)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(rowType, that.rowType)
        && Objects.equals(mods, that.mods)
        && modType == that.modType
        && valueCaptureType == that.valueCaptureType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        partitionToken,
        commitTimestamp,
        serverTransactionId,
        isLastRecordInTransactionInPartition,
        recordSequence,
        tableName,
        rowType,
        mods,
        modType,
        valueCaptureType,
        numberOfRecordsInTransaction,
        numberOfPartitionsInTransaction,
        transactionTag,
        isSystemTransaction);
  }

  @Override
  public String toString() {
    return "DataChangeRecord{"
        + "partitionToken='"
        + partitionToken
        + '\''
        + ", commitTimestamp="
        + commitTimestamp
        + ", serverTransactionId='"
        + serverTransactionId
        + '\''
        + ", isLastRecordInTransactionInPartition="
        + isLastRecordInTransactionInPartition
        + ", recordSequence='"
        + recordSequence
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", rowType="
        + rowType
        + ", mods="
        + mods
        + ", modType="
        + modType
        + ", valueCaptureType="
        + valueCaptureType
        + ", numberOfRecordsInTransaction="
        + numberOfRecordsInTransaction
        + ", numberOfPartitionsInTransaction="
        + numberOfPartitionsInTransaction
        + ", transactionTag='"
        + transactionTag
        + '\''
        + ", isSystemTransaction="
        + isSystemTransaction
        + ", metadata"
        + metadata
        + '}';
  }
}
