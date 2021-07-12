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
package org.apache.beam.sdk.io.gcp.spanner.cdc.model;

import com.google.cloud.Timestamp;
import java.util.List;
import java.util.Objects;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.spanner.cdc.TimestampEncoding;

@DefaultCoder(AvroCoder.class)
public class DataChangeRecord implements ChangeStreamRecord {

  private static final long serialVersionUID = 1138762498767540898L;

  private String partitionToken;

  @AvroEncode(using = TimestampEncoding.class)
  private Timestamp commitTimestamp;

  private String transactionId;
  private boolean isLastRecordInTransactionPartition;
  private String recordSequence;
  private String tableName;
  private List<ColumnType> rowType;
  private List<Mod> mods;
  private ModType modType;
  private ValueCaptureType valueCaptureType;
  private long numberOfRecordsInTransaction;
  private long numberOfPartitionsInTransaction;
  private Metadata metadata;

  /** Default constructor for serialization only. */
  private DataChangeRecord() {}

  public DataChangeRecord(
      String partitionToken,
      Timestamp commitTimestamp,
      String transactionId,
      boolean isLastRecordInTransactionPartition,
      String recordSequence,
      String tableName,
      List<ColumnType> rowType,
      List<Mod> mods,
      ModType modType,
      ValueCaptureType valueCaptureType,
      long numberOfRecordsInTransaction,
      long numberOfPartitionsInTransaction) {
    this.commitTimestamp = commitTimestamp;
    this.partitionToken = partitionToken;
    this.transactionId = transactionId;
    this.isLastRecordInTransactionPartition = isLastRecordInTransactionPartition;
    this.recordSequence = recordSequence;
    this.tableName = tableName;
    this.rowType = rowType;
    this.mods = mods;
    this.modType = modType;
    this.valueCaptureType = valueCaptureType;
    this.numberOfRecordsInTransaction = numberOfRecordsInTransaction;
    this.numberOfPartitionsInTransaction = numberOfPartitionsInTransaction;
    this.metadata = new Metadata(Timestamp.now());
  }

  public String getPartitionToken() {
    return partitionToken;
  }

  public Timestamp getCommitTimestamp() {
    return commitTimestamp;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public boolean isLastRecordInTransactionPartition() {
    return isLastRecordInTransactionPartition;
  }

  public String getRecordSequence() {
    return recordSequence;
  }

  public String getTableName() {
    return tableName;
  }

  public List<ColumnType> getRowType() {
    return rowType;
  }

  public List<Mod> getMods() {
    return mods;
  }

  public ModType getModType() {
    return modType;
  }

  public ValueCaptureType getValueCaptureType() {
    return valueCaptureType;
  }

  public long getNumberOfRecordsInTransaction() {
    return numberOfRecordsInTransaction;
  }

  public long getNumberOfPartitionsInTransaction() {
    return numberOfPartitionsInTransaction;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DataChangeRecord)) {
      return false;
    }
    DataChangeRecord that = (DataChangeRecord) o;
    return isLastRecordInTransactionPartition == that.isLastRecordInTransactionPartition
        && numberOfRecordsInTransaction == that.numberOfRecordsInTransaction
        && numberOfPartitionsInTransaction == that.numberOfPartitionsInTransaction
        && Objects.equals(partitionToken, that.partitionToken)
        && Objects.equals(commitTimestamp, that.commitTimestamp)
        && Objects.equals(transactionId, that.transactionId)
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
        transactionId,
        isLastRecordInTransactionPartition,
        recordSequence,
        tableName,
        rowType,
        mods,
        modType,
        valueCaptureType,
        numberOfRecordsInTransaction,
        numberOfPartitionsInTransaction);
  }

  @Override
  public String toString() {
    return "DataChangeRecord{"
        + "partitionToken='"
        + partitionToken
        + '\''
        + ", commitTimestamp="
        + commitTimestamp
        + ", transactionId='"
        + transactionId
        + '\''
        + ", isLastRecordInTransactionPartition="
        + isLastRecordInTransactionPartition
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
        + ", metadata="
        + metadata
        + '}';
  }

  @DefaultCoder(AvroCoder.class)
  public static class Metadata {

    @AvroEncode(using = TimestampEncoding.class)
    private Timestamp readAt;

    /** Default constructor for serialization only. */
    private Metadata() {}

    public Metadata(Timestamp readAt) {
      this.readAt = readAt;
    }

    public Timestamp getReadAt() {
      return readAt;
    }

    @Override
    public String toString() {
      return "Metadata{" + "readAt=" + readAt + '}';
    }
  }
}
