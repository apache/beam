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
import java.io.Serializable;
import java.util.List;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.spanner.cdc.TimestampEncoding;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;

@DefaultCoder(AvroCoder.class)
public class DataChangesRecord implements Serializable {

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

  /** Default constructor for serialization only. */
  private DataChangesRecord() {}

  public DataChangesRecord(
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
  }

  public String getPartitionToken() {
    return partitionToken;
  }

  public void setPartitionToken(String partitionToken) {
    this.partitionToken = partitionToken;
  }

  public Timestamp getCommitTimestamp() {
    return commitTimestamp;
  }

  public void setCommitTimestamp(Timestamp commitTimestamp) {
    this.commitTimestamp = commitTimestamp;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public void setTransactionId(String transactionId) {
    this.transactionId = transactionId;
  }

  public boolean isLastRecordInTransactionPartition() {
    return isLastRecordInTransactionPartition;
  }

  public void setLastRecordInTransactionPartition(boolean lastRecordInTransactionPartition) {
    isLastRecordInTransactionPartition = lastRecordInTransactionPartition;
  }

  public String getRecordSequence() {
    return recordSequence;
  }

  public void setRecordSequence(String recordSequence) {
    this.recordSequence = recordSequence;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<ColumnType> getRowType() {
    return rowType;
  }

  public void setRowType(List<ColumnType> rowType) {
    this.rowType = rowType;
  }

  public List<Mod> getMods() {
    return mods;
  }

  public void setMods(List<Mod> mods) {
    this.mods = mods;
  }

  public ModType getModType() {
    return modType;
  }

  public void setModType(ModType modType) {
    this.modType = modType;
  }

  public ValueCaptureType getValueCaptureType() {
    return valueCaptureType;
  }

  public void setValueCaptureType(ValueCaptureType valueCaptureType) {
    this.valueCaptureType = valueCaptureType;
  }

  public long getNumberOfRecordsInTransaction() {
    return numberOfRecordsInTransaction;
  }

  public void setNumberOfRecordsInTransaction(long numberOfRecordsInTransaction) {
    this.numberOfRecordsInTransaction = numberOfRecordsInTransaction;
  }

  public long getNumberOfPartitionsInTransaction() {
    return numberOfPartitionsInTransaction;
  }

  public void setNumberOfPartitionsInTransaction(long numberOfPartitionsInTransaction) {
    this.numberOfPartitionsInTransaction = numberOfPartitionsInTransaction;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataChangesRecord that = (DataChangesRecord) o;
    return isLastRecordInTransactionPartition() == that.isLastRecordInTransactionPartition()
        && getNumberOfRecordsInTransaction() == that.getNumberOfRecordsInTransaction()
        && getNumberOfPartitionsInTransaction() == that.getNumberOfPartitionsInTransaction()
        && Objects.equal(getPartitionToken(), that.getPartitionToken())
        && Objects.equal(getCommitTimestamp(), that.getCommitTimestamp())
        && Objects.equal(getTransactionId(), that.getTransactionId())
        && Objects.equal(getRecordSequence(), that.getRecordSequence())
        && Objects.equal(getTableName(), that.getTableName())
        && Objects.equal(getRowType(), that.getRowType())
        && Objects.equal(getMods(), that.getMods())
        && getModType() == that.getModType()
        && getValueCaptureType() == that.getValueCaptureType();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getPartitionToken(),
        getCommitTimestamp(),
        getTransactionId(),
        isLastRecordInTransactionPartition(),
        getRecordSequence(),
        getTableName(),
        getRowType(),
        getMods(),
        getModType(),
        getValueCaptureType(),
        getNumberOfRecordsInTransaction(),
        getNumberOfPartitionsInTransaction());
  }

  @Override
  public String toString() {
    return "DataChangesRecord{"
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
        + '}';
  }
}
