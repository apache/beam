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
package org.apache.beam.sdk.io.gcp.spanner.cdc.usermodel;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class DataChangesRecord implements Serializable {

  private static final long serialVersionUID = 1138762498767540898L;

  private PartitionId partitionId;
  private Timestamp commitTimestamp;
  private TransactionId transactionId;
  private boolean isLastRecordInTransactionPartition;
  private RecordSequence recordSequence;
  private String tableName;
  private List<ColumnType> rowType;
  private List<Mod> mods;
  private ModType modType;
  private ValueCaptureType valueCaptureType;

  public DataChangesRecord(
      PartitionId partitionId,
      Timestamp commitTimestamp,
      TransactionId transactionId,
      boolean isLastRecordInTransactionPartition,
      RecordSequence recordSequence,
      String tableName,
      List<ColumnType> rowType,
      List<Mod> mods,
      ModType modType,
      ValueCaptureType valueCaptureType) {
    this.commitTimestamp = commitTimestamp;
    this.partitionId = partitionId;
    this.transactionId = transactionId;
    this.isLastRecordInTransactionPartition = isLastRecordInTransactionPartition;
    this.recordSequence = recordSequence;
    this.tableName = tableName;
    this.rowType = rowType;
    this.mods = mods;
    this.modType = modType;
    this.valueCaptureType = valueCaptureType;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(PartitionId partitionId) {
    this.partitionId = partitionId;
  }

  public Timestamp getCommitTimestamp() {
    return commitTimestamp;
  }

  public void setCommitTimestamp(Timestamp commitTimestamp) {
    this.commitTimestamp = commitTimestamp;
  }

  public TransactionId getTransactionId() {
    return transactionId;
  }

  public void setTransactionId(TransactionId transactionId) {
    this.transactionId = transactionId;
  }

  public boolean isLastRecordInTransactionPartition() {
    return isLastRecordInTransactionPartition;
  }

  public void setLastRecordInTransactionPartition(boolean lastRecordInTransactionPartition) {
    isLastRecordInTransactionPartition = lastRecordInTransactionPartition;
  }

  public RecordSequence getRecordSequence() {
    return recordSequence;
  }

  public void setRecordSequence(RecordSequence recordSequence) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataChangesRecord that = (DataChangesRecord) o;
    return isLastRecordInTransactionPartition == that.isLastRecordInTransactionPartition
        && Objects.equals(commitTimestamp, that.commitTimestamp)
        && Objects.equals(partitionId, that.partitionId)
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
        commitTimestamp,
        partitionId,
        transactionId,
        isLastRecordInTransactionPartition,
        recordSequence,
        tableName,
        rowType,
        mods,
        modType,
        valueCaptureType);
  }
}
