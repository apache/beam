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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao.DETECT_NEW_PARTITION_SUFFIX;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao.NEW_PARTITION_PREFIX;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao.STREAM_PARTITION_PREFIX;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationException;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationUtils;
import org.apache.beam.sdk.annotations.Internal;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data access object for managing the state of the metadata Bigtable table.
 *
 * <p>Metadata table is shared across many beam jobs. Each beam job uses a specific prefix to
 * identify itself which is used as the row prefix.
 */
@Internal
public class MetadataTableDao {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataTableDao.class);

  private final BigtableDataClient dataClient;
  private final String tableId;
  private final ByteString changeStreamNamePrefix;

  public MetadataTableDao(
      BigtableDataClient dataClient, String tableId, ByteString changeStreamNamePrefix) {
    this.dataClient = dataClient;
    this.tableId = tableId;
    this.changeStreamNamePrefix = changeStreamNamePrefix;
  }

  /** @return the prefix that is prepended to every row belonging to this beam job. */
  public ByteString getChangeStreamNamePrefix() {
    return changeStreamNamePrefix;
  }

  /**
   * Return new partition row key prefix concatenated with change stream name.
   *
   * @return new partition row key prefix concatenated with change stream name.
   */
  private ByteString getFullNewPartitionPrefix() {
    return changeStreamNamePrefix.concat(NEW_PARTITION_PREFIX);
  }

  /**
   * Return stream partition row key prefix concatenated with change stream name.
   *
   * @return stream partition row key prefix concatenated with change stream name.
   */
  private ByteString getFullStreamPartitionPrefix() {
    return changeStreamNamePrefix.concat(STREAM_PARTITION_PREFIX);
  }

  /**
   * Return detect new partition row key concatenated with change stream name.
   *
   * @return detect new partition row key concatenated with change stream name.
   */
  private ByteString getFullDetectNewPartition() {
    return changeStreamNamePrefix.concat(DETECT_NEW_PARTITION_SUFFIX);
  }

  /**
   * Convert stream partition row key to partition to process metadata read from Bigtable.
   *
   * <p>RowKey should be directly from Cloud Bigtable and not altered in any way.
   *
   * @param rowKey row key from Cloud Bigtable
   * @return partition extracted from rowKey
   * @throws InvalidProtocolBufferException if conversion from rowKey to partition fails
   */
  public ByteStringRange convertStreamPartitionRowKeyToPartition(ByteString rowKey)
      throws InvalidProtocolBufferException {
    int prefixLength = changeStreamNamePrefix.size() + STREAM_PARTITION_PREFIX.size();
    return ByteStringRange.toByteStringRange(rowKey.substring(prefixLength));
  }

  /**
   * Convert partition to a Stream Partition row key to query for metadata of partitions that are
   * currently being streamed.
   *
   * @param partition convert to row key
   * @return row key to insert to Cloud Bigtable.
   */
  public ByteString convertPartitionToStreamPartitionRowKey(ByteStringRange partition) {
    return getFullStreamPartitionPrefix().concat(ByteStringRange.serializeToByteString(partition));
  }

  /**
   * Convert new partition row key to partition to process metadata read from Bigtable.
   *
   * <p>RowKey should be directly from Cloud Bigtable and not altered in any way.
   *
   * @param rowKey row key from Cloud Bigtable
   * @return partition extracted from rowKey
   * @throws InvalidProtocolBufferException if conversion from rowKey to partition fails
   */
  public ByteStringRange convertNewPartitionRowKeyToPartition(ByteString rowKey)
      throws InvalidProtocolBufferException {
    int prefixLength = changeStreamNamePrefix.size() + NEW_PARTITION_PREFIX.size();
    return ByteStringRange.toByteStringRange(rowKey.substring(prefixLength));
  }

  /**
   * Convert partition to a New Partition row key to query for partitions ready to be streamed as
   * the result of splits and merges.
   *
   * @param partition convert to row key
   * @return row key to insert to Cloud Bigtable.
   */
  public ByteString convertPartitionToNewPartitionRowKey(ByteStringRange partition) {
    return getFullNewPartitionPrefix().concat(ByteStringRange.serializeToByteString(partition));
  }

  /**
   * @return stream of all the new partitions resulting from splits and merges waiting to be
   *     streamed.
   */
  public ServerStream<Row> readNewPartitions() {
    // It's important that we limit to the latest value per column because it's possible to write to
    // the same column multiple times. We don't want to read and send duplicate tokens to the
    // server.
    Query query =
        Query.create(tableId)
            .prefix(getFullNewPartitionPrefix())
            .filter(FILTERS.limit().cellsPerColumn(1));
    return dataClient.readRows(query);
  }

  /**
   * After a split or merge from a close stream, write the new partition's information to the
   * metadata table.
   *
   * @param newPartition the new partition
   * @param changeStreamContinuationToken the token that can be used to pick up from where the
   *     parent left off
   * @param parentPartition the parent that stopped and split or merged
   * @param lowWatermark the low watermark of the parent stream
   */
  public void writeNewPartition(
      ByteStringRange newPartition,
      ChangeStreamContinuationToken changeStreamContinuationToken,
      ByteStringRange parentPartition,
      Instant lowWatermark) {
    writeNewPartition(
        newPartition,
        changeStreamContinuationToken.toByteString(),
        ByteStringRange.serializeToByteString(parentPartition),
        lowWatermark);
  }

  /**
   * After a split or merge from a close stream, write the new partition's information to the
   * metadata table.
   *
   * @param newPartition the new partition
   * @param newPartitionContinuationToken continuation token for the new partition
   * @param parentPartition the parent that stopped
   * @param lowWatermark low watermark of the parent
   */
  private void writeNewPartition(
      ByteStringRange newPartition,
      ByteString newPartitionContinuationToken,
      ByteString parentPartition,
      Instant lowWatermark) {
    LOG.debug("Insert new partition");
    ByteString rowKey = convertPartitionToNewPartitionRowKey(newPartition);
    RowMutation rowMutation =
        RowMutation.create(tableId, rowKey)
            .setCell(MetadataTableAdminDao.CF_INITIAL_TOKEN, newPartitionContinuationToken, 1)
            .setCell(MetadataTableAdminDao.CF_PARENT_PARTITIONS, parentPartition, 1)
            .setCell(
                MetadataTableAdminDao.CF_PARENT_LOW_WATERMARKS,
                parentPartition,
                lowWatermark.getMillis());
    dataClient.mutateRow(rowMutation);
  }

  /**
   * @return stream of partitions currently being streamed by the beam job that have set a
   *     watermark.
   */
  public ServerStream<Row> readFromMdTableStreamPartitionsWithWatermark() {
    // We limit to the latest value per column.
    Query query =
        Query.create(tableId)
            .prefix(getFullStreamPartitionPrefix())
            .filter(
                FILTERS
                    .chain()
                    .filter(FILTERS.limit().cellsPerColumn(1))
                    .filter(FILTERS.family().exactMatch(MetadataTableAdminDao.CF_WATERMARK))
                    .filter(
                        FILTERS.qualifier().exactMatch(MetadataTableAdminDao.QUALIFIER_DEFAULT)));
    return dataClient.readRows(query);
  }

  /**
   * Update the metadata for the rowKey. This helper adds necessary prefixes to the row key.
   *
   * @param rowKey row key of the row to update
   * @param watermark watermark value to set for the cell
   * @param currentToken continuation token to set for the cell
   */
  private void writeToMdTableWatermarkHelper(
      ByteString rowKey, Instant watermark, @Nullable ChangeStreamContinuationToken currentToken) {
    RowMutation rowMutation =
        RowMutation.create(tableId, rowKey)
            .setCell(
                MetadataTableAdminDao.CF_WATERMARK,
                MetadataTableAdminDao.QUALIFIER_DEFAULT,
                watermark.getMillis());
    if (currentToken != null) {
      rowMutation.setCell(
          MetadataTableAdminDao.CF_CONTINUATION_TOKEN,
          MetadataTableAdminDao.QUALIFIER_DEFAULT,
          currentToken.getToken());
    }
    dataClient.mutateRow(rowMutation);
  }

  /**
   * Update the metadata for the row key represented by the partition.
   *
   * @param partition forms the row key of the row to update
   * @param watermark watermark value to set for the cell
   * @param currentToken continuation token to set for the cell
   */
  public void updateWatermark(
      ByteStringRange partition,
      Instant watermark,
      @Nullable ChangeStreamContinuationToken currentToken) {
    writeToMdTableWatermarkHelper(
        convertPartitionToStreamPartitionRowKey(partition), watermark, currentToken);
  }

  /**
   * Delete the row key represented by the partition. This represents that the partition will no
   * longer be streamed.
   *
   * @param partition forms the row key of the row to delete
   */
  public void deleteStreamPartitionRow(ByteStringRange partition) {
    ByteString rowKey = convertPartitionToStreamPartitionRowKey(partition);
    RowMutation rowMutation = RowMutation.create(tableId, rowKey).deleteRow();
    dataClient.mutateRow(rowMutation);
  }

  /**
   * Delete the row.
   *
   * @param rowKey row key of the row to delete
   */
  public void deleteRowKey(ByteString rowKey) {
    RowMutation rowMutation = RowMutation.create(tableId, rowKey).deleteRow();
    dataClient.mutateRow(rowMutation);
  }

  /**
   * Lock the partition in the metadata table for the DoFn streaming it. Only one DoFn is allowed to
   * stream a specific partition at any time. Each DoFn has an uuid and will try to lock the
   * partition at the very start of the stream. If another DoFn has already locked the partition
   * (i.e. the uuid in the cell for the partition belongs to the DoFn), any future DoFn trying to
   * lock the same partition will and terminate.
   *
   * @param partition form the row key in the metadata table to lock
   * @param uuid id of the DoFn
   * @return true if uuid holds the lock, otherwise false.
   */
  public boolean lockPartition(ByteStringRange partition, String uuid) {
    LOG.debug("Locking partition before processing stream");

    ByteString rowKey = convertPartitionToStreamPartitionRowKey(partition);
    Filter lockCellFilter =
        FILTERS
            .chain()
            .filter(FILTERS.family().exactMatch(MetadataTableAdminDao.CF_LOCK))
            .filter(FILTERS.qualifier().exactMatch(MetadataTableAdminDao.QUALIFIER_DEFAULT))
            .filter(FILTERS.limit().cellsPerRow(1));
    Row row = dataClient.readRow(tableId, rowKey, lockCellFilter);

    // If the query returns non-null row, that means the lock is being held. Check if the owner is
    // same as uuid.
    if (row != null) {
      return row.getCells().get(0).getValue().toStringUtf8().equals(uuid);
    }

    // We cannot check whether a cell is empty, We can check if the cell matches any value. If it
    // does not, we perform the mutation to set the cell.
    Mutation mutation =
        Mutation.create()
            .setCell(MetadataTableAdminDao.CF_LOCK, MetadataTableAdminDao.QUALIFIER_DEFAULT, uuid);
    Filter matchAnyString =
        FILTERS
            .chain()
            .filter(FILTERS.family().exactMatch(MetadataTableAdminDao.CF_LOCK))
            .filter(FILTERS.qualifier().exactMatch(MetadataTableAdminDao.QUALIFIER_DEFAULT))
            .filter(FILTERS.value().regex("\\C*"));
    ConditionalRowMutation rowMutation =
        ConditionalRowMutation.create(tableId, rowKey)
            .condition(matchAnyString)
            .otherwise(mutation);
    return !dataClient.checkAndMutateRow(rowMutation);
  }

  /**
   * Set the version number for DetectNewPartition. This value can be checked later to verify that
   * the existing metadata table is compatible with current beam connector code.
   */
  public void writeDetectNewPartitionVersion() {
    RowMutation rowMutation =
        RowMutation.create(tableId, getFullDetectNewPartition())
            .setCell(
                MetadataTableAdminDao.CF_VERSION,
                MetadataTableAdminDao.QUALIFIER_DEFAULT,
                MetadataTableAdminDao.CURRENT_METADATA_TABLE_VERSION);
    dataClient.mutateRow(rowMutation);
  }

  /**
   * Read and deserialize missing partition and how long they have been missing from the metadata
   * table.
   *
   * @return deserialized missing partitions and duration.
   */
  public HashMap<ByteStringRange, Long> readDetectNewPartitionMissingPartitions() {
    @Nonnull HashMap<ByteStringRange, Long> missingPartitions = new HashMap<>();
    Filter missingPartitionsFilter =
        FILTERS
            .chain()
            .filter(FILTERS.family().exactMatch(MetadataTableAdminDao.CF_MISSING_PARTITIONS))
            .filter(FILTERS.qualifier().exactMatch(MetadataTableAdminDao.QUALIFIER_DEFAULT))
            .filter(FILTERS.limit().cellsPerColumn(1));
    Row row = dataClient.readRow(tableId, getFullDetectNewPartition(), missingPartitionsFilter);

    if (row == null
        || row.getCells(
                MetadataTableAdminDao.CF_MISSING_PARTITIONS,
                MetadataTableAdminDao.QUALIFIER_DEFAULT)
            .isEmpty()) {
      return missingPartitions;
    }
    ByteString serializedMissingPartition =
        row.getCells(
                MetadataTableAdminDao.CF_MISSING_PARTITIONS,
                MetadataTableAdminDao.QUALIFIER_DEFAULT)
            .get(0)
            .getValue();
    try {
      missingPartitions = SerializationUtils.deserialize(serializedMissingPartition.toByteArray());
    } catch (SerializationException | NullPointerException exception) {
      LOG.warn("Failed to deserialize missingPartitions: {}", exception.toString());
    }
    return missingPartitions;
  }

  /**
   * Write to metadata table serialized missing partitions and how long they have been missing.
   *
   * @param missingPartitionDurations missing partitions and duration.
   */
  public void writeDetectNewPartitionMissingPartitions(
      HashMap<ByteStringRange, Long> missingPartitionDurations) {
    byte[] serializedMissingPartition = SerializationUtils.serialize(missingPartitionDurations);
    RowMutation rowMutation =
        RowMutation.create(tableId, getFullDetectNewPartition())
            .setCell(
                MetadataTableAdminDao.CF_MISSING_PARTITIONS,
                ByteString.copyFromUtf8(MetadataTableAdminDao.QUALIFIER_DEFAULT),
                ByteString.copyFrom(serializedMissingPartition));
    dataClient.mutateRow(rowMutation);
  }
}
