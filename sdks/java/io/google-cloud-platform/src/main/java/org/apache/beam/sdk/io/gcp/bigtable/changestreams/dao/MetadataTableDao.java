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
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.formatByteStringRange;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter.microsecondToInstant;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao.DETECT_NEW_PARTITION_SUFFIX;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao.NEW_PARTITION_PREFIX;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao.STREAM_PARTITION_PREFIX;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder.isRowLocked;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder.parseInitialContinuationTokens;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder.parseLockUuid;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder.parseTokenFromRow;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder.parseWatermarkFromRow;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder.parseWatermarkLastUpdatedFromRow;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationException;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationUtils;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.DetectNewPartitionsState;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.NewPartition;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.StreamPartitionWithWatermark;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;
import org.joda.time.Duration;
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

  // New partition marked for deletion should be reevaluated after this delay because it might be
  // output has failed.
  private static final Duration DELETED_NEW_PARTITION_REEVALUATE_DELAY =
      Duration.standardMinutes(1);

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
   * Read the low watermark of the pipeline from Detect New Partition row.
   *
   * @return DetectNewPartitions row from the metadata table.
   */
  public @Nullable DetectNewPartitionsState readDetectNewPartitionsState() {
    Row row =
        dataClient.readRow(
            tableId,
            getFullDetectNewPartition(),
            FILTERS
                .chain()
                .filter(FILTERS.family().exactMatch(MetadataTableAdminDao.CF_WATERMARK))
                .filter(FILTERS.qualifier().exactMatch(MetadataTableAdminDao.QUALIFIER_DEFAULT))
                .filter(FILTERS.limit().cellsPerColumn(1)));
    if (row == null) {
      return null;
    }
    Instant watermark = parseWatermarkFromRow(row);
    Instant timestamp = parseWatermarkLastUpdatedFromRow(row);
    if (watermark == null || timestamp == null) {
      return null;
    }
    return new DetectNewPartitionsState(watermark, timestamp);
  }

  /**
   * @return all the new partitions resulting from splits and merges waiting to be streamed
   *     including ones marked for deletion.
   */
  public List<NewPartition> readNewPartitionsIncludingDeleted()
      throws InvalidProtocolBufferException {
    List<NewPartition> newPartitions = new ArrayList<>();
    Query query =
        Query.create(tableId)
            .prefix(getFullNewPartitionPrefix())
            .filter(Filters.FILTERS.limit().cellsPerColumn(1));
    ServerStream<Row> rows = dataClient.readRows(query);
    for (Row row : rows) {
      ByteStringRange childPartition = convertNewPartitionRowKeyToPartition(row.getKey());
      List<ChangeStreamContinuationToken> tokens = new ArrayList<>();
      // Capture all the initial continuation tokens to request for the new partition.
      for (RowCell cell : row.getCells(MetadataTableAdminDao.CF_INITIAL_TOKEN)) {
        ChangeStreamContinuationToken changeStreamContinuationToken =
            ChangeStreamContinuationToken.fromByteString(cell.getValue());
        tokens.add(changeStreamContinuationToken);
      }
      // There are no parents.
      if (tokens.isEmpty()) {
        continue;
      }
      // Capture all the low watermark and calculate the low watermark among all the parents.
      List<Long> parentLowWatermark = new ArrayList<>();
      for (RowCell cell : row.getCells(MetadataTableAdminDao.CF_PARENT_LOW_WATERMARKS)) {
        parentLowWatermark.add(Longs.fromByteArray(cell.getValue().toByteArray()));
      }
      Instant lowWatermark = Instant.ofEpochMilli(Collections.min(parentLowWatermark));
      newPartitions.add(new NewPartition(childPartition, tokens, lowWatermark));
    }
    return newPartitions;
  }

  /** @return list the new partitions resulting from splits and merges waiting to be streamed. */
  public List<NewPartition> readNewPartitions() throws InvalidProtocolBufferException {
    List<NewPartition> newPartitions = new ArrayList<>();
    Query query =
        Query.create(tableId)
            .prefix(getFullNewPartitionPrefix())
            .filter(Filters.FILTERS.limit().cellsPerColumn(1));
    ServerStream<Row> rows = dataClient.readRows(query);
    for (Row row : rows) {
      long lastUpdatedMicro = 0;
      // We only process parent partition that are not marked for deletion.
      Set<ByteStringRange> deletedParentPartitions = new HashSet<>();
      for (RowCell cell : row.getCells(MetadataTableAdminDao.CF_SHOULD_DELETE)) {
        lastUpdatedMicro = Math.max(lastUpdatedMicro, cell.getTimestamp());
        // Evaluate if the timestamp of the deletion marker is within the last 1 minute.
        if (microsecondToInstant(cell.getTimestamp())
            .plus(DELETED_NEW_PARTITION_REEVALUATE_DELAY)
            .isAfterNow()) {
          deletedParentPartitions.add(ByteStringRange.toByteStringRange(cell.getQualifier()));
        }
      }

      ByteStringRange childPartition = convertNewPartitionRowKeyToPartition(row.getKey());
      List<ChangeStreamContinuationToken> tokens = new ArrayList<>();
      // Capture all the initial continuation tokens to request for the new partition.
      for (RowCell cell : row.getCells(MetadataTableAdminDao.CF_INITIAL_TOKEN)) {
        // Only add parent partition that are not deleted.
        if (!deletedParentPartitions.contains(
            ByteStringRange.toByteStringRange(cell.getQualifier()))) {
          ChangeStreamContinuationToken changeStreamContinuationToken =
              ChangeStreamContinuationToken.fromByteString(cell.getValue());
          tokens.add(changeStreamContinuationToken);
          lastUpdatedMicro = Math.max(lastUpdatedMicro, cell.getTimestamp());
        }
      }
      // All the parent partitions were skipped. We can skip this row.
      if (tokens.isEmpty()) {
        continue;
      }

      // Capture all the low watermark and calculate the low watermark among all the parents.
      List<Long> parentLowWatermark = new ArrayList<>();
      for (RowCell cell : row.getCells(MetadataTableAdminDao.CF_PARENT_LOW_WATERMARKS)) {
        // Only add parent partition that are not deleted.
        if (!deletedParentPartitions.contains(
            ByteStringRange.toByteStringRange(cell.getQualifier()))) {
          parentLowWatermark.add(Longs.fromByteArray(cell.getValue().toByteArray()));
          lastUpdatedMicro = Math.max(lastUpdatedMicro, cell.getTimestamp());
        }
      }
      Instant lowWatermark = Instant.ofEpochMilli(Collections.min(parentLowWatermark));
      newPartitions.add(
          new NewPartition(
              childPartition, tokens, lowWatermark, microsecondToInstant(lastUpdatedMicro)));
    }
    return newPartitions;
  }

  /**
   * After a split or merge from a close stream, write the new partition's information to the
   * metadata table.
   *
   * @param newPartition the new partition
   */
  public void writeNewPartition(NewPartition newPartition) {
    ByteString rowKey = convertPartitionToNewPartitionRowKey(newPartition.getPartition());
    ByteStringRange parentPartition =
        newPartition.getChangeStreamContinuationTokens().get(0).getPartition();
    RowMutation rowMutation =
        RowMutation.create(tableId, rowKey)
            .setCell(
                MetadataTableAdminDao.CF_INITIAL_TOKEN,
                ByteStringRange.serializeToByteString(parentPartition),
                newPartition.getChangeStreamContinuationTokens().get(0).toByteString())
            .setCell(
                MetadataTableAdminDao.CF_PARENT_LOW_WATERMARKS,
                ByteStringRange.serializeToByteString(parentPartition),
                newPartition.getLowWatermark().getMillis())
            .deleteCells(
                MetadataTableAdminDao.CF_SHOULD_DELETE,
                ByteStringRange.serializeToByteString(parentPartition));
    mutateRowWithHardTimeout(rowMutation);
  }

  /**
   * This is the 1st step of 2 phase delete. Mark each parent partition in NewPartition for
   * deletion. This avoids double reading the new partitions row for a while to allow RCSP to clean
   * up partitions that led to it.
   *
   * <p>The reason behind 2 phase delete of NewPartition is to provide the invariant that the
   * metadata table always has a copy of the ChangeStreamContinuationToken for every partition. The
   * alternatives are either delete NewPartition <i>before</i> outputting PartitionRecord to RCSP or
   * delete NewPartition <i>after</i> outputting PartitionRecord to RCSP.
   *
   * <p>The former has the problem that if the output failed and the NewPartition row has been
   * deleted, the token is now lost.
   *
   * <p>The latter has a more complex problem of race between cleaning up NewPartition and RCSP
   * writing StreamPartition. If clean up happens before StreamPartition is written, then at that
   * moment the token is missing. While under normal operations, it should recover because RCSP
   * writes StreamPartition eventually. We want to provide the invariant that there's always a copy
   * of a token for every partition in the metadata table.
   *
   * @param newPartition mark for deletion.
   */
  public void markNewPartitionForDeletion(NewPartition newPartition) {
    ByteString rowKey = convertPartitionToNewPartitionRowKey(newPartition.getPartition());
    RowMutation rowMutation = RowMutation.create(tableId, rowKey);
    for (ChangeStreamContinuationToken token : newPartition.getChangeStreamContinuationTokens()) {
      rowMutation.setCell(
          MetadataTableAdminDao.CF_SHOULD_DELETE,
          ByteStringRange.serializeToByteString(token.getPartition()),
          1);
    }
    mutateRowWithHardTimeout(rowMutation);
  }

  /**
   * This is the 2nd step of 2 phase delete. Delete the newPartition cells. This should take place
   * after the tokens have been written to StreamPartition.
   *
   * <p>It's possible to try to delete multiple parent partition cells but only a subset are marked
   * for deletion. Only the cells marked for deletion are deleted.
   *
   * @param newPartition row that represents the new partition.
   * @return true if successfully deleted entire newPartition.
   */
  public boolean deleteNewPartition(NewPartition newPartition) {
    ByteString rowKey = convertPartitionToNewPartitionRowKey(newPartition.getPartition());

    boolean success = true;

    for (ChangeStreamContinuationToken token : newPartition.getChangeStreamContinuationTokens()) {
      Filter shouldDelete =
          FILTERS
              .chain()
              .filter(FILTERS.family().exactMatch(MetadataTableAdminDao.CF_SHOULD_DELETE))
              .filter(
                  FILTERS
                      .qualifier()
                      .exactMatch(ByteStringRange.serializeToByteString(token.getPartition())))
              .filter(FILTERS.value().exactMatch(ByteString.copyFrom(Longs.toByteArray(1))));
      Mutation mutation =
          Mutation.create()
              .deleteCells(
                  MetadataTableAdminDao.CF_INITIAL_TOKEN,
                  ByteStringRange.serializeToByteString(token.getPartition()))
              .deleteCells(
                  MetadataTableAdminDao.CF_PARENT_LOW_WATERMARKS,
                  ByteStringRange.serializeToByteString(token.getPartition()))
              .deleteCells(
                  MetadataTableAdminDao.CF_SHOULD_DELETE,
                  ByteStringRange.serializeToByteString(token.getPartition()));

      ConditionalRowMutation conditionalRowMutation =
          ConditionalRowMutation.create(tableId, rowKey).condition(shouldDelete).then(mutation);
      success = dataClient.checkAndMutateRow(conditionalRowMutation) && success;
    }

    return success;
  }

  /**
   * Return list of locked StreamPartition and their watermarks.
   *
   * @return list of partitions currently being streamed that have a watermark.
   */
  public List<StreamPartitionWithWatermark> readStreamPartitionsWithWatermark()
      throws InvalidProtocolBufferException {
    LOG.debug(
        "Reading stream partitions from metadata table: "
            + getFullStreamPartitionPrefix().toStringUtf8());
    Filter filterForWatermark =
        FILTERS
            .chain()
            .filter(Filters.FILTERS.limit().cellsPerColumn(1))
            .filter(FILTERS.family().exactMatch(MetadataTableAdminDao.CF_WATERMARK))
            .filter(FILTERS.qualifier().exactMatch(MetadataTableAdminDao.QUALIFIER_DEFAULT));
    Filter filterForLock =
        FILTERS
            .chain()
            .filter(Filters.FILTERS.limit().cellsPerColumn(1))
            .filter(FILTERS.family().exactMatch(MetadataTableAdminDao.CF_LOCK))
            .filter(FILTERS.qualifier().exactMatch(MetadataTableAdminDao.QUALIFIER_DEFAULT));
    Query query =
        Query.create(tableId)
            .prefix(getFullStreamPartitionPrefix())
            .filter(FILTERS.interleave().filter(filterForWatermark).filter(filterForLock));
    ServerStream<Row> rows = dataClient.readRows(query);
    List<StreamPartitionWithWatermark> partitions = new ArrayList<>();
    for (Row row : rows) {
      if (!isRowLocked(row)) {
        continue;
      }
      Instant watermark = MetadataTableEncoder.parseWatermarkFromRow(row);
      if (watermark == null) {
        continue;
      }
      ByteStringRange partition = convertStreamPartitionRowKeyToPartition(row.getKey());
      partitions.add(new StreamPartitionWithWatermark(partition, watermark));
    }
    return partitions;
  }

  /**
   * Read all the StreamPartition and output PartitionRecord to stream them.
   *
   * @return list of PartitionRecord of all StreamPartitions in the metadata table.
   */
  public List<PartitionRecord> readAllStreamPartitions() throws InvalidProtocolBufferException {
    Query query = Query.create(tableId).prefix(getFullStreamPartitionPrefix());
    ServerStream<Row> rows = dataClient.readRows(query);
    List<PartitionRecord> partitions = new ArrayList<>();
    for (Row row : rows) {
      Instant watermark = parseWatermarkFromRow(row);
      String uuid = parseLockUuid(row);
      ByteStringRange partition = convertStreamPartitionRowKeyToPartition(row.getKey());
      String currentToken = parseTokenFromRow(row);
      List<ChangeStreamContinuationToken> initialTokens = parseInitialContinuationTokens(row);
      if (watermark == null) {
        // This is unexpected. Watermark should never be null. We can't just leave this around
        // either, it will cause conflicts. We need to delete it.
        if (uuid != null) {
          releaseStreamPartitionLockForDeletion(partition, uuid);
        }
        deleteStreamPartitionRow(partition);
        LOG.error("Cleaning up corrupted StreamPartition {}", formatByteStringRange(partition));
        continue;
      }
      PartitionRecord partitionRecord;
      if (currentToken != null) {
        ChangeStreamContinuationToken token =
            ChangeStreamContinuationToken.create(partition, currentToken);
        partitionRecord =
            new PartitionRecord(
                partition, Collections.singletonList(token), watermark, Collections.emptyList());
      } else if (!initialTokens.isEmpty()) {
        partitionRecord =
            new PartitionRecord(partition, initialTokens, watermark, Collections.emptyList());
      } else {
        partitionRecord =
            new PartitionRecord(partition, watermark, watermark, Collections.emptyList());
      }

      if (uuid != null) {
        partitionRecord.setUuid(uuid);
      }
      partitions.add(partitionRecord);
    }
    return partitions;
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
    mutateRowWithHardTimeout(rowMutation);
  }

  /**
   * Update the watermark cell for Detect New Partition step.
   *
   * @param watermark watermark value to set for the cell
   */
  public void updateDetectNewPartitionWatermark(Instant watermark) {
    writeToMdTableWatermarkHelper(getFullDetectNewPartition(), watermark, null);
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
   * This is the 1st step of 2 phase delete of StreamPartition. Unset the uuid of StreamPartition if
   * the uuid matches and set the deletion bit. This prepares the StreamPartition to be deleted.
   *
   * <p>The reason to have 2 phase delete is
   *
   * <ul>
   *   <li>If we delete StreamPartition <b>before</b> we write NewPartitions, then we can't satisfy
   *       the invariant that there always exists a continuation token for every row key; no matter
   *       how temporary.
   *   <li>If we delete StreamPartition <b>after</b> we write NewPartitions, we allow a more complex
   *       and rare race problem. After we write the NewPartition but before we delete
   *       StreamPartition the following series of event take place. DNP processes the
   *       NewPartitions. The NewPartitions start streaming and then immediately merge back to the
   *       same partition as the partition we're about to Delete. The new partition tries to lock
   *       the StreamPartition row. Locking fails because the lock is held by the RCSP that we're
   *       deleting.
   *       <p>For example: the RCSP with id <code>1234</code> is working on partition AC. Metadata
   *       table has the row <code>
   *       StreamPartition#AC</code> with <code>uuid=1234</code>. Partition AC splits into AB and
   *       BC. We write <code>NewPartition#AB</code> and <code>
   *       NewPartition#BC</code>. But we haven't deleted <code>StreamPartition#AC</code> yet.
   *       Before we clean up <code>StreamPartition#AC</code>, DNP processes AB and BC. AB and BC
   *       start streaming. AB and BC immediately merge back to AC. DNP processes the <code>
   *       NewPartition#AC</code> and outputs AC. A new RCSP with id 5678 tries to lock <code>
   *       StreamPartition#AC</code> to start processing AC. This fails because RCSP 1234 still
   *       holds the lock. Once the lock fails RCSP 5678 stops. RCSP 1234 eventually cleans up
   *       <code>StreamPartition#AC</code> and no RCSP is streaming AC anymore.
   * </ul>
   *
   * The solution is a 2 phase delete
   *
   * <ol>
   *   <li>Release the lock and mark the StreamPartition for deletion
   *   <li>Write NewPartition rows
   *   <li>Delete StreamPartition if and only if deletion bit is set
   * </ol>
   *
   * We added the deletion bit to prevent accidentally deleting a new StreamPartition as the result
   * of the split and merge race described above. If the above scenario happens, the new partition
   * will be able to lock the StreamPartition row. But we don't want to delete the StreamPartition,
   * so we check if it is still marked for deletion. Obviously, locking the row unsets the deletion
   * bit, so it can't be deleted.
   *
   * @param partition release the lock for this partition
   * @param uuid match the uuid
   * @return true if releasing the lock was successful.
   */
  public boolean releaseStreamPartitionLockForDeletion(ByteStringRange partition, String uuid) {
    ByteString rowKey = convertPartitionToStreamPartitionRowKey(partition);
    Filter lockCellFilter =
        FILTERS
            .chain()
            .filter(FILTERS.family().exactMatch(MetadataTableAdminDao.CF_LOCK))
            .filter(FILTERS.qualifier().exactMatch(MetadataTableAdminDao.QUALIFIER_DEFAULT))
            .filter(FILTERS.value().exactMatch(uuid));
    Mutation deleteCell =
        Mutation.create()
            .deleteCells(MetadataTableAdminDao.CF_LOCK, MetadataTableAdminDao.QUALIFIER_DEFAULT)
            .setCell(
                MetadataTableAdminDao.CF_SHOULD_DELETE, MetadataTableAdminDao.QUALIFIER_DEFAULT, 1);
    ConditionalRowMutation rowMutation =
        ConditionalRowMutation.create(tableId, rowKey).condition(lockCellFilter).then(deleteCell);
    return dataClient.checkAndMutateRow(rowMutation);
  }

  /**
   * This is the 2nd step of 2 phase delete of StreamPartition. Delete the row key represented by
   * the partition. This represents that the partition will no longer be streamed. Only delete if
   * shouldDelete bit is set.
   *
   * @param partition forms the row key of the row to delete
   */
  public boolean deleteStreamPartitionRow(ByteStringRange partition) {
    LOG.debug("Delete metadata row");
    ByteString rowKey = convertPartitionToStreamPartitionRowKey(partition);
    Filter shouldDeleteFilter =
        FILTERS
            .chain()
            .filter(FILTERS.family().exactMatch(MetadataTableAdminDao.CF_SHOULD_DELETE))
            .filter(FILTERS.qualifier().exactMatch(MetadataTableAdminDao.QUALIFIER_DEFAULT))
            .filter(FILTERS.value().exactMatch(ByteString.copyFrom(Longs.toByteArray(1))));
    Mutation deleteRow = Mutation.create().deleteRow();
    ConditionalRowMutation rowMutation =
        ConditionalRowMutation.create(tableId, rowKey)
            .condition(shouldDeleteFilter)
            .then(deleteRow);
    return dataClient.checkAndMutateRow(rowMutation);
  }

  /**
   * Return true if the uuid holds the lock of the partition.
   *
   * @param partition partition to check if lock is held
   * @param uuid to check if it holds the lock
   * @return true if uuid holds the lock, otherwise false.
   */
  public boolean doHoldLock(ByteStringRange partition, String uuid) {
    ByteString rowKey = convertPartitionToStreamPartitionRowKey(partition);
    Filter lockCellFilter =
        FILTERS
            .chain()
            .filter(FILTERS.family().exactMatch(MetadataTableAdminDao.CF_LOCK))
            .filter(FILTERS.qualifier().exactMatch(MetadataTableAdminDao.QUALIFIER_DEFAULT))
            .filter(FILTERS.limit().cellsPerRow(1));
    Row row = dataClient.readRow(tableId, rowKey, lockCellFilter);
    if (row != null) {
      return row.getCells().get(0).getValue().toStringUtf8().equals(uuid);
    }
    return false;
  }

  /**
   * Lock the partition in the metadata table for the DoFn streaming it. Only one DoFn is allowed to
   * stream a specific partition at any time. Each DoFn has an uuid and will try to lock the
   * partition at the very start of the stream. If another DoFn has already locked the partition
   * (i.e. the uuid in the cell for the partition belongs to the DoFn), any future DoFn trying to
   * lock the same partition will fail. Also unset the deletion bit.
   *
   * @param partitionRecord partition to lock
   * @return true if uuid holds or acquired the lock, otherwise false.
   */
  public boolean lockAndRecordPartition(PartitionRecord partitionRecord) {
    if (doHoldLock(partitionRecord.getPartition(), partitionRecord.getUuid())) {
      return true;
    }

    // Record all the initial metadata.
    Mutation mutation =
        Mutation.create()
            .setCell(
                MetadataTableAdminDao.CF_LOCK,
                MetadataTableAdminDao.QUALIFIER_DEFAULT,
                partitionRecord.getUuid())
            .setCell(
                MetadataTableAdminDao.CF_WATERMARK,
                MetadataTableAdminDao.QUALIFIER_DEFAULT,
                partitionRecord.getParentLowWatermark().getMillis())
            .deleteCells(
                MetadataTableAdminDao.CF_SHOULD_DELETE, MetadataTableAdminDao.QUALIFIER_DEFAULT);
    List<ChangeStreamContinuationToken> tokens =
        partitionRecord.getChangeStreamContinuationTokens();
    if (tokens != null) {
      for (ChangeStreamContinuationToken token : tokens) {
        mutation.setCell(
            MetadataTableAdminDao.CF_INITIAL_TOKEN,
            ByteStringRange.serializeToByteString(token.getPartition()),
            token.toByteString());
      }
    }

    // We cannot check whether a cell is empty, We can check if the cell matches any value. If it
    // does not, we perform the mutation to set the cell.
    Filter matchAnyString =
        FILTERS
            .chain()
            .filter(FILTERS.family().exactMatch(MetadataTableAdminDao.CF_LOCK))
            .filter(FILTERS.qualifier().exactMatch(MetadataTableAdminDao.QUALIFIER_DEFAULT))
            .filter(FILTERS.value().regex("\\C*"));
    ConditionalRowMutation rowMutation =
        ConditionalRowMutation.create(
                tableId, convertPartitionToStreamPartitionRowKey(partitionRecord.getPartition()))
            .condition(matchAnyString)
            .otherwise(mutation);

    boolean lockAcquired = !dataClient.checkAndMutateRow(rowMutation);
    if (lockAcquired) {
      LOG.info(
          "RCSP: {} acquired lock for uid: {}",
          formatByteStringRange(partitionRecord.getPartition()),
          partitionRecord.getUuid());
      return true;
    } else {
      // If the lock is already held we need to check if it was acquired by a duplicate
      // work item with the same uuid since we last checked doHoldLock above.
      return doHoldLock(partitionRecord.getPartition(), partitionRecord.getUuid());
    }
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
    mutateRowWithHardTimeout(rowMutation);
  }

  /**
   * Read and deserialize missing partition and how long they have been missing from the metadata
   * table.
   *
   * @return deserialized missing partitions and duration.
   */
  public HashMap<ByteStringRange, Instant> readDetectNewPartitionMissingPartitions() {
    @Nonnull HashMap<ByteStringRange, Instant> missingPartitions = new HashMap<>();
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
      HashMap<ByteStringRange, Instant> missingPartitionDurations) {
    byte[] serializedMissingPartition = SerializationUtils.serialize(missingPartitionDurations);
    RowMutation rowMutation =
        RowMutation.create(tableId, getFullDetectNewPartition())
            .setCell(
                MetadataTableAdminDao.CF_MISSING_PARTITIONS,
                ByteString.copyFromUtf8(MetadataTableAdminDao.QUALIFIER_DEFAULT),
                ByteString.copyFrom(serializedMissingPartition));
    mutateRowWithHardTimeout(rowMutation);
  }

  /**
   * This adds a hard timeout of 40 seconds to mutate row futures. These requests already have a
   * 30-second deadline. This is a workaround for an extremely rare issue we see with requests not
   * respecting their deadlines. This can be removed once we've pinpointed the cause.
   *
   * @param rowMutation Bigtable RowMutation to apply
   */
  @VisibleForTesting
  void mutateRowWithHardTimeout(RowMutation rowMutation) {
    ApiFuture<Void> mutateRowFuture = dataClient.mutateRowAsync(rowMutation);
    try {
      mutateRowFuture.get(
          BigtableChangeStreamAccessor.MUTATE_ROW_DEADLINE.getSeconds() + 10, TimeUnit.SECONDS);
    } catch (TimeoutException timeoutException) {
      mutateRowFuture.cancel(true);
      throw new RuntimeException(
          "Cancelled mutateRow request after exceeding deadline", timeoutException);
    } catch (ExecutionException executionException) {
      if (executionException.getCause() instanceof RuntimeException) {
        throw (RuntimeException) executionException.getCause();
      }
      throw new RuntimeException(executionException);
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(interruptedException);
    }
  }
}
