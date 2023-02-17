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
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import javax.annotation.Nullable;
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
@SuppressWarnings({"UnusedVariable", "UnusedMethod"})
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
  public Range.ByteStringRange convertStreamPartitionRowKeyToPartition(ByteString rowKey)
      throws InvalidProtocolBufferException {
    int prefixLength = changeStreamNamePrefix.size() + STREAM_PARTITION_PREFIX.size();
    return Range.ByteStringRange.toByteStringRange(rowKey.substring(prefixLength));
  }

  /**
   * Convert partition to a Stream Partition row key to query for metadata of partitions that are
   * currently being streamed.
   *
   * @param partition convert to row key
   * @return row key to insert to Cloud Bigtable.
   */
  public ByteString convertPartitionToStreamPartitionRowKey(Range.ByteStringRange partition) {
    return getFullStreamPartitionPrefix()
        .concat(Range.ByteStringRange.serializeToByteString(partition));
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
      Range.ByteStringRange partition,
      Instant watermark,
      @Nullable ChangeStreamContinuationToken currentToken) {
    writeToMdTableWatermarkHelper(
        convertPartitionToStreamPartitionRowKey(partition), watermark, currentToken);
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
}
