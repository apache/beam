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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter.microsecondToInstant;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Helper methods that simplifies some conversion and extraction of metadata table content. */
@Internal
public class MetadataTableEncoder {
  /**
   * Read the watermark cell of a row from ReadRows.
   *
   * @param row row to extract the watermark from
   * @return the watermark of the row
   */
  public static @Nullable Instant parseWatermarkFromRow(Row row) {
    List<RowCell> cells =
        row.getCells(MetadataTableAdminDao.CF_WATERMARK, MetadataTableAdminDao.QUALIFIER_DEFAULT);
    if (cells.size() == 0) {
      return null;
    }
    return Instant.ofEpochMilli(Longs.fromByteArray(cells.get(0).getValue().toByteArray()));
  }

  /**
   * Return the timestamp (the time it was updated) of the watermark cell.
   *
   * @param row row to extract the timestamp from
   * @return the timestamp of the watermark cell.
   */
  public static @Nullable Instant parseWatermarkLastUpdatedFromRow(Row row) {
    List<RowCell> cells =
        row.getCells(MetadataTableAdminDao.CF_WATERMARK, MetadataTableAdminDao.QUALIFIER_DEFAULT);
    if (cells.size() == 0) {
      return null;
    }
    return microsecondToInstant(cells.get(0).getTimestamp());
  }

  /**
   * Read the continuation token cell of a row from ReadRows.
   *
   * @param row to extract the token from
   * @return the token of the row
   */
  public static @Nullable String parseTokenFromRow(Row row) {
    List<RowCell> cells =
        row.getCells(
            MetadataTableAdminDao.CF_CONTINUATION_TOKEN, MetadataTableAdminDao.QUALIFIER_DEFAULT);
    if (cells.size() == 0) {
      return null;
    }
    return cells.get(0).getValue().toStringUtf8();
  }

  /**
   * Returns true if row is locked.
   *
   * @param row to evaluate if it's locked
   * @return true if row is locked. Otherwise, false.
   */
  public static boolean isRowLocked(Row row) {
    return row.getCells(MetadataTableAdminDao.CF_LOCK, MetadataTableAdminDao.QUALIFIER_DEFAULT)
            .size()
        != 0;
  }

  /**
   * Returns the uuid from a row.
   *
   * @param row to extract the uuid
   * @return the uuid of the row
   */
  public static @Nullable String parseLockUuid(Row row) {
    List<RowCell> cells =
        row.getCells(MetadataTableAdminDao.CF_LOCK, MetadataTableAdminDao.QUALIFIER_DEFAULT);
    if (cells.size() == 0) {
      return null;
    }
    return cells.get(0).getValue().toStringUtf8();
  }

  /**
   * Return a list of initial token from a row.
   *
   * @param row to extract the initial tokens
   * @return a list of tokens from the row
   * @throws InvalidProtocolBufferException when failed to parse tokens.
   */
  public static List<ChangeStreamContinuationToken> parseInitialContinuationTokens(Row row)
      throws InvalidProtocolBufferException {
    List<ChangeStreamContinuationToken> tokens = new ArrayList<>();
    List<RowCell> cells = row.getCells(MetadataTableAdminDao.CF_INITIAL_TOKEN);
    for (RowCell rowCell : cells) {
      tokens.add(ChangeStreamContinuationToken.fromByteString(rowCell.getValue()));
    }
    return tokens;
  }
}
