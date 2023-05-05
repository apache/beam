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

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;
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
   * Read the continuation token cell of a row from ReadRows.
   *
   * @param row to extract the token from
   * @return the token of the row
   */
  public static @Nullable String getTokenFromRow(Row row) {
    List<RowCell> cells =
        row.getCells(
            MetadataTableAdminDao.CF_CONTINUATION_TOKEN, MetadataTableAdminDao.QUALIFIER_DEFAULT);
    if (cells.size() == 0) {
      return null;
    }
    return cells.get(0).getValue().toStringUtf8();
  }
}
