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
package org.apache.beam.sdk.io.iceberg.cdc;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.Comparator;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.util.StructProjection;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Primary-key-projection and overlap-range comparison helper.
 *
 * <p>Used by {@link LocalResolveDoFn} and {@link ReadFromChangelogs} to decide whether a record's
 * PK falls within an overlap of two opposing tasks. If so, the record needs to be compared with
 * others to determine if it is part of an update pair.
 */
final class OverlapRange {
  private final Schema recordIdSchema;
  private final StructProjection recordIdProjection;
  private final Comparator<StructLike> idComp;

  private OverlapRange(
      Schema recordIdSchema, StructProjection recordIdProjection, Comparator<StructLike> idComp) {
    this.recordIdSchema = recordIdSchema;
    this.recordIdProjection = recordIdProjection;
    this.idComp = idComp;
  }

  static OverlapRange forScanConfig(IcebergScanConfig scanConfig) {
    Schema tableSchema =
        TableCache.get(scanConfig.getCatalogConfig(), scanConfig.getTableIdentifier()).schema();
    Schema fullSchema =
        CdcOutputUtils.readSchemaWithRowMetadata(scanConfig.getMetadataColumns(), tableSchema);
    StructProjection projection = StructProjection.create(fullSchema, scanConfig.recordIdSchema());
    return new OverlapRange(
        scanConfig.recordIdSchema(), projection, scanConfig.recordIdComparator());
  }

  StructProjection recordIdProjection() {
    return recordIdProjection;
  }

  Schema recordIdSchema() {
    return recordIdSchema;
  }

  /** Converts a Beam Row (overlap bound) back to an Iceberg {@link StructLike}. */
  @Nullable
  StructLike toStructLike(@Nullable Row beamBound) {
    if (beamBound == null) {
      return null;
    }
    return IcebergUtils.beamRowToIcebergRecord(recordIdSchema, beamBound);
  }

  /**
   * Wraps the record to project its Primary Key, then checks if the PK is within the overlap {@code
   * [lower, upper]} (inclusive). Can be paired with a subsequent {@link #recordIdProjection()} call
   * to fetch the PK value.
   *
   * <p>If either bound is null, we conservatively assume it falls within the overlap.
   */
  boolean contains(Record rec, @Nullable StructLike lower, @Nullable StructLike upper) {
    checkStateNotNull(recordIdProjection).wrap(rec);

    if (lower == null || upper == null) {
      return true;
    }
    return idComp.compare(recordIdProjection, lower) >= 0
        && idComp.compare(recordIdProjection, upper) <= 0;
  }
}
