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

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.util.DateTimeUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Re-stamps each output row using the configured {@code watermarkColumn}'s value, so the source's
 * output watermark advances per record rather than per snapshot.
 *
 * <p>If the configured column's value on a record is null or missing, this DoFn is a pass-through,
 * preserving the snapshot commit timestamp.
 *
 * <p>The {@link #getAllowedTimestampSkew()} return is intentionally generous — the user's watermark
 * column may produce values well before the snapshot commit time (event-time data can lag
 * wall-clock by hours or days). Restricting the skew here would force the source to drop legitimate
 * output.
 */
class ApplyWatermarkColumn extends DoFn<Row, Row> {
  private final String watermarkColumn;
  private final TimeUnit timeUnit;

  ApplyWatermarkColumn(String watermarkColumn, @Nullable String timeUnit) {
    this.watermarkColumn = watermarkColumn;
    this.timeUnit = timeUnit != null ? TimeUnit.valueOf(timeUnit.toUpperCase()) : MICROSECONDS;
  }

  @ProcessElement
  public void process(@Element Row row, OutputReceiver<Row> out) {
    @Nullable
    Instant instant =
        getInstant(row.getValue(watermarkColumn), row.getSchema().getField(watermarkColumn));
    if (instant != null) {
      out.outputWithTimestamp(row, instant);
    } else {
      out.output(row);
    }
  }

  private @Nullable Instant getInstant(@Nullable Object value, Schema.Field field) {
    if (value == null) {
      return null;
    }
    switch (field.getType().getTypeName()) {
      case INT64:
        return Instant.ofEpochMilli(timeUnit.toMillis((Long) value));
      case DATETIME:
        return (Instant) value;
      case LOGICAL_TYPE:
        String logicalType =
            Preconditions.checkStateNotNull(field.getType().getLogicalType()).getIdentifier();
        if (logicalType.equals(SqlTypes.DATETIME.getIdentifier())) {
          return Instant.ofEpochMilli(
              MICROSECONDS.toMillis(DateTimeUtil.microsFromTimestamp((LocalDateTime) value)));
        } else if (logicalType.equals(SqlTypes.TIMESTAMP.getIdentifier())
            || logicalType.equals(org.apache.beam.sdk.schemas.logicaltypes.Timestamp.IDENTIFIER)) {
          return Instant.ofEpochMilli(
              MICROSECONDS.toMillis(DateTimeUtil.microsFromInstant((java.time.Instant) value)));
        } else {
          throw new UnsupportedOperationException("Unexpected logical type: " + logicalType);
        }
      default:
        throw new UnsupportedOperationException("Unexpected Beam type: " + field.getType());
    }
  }

  @Override
  public Duration getAllowedTimestampSkew() {
    // Generous skew to cover backfill of historical data and late-arriving CDC patterns.
    return Duration.standardDays(365);
  }
}
