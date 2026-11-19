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
package org.apache.beam.sdk.schemas.variant;

/**
 * The logical type of a single {@link Variant} value, as defined by the <a
 * href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">Parquet Variant
 * encoding</a>.
 *
 * <p>Short strings and long strings are both reported as {@link #STRING}.
 */
public enum VariantType {
  /** Variant null (JSON null). Distinct from a SQL/Java null field. */
  NULL,
  BOOLEAN,
  INT8,
  INT16,
  INT32,
  INT64,
  DOUBLE,
  /** Decimal with precision up to 9. */
  DECIMAL4,
  /** Decimal with precision up to 18. */
  DECIMAL8,
  /** Decimal with precision up to 38. */
  DECIMAL16,
  DATE,
  /** Timestamp with time zone (UTC adjusted), microsecond precision. */
  TIMESTAMP,
  /** Timestamp without time zone, microsecond precision. */
  TIMESTAMP_NTZ,
  FLOAT,
  BINARY,
  STRING,
  /** Time of day without time zone, microsecond precision. */
  TIME,
  /** Timestamp with time zone (UTC adjusted), nanosecond precision. */
  TIMESTAMP_NANOS,
  /** Timestamp without time zone, nanosecond precision. */
  TIMESTAMP_NTZ_NANOS,
  UUID,
  OBJECT,
  ARRAY
}
