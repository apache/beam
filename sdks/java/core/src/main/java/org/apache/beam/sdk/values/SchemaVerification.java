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
package org.apache.beam.sdk.values;

import java.io.Serializable;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utilities for verifying and normalizing {@link Row} / {@link org.apache.beam.sdk.schemas.Schema}
 * option field values.
 */
public abstract class SchemaVerification implements Serializable {
  /**
   * Validates and normalizes {@code value} against {@code type} (e.g., copying sliced {@link
   * java.nio.ByteBuffer}s to {@code byte[]}, converting {@link org.joda.time.base.AbstractInstant}
   * to {@link org.joda.time.Instant}, and round-tripping {@link
   * org.apache.beam.sdk.schemas.Schema.LogicalType} values).
   *
   * @param value field value to verify (may be {@code null} only when {@code type.getNullable()})
   * @param type expected {@link FieldType}
   * @param fieldName field or option name used in exception messages
   * @return normalized value (may be {@code null} for nullable fields or nullable logical types
   *     whose conversion returns {@code null})
   * @throws IllegalArgumentException if {@code value} is {@code null} (or converts to {@code null})
   *     for a non-nullable field type
   * @throws ClassCastException if {@code value} is incompatible with {@code type}
   */
  public static @Nullable Object verifyFieldValue(
      @Nullable Object value, FieldType type, String fieldName) {
    return RowUtils.captureValue(type, value, fieldName);
  }
}
