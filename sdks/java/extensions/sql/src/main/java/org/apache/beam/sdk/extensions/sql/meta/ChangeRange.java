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
package org.apache.beam.sdk.extensions.sql.meta;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * The changes a {@code CHANGES(...)} call reads from a table.
 *
 * <p>A range is bounded either by versions or by timestamps, and its bounds are inclusive. A range
 * without a start begins wherever the connector starts by default, and a range without an end reads
 * up to the latest change.
 */
@AutoValue
public abstract class ChangeRange implements Serializable {
  public abstract @Nullable Long startVersion();

  public abstract @Nullable Long endVersion();

  public abstract @Nullable Instant startTimestamp();

  public abstract @Nullable Instant endTimestamp();

  /** All changes, as read by {@code CHANGES(t)}. */
  public static ChangeRange all() {
    return new AutoValue_ChangeRange(null, null, null, null);
  }

  /** The changes between two versions, either of which may be open (null). */
  public static ChangeRange versions(@Nullable Long start, @Nullable Long end) {
    return new AutoValue_ChangeRange(start, end, null, null);
  }

  /**
   * The changes between two timestamps, either of which may be open (null). In SQL, a timestamp
   * bound is an ISO 8601 string or a TIMESTAMP literal. The default offset is UTC.
   */
  public static ChangeRange timestamps(@Nullable Instant start, @Nullable Instant end) {
    return new AutoValue_ChangeRange(null, null, start, end);
  }
}
