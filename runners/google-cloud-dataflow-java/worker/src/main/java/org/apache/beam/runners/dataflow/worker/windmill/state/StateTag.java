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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Range;

/**
 * When combined with a key and computationId, represents the unique address for state managed by
 * Windmill.
 */
@AutoValue
public abstract class StateTag<RequestPositionT> {
  static <RequestPositionT> StateTag<RequestPositionT> of(
      Kind kind, ByteString tag, String stateFamily, @Nullable RequestPositionT requestPosition) {
    return new AutoValue_StateTag.Builder<RequestPositionT>()
        .setKind(kind)
        .setTag(tag)
        .setStateFamily(stateFamily)
        .setRequestPosition(requestPosition)
        .build();
  }

  public static <RequestPositionT> StateTag<RequestPositionT> of(
      Kind kind, ByteString tag, String stateFamily) {
    return of(kind, tag, stateFamily, null);
  }

  abstract Kind getKind();

  abstract ByteString getTag();

  abstract String getStateFamily();

  /**
   * For {@link Kind#BAG, Kind#ORDERED_LIST, Kind#VALUE_PREFIX, KIND#MULTIMAP_SINGLE_ENTRY,
   * KIND#MULTIMAP_ALL} kinds: A previous 'continuation_position' returned by Windmill to signal the
   * resulting state was incomplete. Sending that position will request the next page of values.
   * Null for first request.
   *
   * <p>Null for other kinds.
   */
  @Nullable
  public abstract RequestPositionT getRequestPosition();

  /** For {@link Kind#ORDERED_LIST} kinds: the range to fetch or delete. */
  @Nullable
  abstract Range<Long> getSortedListRange();

  /** For {@link Kind#MULTIMAP_SINGLE_ENTRY} kinds: the key in the multimap to fetch or delete. */
  @Nullable
  abstract ByteString getMultimapKey();

  /**
   * For {@link Kind#MULTIMAP_ALL} kinds: will only return the keys of the multimap and not the
   * values if true.
   */
  @Nullable
  abstract Boolean getOmitValues();

  public abstract Builder<RequestPositionT> toBuilder();

  public enum Kind {
    VALUE,
    BAG,
    WATERMARK,
    ORDERED_LIST,
    VALUE_PREFIX,
    MULTIMAP_SINGLE_ENTRY,
    MULTIMAP_ALL
  }

  @AutoValue.Builder
  abstract static class Builder<RequestPositionT> {
    abstract Builder<RequestPositionT> setKind(Kind kind);

    abstract Builder<RequestPositionT> setTag(ByteString tag);

    abstract Builder<RequestPositionT> setStateFamily(String stateFamily);

    abstract Builder<RequestPositionT> setRequestPosition(
        @Nullable RequestPositionT requestPosition);

    abstract Builder<RequestPositionT> setSortedListRange(@Nullable Range<Long> sortedListRange);

    abstract Builder<RequestPositionT> setMultimapKey(@Nullable ByteString encodedMultimapKey);

    abstract Builder<RequestPositionT> setOmitValues(Boolean omitValues);

    abstract StateTag<RequestPositionT> build();
  }
}
