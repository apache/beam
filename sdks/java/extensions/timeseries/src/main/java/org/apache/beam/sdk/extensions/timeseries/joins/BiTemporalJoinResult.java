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
package org.apache.beam.sdk.extensions.timeseries.joins;

import com.google.common.base.MoreObjects;
import java.util.Arrays;
import java.util.Objects;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

/**
 * Object used to return the results of a BiTemporalStreams.
 *
 * @param <K> key
 * @param <V1> left stream type
 * @param <V2> right stream type
 */
public class BiTemporalJoinResult<K, V1, V2> {

  private K key;
  private TimestampedValue<V1> leftValue;
  private TimestampedValue<V2> rightValue;
  private Boolean matched = false;

  public BiTemporalJoinResult() {}

  public BiTemporalJoinResult(
      K key, TimestampedValue<V1> leftValue, TimestampedValue<V2> rightValue, Boolean matched) {
    this.leftValue = leftValue;
    this.rightValue = rightValue;
    this.key = key;
    this.matched = matched;
  }

  public static <K, V1, V2> BiTemporalJoinResult<K, V1, V2> of() {
    return new BiTemporalJoinResult<K, V1, V2>();
  }

  public TimestampedValue<V1> getLeftData() {
    return leftValue;
  }

  public TimestampedValue<V2> getRightData() {
    return rightValue;
  }

  public K getKey() {
    return key;
  }

  public void setMatched(Boolean matched) {
    this.matched = matched;
  }

  public Boolean getMatched() {
    return matched;
  }

  public BiTemporalJoinResult setLeftData(KV<K, V1> leftValue, Instant instant) {
    this.leftValue = TimestampedValue.of(leftValue.getValue(), instant);
    this.key = leftValue.getKey();
    return this;
  }

  public BiTemporalJoinResult setRightData(KV<K, V2> rightValue, Instant instant) {
    this.rightValue = TimestampedValue.of(rightValue.getValue(), instant);
    this.key = rightValue.getKey();
    return this;
  }

  @Override
  public int hashCode() {
    // Objects.deepEquals requires Arrays.deepHashCode for correctness
    return Arrays.deepHashCode(new Object[] {key, leftValue, rightValue});
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .addValue(key)
        .addValue(leftValue)
        .addValue(rightValue)
        .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof BiTemporalJoinResult)) {
      return false;
    }
    BiTemporalJoinResult<?, ?, ?> otherBiStream = (BiTemporalJoinResult<?, ?, ?>) other;
    // Arrays are very common as values and keys, so deepEquals is mandatory
    return Objects.deepEquals(this.key, otherBiStream.key)
        && Objects.deepEquals(this.leftValue, otherBiStream.leftValue)
        && Objects.deepEquals(this.rightValue, otherBiStream.rightValue);
  }
}
