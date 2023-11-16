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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/** A restriction represented by a range of timestamps [from, to). */
public class TimestampRange implements Serializable {

  private static final long serialVersionUID = -3597929310338724839L;
  private final Timestamp from;
  private final Timestamp to;

  /**
   * Constructs a timestamp range. The range represents a closed-open interval [from, to). The
   * timestamp {@code to} must be greater or equal to the timestamp {@code from}, otherwise an
   * {@link IllegalArgumentException} will be thrown.
   */
  public static TimestampRange of(Timestamp from, Timestamp to) {
    return new TimestampRange(from, to);
  }

  @VisibleForTesting
  TimestampRange(Timestamp from, Timestamp to) {
    checkArgument(from.compareTo(to) <= 0, "Malformed range [%s, %s)", from, to);
    this.from = from;
    this.to = to;
  }

  /** Returns the range start timestamp (inclusive). */
  public Timestamp getFrom() {
    return from;
  }

  /** Returns the range end timestamp (exclusive). */
  public Timestamp getTo() {
    return to;
  }

  @Override
  public String toString() {
    return "[" + from + ", " + to + ')';
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimestampRange)) {
      return false;
    }
    TimestampRange that = (TimestampRange) o;
    return Objects.equals(from, that.from) && Objects.equals(to, that.to);
  }

  @Override
  public int hashCode() {
    return Objects.hash(from, to);
  }
}
