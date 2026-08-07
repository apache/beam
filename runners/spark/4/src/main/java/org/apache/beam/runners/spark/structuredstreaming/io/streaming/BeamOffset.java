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
package org.apache.beam.runners.spark.structuredstreaming.io.streaming;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An opaque epoch counter used as the Spark streaming {@link Offset} of a Beam unbounded source.
 *
 * <p>The offset carries no information about the position inside the wrapped Beam source. The
 * driver never reads from the source and never inspects its progress, it only needs a monotonically
 * increasing value so that Spark keeps planning micro-batches. The actual read position lives in
 * the executor side {@link BeamReaderCache} as a Beam {@code CheckpointMark}.
 */
public class BeamOffset extends Offset {

  /** The offset every Beam unbounded stream starts at. */
  public static final BeamOffset ZERO = new BeamOffset(0L);

  private static final Pattern EPOCH_PATTERN = Pattern.compile("-?\\d+");

  private final long epoch;

  public BeamOffset(long epoch) {
    this.epoch = epoch;
  }

  /** The epoch counter value. */
  public long epoch() {
    return epoch;
  }

  @Override
  public String json() {
    return "{\"epoch\":" + epoch + "}";
  }

  /** Parses the form produced by {@link #json()}, a bare number is also accepted. */
  public static BeamOffset fromJson(String json) {
    Matcher matcher = EPOCH_PATTERN.matcher(json);
    if (!matcher.find()) {
      throw new IllegalArgumentException("Not a valid BeamOffset: " + json);
    }
    return new BeamOffset(Long.parseLong(matcher.group()));
  }

  @Override
  public boolean equals(@Nullable Object o) {
    return o instanceof BeamOffset && ((BeamOffset) o).epoch == epoch;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(epoch);
  }

  @Override
  public String toString() {
    return json();
  }
}
