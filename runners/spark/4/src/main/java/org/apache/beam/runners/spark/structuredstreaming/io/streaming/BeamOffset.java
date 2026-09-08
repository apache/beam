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

import org.apache.spark.sql.connector.read.streaming.Offset;

/**
 * Opaque epoch counter used as the Spark {@link Offset} of a Beam unbounded source.
 *
 * <p>The read position lives in Beam checkpoint marks on the executors, see {@link
 * BeamSourceCheckpoint}. Equality is the base class comparison of {@link #json()}.
 */
public class BeamOffset extends Offset {

  public static final BeamOffset ZERO = new BeamOffset(0L);

  private final long epoch;

  public BeamOffset(long epoch) {
    this.epoch = epoch;
  }

  public long epoch() {
    return epoch;
  }

  @Override
  public String json() {
    return Long.toString(epoch);
  }

  public static BeamOffset fromJson(String json) {
    try {
      return new BeamOffset(Long.parseLong(json.trim()));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Not a valid BeamOffset: " + json, e);
    }
  }

  @Override
  public String toString() {
    return json();
  }
}
