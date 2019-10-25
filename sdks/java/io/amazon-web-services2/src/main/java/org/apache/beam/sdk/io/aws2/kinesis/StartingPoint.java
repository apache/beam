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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Objects;
import org.joda.time.Instant;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.common.InitialPositionInStream;

/**
 * Denotes a point at which the reader should start reading from a Kinesis stream. It can be
 * expressed either as an {@link InitialPositionInStream} enum constant or a timestamp, in which
 * case the reader will start reading at the specified point in time.
 */
class StartingPoint implements Serializable {

  private final InitialPositionInStream position;
  private final Instant timestamp;

  public StartingPoint(InitialPositionInStream position) {
    this.position = checkNotNull(position, "position");
    this.timestamp = null;
  }

  public StartingPoint(Instant timestamp) {
    this.timestamp = checkNotNull(timestamp, "timestamp");
    this.position = null;
  }

  public InitialPositionInStream getPosition() {
    return position;
  }

  public String getPositionName() {
    return position != null ? position.name() : ShardIteratorType.AT_TIMESTAMP.name();
  }

  public Instant getTimestamp() {
    return timestamp != null ? timestamp : null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StartingPoint that = (StartingPoint) o;
    return position == that.position && Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(position, timestamp);
  }

  @Override
  public String toString() {
    if (timestamp == null) {
      return position.toString();
    } else {
      return "Starting at timestamp " + timestamp;
    }
  }
}
