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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.CommitResponse;
import java.io.Serializable;
import java.util.Objects;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.encoder.TimestampEncoding;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The results of committing a batch of mutations to Cloud Spanner via {@link
 * SpannerIO.Write#withWriteResults()}.
 */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(AvroCoder.class)
public class WriteResults implements Serializable {

  private static final long serialVersionUID = 1L;

  private long mutationCount;

  @AvroEncode(using = TimestampEncoding.class)
  private @Nullable Timestamp commitTimestamp;

  @AvroEncode(using = TimestampEncoding.class)
  private @Nullable Timestamp snapshotTimestamp;

  /** Default constructor for Avro serialization only. */
  private WriteResults() {}

  private WriteResults(
      long mutationCount,
      @Nullable Timestamp commitTimestamp,
      @Nullable Timestamp snapshotTimestamp) {
    this.mutationCount = mutationCount;
    this.commitTimestamp = commitTimestamp;
    this.snapshotTimestamp = snapshotTimestamp;
  }

  /** The number of mutations applied by Cloud Spanner in this commit, or 0 if unavailable. */
  public long getMutationCount() {
    return mutationCount;
  }

  /** The timestamp at which the write was committed in Cloud Spanner. */
  public @Nullable Timestamp getCommitTimestamp() {
    return commitTimestamp;
  }

  /** The snapshot timestamp of the transaction, if returned by Cloud Spanner. */
  public @Nullable Timestamp getSnapshotTimestamp() {
    return snapshotTimestamp;
  }

  public static WriteResults create(
      long mutationCount,
      @Nullable Timestamp commitTimestamp,
      @Nullable Timestamp snapshotTimestamp) {
    return new WriteResults(mutationCount, commitTimestamp, snapshotTimestamp);
  }

  public static WriteResults fromCommitResponse(@Nullable CommitResponse commitResponse) {
    if (commitResponse == null) {
      return create(0L, null, null);
    }
    long mutationCount =
        commitResponse.hasCommitStats() ? commitResponse.getCommitStats().getMutationCount() : 0L;
    return create(
        mutationCount, commitResponse.getCommitTimestamp(), commitResponse.getSnapshotTimestamp());
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WriteResults)) {
      return false;
    }
    WriteResults that = (WriteResults) o;
    return mutationCount == that.mutationCount
        && Objects.equals(commitTimestamp, that.commitTimestamp)
        && Objects.equals(snapshotTimestamp, that.snapshotTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mutationCount, commitTimestamp, snapshotTimestamp);
  }

  @Override
  public String toString() {
    return "WriteResults{"
        + "mutationCount="
        + mutationCount
        + ", commitTimestamp="
        + commitTimestamp
        + ", snapshotTimestamp="
        + snapshotTimestamp
        + '}';
  }
}
