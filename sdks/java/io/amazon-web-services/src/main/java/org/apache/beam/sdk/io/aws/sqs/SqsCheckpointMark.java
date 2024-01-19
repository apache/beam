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
package org.apache.beam.sdk.io.aws.sqs;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class SqsCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

  /**
   * If the checkpoint is for persisting: the reader who's snapshotted state we are persisting. If
   * the checkpoint is for restoring: {@literal null}. Not persisted in durable checkpoint. CAUTION:
   * Between a checkpoint being taken and {@link #finalizeCheckpoint()} being called the 'true'
   * active reader may have changed.
   */
  private transient @Nullable SqsUnboundedReader reader;

  /**
   * If the checkpoint is for persisting: The ids of messages which have been passed downstream
   * since the last checkpoint. If the checkpoint is for restoring: {@literal null}. Not persisted
   * in durable checkpoint.
   */
  private @Nullable List<String> safeToDeleteIds;

  /**
   * If the checkpoint is for persisting: The receipt handles of messages which have been received
   * from SQS but not yet passed downstream at the time of the snapshot. If the checkpoint is for
   * restoring: Same, but recovered from durable storage.
   */
  @VisibleForTesting final List<String> notYetReadReceipts;

  public SqsCheckpointMark(
      SqsUnboundedReader reader, List<String> messagesToDelete, List<String> notYetReadReceipts) {
    this.reader = reader;
    this.safeToDeleteIds = ImmutableList.copyOf(messagesToDelete);
    this.notYetReadReceipts = ImmutableList.copyOf(notYetReadReceipts);
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    checkState(reader != null && safeToDeleteIds != null, "Cannot finalize a restored checkpoint");
    // Even if the 'true' active reader has changed since the checkpoint was taken we are
    // fine:
    // - The underlying SQS topic will not have changed, so the following deletes will still
    // go to the right place.
    // - We'll delete the ACK ids from the readers in-flight state, but that only affect
    // flow control and stats, neither of which are relevant anymore.
    try {
      reader.delete(safeToDeleteIds);
    } finally {
      int remainingInFlight = reader.numInFlightCheckpoints.decrementAndGet();
      checkState(remainingInFlight >= 0, "Miscounted in-flight checkpoints");
      reader.maybeCloseClient();
      reader = null;
      safeToDeleteIds = null;
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SqsCheckpointMark that = (SqsCheckpointMark) o;
    return Objects.equal(safeToDeleteIds, that.safeToDeleteIds);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(safeToDeleteIds);
  }
}
